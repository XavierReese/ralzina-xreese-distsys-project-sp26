#!/usr/bin/env python3
'''
Coordinator.py

Author: Rene Alzina + Xavier Reese
Date April 2026
'''

import threading
import json

COORDINATOR_TYPE = "coordinator"
COORDINATOR_PROJECT = "dist_job_coordinator"

CATALOG_HOST = "catalog.cse.nd.edu"
CATALOG_PORT = 9097

# ----------------
# Message Helpers
# ----------------

def send_message(sock: socket.socket, message: bytes) -> None:
    """4-byte big-endian length prefix + payload."""
    sock.sendall(len(message).to_bytes(4, byteorder="big") + message)


def recv_message(sock: socket.socket) -> bytes:
    """Read a framed message off the socket. Raises ConnectionError on close."""
    raw_len = _recv_exact(sock, 4)
    return _recv_exact(sock, int.from_bytes(raw_len, byteorder="big"))


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed unexpectedly.")
        buf.extend(chunk)
    return bytes(buf)

def identify_peer(first_msg):
    '''
    identify first message as either client or worker
    '''
    if first_msg[:4] == "JOIN":
        return "client", first_msg.decode("utf-8", errors="replace")

    else:
        return "worker", None #TODO parse and return worker message

# ------------------------------------
# Catalog Update
# - called in coord.start as heartbeat
# ------------------------------------

def update(port):
    self.catalog_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.catalog_s.connect((CATALOG_HOST, CATALOG_PORT))

    while True:
        u = {
                "type": COORDINATOR_TYPE,
                "port": port,
                "owner": "xreese", # can this be both?
                "project": COORDINATOR_PROJECT
            }

        try:
            msg = json.dumps(u).encode('utf-8')
            self.catalog_s.sendto(msg, (CATALOG_HOST, CATALOG_PORT))
            print("[COORD] Catalog Update Sent")
        except Exception as e:
            print(f"Failed to send update: {e}")
        time.sleep(60)

# -------------------
# Coordinator
# - manages state of jobs and connections
# -------------------

class Coordinator:
    def __init__(self, coord_name="coordinator"):

        self.coord_name = coord_name

        # --- State of Clients, Workers, and Jobs ---
        self.clients: dict[str, int] = {} # username -> socket
        self.workers: dict[int, int] = {} # worker_id -> socket
        
        job_queue = []

        self.running_jobs: dict[int: int] = {}  # job_id -> worker_id

        self.lock = threading.Lock() # separate threads for clients & workers use this to lock coord state

        # --- Persistence ---
        self.ckpt_path = f"{self.coord_name}.ckpt"
        self.txn_path  = f"{self.coord_name}.txn"
        self.jobs_dir  = f"{self.coord_name}_jobs"   # one subdir per job_id on disk
        os.makedirs(self.jobs_dir, exist_ok=True)

        # --- Recover from last checkpoint + transaction log ---
        self._recover()

    # -------------------------------------------------------
    # Start
    # - called in main()
    # - starts various threads then waits for new connections
    # -------------------------------------------------------
    def start():
        # Catalog Update Heartbeat
        threading.Thread(target=update, args=(self.port), daemon=True).start()

        # Worker Watcher (hears heartbeats and notices dead workers)
        # TODO

        # Dispatcher (pull from job_queue, send to best worker)
        # TODO

        
        self._accept_loop()

    # ----------------------------------
    # Accept Loop
    # - waits for new connection
    # - identify client or worker
    # - create handler thread
    # ---------------------------------

    def _accept_loop(self) -> None:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("", self.port))
        server_sock.listen(16)

        while True:
            conn, addr = server_sock.accept()
            threading.Thread(
                target=self._identify_and_dispatch,
                args=(conn, addr),
                daemon=True
            ).start()

    def _identify_and_dispatch(self, conn: socket.socket, addr) -> None:
        """
        - Identify connection as client or worker using first message
        - Send to correct handler loop
        """
        try:
            first_msg = recv_message(conn)
        except ConnectionError:
            conn.close()
            return

        peer_type, parsed = identify_peer(first_msg)

        if peer_type == "client":
            self._handle_client_connection(conn, addr, parsed)
        else:
            self._handle_worker_connection(conn, addr, parsed)

    # ------------------------
    # Client Handler
    # - runs on its own thread per client
    # - started by accept_loop
    # ------------------------

    def _handle_client_connection(self, conn: socket.socket, addr, join_text: str) -> None:
        """
        Runs on its own thread for each client connection.
 
        On connect:
          1. Parse JOIN <username>
          2. Register (or re-register) the client socket
          3. Flush any pending results that arrived while they were offline
          4. Loop reading commands until the socket closes
 
        Commands:
          SUBMIT_JOB <username> <exec_script> <out1,out2,...>\n<zip bytes>
          JOB_STATS  <username>
        """
        # Parse JOIN
        parts    = join_text.strip().split(" ", 1)
        username = parts[1] if len(parts) > 1 else join_text.strip()
 
        print(f"[CLIENT] {username} connected from {addr}")
 
        with self.lock:
            if username not in self.clients:
                self.clients[username] = {"sock": conn, "pending_results": []} # register new client
            else:
                self.clients[username]["sock"] = conn # update socket of re-connected client

        self._send_pending_results(username) # TODO

        # Send OK greeting
        try:
            send_message(conn, json.dumps({"status": "ok", "message": "hello"}).encode())
        except OSError:
            self._disconnect_client(username) # TODO
            return

        # Command loop - rcv commands from client
        while True:
            try:
                message = recv_message(conn)
            except ConnectionError:
                break

            self._handle_client_message(username, conn, message)

        self._disconnect_client(username) # TODO

    # -----------------------------------------------------------------------
    # Persistence — checkpoint + transaction log
    # -----------------------------------------------------------------------

    def _recover(self) -> None:
        """
        On startup, restore state from the last checkpoint then replay the
        transaction log on top of it.

        This is the same write-ahead log (WAL) pattern your Worker uses.
        """
        # --- CKPT ---
        if os.path.exists(self.ckpt_path):
            try:
                with open(self.ckpt_path) as f:
                    ckpt = json.load(f)
                self.jobs        = ckpt.get("jobs", {})
                self.clients     = ckpt.get("clients", {})
                self.next_job_id = ckpt.get("next_job_id", 0)

                # Note: workers must reconnect
                self.workers = {}
                print(f"[COORD] Loaded checkpoint: {len(self.jobs)} jobs")
            except Exception as exc:
                print(f"[COORD] Could not load checkpoint: {exc}")

        # --- Txn Log ---
        if os.path.exists(self.txn_path):
            try:
                with open(self.txn_path) as f:
                    for line in f:
                        if not line.strip():
                            continue
                        self._apply_txn(json.loads(line))
                print(f"[COORD] Replayed transaction log")
            except Exception as exc:
                print(f"[COORD] Error replaying transaction log: {exc}")

        # --- Re-fill Job Queue + Running Jobs ---
        for job_id, job in self.jobs.items():
            if job["status"] in ("queued", "running"):
                print(f"[COORD] Re-enqueuing job {job_id} (was {job['status']})")
                job["status"] = "queued"
                job["worker"] = None
                self.job_queue.put(job_id)

    def _apply_txn(self, entry: dict) -> None:
        """Apply a single transaction log entry to in-memory state."""
        event  = entry.get("event")
        job_id = entry.get("job_id")
 
        if event == "queued" and "job" in entry:
            job = entry["job"]
            self.jobs[job["job_id"]] = job
            self.next_job_id = max(self.next_job_id, int(job["job_id"]) + 1)
        elif event == "dispatched" and job_id:
            if job_id in self.jobs:
                self.jobs[job_id]["status"] = "running"
                self.jobs[job_id]["worker"] = entry.get("worker")
        elif event == "finished" and job_id:
            if job_id in self.jobs:
                self.jobs[job_id]["status"] = "finished"
        elif event == "requeued" and job_id:
            if job_id in self.jobs:
                self.jobs[job_id]["status"] = "queued"
                self.jobs[job_id]["worker"] = None
 
    def _write_txn(self, entry: dict) -> None:
        """
        Append one entry to the transaction log.
        Called BEFORE ack is sent
        """
        with open(self.txn_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())   # force to disk, not just OS buffer
 
    def checkpoint(self) -> None:
        """
        Write full state to disk and reset txn log
 
        TODO: call this periodically from a background thread so the txn
        log doesn't grow forever.
        """
        with self.lock:
            snapshot = {
                "jobs":        self.jobs,
                "clients":     {u: {"pending_results": c["pending_results"]}
                                for u, c in self.clients.items()},
                "next_job_id": self.next_job_id,
            }
 
        tmp = self.ckpt_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snapshot, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.ckpt_path)   # atomic rename
 
        # Safe to truncate the log now
        open(self.txn_path, "w").close()


# ---------------------
# main loop
# --------------------

    

#################
# Entry Point
################

def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed job coordinator")
    parser.add_argument("--port", required=True, type=int, help="Port to listen on")
    args = parser.parse_args()

    coord = Coordinator(args.port)
    coord.start()


if __name__ == "__main__":
    main()
