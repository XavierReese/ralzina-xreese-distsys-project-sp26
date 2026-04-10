#!/usr/bin/env python3
'''
Coordinator.py

Author: Rene Alzina + Xavier Reese
Date April 2026
'''

import threading
import json
import socket
import time
import os
import select
import base64
from collections import deque

COORDINATOR_TYPE = "coordinator"
COORDINATOR_PROJECT = "dist_job_coordinator"

CATALOG_HOST = "catalog.cse.nd.edu"
CATALOG_PORT = 9097

BUFSIZ = 4096
MAX_BACKOFF = 64

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

# -------------------
# Coordinator
# - manages state of jobs and connections
# -------------------

class Coordinator:
    def __init__(self, port=0, coord_name="coordinator"):

        self.coord_name = coord_name
        self.port = port

        # --- State of Clients, Workers, and Jobs ---
        """
        Rene: self.clients doesn't have a specific type structure, the structure should be:
        self.clients = {
            "username": {
                "socket": <socket_object>,
                "pending_results": [
                    {"job_id": 101, "status": "finished", ...},
                    ...
                ],
                "fileno": fileno from socket for epoll
            }
        }
        Regardless, I'm not working with clients, just a thought
        """
        self.clients = {"c1": {"fileno": 2}} 
        self.client_queue = []
        self.contact_workers = {} # worker_fd -> socket_type, worker_id for epoll
        self.workers = {} # worker_id -> stats for heartbeat
        
        # Rene: based on ckpt, this should be self.jobs imo
        """
        self.jobs[job_id] = {
            "client_id": "client",
            "worker_id": worker_id,     # or None if not scheduled / rescheduled,
            "status": "running",
            "script": "start.sh"        # script to run program
            "result": None              # store stdout, stderr
        }
        """
        # Testing only
        self.jobs = {} 
        self.jobs[1] = {
                    "client_id": "c1",
                    "worker_id": "w1",     
                    "status": "not_started",
                    "script": "start.sh",
                    "zip_path": "coordinator_jobs/1.zip",
                    "result": None  
                }
        self.job_queue = deque([1])
        self.next_job_id = 0

        self.lock = threading.Lock() # separate threads for clients & workers use this to lock coord state

        # --- Persistence ---
        self.ckpt_path = f"{self.coord_name}.ckpt"
        self.txn_path  = f"{self.coord_name}.txn"

        # Rene: the coord shouldn't have one subdir per job_id. I say it should have 
        # one general job directory, and just save the zip files in that directory
        # it's the worker's responsibility to create a subdir per job_id since it's
        # the worker that will actually run the code, not the coord.
        # So this line of code is fine, leave as is, I just mean there shouldn't be any more job directories other than this one
        self.jobs_dir  = f"{self.coord_name}_jobs"   # one subdir per job_id on disk

        os.makedirs(self.jobs_dir, exist_ok=True)

        # --- Recover from last checkpoint + transaction log ---
        self._recover()

    # -------------------------------------------------------
    # Start
    # - called in main()
    # - starts various threads then waits for new connections
    # -------------------------------------------------------
    def start(self):
        # Rene: Created socket before calling update thread, it's safer
        # saved socket as an attribute rather than locally
        # Connect to HOST, PORT
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(("", self.port))
        self.server_sock.listen(16)

        _, self.port = self.server_sock.getsockname()
        print(f"[COORD] Listening on port {self.port}")

        # Set log count to 0 before starting up
        self.log_count = 0

        # Start epoll and connections
        # Testing, should just be = {}
        self.connections = {
            2: {
                "socket": None,
                "sock_type": "client",      # both client & worker use two sockets, this tells you what socket it is
                "id": None,             # either username or worker_id
                "type": None,           # either worker or client
                "recv_buffer": b"",
                "send_buffer": b""
            }
        }
        self.send_ack = {}
        self.recv_ack = set()
        self.epoll = select.epoll()

        # Catalog Update Heartbeat
        threading.Thread(target=self.update, args=(self.port,), daemon=True).start()

        # Worker Watcher (hears heartbeats and notices dead workers)
        # TODO

        # Dispatcher (pull from job_queue, send to best worker)
        # TODO
        
        # self._accept_loop()
        self.run()

    # Rene, trying epoll
    # All the following functions are mine up to def update (not invlusive so def update is not mine)
    def run(self):
        epoll = self.epoll

        self.server_sock.setblocking(False)
        epoll.register(self.server_sock.fileno(), select.EPOLLIN)

        connections = self.connections
        send_ack = self.send_ack # id -> type of ack (did you just register? schedule job with cilent or worker? )

        try:
            while True:         
                events = epoll.poll(1) # 1 second timeout

                for fileno, event in events:
                    if fileno == self.server_sock.fileno():
                        # Accepting clients
                        client_socket, _ = self.server_sock.accept()
                        # Client socket is the socket to talk to the received connection
                        client_socket.setblocking(False)
                        epoll.register(client_socket.fileno(), select.EPOLLIN)
                        connections[client_socket.fileno()] = {
                            "socket": client_socket,
                            "sock_type": None,      # both client & worker use two sockets, this tells you what socket it is
                            "id": None,             # either username or worker_id
                            "type": None,           # either worker or client
                            "recv_buffer": b"",
                            "send_buffer": b""
                        }

                    elif event & select.EPOLLIN:
                        connection = connections[fileno]
                        client_socket = connection["socket"]

                        # testing
                        if connection["sock_type"] == "client":
                            continue

                        self.read_buffer(connection)
                        buffer = connection["recv_buffer"]

                        if not buffer:
                            print(f"[COORD] {connection["type"]}_{connection["id"]}_{connection["sock_type"]} disconnected")
                            # Client broke connection
                            epoll.unregister(fileno)
                            del connections[fileno]
                            client_socket.close()
                            continue
                            
                        # If connection is new, it will send its information
                        # If it's not then it will send a normal request
                        # Normal requests don't return anything in handle_request
                        # registration requests return the worker/client data in handle_request
                        if connections[fileno]["id"] == None:
                            id, sock_type, type = self.handle_request(connections[fileno], fileno)
                            if id != None:
                                connections[fileno]["id"] = id
                                connections[fileno]["type"] = type
                                connections[fileno]["sock_type"] = sock_type
                                self.workers[id] = {}
                                send_ack[id] = {
                                    "ack_type": "register",
                                    "type": type,
                                    "sock_type": sock_type
                                }
                        else:
                            self.handle_request(connections[fileno], fileno)

                    elif event & select.EPOLLOUT:
                        connection = connections[fileno]
                        client_socket = connection["socket"]

                        if self.send_response(connections[fileno]):
                            if not connections[fileno]["send_buffer"]:
                                connection = connections[fileno]["id"]
                                if connection in send_ack:

                                    if send_ack[connection]["ack_type"] == "register":
                                        print(f"[COORD] Finished sending OK to {connection} for {send_ack[connection]}.")

                                        if send_ack[connection]["type"] == "worker" and send_ack[connection]["sock_type"] == "req_sock":
                                            if connection in self.workers:
                                                # Example:
                                                # workers[worker_id] = fileno
                                                # We only need to know fileno of req_sock, not res_sock
                                                # We initiate conversation to worker to send requests
                                                self.workers[connection]["fileno"] = fileno
                                                print(f"Registered req_sock from {connection}, checking job_queue after we receive heartbeat")

                                    del send_ack[connection]
                                epoll.modify(fileno, select.EPOLLIN)
                        else:
                            print(f"{connection["id"]} disconnected")
                            epoll.unregister(fileno)
                            del connections[fileno]
                            client_socket.close()
                            continue
        except Exception as e:
            print(f"[COORD] crashed:",e)
            raise

    def read_buffer(self, connection):
        client_socket = connection["socket"]

        # New message
        try:
            # Read message length
            length_bytes = client_socket.recv(BUFSIZ)
            connection["recv_buffer"] += length_bytes
        except ConnectionError:
            connection["recv_buffer"] = b""

    def handle_request(self, connection, fileno):
        buffer = connection["recv_buffer"]

        r = None
                
        while True:
            # Need at least 4 bytes to know message length
            if len(buffer) < 4:
                break

            # Read length if available
            message_len = int.from_bytes(buffer[:4], "big")

            # Check if full message arrived:
            if len(buffer) < 4 + message_len:
                break

            # Read full message
            message_bytes = buffer[4:4+message_len]

            # Remove processed bytes from buffer
            buffer = buffer[4 + message_len:]

            # Execute request
            r = self.execute(message_bytes, connection, fileno)

        # Save remaining data to be handled later
        connection["recv_buffer"] = buffer

        return r
    
    def clear_job_queue(self):
        i = 0
        n = len(self.job_queue)
        while i < n:
            print(self.job_queue)
            print(self.job_queue)
            job_id = self.job_queue.popleft()
            print(self.job_queue)
            print(f"Trying to schedule {job_id}")
            client_id = self.jobs[job_id]["client_id"]
            script = self.jobs[job_id]["script"]
            zip_path = self.jobs[job_id]["zip_path"]
            self.schedule_job(client_id, script, zip_path, job_id)
            i += 1
    
    def execute(self, message_bytes, connection, fileno):
        print(message_bytes)
        try:
            request = json.loads(message_bytes.decode("utf-8"))
        
        except (TypeError, ValueError):
            response = {
                "status": "invalid",
                "message": "Request is not valid JSON"
            }
            self.schedule_response(response, connection, fileno)
            return 

        # validate fields
        if "type" not in request:
            print(request)
            response = {
                "status": "invalid",
                "message": "You must specify if you're a client or worker in request['type']"
            }

            self.schedule_response(response, connection, fileno)
            return

        # Perform operation
        match request["type"]:
            case "worker":
                match request["method"]:
                    case "register":
                        if self.invalid_args(["id", "sock_type"], request, connection, fileno):
                            return
                        
                        id = request["id"]
                        sock_type = request["sock_type"]
                        type = request["type"]

                        response = {
                            "status": "ok",
                            "message": "Registered"
                        }

                        self.schedule_response(response, connection, fileno)
                        
                        return id, sock_type, type

                    case "heartbeat":
                        if self.invalid_args(["id", "cpu_load", "free_main_mem_mb", "free_disk_mem_gb", "available_jobs"], request, connection, fileno):
                            return
                        
                        id = request["id"]
                        cpu_load = request["cpu_load"]
                        free_main_mem_mb = request["free_main_mem_mb"]
                        free_disk_mem_gb = request["free_disk_mem_gb"]
                        available_jobs = request["available_jobs"]

                        self.workers[id]["cpu_load"] = cpu_load
                        self.workers[id]["free_main_mem_mb"] = free_main_mem_mb
                        self.workers[id]["free_disk_mem_gb"] = free_disk_mem_gb
                        self.workers[id]["available_jobs"] = available_jobs

                        print(f"[COORD] Received heartbeat from {request["type"]} {id}")

                        if len(self.job_queue) > 0:
                            print(f"Trying to clear job_queue with {id}")
                            self.clear_job_queue()

                    case "ack":
                        if fileno in self.recv_ack:
                            if self.invalid_args(["ack_type", "status", "job_id"], request, connection, fileno):
                                return

                            job_id = request["job_id"]
                            
                            if request["ack_type"] == "schedule":
                                if request["status"] == "success":
                                    self.jobs[job_id]["status"] = "running"
                                else:
                                    job = self.jobs[job_id]
                                    job["worker_id"] = None
                                    self.schedule_job(job["client_id"], job["script"], job["zip_path"])
                            
                            if request["ack_type"] == "stop":
                                if request["status"] == "success":
                                    try:
                                        del self.jobs[job_id]
                                    except KeyError:
                                        pass
                                else:
                                    request = {
                                        "method": "stop",
                                        "job_id": job_id
                                    }

                                    self.schedule_response(request, self.connections[fileno], fileno)
                        else:
                            response = {
                                "status": "error",
                                "message":  "Received unexpected ack"
                            }
                            self.schedule_response(response, connection, fileno)

                    case "output":
                        if self.invalid_args(["zip_bytes", "job_id", "stdout", "stderr", "exit_code", "id"], request, connection, fileno):
                            return

                        print(f"Recieved output from {request["id"]} for job {request["job_id"]}")
                        print("Output:")
                        print(f"stdout: {request["stdout"]}")
                        print(f"stderr: {request["stderr"]}")


                        print("Saving to disk...")
                        
                        # self.log(all of this information to log file)

                        # del self.jobs[job_id]

                        self.client_queue.append(self.jobs[request["job_id"]]["client_id"])

                        print("Attempting to send result to client...")

                    case _:
                        response = {
                            "status": "invalid",
                            "message":  "Invalid method requested"
                        }
                        self.schedule_response(response, connection, fileno)

            case "client":
                # Handle each possible client operation
                match request["method"]:
                    case "schedule":
                        if self.invalid_args(["zip_file", "script", "id"], request, connection, fileno):
                            return

                        job_id = self.next_job_id
                        self.next_job_id += 1

                        zip_path = os.path.join(self.jobs_dir, f"{job_id}.zip")

                        encoded_zip = request["zip_file"]
                        zip_bytes = base64.b64decode(encoded_zip)

                        try:
                            with open(zip_path, "wb") as f:
                                f.write(zip_bytes)
                            print(f"Saved zip file to {zip_path}")
                        except Exception as e:
                            print(f"Failed to write zip file {zip_path}: {e}")


                        # Send ack to client that you received and processed request
                        response = {
                            "status": "ok",
                            "message":  f"Job request {job_id} received and processesd"
                        }
                        self.schedule_response(response, connection, fileno)

                        # contact a worker

                        self.schedule_job(fileno, request["script"], zip_path)

                    case "stop":
                        if self.invalid_args(["id"], request, connection, fileno):
                            return

                        response = {
                            "status": "ok",
                            "message":  f"Job request {job_id} received and processesd"
                        }
                        self.schedule_response(response, connection, fileno)

                        self.jobs[job_id]["status"] = "stop"

                        request = {
                            "method": "stop",
                            "job_id": job_id
                        }

                        worker_fd = self.workers[self.jobs[job_id]["worker_id"]]["fileno"]

                        self.schedule_response(request, self.connections[worker_fd], worker_fd)

                        self.recv_ack.add(worker_fd)

                    case _:
                        response = {
                            "status": "invalid",
                            "message":  "Invalid method requested"
                        }
                        self.schedule_response(response, connection, fileno)

    
    def schedule_job(self, client_id, script, zip_path, job_id=None):
        # Logic to select which worker to run

        if not job_id:
            job_id = self.next_job_id
            self.next_job_id += 1
        
        for worker_id in self.select_worker():
            print(f"Trying to schedule {job_id} in {worker_id}")
            try:
                worker_fd = self.workers[worker_id]["fileno"]
                                    
                self.jobs[job_id] = {
                    "client_id": client_id,
                    "worker_id": worker_id,     
                    "status": "not_started",
                    "script": script,
                    "zip_path": zip_path,
                    "result": None  
                }

                try:
                    with open(zip_path, "rb") as f:
                        zip_bytes = f.read()
                except FileNotFoundError:
                    print(f"Error: The file at {zip_path} was not found.")
                    return 
                except Exception as e:
                    print(f"An unexpected error occurred when scheduling {client_id}_{job_id}: {e}")
                    return 
                
                encoded_bytes = base64.b64encode(zip_bytes).decode('utf-8')

                request = {
                    "method": "schedule",
                    "zip_bytes": encoded_bytes,
                    "job_id": job_id,
                    "script": script
                }
                print(f"about to schedule message to worker to execute job {request}")
                self.schedule_response(request, self.connections[worker_fd], worker_fd)
                print("scheduled message iwth job")
                self.recv_ack.add(worker_fd)
                print("expecting worker ack")

                return
                
            except Exception as e:
                print(f"[COORD] Worker {worker_id} failed: [{e}], trying another worker")

            response = {
                "status": "error",
                "message": f"job {job_id} failed to schedule"
            }
            self.schedule_response(response, self.connections[self.clients[client_id]["fileno"]], self.clients[client_id]["fileno"])

        print(f"[COORD] All workers failed or no workers active")
        self.job_queue.append(job_id)

    def select_worker(self):
        if not self.workers:
            print("No workers available to run a job")
            return []
        
        print(self.workers)
        
        eligible_ids = [
            wid for wid, stats in self.workers.items() 
            if "available_jobs" in stats and "cpu_load" in stats and "free_main_mem_mb" in stats and "free_disk_mem_gb" in stats and stats.get("available_jobs", 0) > 0
        ]

        def worker_score(worker_id):
            stats = self.workers[worker_id]

            return filter(lambda x: x["available_jobs"] > 0,(
                stats["available_jobs"],     # Priority 1: Highest available slots
                -stats["cpu_load"],          # Priority 2: Lowest CPU % (tie-breaker)
                stats["free_main_mem_mb"],   # Priority 3: Highest RAM (tie-breaker)
                stats["free_disk_mem_gb"]    # Priority 4: Highest Disk (tie-breaker)
            ))
        
        return sorted(eligible_ids, key=worker_score, reverse=True)

    def invalid_args(self, args, request, connection, fileno):
        for arg in args:
            if arg not in request:
                response = {
                    "status": "invalid",
                    "message": f"invalid, {arg} not present"
                }
                self.schedule_response(response, connection, fileno)
                return True
        return False
    
    def schedule_response(self, response, connection, fileno):
        try:
            pre_response = json.dumps(response).encode("utf-8")
            response_length = len(pre_response).to_bytes(4, byteorder="big")
            final_response = response_length + pre_response
        except (TypeError, ValueError):
            print(f"[COORD] Error: Couldn't serialize response to JSON")
    
        connection["send_buffer"] += final_response
        self.epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT)

    def send_response(self, connection):
        client_socket = connection["socket"]
        send_buffer = connection["send_buffer"]

        if send_buffer:
            try:
                sent = client_socket.send(send_buffer)
                connection["send_buffer"] = send_buffer[sent:]
                return True
            except BlockingIOError:
                return True # try again next EPOLLOUT

            except socket.error as e:
                print(f"[COORD] Network Error: Failed to send response: {e}")
                return False # network error, client disconnected

    # ------------------------------------
    # Catalog Update
    # - called in coord.start as heartbeat
    # ------------------------------------
    # Rene, I placed it here because I suppose this is part of coordinator
    def update(self, port):
        # Rene: you used to have self.catalog_s, but if the socket
        # is created each time, might as well just not make it an attribute
        while True:
            u = {
                    "type": COORDINATOR_TYPE,
                    "port": port,
                    "owner": "xreese", # can this be both?
                    "project": COORDINATOR_PROJECT
                }

            try:
                catalog_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                catalog_s.connect((CATALOG_HOST, CATALOG_PORT))

                msg = json.dumps(u).encode('utf-8')
                catalog_s.sendto(msg, (CATALOG_HOST, CATALOG_PORT))
                print("[COORD] Catalog Update Sent")
            except Exception as e:
                print(f"Failed to send update: {e}")

            catalog_s.close()
            time.sleep(60)

    # ----------------------------------
    # Accept Loop
    # - waits for new connection
    # - identify client or worker
    # - create handler thread
    # ---------------------------------

    def _accept_loop(self) -> None:
        while True:
            conn, addr = self.server_sock.accept()
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
                self.job_queue.append(job_id)

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
        Rene: Have an internal log_count so that you don't need threads + locking (nobody wnats that)
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
    """
    Rene: couldn't we remove arguments since port is 0 since we don't care which port?
    parser = argparse.ArgumentParser(description="Distributed job coordinator")
    parser.add_argument("--port", required=True, type=int, help="Port to listen on")
    args = parser.parse_args()

    coord = Coordinator(args.port)
    """
    coord = Coordinator()
    coord.start()

if __name__ == "__main__":
    main()
