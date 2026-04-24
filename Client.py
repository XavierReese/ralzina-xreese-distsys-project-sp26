"""
client.py  –  Interactive session client for the distributed job coordinator.

Usage:
    python client.py --name <username>

The coordinator is discovered automatically via the ND catalog service.
Once connected, type commands at the prompt:
    submit --job_dir <dir> --exec <script> --job_name <name> [--outputs <rel/path> ...] [--out-dir <local_dir>]
    stats
    quit

Results are pushed automatically by the coordinator and extracted into --out-dir
as soon as they arrive, while you continue to interact normally.
"""

import argparse
import io
import os
import socket
import threading
import time
import zipfile
import shlex
import json
import base64
import queue

import requests


# ---------------------------------------------------------------------------
# Catalog constants
# - used for coordinator discovery
# ---------------------------------------------------------------------------

CATALOG_URL        = "http://catalog.cse.nd.edu:9097/query.json"
COORDINATOR_TYPE   = "coordinator"
COORDINATOR_PROJECT = "dist_job_coordinator"


# ---------------------------------------------------------------------------
# Catalog discovery
# - find coordinator
# - return host/port
# ---------------------------------------------------------------------------

def discover_coordinator() -> tuple[str, int]:
    """
    Poll the ND catalog until a coordinator entry is found.
    Returns (host, port) of the most recently heard-from coordinator.
    """
    delay = 1
    while True:
        try:
            response = requests.get(CATALOG_URL, timeout=10)
            response.raise_for_status()
            services = response.json()

            most_recent = None
            for entry in services:
                if (entry.get("type")    == COORDINATOR_TYPE and
                    entry.get("project") == COORDINATOR_PROJECT):
                    if (not most_recent or
                            entry.get("lastheardfrom", 0) > most_recent.get("lastheardfrom", 0)):
                        most_recent = entry

            if most_recent:
                host = most_recent["name"]
                port = most_recent["port"]
                print(f"[INFO] Discovered coordinator at {host}:{port}")
                return host, port

            raise Exception("Coordinator not found in catalog.")

        except Exception as exc:
            print(f"[INFO] Discovery failed ({exc}), retrying in {delay}s ...")
            time.sleep(delay)
            delay = min(delay * 2, 128)


# ---------------------------------------------------------------------------
# Low-level socket helpers
# - send & receive msgs over given TCP socket
# ---------------------------------------------------------------------------

def send_message(sock: socket.socket, message: bytes) -> None:
    """
    Sends: 4-byte big-endian length prefix followed by the payload.
    """
    sock.sendall(len(message).to_bytes(4, byteorder="big") + message)


def recv_message(sock: socket.socket) -> bytes:
    """
    Receives: 4-byte length header then the payload.
    
    ERR Handling: ConnectionError if the coordinator closes the connection early.
    """
    raw_len = _recv_exact(sock, 4)
    return _recv_exact(sock, int.from_bytes(raw_len, byteorder="big"))


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed by coordinator unexpectedly.")
        buf.extend(chunk)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Connection helper
# - calls discover_coordinator
# - open TCP
# - send register request
# ---------------------------------------------------------------------------

def connect_to_coordinator(username: str) -> socket.socket:
    """
    Discover the coordinator, open a TCP connection, and send register request
    Retries indefinitely on failure (re-discovering via catalog each time).
    """
    while True:
        host, port = discover_coordinator()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))

            join_msg = {
                    "method": "register",
                    "type": "client",
                    "username": username
            }
            send_message(sock, json.dumps(join_msg).encode('utf-8'))
            print(f"[REGISTER] Requested to join coordinator as '{username}'")
            return sock
        except OSError as exc:
            print(f"[REGISTER] Connection failed ({exc}), rediscovering ...")
            time.sleep(2)


# ---------------------------------------------------------------------------
# Zipping helpers
# ---------------------------------------------------------------------------

def zip_directory(dir_path: str) -> bytes:
    """
    Recursively zip dir_path, preserving the top-level directory name,
    and return the raw zip bytes.
    """
    buf      = io.BytesIO()
    dir_path = os.path.abspath(dir_path)
    dir_name = os.path.basename(dir_path)

    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(dir_path):
            for fname in files:
                full_path    = os.path.join(root, fname)
                archive_name = os.path.join(dir_name, os.path.relpath(full_path, dir_path))
                zf.write(full_path, archive_name)

    return buf.getvalue()

# Rene: I added out_dir because it wasn't defined, did you mean to pass in out_dir
# as an arg?
def extract_zip(zip_bytes: bytes, username: str, name: str, job_id: str) -> None:
    os.makedirs(f'./{username}', exist_ok=True)
    output_path = os.path.join(username, f"{name}--{job_id}")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        zf.extractall(output_path)


# ---------------------------------------------------------------------------
# Message builders
# - used to build messages to be sent to coordinator
# ---------------------------------------------------------------------------

def build_submit_message(username: str,
                         exec_script: str,
                         # outputs: list[str],
                         zip_bytes: bytes,
                         name: str) -> bytes:
    message_dict = {
        "method": "submit",
        "username": username,
        "script": exec_script,
        "name": name,
        # "outputs": outputs,
        "zip_file": base64.b64encode(zip_bytes).decode('utf-8'),
        "type": "client"
    }

    return json.dumps(message_dict).encode('utf-8')



def build_stats_message(username: str) -> bytes:
    message_dict = {
        "method": "stats",
        "username": username,
        "type": "client"
    }

    return json.dumps(message_dict).encode('utf-8')

def build_stop_message(job_id: int) -> bytes:
    message_dict = {
            "method": "stop",
            "type": "client",
            "job_id": job_id
    }

    return json.dumps(message_dict).encode('utf-8')

def build_output_ack(job_id: str) -> bytes:
    message_dict = {
        "type": "client",
        "method": "ack",
        "ack_type": "output",
        "status": "ok",
        "job_id": job_id
    }
    return json.dumps(message_dict).encode('utf-8')

# ---------------------------------------------------------------------------
# Session
# - manages state
# - seen by both TCP threads
# ---------------------------------------------------------------------------

class Session:
    """
    Shared state between the main (REPL) thread and the background receiver thread.

    pending_out_dirs maps job_id -> local output directory so the receiver
    thread knows where to extract results when a RESULT message arrives.
    """

    def __init__(self, sock: socket.socket, username: str):
        self.sock                             = sock
        self.username                         = username
        self.connected                        = False
        self.send_q                           = queue.Queue()

    def push_msg(self, msg):
        self.send_q.put(msg)

# ---------------------------------------------------------------------------
# Background sender thread
# - continuously pops and send messages from queue
# ---------------------------------------------------------------------------

def sender_loop(session: Session) -> None:
    """
    Runs on its own thread. Pops messages from thread-safe queue and sends them.
    Avoids conflicts between receiver thread sending acks, and messages based on client input
    """
    while True:
        msg = session.send_q.get()

        if msg is None: break

        try:
            send_message(session.sock, msg)
        except OSError as exc:
            print(f"[ERROR] Failed to send message: {exc}")
            return


# ---------------------------------------------------------------------------
# Background receiver thread
# - blocks on receiving function
# - reads messages from coordinator
# ---------------------------------------------------------------------------

def receiver_loop(session: Session) -> None:
    """
    Runs on its own thread. Sits blocking on recv_message() and dispatches anything
    the coordinator pushes without interrupting client input.
    Reads results, status, errors.
    """
    while True:
        try:
            message = recv_message(session.sock)
        except ConnectionError as exc:
            print(f"\n[DISCONNECTED] {exc}")

            session.connected = False
            new_sock = connect_to_coordinator(session.username) # triggers retries until connected
            session.sock = new_sock
            continue

        _handle_push(session, message)
        print("> ", end="", flush=True)  # restore prompt after async output


def _handle_push(session: Session, message: bytes) -> None:
    """Dispatch a coordinator-pushed message to the right handler."""
    try:
        msg = json.loads(message.decode('utf-8'))
    except Exception as e:
        print(f'[ERROR] Failed to decode message: {message}\nError: {e}')
        return

    if "tag" not in msg or msg.get("tag") is None:
        print(f'[ERROR] Invalid message received from coordinator - no tag: {msg}')

    tag = msg.get("tag")
    ok = msg.get("status", "error") == "ok"

    if not ok:
        message = msg.get("message", "no error provided")
        print(f'[ERROR] {message}')
    elif tag == "register":
        session.connected = True
        print(f'[JOIN] Successful')
    elif tag == "stats":
        jobs = msg.get("message", [])
        if len(jobs) == 0:
            print(f'[STATS] No jobs associated with user {session.username}')
        else:
            for name, status, job_id in jobs:
                print(f'\n{name}: {status} with job_id {job_id}', end="")
            print()
    elif tag == "stop_ack":
        job_id = msg.get("job_id", "UNKNOWN")
        print(f'[STOP] request to stop job {job_id} received and acknowledged')
    elif tag == "stop":
        job_id = msg.get("job_id", "UNKNOWN")
        print(f'[STOP] job {job_id} stopped')
    elif tag == "submit":
        job_id = msg.get("job_id")
        name = msg.get("name", "NA")
        if not job_id:
            print(f'[ERROR] Internal Error: No job_id provided by coordinator')
        else:
            print(f'\r[SUBMIT] Job Submitted. When the job has completed and you are logged in, the results will be automatically downloaded to ./{session.username}/{name}')
    elif tag == "output":
        name = msg.get("name", "NA")
        job_id = msg["job_id"]

        print(time.time())

        encoded_zip = msg["zip_bytes"]
        zip_bytes = base64.b64decode(encoded_zip)

        extract_zip(zip_bytes, session.username, name, job_id)

        print(f"[OUTPUT] Received output from job {name}--{job_id}")
        handle_output(session, job_id)
    else:
        print(f'[ERROR] No tag provided')

# ---------------------------------------------------------------------------
# REPL command handlers
# ---------------------------------------------------------------------------

def handle_submit(session: Session, args: argparse.Namespace) -> None:
    if not os.path.isdir(args.job_dir):
        print(f"[ERROR] Not a directory: {args.job_dir}")
        return

    # outputs = args.outputs or []

    print(f"[INFO] Zipping {args.job_dir} ...")
    try:
        zip_bytes = zip_directory(args.job_dir)
    except Exception as exc:
        print(f"[ERROR] Failed to zip directory: {exc}")
        return
    
    msg = build_submit_message(
        username    = session.username,
        exec_script = args.exec,
        # outputs     = outputs,
        zip_bytes   = zip_bytes,
        name        = args.job_name
    )

    print(time.time())
    session.push_msg(msg)


def handle_stats(session: Session) -> None:
    session.push_msg(build_stats_message(session.username))

def handle_stop(session: Session, args: argparse.Namespace) -> None:
    session.push_msg(build_stop_message(args.job_id))

def handle_output(session: Session, job_id: str) -> None:
    session.push_msg(build_output_ack(job_id))

# ---------------------------------------------------------------------------
# Interactive REPL parser
# ---------------------------------------------------------------------------

def make_repl_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="", add_help=False, exit_on_error=False)
    sub    = parser.add_subparsers(dest="command")

    p_submit = sub.add_parser("submit", exit_on_error=False)
    p_submit.add_argument("--job_dir",     required=True,
                          help="Path to the job directory")
    p_submit.add_argument("--exec",    required=True,
                          help="Entry-point script relative to the job directory root")
    p_submit.add_argument("--job_name",    required=True,
                          help="Name to identify job")
    

    sub.add_parser("stats", exit_on_error=False)

    p_stop = sub.add_parser("stop", exit_on_error=False)
    p_stop.add_argument("--job_id",     required=True,
                          help="job id from stats page")

    sub.add_parser("quit",  exit_on_error=False)

    sub.add_parser("help",  exit_on_error=False)

    sub.add_parser("clear", exit_on_error=False)

    return parser


# Removed: [--outputs <path> ...] [--out-dir <dir>] after <name>
# Removed on line under Zip <dir>: --outputs lists relative paths to retrieve when done (e.g. results/ logs/out.txt).
#     Results are extracted automatically when the coordinator pushes them back.

HELP_TEXT = """\
Commands:
  submit --job_dir <dir> --exec <script> --job_name <name> 
      Zip <dir> and submit it. --exec is the entry-point script inside the dir.
      --job_name is a human readable way to refer to that job when stats is called

  stop --job_id <job_id>
      Stop a running job
      
  stats
      Query job stats from the coordinator.

  clear
      Clear terminal

  quit
      Disconnect and exit.
"""


# ---------------------------------------------------------------------------
# Main session loop
# - starts receive thread in background
# - waits on client input, parses and handles commands
# ---------------------------------------------------------------------------

def run_session(username: str) -> None:
    sock    = connect_to_coordinator(username)
    session = Session(sock, username)

    recv_thread = threading.Thread(
        target=receiver_loop, args=(session,), daemon=True, name="receiver"
    )
    recv_thread.start()

    send_thread = threading.Thread(
        target=sender_loop, args=(session,), daemon=True, name="sender"
    )
    send_thread.start()

    repl_parser = make_repl_parser()
    print(HELP_TEXT)

    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[INFO] Disconnecting.")
            break

        if not line:
            continue

        try:
            args = repl_parser.parse_args(shlex.split(line))
        except (argparse.ArgumentError, SystemExit):
            print("[ERROR] Unrecognised command or bad arguments. Type 'help' for usage.")
            continue

        if args.command == "quit":
            break
        elif args.command in ("help", None):
            print(HELP_TEXT)
        elif args.command == "submit":
            handle_submit(session, args)
        elif args.command == "stats":
            handle_stats(session)
        elif args.command == "stop":
            handle_stop(session, args)
        elif args.command == "clear":
            os.system("clear")

    sock.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="interactive client for the distributed job coordinator"
    )
    parser.add_argument("--name", required=True, help="your username")
    args = parser.parse_args()

    run_session(args.name)


if __name__ == "__main__":
    main()
