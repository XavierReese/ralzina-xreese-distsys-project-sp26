"""
client.py  –  Interactive session client for the distributed job coordinator.

Usage:
    python client.py --name <username>

The coordinator is discovered automatically via the ND catalog service.
Once connected, type commands at the prompt:
    submit --job <dir> --exec <script> [--outputs <rel/path> ...] [--out-dir <local_dir>]
    stats
    quit

Results are pushed automatically by the coordinator and extracted into --out-dir
as soon as they arrive, while you continue to interact normally.
"""

import argparse
import io
import os
import socket
import sys
import threading
import time
import zipfile

import requests


# ---------------------------------------------------------------------------
# Catalog constants
# - used for coordinator discovery
# ---------------------------------------------------------------------------

CATALOG_URL        = "http://catalog.cse.nd.edu:9097/query.json"
COORDINATOR_TYPE   = "coordinator"          # TODO agree w/Rene
COORDINATOR_PROJECT = "dist_job_coordinator" # TODO agree w/Rene


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
    # TODO: agree w/Rene
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
# - send JOIN
# ---------------------------------------------------------------------------

def connect_to_coordinator(username: str) -> socket.socket:
    """
    Discover the coordinator, open a TCP connection, and send JOIN <username>.
    Retries indefinitely on failure (re-discovering via catalog each time).
    """
    while True:
        host, port = discover_coordinator()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            send_message(sock, f"JOIN {username}".encode())
            print(f"[INFO] Joined coordinator as '{username}'")
            return sock
        except OSError as exc:
            print(f"[INFO] Connection failed ({exc}), rediscovering ...")
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


def extract_zip(zip_bytes: bytes, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        zf.extractall(out_dir)


# ---------------------------------------------------------------------------
# Message builders
# - used to build messages to be sent to coordinator
# ---------------------------------------------------------------------------

def build_submit_message(username: str,
                         exec_script: str,
                         outputs: list[str],
                         zip_bytes: bytes) -> bytes:
    """
    SUBMIT_JOB <username> <exec_script> <out1,out2,...>\n<zip bytes>

    TODO: replace header format with whatever is agreed with your partner.
    """
    outputs_str = ",".join(outputs) if outputs else ""
    header = f"SUBMIT_JOB {username} {exec_script} {outputs_str}\n".encode()
    return header + zip_bytes


def build_stats_message(username: str) -> bytes:
    """
    JOB_STATS <username>
    """
    return f"JOB_STATS {username}".encode()


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
        self.pending_out_dirs: dict[str, str] = {} #TODO Whats this?
        self._lock                            = threading.Lock()

    def register_job(self, job_id: str, out_dir: str) -> None:
        with self._lock:
            self.pending_out_dirs[job_id] = out_dir

    def pop_out_dir(self, job_id: str) -> str | None:
        with self._lock:
            return self.pending_out_dirs.pop(job_id, None)


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
            # TODO reconnect + re-JOIN here?
            break

        _handle_push(session, message)
        print("> ", end="", flush=True)  # restore prompt after async output


def _handle_push(session: Session, message: bytes) -> None:
    """Dispatch a coordinator-pushed message to the right handler."""
    newline_idx  = message.find(b"\n")
    header_bytes = message[:newline_idx] if newline_idx != -1 else message
    payload      = message[newline_idx + 1:] if newline_idx != -1 else b""

    parts = header_bytes.decode(errors="replace").split(" ", 2)
    tag   = parts[0] if parts else ""

    if tag == "RESULT":
        # Format: RESULT <job_id>\n<zip bytes>
        job_id  = parts[1] if len(parts) > 1 else "unknown"
        out_dir = session.pop_out_dir(job_id) or f"./job_{job_id}_results"

        print(f"\n[RESULT] Job {job_id} complete. Extracting to {out_dir} ...")
        try:
            extract_zip(payload, out_dir)
            print(f"[RESULT] Done — files in {out_dir}")
        except Exception as exc:
            print(f"[ERROR] Failed to extract results for job {job_id}: {exc}")

    elif tag == "OK":
        # TODO: call session.register_job(job_id, pending_out_dir) if this is a SUBMIT_JOB ack
        print(f"\n[OK] {' '.join(parts[1:])}")

    elif tag == "STATUS":
        print(f"\n[STATUS] {' '.join(parts[1:])}")

    elif tag == "ERROR":
        print(f"\n[ERROR] {' '.join(parts[1:])}")

    else:
        print(f"\n[COORDINATOR] {header_bytes.decode(errors='replace')}")


# ---------------------------------------------------------------------------
# REPL command handlers
# ---------------------------------------------------------------------------

def handle_submit(session: Session, args: argparse.Namespace) -> None:
    if not os.path.isdir(args.job):
        print(f"[ERROR] Not a directory: {args.job}")
        return

    outputs = args.outputs or []
    out_dir = args.out_dir or f"./results_{os.path.basename(args.job.rstrip('/'))}"

    print(f"[INFO] Zipping {args.job} ...")
    try:
        zip_bytes = zip_directory(args.job)
    except Exception as exc:
        print(f"[ERROR] Failed to zip directory: {exc}")
        return

    msg = build_submit_message(
        username    = session.username,
        exec_script = args.exec,
        outputs     = outputs,
        zip_bytes   = zip_bytes,
    )

    try:
        send_message(session.sock, msg)
    except OSError as exc:
        print(f"[ERROR] Failed to send job: {exc}")
        return

    print(f"[INFO] Job submitted. Results will auto-extract to: {out_dir}")


def handle_stats(session: Session) -> None:
    try:
        send_message(session.sock, build_stats_message(session.username))
    except OSError as exc:
        print(f"[ERROR] {exc}")


# ---------------------------------------------------------------------------
# Interactive REPL parser
# ---------------------------------------------------------------------------

def make_repl_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="", add_help=False, exit_on_error=False)
    sub    = parser.add_subparsers(dest="command")

    p_submit = sub.add_parser("submit", exit_on_error=False)
    p_submit.add_argument("--job",     required=True,
                          help="Path to the job directory")
    p_submit.add_argument("--exec",    required=True,
                          help="Entry-point script relative to the job directory root")
    p_submit.add_argument("--outputs", nargs="*", default=[],
                          help="Relative paths inside the job dir to retrieve on completion")
    p_submit.add_argument("--out-dir", dest="out_dir", default=None,
                          help="Local directory to auto-extract results into")

    sub.add_parser("stats", exit_on_error=False)
    sub.add_parser("quit",  exit_on_error=False)
    sub.add_parser("help",  exit_on_error=False)

    return parser


HELP_TEXT = """\
Commands:
  submit --job <dir> --exec <script> [--outputs <path> ...] [--out-dir <dir>]
      Zip <dir> and submit it. --exec is the entry-point script inside the dir.
      --outputs lists relative paths to retrieve when done (e.g. results/ logs/out.txt).
      Results are extracted automatically when the coordinator pushes them back.

  stats
      Query job stats from the coordinator.

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
            args = repl_parser.parse_args(line.split())
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
