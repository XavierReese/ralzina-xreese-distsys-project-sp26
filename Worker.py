"""
Worker.py  -  worker program for the distributed job coordinator.

Usage:
    python Worker.py <worker_name> <coord_name> <max_jobs>

The coordinator is discovered automatically via the ND catalog service.
Once connected, worker sends initial registration with stats and worker_name
and waits for acknowledgement from coordinator before continuien

Worker listens to coordinator for job requests and runs each job.

Design choices:
- Worker will not always need an ack. Ack is when worker initiates convesration and needs ack.
That is only needd during registration. After registration, coordinator contacts worker to
send job and needs an ack from worker. When worker finishes job it sends result to coordinator
and needs an ack. Since the ack is both ways, I decided to use a send_thread that sends messages.
The worker will put message_id's and specify if it needs an ack when putting in send queue, and
it will have a queue of internal acks with message id's that it's waiting for and if the worker
receives a message from coordinator of type ack, then it will check off that ack and proceed with
the final part of the operation.
"""

import http.client
import json
import time
import socket
import sys
import os
import shutil
import threading
import queue
import subprocess
import base64
import zipfile

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Worker constants
MAX_BACKOFF         = 64
BUFSIZ              = 4096
HEARTBEAT_INTERVAL  = 60
MAX_LOG_COUNT       = 100

# Catalog constant
CATALOG_URL         = "catalog.cse.nd.edu"
CATALOG_PORT        = 9097
COORDINATOR_TYPE    = "coordinator"          
COORDINATOR_PROJECT = "dist_job_coordinator"

# ---------------------------------------------------------------------------
# Worker class
# - Manages all functions to receive jbos and send results to coordinator
# ---------------------------------------------------------------------------

class Worker:
    # ---------------------------------------------------------------------------
    # Startup functions
    # - init:
    #   - create or find worker directory
    #   - load last checkpoint-log if it exists
    #   - connect to coordinator, send registration and wait for ack
    #   - startup heartbeat thread and sending thread
    # - connect_to_coordinator: restarts socket and tries to connect to coordinator with backoff
    # - find_coordinator: contacts name server to find coordinator and returns True if it connected, False if not
    # - register: specific function to send stats at the beginning of program.
    #   - I couldn't use the send_thread since the thread starts until after registration
    # ---------------------------------------------------------------------------
    def __init__(self, worker_name, coord_name, max_jobs):
        self.worker_name = worker_name
        self.worker_dir = f"{self.worker_name}_dir"
        self.worker_ckpt = f"{self.worker_name}.ckpt"
        self.worker_log = f"{self.worker_name}.txn"
        self.coord_name = coord_name

        self.main_sock = None
        self.main_lock = threading.Lock() # need a lock to share with heartbeat thread
        self.send_sock = None

        self.max_jobs = max_jobs
        self.running_jobs = []
        self.jobs_lock = threading.Lock()

        self.log_count = 0

        # Create worker directory
        os.makedirs(self.worker_dir, exist_ok=True)

        if os.path.exists(self.worker_ckpt):
            with open(self.worker_ckpt, "r") as ckpt:
                data = json.load(ckpt)

            for job, info in data.items():
                self.running_jobs[job] = info

        # Apply everything listed on log file
        if os.path.exists(self.worker_log):
            with open(self.worker_log, "r") as f:
                for line in f:
                    self.log_count += 1
                    if not line.strip():
                        continue

                    entry = json.loads(line)
                    status = entry["status"]
                    job = entry["job"]

                    if status == "scheduled":
                        self.running_jobs[job] = entry["info"]
                    elif status == "finished":
                        del self.running_jobs[job]

        # Create main and send socket and send registration for each
        # main socket
        self.start_sock(self.main_sock, "main_sock")
        
        # send socket
        self.start_sock(self.send_sock, "send_sock")

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
        heartbeat_thread.start()
        print("Worker heartbeat thread started")

        # Start message sender thread
        send_thread = threading.Thread(target=self.send_thread, daemon=True)
        send_thread.start()
        print("Worker send thread started")

        self.run()

    def start_sock(self, sock, sock_type):
        self.connect_to_coordinator(sock)

        # Retry until connected
        while True:
            # send registration
            while not self.register(sock, sock_type):
                self.connect_to_coordinator(sock)

            self.recv_ack(sock, sock_type)

            self.connect_to_coordinator(sock)

    def recv_ack(self, sock, sock_type):
        try:
            # Receive registration ack
            # If anything fails, restart connection to coordinator
            length_bytes = self.recv_exact(4, sock)
            resp_len = int.from_bytes(length_bytes, "big")

            resp_bytes = self.recv_exact(resp_len, sock)
                            
            try:
                response = json.loads(resp_bytes.decode("utf-8"))
                    
                if response["status"] == "failed":
                    print(f"{sock_type}: ack failed")
                else:
                    print(f"{sock_type}: ack to {self.coord_name} succeeded")
                    return True
            except json.JSONDecodeError:
                print(f"{sock_type}: Could not read ack from coordinator")

        except Exception as e:
            print(f"{sock_type}: Worker error when receiving ack: {e}")

        return False

    def connect_to_coordinator(self, sock):
        print("Attempting to connect to coordinator")

        # Connect to coordinator
        # After connecting, sock is the socket to talk to the coordinator
        backoff = 1
        while True:
            sock = self.find_coordinator() 
            if sock is not None:
                return
            if backoff >= MAX_BACKOFF:
                print(f"Max backoff reached: {MAX_BACKOFF}. Quitting...")
                sys.exit(1)

            print(f"Retrying in {backoff}s")
            time.sleep(backoff)
            backoff *= 2

    def find_coordinator(self):
        """
        Section 1:
        - Connect to catalog to get most recent registration of coordinator

        Section 2:
        - Attempt to connect to coordinator
        """

        # Section 1
        try:
            conn = http.client.HTTPConnection(CATALOG_URL, CATALOG_PORT)
            conn.request("GET", "/query.json")
            response = conn.getresponse()
            
        except Exception:
            print(f"Worker for {self.coord_name} Could not contact catalog")
            return None
        
        if response.status != 200:
            print(f"[Worker for {self.coord_name} Could not contact catalog: HTTP error {response.status}")
            return None
        
        data = response.read()
        json_string = data.decode()
        services = json.loads(json_string)

        conn.close()

        matching_services = [
            (s["name"], s["port"], s["lastheardfrom"], s["coord_name"])
            for s in services
            if ("type" in s and s["type"] == COORDINATOR_TYPE) and ("coord_name" in s and s["coord_name"] == self.coord_name)
        ]

        if matching_services:
            latest_service = max(matching_services, key=lambda x: x[2])
        else:
            print("Worker contacted name server but found no coordinator")
            return None

        # Section 2
        host, port = latest_service[0], latest_service[1]

        try:
            # Create socket
            new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Disable Nagle's Algorithm
            new_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            new_sock.settimeout(5)
                    
            new_sock.connect((host, port))

            return new_sock
            
        except Exception as e:
            print(f"Worker for {self.coord_name} socket creation failed: {e}")
            return None

    def register(self, sock, sock_type):
        response = {
            "sock_type": sock_type,
            "type": "register"
        }

        try:
            pre_response = json.dumps(response).encode("utf-8")
            response_length = len(pre_response).to_bytes(4, byteorder="big")
            final_response = response_length + pre_response
        except (TypeError, ValueError):
            print(f"{sock_type}: Couldn't serialize response to JSON")
            return False

        # Retry on failure
        try:
            sock.sendall(final_response)
            return True
        except (socket.error, BrokenPipeError) as e:
            print(f"{sock_type} Network Error: {e}")
                    
            return False
            
    # ---------------------------------------------------------------------------
    # Worker stats functions
    # - get stats: calls al other functions to get computation stats
    # - cpu_load: determines cpu load as a %, the lower the better
    # - free_main_mem_mb: mb of free RAM
    # - free_disk_mem_gb: gb of free disk
    # ---------------------------------------------------------------------------
    def get_stats(self):
        stats =  {
            "type": "heartbeat",
            "worker_name": self.worker_name,
            "cpu_load": self.cpu_load(),
            "free_main_mem_mb": self.free_main_mem_mb(),
            "free_disk_mem_gb": self.free_disk_mem_gb(),
            "available_jobs": self.max_jobs - len(self.running_jobs)
        }

        return stats

    def cpu_load(self):
        total_load_over_past_1min = os.getloadavg()[0]
        avg_load_per_cpu_over_past_1min = round((total_load_over_past_1min / os.cpu_count()) * 100, 1) # Round up to 1 decimal

        return avg_load_per_cpu_over_past_1min

    def free_main_mem_mb(self):
        mem_available_mb = 0
        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f:
                    if 'MemAvailable' in line:
                        kb = int(line.split()[1])
                        mem_available_mb = kb // 1024
                        break
        except FileNotFoundError:
            mem_available_mb = -1 # not on a Linux environment
        
        return mem_available_mb

    def free_disk_mem_gb(self):
        _, _, free_bytes = shutil.disk_usage("/") # returns total, used, free bytes of disk

        free_gb = free_bytes // (1024**3)

        return free_gb
    
    # ---------------------------------------------------------------------------
    # Main worker functions
    # - Handles messages from coordinator
    # - run: receive messages
    # - execute: process messages
    # ---------------------------------------------------------------------------
    def run(self):
        data = b""
        while True:
            # Get new message
            try:
                # Read message (blocks)
                buffer = self.main_sock.recv(BUFSIZ)
                data += buffer
            except ConnectionError:
                data = b""

            if not data:
                print("Coordinator broke connection. Attempting to reconnect...")
                self.connect_to_coordinator()

            # Need at least 4 bytes to know message length
            if len(data) < 4:
                continue

            # Read length if available
            message_len = int.from_bytes(data[:4], "big")

            # Check if full message available
            if len(data) < 4 + message_len:
                continue

            # Read full message
            message_bytes = data[4:4+message_len]

            # Remove processed bytes from buffer
            data = data[4 + message_len:]

            # Execute request
            self.execute(message_bytes)

    def execute(self, message_bytes):
        """
        Process a message

        Functions:
        - Receive job request from coordinator: process and send ack after
        - Receive request to stop job from coordinator: process and send ack after
        - Receive ack from coordinator: receive ack then resume final processing part
        """
        try:
            request = json.loads(message_bytes.decode("utf-8"))
        except (TypeError, ValueError):
            response = {
                "status": "invalid",
                "value": "Request is not valid JSON"
            }
            with self.main_lock:
                self.send_message(response)

        # validate fields
        if "method" not in request:
            response = {
                "status": "invalid",
                "message": "Missing method"
            }
            with self.main_lock:
                self.send_message(response)
            return
        
        # Perform operation
        match request["method"]:
            # Job request
            case "schedule":
                if self.invalid_args(["zip", "job_id", "command_file"], request):
                    return
                
                zip_data = base64.b64decode(request["zip"])
                task_dir = f"{self.worker_dir}/{request["job_id"]}"
                zip_path = f"{task_dir}/{request["job_id"]}.zip" # working environment zip file
                
                os.makedirs(task_dir, exist_ok=True)

                with open(zip_path, "wb") as f:
                    f.write(zip_data)

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(task_dir)

                bash_script = os.path.join(task_dir, request["command_file"])
                os.chmod(bash_script, 0o755)

                process = subprocess.Popen(
                    ["bash", bash_script],
                    cwd=task_dir
                )

                with self.jobs_lock:
                    self.running_jobs[request["job_id"]] = process 

                response = {
                    "type": "ack",
                    "status": "scheduled",
                    "job_id": request["job_id"]
                }
            
            # Stop job
            case "stop":
                if self.invalid_args(["job_id"], request):
                    return
                
                job_id = request["job_id"]

                with self.jobs_lock:
                    process = self.running_jobs[job_id]

                    print(f"Stopping job {job_id} from coordinator request")
                    
                    process.terminate()

                    del self.running_jobs[job_id]

                response = {
                    "type": "ack",
                    "status": "terminated",
                    "job_id": job_id
                }

            case _:
                response = {
                    "status": "invalid",
                    "message": "Invalid method requested"
                }
            
        with self.main_lock:
            self.send_message(response, self.main_sock)
        
    def invalid_args(self, args, request):
        for arg in args:
            if arg not in request:
                response = {
                    "status": "invalid",
                    "message": f"invalid, {arg} not present"
                }
                with self.main_lock:
                    self.send_message(response)
                return True
        return False

    # ---------------------------------------------------------------------------
    # Messaging Functions
    # - recv_exact: receive exact amount of bytes
    # - heartbeat: thread to send heartbeat to coordinator
    # - send_thread: thread that sends messages to coordinator from send_queue
    # ---------------------------------------------------------------------------
    def recv_exact(self, bytes_len, sock):
        data = b""
        while len(data) < bytes_len:
            chunk = sock.recv(bytes_len - len(data))
            if not chunk:
                raise Exception("Socket closed while receiving data")
            data += chunk
        return data
    
    def heartbeat(self):
        with self.main_lock:
            self.send_message(self.get_stats(), self.main_sock)

        time.sleep(HEARTBEAT_INTERVAL)

    def send_message(self, message, sock):
        # send response
        try:
            pre_response = json.dumps(message).encode("utf-8")
            response_length = len(pre_response).to_bytes(4, byteorder="big")
            final_response = response_length + pre_response
        except (TypeError, ValueError):
            print(f"Worker Error: Couldn't serialize response to JSON")
            return

        # Retry on failure
        while True:
            try:
                sock.sendall(final_response)
                break
            except (socket.error, BrokenPipeError) as e:
                print(f"Worker Network Error: {e}")
                
                self.start_sock(sock)
        
    def send_thread(self):
        """
        Send responses from queue
        """
        while True:
            time.sleep(0.5)

            with self.jobs_lock:
                check_jobs = self.running_jobs.copy()

                for job_id, job in check_jobs.items():
                    exit_code = job.poll()

                    if exit_code is not None:
                        stdout, stderr = job.communicate()

                        message = {
                            "type": "job output",
                            "job_id": job_id,
                            "stdout": stdout,
                            "stderr": stderr,
                            "exit_code": exit_code
                        }

                        # Send output
                        self.send_message(message, self.send_sock)

                        # Retry until we receive ack (idempotent)
                        while not self.recv_ack(self.send_sock, "send_sock"):
                            self.start_sock(self.send_sock, "send_sock")
                            self.send_message(message, self.send_sock)

                        # Remove job
                        with self.jobs_lock:
                            if job in self.running_jobs:
                                del self.running_jobs[job_id]

"""

Initialization:
Worker starts
Worker checks name server for coordinator
- No coordinator, just start retrying and print no coordinator found, retrying. Eventually after certain retries just quit

coordinator will register worker

worker immediately sends heartbeat after startup and after that it sends it again every 
HEARTBEAT_INTERVAL seconds

That way workers only send heartbeats based on when they registered so that coordinator is never 
overloaded

Client talks to Coordinator and sends executable
Coordinator talks to worker and sends executable
worker receives executable with bash script to run and runs it 
worker sends result to coordinator when done
"""

def main():
    if len(sys.argv) < 4:
        sys.exit(1)

    worker_name = sys.argv[1]
    coord_name = sys.argv[2]
    max_jobs = int(sys.argv[3])

    Worker(worker_name,coord_name,max_jobs)

if __name__ == "__main__":
    main()