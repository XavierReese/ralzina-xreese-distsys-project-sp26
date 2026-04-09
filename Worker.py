"""
Worker.py  -  worker program for the distributed job coordinator.

Usage:
    python Worker.py <worker_name> <max_jobs>

The coordinator is discovered automatically via the ND catalog service.
Once connected, worker creates two socks and registers each sock separately with coordinator
and waits for acknowledgement from coordinator before continuing

Worker listens to coordinator for job requests, sends ack, and processes them

Once jobs finish, worker contacts coordinator to notify of result (stdout & stderr) and waits for ack

Design choices:
- Two sockets: Since there's times when worker initiates communication (send job resulst)
and others when coordinator initiates communication (schedule new job), it's easier to
have one socket for each type. Req_sock handles requests from coordinator and sends ack when request is processed.
Res_sock handles sending job outputs to coordinator expecting an ack from coordinator.
- Each socket is handled in a separate thread
- The heartbeat thread shares the req_sock, so we must lock when using it to send only
"""

import http.client
import json
import time
import socket
import sys
import os
import shutil
import threading
import subprocess
import base64
import zipfile
import argparse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Worker constants
MAX_BACKOFF         = 64
BUFSIZ              = 4096
HEARTBEAT_INTERVAL  = 60
MAX_LOG_COUNT       = 100
MAX_JOBS            = 2

# Catalog constant
CATALOG_URL         = "catalog.cse.nd.edu"
CATALOG_PORT        = 9097
COORDINATOR_TYPE    = "coordinator"          
COORDINATOR_PROJECT = "dist_job_coordinator"

# ---------------------------------------------------------------------------
# Worker class
# - Manages all functions to receive jobs and send results to coordinator
# ---------------------------------------------------------------------------

class Worker:
    # ---------------------------------------------------------------------------
    # Startup functions
    # - init:
    #   - start with a fresh working directory
    #   - connect to coordinator, send registration and wait for ack
    #   - startup heartbeat thread and response thread
    # - start_sock: handles all the logic to create a new socket from scratch
    # - connect_to_coordinator: tries to connect to coordinator with backoff
    # - find_coordinator: contacts name server to find coordinator and returns True if it connected, False if not
    # - register: specific function to register a worker's socket
    # ---------------------------------------------------------------------------
    def __init__(self, worker_name, max_jobs=MAX_JOBS):
        self.worker_name = worker_name
        self.worker_dir = f"{self.worker_name}_dir"

        self.req_sock = None
        self.req_lock = threading.Lock() # need a lock to share with heartbeat thread
        self.res_sock = None

        self.max_jobs = max_jobs
        self.running_jobs = {}
        self.jobs_lock = threading.Lock()

        if os.path.exists(self.worker_dir):
            print(f"Detected existing workspace. Cleaning up old data...")
            # rm -rf
            shutil.rmtree(self.worker_dir)

        # Create worker directory
        os.makedirs(self.worker_dir, exist_ok=True)

        # Create main and send socket and send registration for each
        # main socket
        self.req_sock = self.start_sock("req_sock")
        
        # send socket
        self.res_sock = self.start_sock("res_sock")

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
        heartbeat_thread.start()
        print("Worker heartbeat thread started")

        # Start message sender thread
        res_thread = threading.Thread(target=self.res_thread, daemon=True)
        res_thread.start()
        print("Worker response thread started")

        self.run()

    def start_sock(self, sock_type):
        sock = self.connect_to_coordinator()

        # Retry until connected
        while True:
            # send registration
            while not self.register(sock, sock_type):
                sock = self.connect_to_coordinator()

            if self.recv_ack(sock, sock_type):
                return sock

            self.connect_to_coordinator(sock)

    def connect_to_coordinator(self):
        print("Attempting to connect to coordinator")

        # Connect to coordinator
        # After connecting, sock is the socket to talk to the coordinator
        backoff = 1
        while True:
            sock = self.find_coordinator() 
            if sock is not None:
                return sock
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
            print(f"Worker for {COORDINATOR_TYPE} Could not contact catalog")
            return None
        
        if response.status != 200:
            print(f"[Worker for {COORDINATOR_TYPE} Could not contact catalog: HTTP error {response.status}")
            return None
        
        data = response.read()
        json_string = data.decode()
        services = json.loads(json_string)

        conn.close()

        matching_services = [
            (s["name"], s["port"], s["lastheardfrom"])
            for s in services
            if ("type" in s and s["type"] == COORDINATOR_TYPE) and ("project" in s and s["project"] == COORDINATOR_PROJECT) and ("owner" in s and s["owner"] == "xreese")
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
            print(f"Worker for {COORDINATOR_TYPE} socket creation failed: {e}")
            return None

    def register(self, sock, sock_type):
        response = {
            "sock_type": sock_type,
            "method": "register",
            "type": "worker",
            "id": self.worker_name,
        }

        pre_response = json.dumps(response).encode("utf-8")
        response_length = len(pre_response).to_bytes(4, byteorder="big")
        final_response = response_length + pre_response

        # Return True if succeeded, False if failed
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
    # - run: receive messages
    # - execute: process messages
    # - invalid_args: check if any expected args are not present
    # ---------------------------------------------------------------------------
    def run(self):
        data = b""
        while True:
            # Get new message
            try:
                # Read message (blocks)
                buffer = self.req_sock.recv(BUFSIZ)
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
            with self.req_lock:
                self.send_message(response, self.req_sock)

        # validate fields
        if "method" not in request:
            response = {
                "status": "invalid",
                "message": "Missing method"
            }
            with self.req_lock:
                self.send_message(response, self.req_sock)
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
                
                try:
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
                except Exception as e:
                    response = {
                        "status": "error",
                        "message": f"Internal worker error: {e}"
                    }
            
            # Stop job
            case "stop":
                if self.invalid_args(["job_id"], request):
                    return
                
                job_id = request["job_id"]

                try:
                    with self.jobs_lock:
                        process = self.running_jobs[job_id]
                            
                        process.terminate()

                        print(f"Terminated job {job_id} from coordinator request")

                        del self.running_jobs[job_id]
                    
                    response = {
                        "status": "success",
                        "message": "terminated job",
                        "job_id": job_id
                    }
                except KeyError:
                    response = {
                        "status": "error",
                        "message": "job id not found",
                        "job_id": job_id
                    }

                except Exception as e:
                    response = {
                        "status": "error",
                        "message": f"Internal worker error: {e}",
                        "job_id": job_id
                    }

            case _:
                response = {
                    "status": "error",
                    "message": "Invalid method requested"
                }
            
        with self.req_lock:
            self.send_message(response, self.req_sock)
        
    def invalid_args(self, args, request):
        for arg in args:
            if arg not in request:
                response = {
                    "status": "invalid",
                    "message": f"invalid, {arg} not present"
                }
                with self.req_lock:
                    self.send_message(response, self.req_sock)
                return True
        return False

    # ---------------------------------------------------------------------------
    # Messaging Functions
    # - recv_exact: receive exact amount of bytes
    # - recv_ack: receive acknowlegement from coordinator
    # - heartbeat: thread to send heartbeat to coordinator
    # - send_message: send message to coordinator
    # - res_thread: thread that notifies coordinator of finished jobs
    # ---------------------------------------------------------------------------
    def recv_exact(self, bytes_len, sock):
        data = b""
        while len(data) < bytes_len:
            chunk = sock.recv(bytes_len - len(data))
            if not chunk:
                raise Exception("Socket closed while receiving data")
            data += chunk
        return data
    
    def recv_ack(self, sock, sock_type):
        try:
            length_bytes = self.recv_exact(4, sock)
            resp_len = int.from_bytes(length_bytes, "big")

            resp_bytes = self.recv_exact(resp_len, sock)
                            
            try:
                response = json.loads(resp_bytes.decode("utf-8"))
                    
                if response["status"] == "failed":
                    print(f"{sock_type}: ack failed")
                    return False
                else:
                    print(f"{sock_type}: ack to {COORDINATOR_TYPE} succeeded")
                    return True
            except json.JSONDecodeError:
                print(f"{sock_type}: Could not read ack from coordinator")
                return False

        except Exception as e:
            print(f"{sock_type}: Worker error when receiving ack: {e}")

        return False
    
    def heartbeat(self):
        with self.req_lock:
            self.send_message(self.get_stats(), self.req_sock)

        time.sleep(HEARTBEAT_INTERVAL)

    def send_message(self, message, sock):
        # send response
        pre_response = json.dumps(message).encode("utf-8")
        response_length = len(pre_response).to_bytes(4, byteorder="big")
        final_response = response_length + pre_response    

        # Retry on failure
        while True:
            try:
                sock.sendall(final_response)
                break
            except (socket.error, BrokenPipeError) as e:
                print(f"Worker Network Error: {e}")
                
                self.start_sock(sock)
        
    def res_thread(self):
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
                        self.send_message(message, self.res_sock)

                        # Retry until we receive ack (idempotent)
                        while not self.recv_ack(self.res_sock, "res_sock"):
                            self.start_sock(self.res_sock, "res_sock")
                            self.send_message(message, self.res_sock)

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

Client talks to Coordinator and sends zipped working directory
Coordinator talks to worker and sends zipped working directory
worker receives zipped working directory and unzips it. Then runs bash script 
worker sends result to coordinator when done
"""

def main():
    parser = argparse.ArgumentParser(description="Distributed job worker")
    parser.add_argument("--worker", required=True, type=str, help="Worker name")
    parser.add_argument("--max_jobs", type=int, default=MAX_JOBS, help=f"Max jobs worker can hold (default: {MAX_JOBS})")
    args = parser.parse_args()

    Worker(args.worker,args.max_jobs)

if __name__ == "__main__":
    main()
