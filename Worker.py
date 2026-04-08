"""
Worker.py  -  worker program for the distributed job coordinator.

Usage:
    python Worker.py <worker_name> <coord_name> <max_jobs>

The coordinator is discovered automatically via the ND catalog service.
Once connected, worker sends initial registration with stats and worker_name
and waits for acknowledgement from coordinator before continuien

Worker listens to coordinator for job requests and runs each job.
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Worker constants
MAX_BACKOFF = 64
BUFSIZ = 4096

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
        self.coord_name = coord_name
        self.coord_sock = None
        self.max_jobs = max_jobs
        self.running_jobs = {}
        self.log_count = 0
        self.registered = False
        self.send_lock = threading.Lock()
        self.send_queue = queue.Queue()

        # Create worker directory
        os.makedirs(f"{self.worker_name}", exist_ok=True)

        if os.path.exists(f"{self.worker_name}.ckpt"):
            with open(f"{self.worker_name}.ckpt", "r") as ckpt:
                data = json.load(ckpt)

            for job, info in data.items():
                self.running_jobs[job] = info

        # Apply everything listed on log file
        if os.path.exists(f"{self.worker_name}.txn"):
            with open(f"{self.worker_name}.txn", "r") as f:
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

        # Connect to coordinator and send computation stats
        # Only send stats on startup, and after that start heartbeat thread
        # This assumes that the coordinator will make sure to always remember the 
        # past workers if there are any crashes
        # If all workers would sent stats when coordinator came back up, the network
        # could get overloaded

        self.connect_to_coordinator()

        # Retry until connected
        while True:
            # send heartbeat as registration
            while not self.register():
                self.connect_to_coordinator()

            try:
                # Receive registration ack
                # If anything fails, restart connection to coordinator
                length_bytes = self.recv_exact(4)
                resp_len = int.from_bytes(length_bytes, "big")

                resp_bytes = self.recv_exact(resp_len)
                            
                try:
                    response = json.loads(resp_bytes.decode("utf-8"))
                    
                    if response["status"] == "failed":
                        print("Registration failed")
                    else:
                        print(f"Registration with {self.coord_name} succeeded")
                        self.registered = True
                        break
                except json.JSONDecodeError:
                    print("Could not read response from server")

            except Exception as e:
                print(f"Worker error when registering: {e}")

            self.connect_to_coordinator

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
        heartbeat_thread.start()
        print("Worker heartbeat started")

        # Start message sender thread
        send_thread = threading.Thread(target=self.send_thread, daemon=True)
        send_thread.start()
        print("Worker send thread started")

        self.run()

    def connect_to_coordinator(self):
        print("Attempting to connect to coordinator")

        with self.send_lock:
            if self.coord_sock: 
                self.coord_sock.close()
            self.coord_sock = None

        # Connect to coordinator
        # After connecting, coord_sock is the socket to talk to the coordinator
        backoff = 1
        while not self.find_coordinator():
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
            return False
        
        if response.status != 200:
            print(f"[Worker for {self.coord_name} Could not contact catalog: HTTP error {response.status}")
            return False
        
        data = response.read()
        json_string = data.decode()
        services = json.loads(json_string)

        conn.close()

        matching_services = [
            (s["name"], s["port"], s["lastheardfrom"], s["coord_name"])
            for s in services
            if ("type" in s and s["type"] == "coordinator") and ("coord_name" in s and s["coord_name"] == self.coord_name)
        ]

        if matching_services:
            latest_service = max(matching_services, key=lambda x: x[2])
        else:
            print("Worker contacted name server but found no coordinator")
            return False

        # Section 2
        host, port = latest_service[0], latest_service[1]

        try:
            # Create socket
            new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Disable Nagle's Algorithm
            new_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            new_sock.settimeout(5)
                    
            new_sock.connect((host, port))

            with self.send_lock:
                self.coord_sock = new_sock

            print(f"Worker for {self.coord_name} Connected")

            return True
            
        except Exception as e:
            print(f"Worker for {self.coord_name} socket creation failed: {e}")
            with self.send_lock:
                if self.coord_sock: 
                    self.coord_sock.close()
                self.coord_sock = None
            return False

    def register(self):
        response = self.get_stats()

        try:
            pre_response = json.dumps(response).encode("utf-8")
            response_length = len(pre_response).to_bytes(4, byteorder="big")
            final_response = response_length + pre_response
        except (TypeError, ValueError):
            print(f"Worker Error: Couldn't serialize response to JSON")
            return False

        # Retry on failure
        while True:
            current_sock = self.coord_sock
                    
            if current_sock is None:
                print("Worker: Socket is None, reconnecting...")
                return False

            try:
                self.coord_sock.sendall(final_response)
                return True
            except (socket.error, BrokenPipeError) as e:
                print(f"Worker Network Error: {e}")
                    
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
            "type": "heartbeat" if self.registered else "register",
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
                # Read message
                buffer = self.coord_sock.recv(BUFSIZ)
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
        - Receive job request from coordinator
        - Receive request to stop job from coordinator
        """
        try:
            request = json.loads(message_bytes.decode("utf-8"))
            
            # Actual implementation
        
        except (TypeError, ValueError):
            response = {
                "status": "invalid",
                "value": "Request is not valid JSON"
            }
            self.send_queue.put(response)

    # ---------------------------------------------------------------------------
    # Messaging Functions
    # - recv_exact: receive exact amount of bytes
    # - heartbeat: thread to send heartbeat to coordinator
    # - send_thread: thread that sends messages to coordinator from send_queue
    # ---------------------------------------------------------------------------
    def recv_exact(self, bytes_len):
        data = b""
        while len(data) < bytes_len:
            chunk = self.coord_sock.recv(bytes_len - len(data))
            if not chunk:
                raise Exception("Socket closed while receiving data")
            data += chunk
        return data

    def heartbeat(self):
        while True:
            self.send_queue.put(self.get_stats())
            time.sleep(60)
        
    def send_thread(self):
        """
        Send responses from queue
        """
        while True:
            response = self.send_queue.get()

            try:
                pre_response = json.dumps(response).encode("utf-8")
                response_length = len(pre_response).to_bytes(4, byteorder="big")
                final_response = response_length + pre_response
            except (TypeError, ValueError):
                print(f"Worker Error: Couldn't serialize response to JSON")
                continue

            # Retry on failure
            while True:
                # We ONLY lock during the attempt to use the socket
                with self.send_lock:
                    current_sock = self.coord_sock
                    
                if current_sock is None:
                    print("Worker: Socket is None, reconnecting...")
                    self.connect_to_coordinator()
                    continue

                try:
                    # Use the lock ONLY for the physical send
                    with self.send_lock:
                        self.coord_sock.sendall(final_response)
                    break
                except (socket.error, BrokenPipeError) as e:
                    print(f"Worker Network Error: {e}")
                    
                    self.connect_to_coordinator()

"""

Initialization:
Worker starts
Worker checks name server for coordinator
- No coordinator, just start retrying and print no coordinator found, retrying. Eventually after certain retries just quit

coordinator will register worker

coordinator then polls workers to see their availability
- coordinator knows worker is emptuy 
- does coordinator have to ask every single worker?
- should it just ask once?

Client talks to Coordinator and sends executable
Coordinator talks to worker 

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