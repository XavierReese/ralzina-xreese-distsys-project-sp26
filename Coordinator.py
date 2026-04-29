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
import uuid
import traceback

COORDINATOR_TYPE = "coordinator"
COORDINATOR_PROJECT = "dist_job_coordinator"

CATALOG_HOST = "catalog.cse.nd.edu"
CATALOG_PORT = 9097

BUFSIZ = 4096
MAX_BACKOFF = 64

MAX_LOG = 100

# -------------------
# Coordinator
# - manages state of jobs and connections
# -------------------

class Coordinator:
    def __init__(self, port=0, coord_name="coordinator"):

        self.coord_name = coord_name
        self.port = port

        # --- State of Clients, Workers, and Jobs ---
        self.clients = {} 
        self.client_output_queue = deque() # Keep track if we must send any output to client

        self.contact_workers = {} # worker_fd -> socket_type, worker_id for epoll
        self.workers = {} # worker_id -> stats for update

        self.jobs = {} 
        self.job_queue = deque()

        """
         self.clients = {
            "client_id": {
                "pending_results": [job_id list],
                "finished_results": [job_id list],
                "fileno": fileno from socket for epoll
            }
        }

        self.workers = {
            "worker_id": {
                "fileno": fileno,
                "cpu load and ther stats": stats,
                "running_jobs": [job_id list],
                "max_jobs"
            }
        }

        self.jobs[job_id] = {
            "client_id": "client",
            "worker_id": worker_id,     # or None if not scheduled / rescheduled,
            "status": "running",
            "script": "start.sh"        # script to run program
            "name": "my cool job"       # submitted by client
        }
        """

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
        self.connections = {}
        self.epoll = select.epoll()

        # Catalog Update Heartbeat
        threading.Thread(target=self.update, args=(self.port,), daemon=True).start()

        self.run()

    # Rene, trying epoll
    # All the following functions are mine up to def update (not invlusive so def update is not mine)
    def run(self):
        epoll = self.epoll

        self.server_sock.setblocking(False)
        epoll.register(self.server_sock.fileno(), select.EPOLLIN)

        connections = self.connections

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
                            "sock_type": None,      # worker uses two sockets, this tells you what socket it is, client leaves this as None
                            "id": None,             # either client_id or worker_id
                            "type": None,           # either "worker" or "client"
                            "recv_buffer": b"",
                            "send_queue": deque(),  # queue of messages to be sent
                            "sent_bytes": 0         # Track progress of current message being sent
                        }

                    elif event & select.EPOLLIN:
                        connection = connections[fileno]
                        client_socket = connection["socket"]

                        self.read_buffer(connection)
                        buffer = connection["recv_buffer"]

                        if not buffer:
                            print(f"[COORD] {connection["type"]}_{connection["id"]} disconnected")

                            if connection["type"] == "worker" and connection["id"] in self.workers:
                                for job_id in self.workers[connection["id"]]["running_jobs"] :
                                    self.job_queue.append(job_id)

                                del self.workers[connection["id"]]

                                self.clear_job_queue()
                            elif connection["type"] == "client" and connection["id"] in self.clients:
                                self.clients[connection["id"]]["fileno"] = None

                            epoll.unregister(fileno)
                            del connections[fileno]
                            client_socket.close()
                            continue
                            
                        # If connection is new, it will send its information
                        # If it's not then it will send a normal request
                        # Normal requests don't return anything in handle_request
                        # registration requests return the worker/client data in handle_request
                        if connections[fileno]["id"] == None:
                            response = self.handle_request(connections[fileno], fileno)

                            if response == None:
                                continue

                            id, sock_type, type, max_jobs = response

                            if id != None:
                                connections[fileno]["id"] = id
                                connections[fileno]["type"] = type
                                connections[fileno]["sock_type"] = sock_type
                                print(f"[{id}] Registered {type}")
                                if type == "worker":
                                    self.workers[id] = {
                                        "fileno": fileno,
                                        "running_jobs": set(),
                                        "max_jobs": max_jobs
                                    }
                                elif type == "client":
                                    if id not in self.clients:
                                        self.clients[id] = {
                                            "pending_results": set(),
                                            "finished_results": set(),
                                            "fileno": fileno
                                        }
                                    else:
                                        self.clients[id]["fileno"] = fileno

                                        # Send pending results
                                        for job_id in self.clients[id]["finished_results"]:
                                            encoded_bytes = self.zip_to_encoded_bytes(job_id)   

                                            if not encoded_bytes:
                                                continue

                                            name = self.jobs[job_id]["name"]

                                            result = {
                                                "tag": "output",
                                                "name": name,
                                                "job_id": job_id,
                                                "zip_bytes": encoded_bytes,
                                                "status": "ok"
                                            }

                                            print(f"[{id}] Client {id} reconnected, sending output")
                                            self.schedule_response(result, connections[fileno], fileno)
                            else:
                                print("[ERROR] Error registering socket")
                                epoll.unregister(fileno)
                                del connections[fileno]
                                client_socket.close()

                        else:
                            self.handle_request(connections[fileno], fileno)

                    elif event & select.EPOLLOUT:
                        connection = connections[fileno]
                        client_socket = connection["socket"]

                        # Connection failed
                        if not self.send_response(connection):
                            # TODO
                            # If it was a worker, reschedule all jobs
                            # If it was a client AND it's a program output, append to client send queue

                            type = connection["type"]

                            if type == "client":
                                self.clients[connection["id"]]["fileno"] = None

                                failed_msg = connection["send_queue"].popleft()

                                if "method" in failed_msg and failed_msg["method"] == "output":
                                    if "job_id" in failed_msg:
                                        self.clients[connection["id"]]["finished_results"].add(failed_msg["job_id"])
                                    else:
                                        print(f"[ERROR] Tried to add message with output to back to client {connection["id"]} finished results but had no job_id: {failed_msg}")
                            elif type == "worker":
                                running_jobs = self.workers[connection["id"]]["running_jobs"]

                                del self.workers[connection["id"]]

                                for job_id in running_jobs:
                                    client_id = self.jobs[job_id]["client_id"]
                                    script = self.jobs[job_id]["script"]

                                    self.schedule_job(client_id, script, job_id)

                            print(f"[{connection["id"]}] disconnected")
                            epoll.unregister(fileno)
                            del connections[fileno]
                            client_socket.close()
                            continue
        except Exception as e:
            print(f"[ERROR] coordinator crashed:",e)
            traceback.print_exc()
            exit(1)

    def zip_to_encoded_bytes(self, job_id):
        zip_path = os.path.join(self.jobs_dir, f"{job_id}_output.zip")
        
        try:
            with open(zip_path, "rb") as f:
                zip_bytes = f.read()
        except FileNotFoundError:
            return None
        except Exception as e:
            return None
                        
        return base64.b64encode(zip_bytes).decode('utf-8')
    
    def encoded_bytes_to_zip(self, bytes, job_id):
        zip_path = os.path.join(self.jobs_dir, f"{job_id}_output.zip")
        encoded_zip = bytes
        zip_bytes = base64.b64decode(encoded_zip)

        try:
            with open(zip_path, "wb") as f:
                f.write(zip_bytes)
            print(f"[ZIP] Saved zip file to {zip_path}")
            return True
        except Exception as e:
            print(f"[ZIP] Failed to write zip file {zip_path}: {e}")
            return False

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

        # Need at least 4 bytes to know message length
        if len(buffer) < 4:
            print('[DEBUG] coord handle_request: not enough for msg length')
            return None

        # Read length if available
        message_len = int.from_bytes(buffer[:4], "big")
                
        # Need at least 4 bytes to know message length
        if len(buffer) < 4:            
            return None

        # Read length if available
        message_len = int.from_bytes(buffer[:4], "big")

        # Check if full message arrived:
        if len(buffer) < 4 + message_len:
            return None

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
            job_id = self.job_queue.popleft()
            client_id = self.jobs[job_id]["client_id"]
            script = self.jobs[job_id]["script"]
            self.schedule_job(client_id, script, job_id)
            i += 1
        
    def clear_output_queue(self):
        i = 0
        n = len(self.client_output_queue)
        while i < n:
            self.send_output(self.client_output_queue.popleft())
            i += 1
    
    def execute(self, message_bytes, connection, fileno):

        try:
            request = json.loads(message_bytes.decode("utf-8"))
        
        except (TypeError, ValueError):
            response = {
                "status": "error",
                "message": "Request is not valid JSON"
            }
            self.schedule_response(response, connection, fileno)
            return 
        
        # Check if it's an error message
        if "status" in request and request["status"] == "error":
            if "message" not in request:
                response = {
                    "status": "error",
                    "message": "Error message didn't include a message"
                }
                self.schedule_response(response, connection, fileno)
            
            print(f"[ERROR] Received error from {connection["type"]} {connection["id"]}: {request["message"]}")

        type = request.get("type", connection["type"])

        # Perform operation
        match type:
            case "worker":
                match request["method"]:
                    case "register":
                        if self.invalid_args(["id", "sock_type", "max_jobs"], request, connection, fileno):
                            return
                        
                        id = request["id"]
                        sock_type = request["sock_type"]
                        max_jobs = request["max_jobs"]

                        response = {
                            "status": "ok",
                            "tag": "register",
                            "message": "Registration successful!"
                        }

                        self.schedule_response(response, connection, fileno)
                        
                        return id, sock_type, type, max_jobs

                    case "update":
                        if self.invalid_args(["id", "cpu_load", "free_main_mem_mb", "free_disk_mem_gb"], request, connection, fileno):
                            return
                        
                        id = request["id"]
                        cpu_load = request["cpu_load"]
                        free_main_mem_mb = request["free_main_mem_mb"]
                        free_disk_mem_gb = request["free_disk_mem_gb"]

                        self.workers[id]["cpu_load"] = cpu_load
                        self.workers[id]["free_main_mem_mb"] = free_main_mem_mb
                        self.workers[id]["free_disk_mem_gb"] = free_disk_mem_gb

                        print(f"[{id}] Received update from {connection["type"]} {id}")

                        if len(self.job_queue) > 0:
                            print(f"[{id}] Sending Jobs to new Worker {id}")
                            self.clear_job_queue()

                    case "ack":
                        if self.invalid_args(["ack_type", "status", "job_id"], request, connection, fileno):
                            return

                        job_id = request.get("job_id")
                        worker_id = connection["id"]
                            
                        if request["ack_type"] == "schedule":
                            if request["status"] == "ok":
                                self.jobs[job_id]["status"] = "running"
                                self.workers[worker_id]["running_jobs"].add(job_id)
                                print(f"[{worker_id}] Received ack of scheduled job {job_id}")
                            else:
                                job = self.jobs[job_id]
                                job["worker_id"] = None
                                self.schedule_job(job["client_id"], job["script"], job_id)
                        
                        elif request["ack_type"] == "stop":
                            if request["status"] != "ok":
                                request = {
                                    "method": "stop",
                                    "job_id": job_id
                                }

                                self.schedule_response(request, connection, fileno)

                        else:
                            response = {
                                "status": "error",
                                "message":  "Received unexpected ack"
                            }
                            self.schedule_response(response, connection, fileno)

                    case "output": # from worker
                        if self.invalid_args(["zip_bytes", "job_id"], request, connection, fileno):
                            return

                        print(f"[{request["id"]}] Received output from {request["id"]} for job {request["job_id"]}")

                        job_id = request["job_id"]
                        worker_id = connection["id"]

                        if job_id not in self.workers[worker_id]["running_jobs"]:
                            response = {
                                "status": "error",
                                "message": f"Job_id {job_id} was not scheduled in worker {worker_id}"
                            }
                            self.schedule_response(response, connection, fileno)
                            print(f"[ERROR] {job_id} wasn't scheduled in worker {worker_id}")
                            return

                        # bytes to zip
                        if not self.encoded_bytes_to_zip(request["zip_bytes"], job_id):
                            response = {
                                "status": "error",
                                "message": f"failed to save {job_id} output"
                            }
                            print(f"[ERROR] Failed to save {job_id} output to disk")
                            self.schedule_response(response, connection, fileno)
                            return

                        self.workers[worker_id]["running_jobs"].remove(job_id)
                        self._write_txn("finished", job_id)

                        print("[COORD] Saved output to disk")
                        
                        response = {
                            "status": "ok",
                            "tag": "output",
                            "job_id": job_id,
                            "message": f"Saved {job_id} output"
                        }
                        self.schedule_response(response, connection, fileno)
                        
                        # Send output to client
                        self.send_output(job_id)

                        self.clear_job_queue()

                        return

                    case _:
                        response = {
                            "status": "error",
                            "message":  "Invalid method requested"
                        }
                        self.schedule_response(response, connection, fileno)

            case "client":
                # Handle each possible client operation
                match request["method"]:
                    case "stop":
                        if self.invalid_args(["job_id"], request, connection, fileno):
                            return
                        
                        job_id = request["job_id"]
                        worker_id = self.jobs[job_id]["worker_id"]
                        client_id = self.jobs[job_id]["client_id"]

                        self.clients[client_id]["pending_results"].remove(job_id)
                        self.workers[worker_id]["running_jobs"].remove(job_id)

                        del self.jobs[job_id]

                        response = {
                            "status": "ok",
                            "tag": "stop_ack",
                            "job_id": job_id,
                            "message":  f"Job {job_id} has been stopped"
                        }

                        self.schedule_response(response, connection, fileno)

                        request = {
                            "method": "stop",
                            "job_id": job_id
                        }

                        worker_fd = self.workers[worker_id]["fileno"]

                        self.schedule_response(request, self.connections[worker_fd], worker_fd)


                        

                    case "register":
                        if self.invalid_args(["client_id"], request, connection, fileno):
                            return

                        id = request["client_id"]
                        sock_type = None

                        response = {
                            "status": "ok",
                            "tag": "register",
                            "message": f"Registered as {id}"
                        }

                        print(f'[{id}] client {id} joined')

                        self.schedule_response(response, connection, fileno)

                        return id, None, type, None

                    case "ack": # client

                        client_id = connection["id"]
                        if self.invalid_args(["ack_type", "status", "job_id"], request, connection, fileno):
                            return
                            
                        job_id = request["job_id"]
                            
                        if request["ack_type"] == "output":
                            if request["status"] != "ok":
                                self.send_output(job_id)
                                
                            else:
                                client_id = connection["id"]
                                print(f"[{client_id}] Received acknowledgement of output received")
                                self.clients[client_id]["finished_results"].remove(job_id)
                                del self.jobs[job_id] 
                                self._write_txn("output", job_id)

                    case "stats":
                        if self.invalid_args(["client_id"], request, connection, fileno):
                            return

                        client_id = request["client_id"]

                        jobs = []

                        for job_id in self.clients[client_id]["pending_results"]:
                            status = self.jobs[job_id]["status"]
                            name = self.jobs[job_id]["name"]
                            jobs.append([name, status, job_id])

                        response = {
                            "status": "ok",
                            "tag": "stats",
                            "message": jobs
                        }
                        self.schedule_response(response, connection, fileno)
                        return

                    case "submit":
                        if self.invalid_args(["zip_file", "script", "name", "client_id"], request, connection, fileno):
                            return

                        job_id = str(uuid.uuid4())

                        client_id = request["client_id"]

                        zip_path = os.path.join(self.jobs_dir, f"{job_id}_input.zip")

                        encoded_zip = request["zip_file"]
                        zip_bytes = base64.b64decode(encoded_zip)

                        name = request["name"]
                        script = request["script"]

                        try:
                            with open(zip_path, "wb") as f:
                                f.write(zip_bytes)

                            self.clients[client_id]["pending_results"].add(job_id)
                            self.jobs[job_id] = {
                                "client_id": client_id,
                                "status": "not_started",
                                "script": script,
                                "name": name
                            }

                            self._write_txn("submitted", job_id, self.jobs[job_id])

                            print(f"[ZIP] Saved zip file to {zip_path}")
                            response = {
                                "status": "ok",
                                "tag": "submit",
                                "job_id": job_id,
                                "name": name,
                                "message":  f"Job request {name} received and started"
                            }
                            self.schedule_response(response, connection, fileno)

                            # contact a worker

                            self.schedule_job(client_id, script, job_id)
                        except Exception as e:
                            print(f"[ERROR] Failed to write zip file {zip_path}: {e}")

                            response = {
                                "status": "error",
                                "message":  f"Failed to process submit request"
                            }
                            self.schedule_response(response, connection, fileno)

                    case _:
                        response = {
                            "status": "error",
                            "message":  "Invalid method requested"
                        }
                        self.schedule_response(response, connection, fileno)
            case _:
                response = {
                    "status": "error",
                    "message": "Received unidentified connection"
                }
                print(type)
                exit()
                self.schedule_response(response, connection, fileno)


    def schedule_job(self, client_id, script, job_id=None):
        # Logic to select which worker to run

        if not job_id:
            job_id = str(uuid.uuid4())

        zip_path = os.path.join(self.jobs_dir, f"{job_id}_input.zip")

        worker_id = self.select_worker()

        if worker_id == None:
            self.job_queue.append(job_id)
            return 
        
        print(f"[{worker_id}] Trying to schedule {job_id} in {worker_id}")

        try:
            worker_fd = self.workers[worker_id]["fileno"]
                                    
            self.jobs[job_id]["worker_id"] = worker_id

            try:
                with open(zip_path, "rb") as f:
                    zip_bytes = f.read()
            except FileNotFoundError:
                print(f"[ERROR] The file at {zip_path} was not found.")
                return 
            except Exception as e:
                print(f"[ERROR] An unexpected error occurred when scheduling {client_id}_{job_id}: {e}")
                return 
            
            encoded_bytes = base64.b64encode(zip_bytes).decode('utf-8')

            request = {
                "method": "schedule",
                "zip_bytes": encoded_bytes,
                "job_id": job_id,
                "script": script
            }

            self.workers[worker_id]["running_jobs"].add(job_id)

            print(f"[{worker_id}] Scheduling job with worker")
            self.schedule_response(request, self.connections[worker_fd], worker_fd)

            return
                
        except Exception as e:
            print(f"[ERROR] Worker {worker_id} failed: [{e}], trying another worker")

        print(f"[ERROR] All workers failed or no workers active")
        self.job_queue.append(job_id)

    def select_worker(self):
        if not self.workers:
            print("[COORD] No workers available to run a job")
            return None
        
        eligible_ids = [
            wid for wid, stats in self.workers.items() 
            if "cpu_load" in stats and "free_main_mem_mb" in stats and "free_disk_mem_gb" in stats and self.available_jobs(wid) > 0
        ]

        if not eligible_ids:
            print("No workers available to run a job")
            return None

        def worker_score(worker_id):
            stats = self.workers[worker_id]

            return (
                self.available_jobs(worker_id),     # Priority 1: Highest available slots
                -stats["cpu_load"],                 # Priority 2: Lowest CPU % (tie-breaker)
                stats["free_main_mem_mb"],          # Priority 3: Highest RAM (tie-breaker)
                stats["free_disk_mem_gb"]           # Priority 4: Highest Disk (tie-breaker)
            )
        
        return max(eligible_ids, key=worker_score)
    
    def available_jobs(self, worker_id):
        return self.workers[worker_id]["max_jobs"] - len(self.workers[worker_id]["running_jobs"])

    def invalid_args(self, args, request, connection, fileno):
        for arg in args:
            if arg not in request:
                response = {
                    "status": "error",
                    "message": f"Arg {arg} not present in request"
                }
                self.schedule_response(response, connection, fileno)
                return True
        return False

    def error_res(self, err_message: str, connection, fileno):
        ''' Schedule an error response '''
        response = {
            "tag": "ERROR",
            "status": "invalid",
            "message": err_message
        }
        self.schedule_response(response, connection, fileno)
    
    def error_res(self, err_message: str, tag: str, connection, fileno):
        ''' Schedule an error response '''
        response = {
            "status": "error",
            "message": err_message
        }
        self.schedule_response(response, connection, fileno)
    
    def schedule_response(self, response, connection, fileno):
        try:
            pre_response = json.dumps(response).encode("utf-8")
            response_length = len(pre_response).to_bytes(4, byteorder="big")
            final_response = response_length + pre_response
        except (TypeError, ValueError):
            print(f"[ERROR] Error: Couldn't serialize response to JSON")
    
        connection["send_queue"].append(final_response)
        self.epoll.modify(fileno, select.EPOLLIN | select.EPOLLOUT)

    def send_response(self, connection):
        queue = connection["send_queue"]
        if not queue:
            return True

        client_socket = connection["socket"]
        curr_msg = queue[0]
        offset = connection["sent_bytes"]

        try:
            sent = client_socket.send(curr_msg[offset:])
            connection["sent_bytes"] += sent

            if connection["sent_bytes"] == len(curr_msg):
                queue.popleft()
                connection["sent_bytes"] = 0

            return True
        except BlockingIOError:
            return True # try again next EPOLLOUT

        except socket.error as e:
            print(f"[ERROR] Network Error: Failed to send response: {e}")
            return False # network error, client disconnected
        
    def send_output(self, job_id):
        encoded_bytes = self.zip_to_encoded_bytes(job_id)

        name = self.jobs[job_id]["name"]

        client_id = self.jobs[job_id]["client_id"]
        client_fd = self.clients[client_id]["fileno"]

        print(f"[COORD] Trying to send output to {client_id} for {name}--{job_id}")

        if not encoded_bytes:
            print(f"[ERROR] Error when reading zip file for {job_id}.")

            request = {
                "status": "error",
                "message": f"Error when processing job {name}--{job_id}, please resubmit."
            }

            del self.jobs[job_id]

            if job_id in self.clients[client_id]["finished_results"]:
                self.clients[client_id]["finished_results"].remove(job_id)
            elif job_id in self.clients[client_id]["pending_results"]:
                self.clients[client_id]["pending_results"].remove(job_id)

        else:
            request = {
                "tag": "output",
                "status": "ok",
                "zip_bytes": encoded_bytes,
                "name": name,
                "job_id": job_id
            }

            self.jobs[job_id]["status"] = "finished"

            if job_id not in self.clients[client_id]["finished_results"]:
                self.clients[client_id]["finished_results"].add(job_id)
            if job_id in self.clients[client_id]["pending_results"]:
                self.clients[client_id]["pending_results"].remove(job_id)

        if client_fd in self.connections:
            print(f"[{self.connections[client_fd]["id"]}] Scheduling job output response for {name}")
            self.schedule_response(request, self.connections[client_fd], client_fd)
                
        else:
            print(f"[{client_id}] Client not connected, saving output for later")

    # -----------------------------------
    # State Updating Functions
    # - standardize adding/removing jobs, etc
    # -----------------------------------

    def incoming_job(self, client_id, script, outputs, dir_zip):
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            "client_id": client_id,
            "worker_id": None,     # or None if not scheduled / rescheduled,
            "status": "not_started",
            "script": script,        # script to run program
            "output_files": outputs,
            "zip": dir_zip,
            "result": None          # store stdout, stderr
        }
        
        return job_id

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
                print(f"[ERROR] Failed to send update: {e}")

            catalog_s.close()
            time.sleep(60)

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
                self.jobs        = ckpt

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

        # --- Re-fill Job Queue + Clients ---
        # Note: workers must reconnect
        self.workers = {}

        # Note: clients are found from jobs
        self.clients = {}

        for job_id, job in self.jobs.items():
            client_id = job["client_id"]
            
            if client_id not in self.clients:
                self.clients[client_id] = {
                    "pending_results": set(),
                    "finished_results": set(),
                }

            if job["status"] == "finished":
                self.clients[client_id]["finished_results"].add(job_id)
                self.client_output_queue.append(job_id)
            else:
                self.job_queue.append(job_id)
                self.clients[client_id]["pending_results"].add(job_id)

        self.clear_output_queue()

    def _apply_txn(self, entry: dict) -> None:
        """
        Apply a single transaction log entry to in-memory state.
        
        entry = {
            "event": event,
            "job_id": job_id,
            "job": job dict # optional
        }
        """

        event  = entry.get("event")
        job_id = entry.get("job_id")
 
        if event == "submitted" and "job" in entry: # job received from client
            job = entry["job"]
            self.jobs[job_id] = job
        elif event == "finished" and job_id: # received job output from worker
            if job_id in self.jobs:
                self.jobs[job_id]["status"] = "finished"
        elif event == "output" and job_id: # sent job output to client
            if job_id in self.jobs:
                del self.jobs[job_id]
        elif event == "stop" and job_id: # client stopped job
            if job_id in self.jobs:
                del self.jobs[job_id]
 
    def _write_txn(self, event: str, job_id: str, job: dict = None) -> None:
        """
        Append one entry to the transaction log.
        Called BEFORE ack is sent
        """
        entry = {
            "event": event,
            "job_id": job_id,
        }

        if job:
            entry["job"] = job

        with open(self.txn_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())   # force to disk, not just OS buffer

        self.log_count += 1

        if self.log_count > MAX_LOG:
            print("[COORD] Generating checkpoint")
            self.checkpoint()
            self.log_count = 0
 
    def checkpoint(self) -> None:
        """
        Write full state to disk and reset txn log
        """
        with self.lock:
            snapshot = self.jobs
 
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


"""
Rene:
Personal notes:
- Don't do anything until you have sent the ack
If a client registers, and you register the client in self.clients, but
then ack fails, now you have to clean up all what you already started
doing for that client, so it's better to just wait until you sent the
ack and you know the sending succeeded.

- Should we add a job_name? when client submits a job, if we make the job_id
be a uuid then if client requests stats that would be 23453425352342 but is it
better for client to submit a job with a job_name for when they request stats?
I added a field to self.jobs of self.jobs["name"] with the job_name and made it
required for the client to submit a job name when submitting a job

- We currently rely on clients and workers to submit their id's when they
start running in the command line. This is ok but it could be removed
by making the client and worker programs persistently store their id's
somehow but obviously that would be too much extra effort, but it would
be something to put in the presentation as future improvements.

TODO: worker fails, reschedule all its jobs and delete from self.workers
client fails,
client isn't there, store results in self.clients? add a field

TODO: fix the zip logic, client shouldn't save to a directory based on
job_name, it should be by job_name_job_id something like that
so that it's easily identifiable and unique
Worker should find a way to work inside the directory so that it's not
worker1/job_id/[job_id.zip, test1.txt, test1/test1.sh]

It should be
worker1/job_id/[test1.sh, test1.txt] when client receives it
so clean it up
"""
