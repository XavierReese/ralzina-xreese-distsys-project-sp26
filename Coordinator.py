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

COORDINATOR_TYPE = "coordinator"
COORDINATOR_PROJECT = "dist_job_coordinator"

CATALOG_HOST = "catalog.cse.nd.edu"
CATALOG_PORT = 9097

BUFSIZ = 4096
MAX_BACKOFF = 64

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
                "pending_results": [
                    {"job_id": 101, "status": "finished", ...},
                    ...
                ],
                "fileno": fileno from socket for epoll
            }
        }
        Regardless, I'm not working with clients, just a thought
        """
        self.clients = {} 
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
        self.jobs = {} 
        self.job_queue = deque()

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
        self.send_ack_worker = {}
        self.send_ack_client = {}
        self.recv_ack_worker = set()
        self.recv_ack_client = set()
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
        send_ack_worker = self.send_ack_worker # id -> type of ack (did you just register? schedule job with cilent or worker? )
        send_ack_client = self.send_ack_client

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
                            "id": None,             # either username or worker_id
                            "type": None,           # either worker or client
                            "recv_buffer": b"",
                            "send_buffer": b""
                        }

                    elif event & select.EPOLLIN:
                        connection = connections[fileno]
                        client_socket = connection["socket"]

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
                                if type == "worker":
                                    self.workers[id] = {}
                                    send_ack_worker[id] = {
                                        "ack_type": "register",
                                        "sock_type": sock_type
                                    }
                                elif type == "client":
                                    self.clients[id] = {}
                                    send_ack_client[id] = {
                                        "ack_type": "register",
                                    }
                        else:
                            self.handle_request(connections[fileno], fileno)

                    elif event & select.EPOLLOUT:
                        connection = connections[fileno]
                        client_socket = connection["socket"]

                        if self.send_response(connections[fileno]):
                            if not connections[fileno]["send_buffer"]:
                                connection = connections[fileno]["id"]
                                if connection in send_ack_worker:

                                    if send_ack_worker[connection]["ack_type"] == "register":
                                        print(f"[COORD] Finished sending OK to {connection} for {send_ack_worker[connection]}.")

                                        if send_ack_worker[connection]["sock_type"] == "req_sock":
                                            if connection in self.workers:
                                                # Example:
                                                # workers[worker_id] = fileno
                                                # We only need to know fileno of req_sock, not res_sock
                                                # We initiate conversation to worker to send requests
                                                self.workers[connection]["fileno"] = fileno
                                                print(f"Registered req_sock from {connection}, checking job_queue after we receive heartbeat")

                                    del send_ack_worker[connection]
                                
                                elif connection in send_ack_client:

                                    if send_ack_client[connection]["ack_type"] == "register":
                                        print(f"[COORD] Finished sending OK to {connection} for {send_ack_worker[connection]}.")

                                        self.clients[id]["fileno"] = connection["fileno"]
                                        if len(self.clients[id]["pending_results"]) > 0:
                                            # TODO Xavier: return pending results as well if there are any available results for this client
                                            pass
                                        else:
                                            self.clients[id]["pending_results"] = []

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
                
        # Need at least 4 bytes to know message length
        if len(buffer) < 4:            
            # Rene: is this necessary? print('[DEBUG] coord handle_request: not enough for msg length')
            return None

        # Read length if available
        message_len = int.from_bytes(buffer[:4], "big")

        # Check if full message arrived:
        if len(buffer) < 4 + message_len:
            # Rene: is this necessary? print('[DEBUG] coord handle_request: incomplete message received')
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
            print(self.job_queue)
            print(self.job_queue)
            job_id = self.job_queue.popleft()
            print(self.job_queue)
            print(f"Trying to schedule {job_id}")
            client_id = self.jobs[job_id]["client_id"]
            script = self.jobs[job_id]["script"]
            name = self.jobs[job_id]["name"]
            self.schedule_job(client_id, script, name, job_id)
            i += 1
    
    def execute(self, message_bytes, connection, fileno):

        try:
            request = json.loads(message_bytes.decode("utf-8"))
        
        except (TypeError, ValueError):
            response = {
                "status": "error",
                "tag": "json",
                "message": "Request is not valid JSON"
            }
            self.schedule_response(response, connection, fileno)
            return 

        # validate fields
        if "type" not in request:
            response = {
                "status": "error",
                "tag": "type",
                "message": "You must specify if you're a client or worker in the request"
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
                            "tag": "register",
                            "message": "Registration successful!"
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
                            print(f"Trying to clear job_queue with addition of {id}")
                            self.clear_job_queue()

                    case "ack":
                        if fileno in self.recv_ack_worker:
                            if self.invalid_args(["ack_type", "status", "job_id"], request, connection, fileno):
                                return

                            job_id = request["job_id"]
                            
                            if request["ack_type"] == "schedule":
                                if request["status"] == "ok":
                                    self.jobs[job_id]["status"] = "running"
                                else:
                                    job = self.jobs[job_id]
                                    job["worker_id"] = None
                                    self.schedule_job(job["client_id"], job["script"], job["name"], job_id)
                            
                            if request["ack_type"] == "stop":
                                if request["status"] == "ok":
                                    try:
                                        del self.jobs[job_id]
                                    except KeyError:
                                        pass
                                    # Tell client that you have stopped the process
                                    response = {
                                        "status": "ok",
                                        "tag": "stop",
                                        "job_id": job_id,
                                        "message":  f"{job_id} has been stopped"
                                    }
                                    self.schedule_response(response, connection, fileno)
                                else:
                                    request = {
                                        "method": "stop",
                                        "job_id": job_id
                                    }

                                    self.schedule_response(request, self.connections[fileno], fileno)
                        else:
                            response = {
                                "status": "error",
                                "tag": "ack",
                                "message":  "Received unexpected ack"
                            }
                            self.schedule_response(response, connection, fileno)

                    case "output":
                        if self.invalid_args(["zip_bytes", "job_id", "id"], request, connection, fileno):
                            return

                        print(f"Recieved output from {request["id"]} for job {request["job_id"]}")

                        print("Saving to disk...")

                        job_id = request["job_id"]
                        worker_id = request["id"]

                        zip_path = os.path.join(self.jobs_dir, f"{job_id}_output.zip")
                        encoded_zip = request["zip_file"]
                        zip_bytes = base64.b64decode(encoded_zip)

                        try:
                            with open(zip_path, "wb") as f:
                                f.write(zip_bytes)
                            print(f"Saved zip file to {zip_path}")
                        except Exception as e:
                            print(f"Failed to write zip file {zip_path}: {e}")

                            response = {
                                "status": "error",
                                "tag": "output",
                                "job_id": job_id,
                                "message": f"failed to save {job_id} output"
                            }
                            self.schedule_response(response, connection, fileno)
                            return
                        
                        response = {
                            "status": "ok",
                            "tag": "output",
                            "job_id": job_id,
                            "message": f"Saved {job_id} output"
                        }
                        self.schedule_response(response, connection, fileno)

                        # Send output to client

                        try:
                            with open(zip_path, "rb") as f:
                                zip_bytes = f.read()
                        except FileNotFoundError:
                            print(f"Error: The file at {zip_path} was not found.")
                            return 
                        except Exception as e:
                            print(f"An unexpected error occurred when receiving output of_{job_id} from {worker_id}: {e}")
                            return 
                        
                        encoded_bytes = base64.b64encode(zip_bytes).decode('utf-8')

                        name = self.jobs[job_id]["name"]

                        username = self.jobs[job_id]["client_id"]
                        client_fd = self.clients[username]["fileno"]

                        request = {
                            "method": "output",
                            "zip_bytes": encoded_bytes,
                            "name": name,
                        }
                        print(f"about to schedule message to client to receive output from {name}")
                        self.schedule_response(request, self.connections[client_fd], client_fd)
                        print("scheduled message with job")
                        self.recv_ack_client.add(client_fd)
                        print("expecting client ack")

                        self.client_queue.append(job_id) # add job id to client queue meaning we must send the output to a client

                        self.clear_job_queue()

                        return


                    case _:
                        response = {
                            "status": "error",
                            "tag": "method",
                            "message":  "Invalid method requested"
                        }
                        self.schedule_response(response, connection, fileno)

            case "client":
                # Handle each possible client operation
                match request["method"]:
                    case "stop":
                        if self.invalid_args(["id"], request, connection, fileno):
                            return

                        response = {
                            "status": "ok",
                            "tag": "stop_ack",
                            "job_id": job_id,
                            "message":  f"Job request to stop {job_id} received and processesd"
                        }

                        self.jobs[job_id]["status"] = "stop"

                        request = {
                            "method": "stop",
                            "job_id": job_id
                        }

                        worker_fd = self.workers[self.jobs[job_id]["worker_id"]]["fileno"]

                        self.schedule_response(request, self.connections[worker_fd], worker_fd)

                        self.recv_ack_worker.add(worker_fd)

                        self.schedule_response(response, connection, fileno)

                    case "register":
                        if self.invalid_args(["username"], request, connection, fileno):
                            return

                        id = request["username"]
                        sock_type = None
                        type = request["type"]

                        response = {
                            "status": "ok",
                            "tag": "register",
                            "message": f"Registered as {id}"
                        }

                        print(f'[CLIENT_JOIN] client {id} joined')

                        return id, None, type

                    case "ack":
                        # TODO implement function when coordinator must receive an ack from client
                        # Look at case "ack" above in worker for reference

                        # TODO add ack for receiving confirmation of output from client
                        # Check if client queue is necessary
                        pass

                    case "stats":

                        if self.invalid_args(["username"], request, connection, fileno):
                            return

                        username = request["username"]

                        # Rene: do we really need to check this? I added the ack check on registration so I don't think this 
                        # error would hapen
                        if username not in self.clients:
                            self.error_res("Please JOIN first", "register", connection, fileno)
                            return

                        # Rene: I don't think this error would happen either, the fileno is associated to the connection,
                        # and the connection has the username, so I think it's extra
                        if fileno != self.clients[username]["fileno"]:
                            self.error_res(f'Socket not associated with {username}, please leave and rejoin', "register",connection, fileno)


                        jobs = []
                        # Send (job_id, status)
                        for job_id in self.clients[username]["pending"]:
                            status = self.jobs[job_id]["status"]
                            name = self.jobs[job_id]["name"]
                            jobs.append([name, status])

                        #print(f'DEBUG: JOBS: {jobs}')
                        response = {
                            "status": "ok",
                            "tag": "stats",
                            "message": jobs
                        }
                        self.schedule_response(response, connection, fileno)
                        return

                    case "submit":
                        if self.invalid_args(["zip_file", "script", "name"], request, connection, fileno):
                            return

                        job_id = str(uuid.uuid4())

                        zip_path = os.path.join(self.jobs_dir, f"{job_id}_input.zip")

                        encoded_zip = request["zip_file"]
                        zip_bytes = base64.b64decode(encoded_zip)

                        name = request["name"]
                        script = request["script"]

                        try:
                            with open(zip_path, "wb") as f:
                                f.write(zip_bytes)

                            print(f"Saved zip file to {zip_path}")
                            response = {
                                "status": "ok",
                                "tag": "submit",
                                "job_id": job_id
                                "message":  f"Job request {name} received and started"
                            }
                            self.schedule_response(response, connection, fileno)

                            # contact a worker

                            self.schedule_job(fileno, script, name, job_id)
                        except Exception as e:
                            print(f"Failed to write zip file {zip_path}: {e}")

                            response = {
                                "status": "error",
                                "tag": "submit",
                                "message":  f"Failed to process submit request"
                            }
                            self.schedule_response(response, connection, fileno)

                    case _:
                        response = {
                            "status": "error",
                            "tag": "method",
                            "message":  "Invalid method requested"
                        }
                        self.schedule_response(response, connection, fileno)
            case _:
                response = {
                    "status": "error",
                    "tag": "type",
                    "mesasge": "Must specify if type is client or worker only"
                }


    def schedule_job(self, client_id, script, name, job_id=None):
        # Logic to select which worker to run

        if not job_id:
            job_id = str(uuid.uuid4())

        zip_path = f"{job_id}_input.zip"
        
        worker_id = self.select_worker()
        print(f"Trying to schedule {job_id} in {worker_id}")
        try:
            worker_fd = self.workers[worker_id]["fileno"]
                                    
            self.jobs[job_id] = {
                "client_id": client_id,
                "worker_id": worker_id,     
                "status": "not_started",
                "script": script,
                "result": None,
                "name": name
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
            print("scheduled message with job")
            self.recv_ack_worker.add(worker_fd)
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
            return None
        
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
        
        return max(eligible_ids, key=worker_score)

    def invalid_args(self, args, request, connection, fileno):
        for arg in args:
            if arg not in request:
                response = {
                    "status": "error",
                    "tag": "arg",
                    "message": f"invalid, {arg} not present"
                }
                self.schedule_response(response, connection, fileno)
                return True
        return False
    
    def error_res(self, err_message: str, tag: str, connection, fileno):
        ''' Schedule an error response '''
        response = {
            "status": "error",
            "tag": tag,
            "message": err_message
        }
        self.schedule_response(response, connection, fileno)
    
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

    # -----------------------------------
    # State Updating Functions
    # - standardize adding/removing jobs, etc
    # Rene: remove? check my own schedule logic
    # -----------------------------------

    def incoming_job(self, username, script, outputs, dir_zip):
        job_id = str(uuid.uuid4())
        self.jobs[job_id] = {
            "client_id": username,
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
                print(f"Failed to send update: {e}")

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
                self.jobs        = ckpt.get("jobs", {})
                self.clients     = ckpt.get("clients", {})

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
"""
