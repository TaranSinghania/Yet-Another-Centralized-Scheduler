"""
The master program
Functions: 
* Listen for requests from the client
* Send tasks to workers and track them
* Schedule tasks to the worker  

Communications between workers happens through JSON  
Use the utility module for all communications  

! Start all workers before running this file

Deadlock prevention method:  
Each thread holds max one lock at any given time
To increase readability replace acquire-release with a `with` block where possible
"""
import sys
import uuid
import json
import socket
import time
import random
import threading


HOST = "localhost"
# Port to listen for requests from client
CLIENT_SIDE_PORT = 5000
# Port to listen for updates from workers
WORKER_SIDE_PORT = 5001
CLIENT_SIDE_ADDR = (HOST, CLIENT_SIDE_PORT)
WORKER_SIDE_ADDR = (HOST, WORKER_SIDE_PORT)

# Task types, can be any arbitrary numbers
MAPPER = 420
REDUCER = 51

# Check for command line arguments
if(len(sys.argv) < 3):
    print("Incorrect Usage. Correct Usage: Master.py <path to config file> <Scheduling algorithm - R|RR|LL>")
    sys.exit()

path_to_config_file = sys.argv[1] # Read path to config file
scheduling_algorithm = sys.argv[2] # Read scheduling algo to be used


"""
Scheduler classes
Ported from the scheduler module from the previous setup
"""
class Scheduler:
    """
    A single function to pick a worker from a list of worker
    """
    # Useful for log file naming
    name = "Default-Scheduler"
    def select(self, workers: list, lock: threading.Lock):
        # Acquire lock while searching
        # Return the worker_id of a single worker
        # Return -1 if no slot is available
        raise NotImplementedError


class Random(Scheduler):
    """
    Randomly select a worker with a free slot
    """
    name = "Random"
    def select(self, workers: list, lock: threading.Lock):
        # If a worker is not free remove from random selection
        yet_to_try = list(enumerate(workers.copy()))
        while yet_to_try:
            index = random.randrange(0, len(yet_to_try))
            real_index, worker = yet_to_try[index]
            if worker.free > 0:
                # Found a worker with free slots
                return real_index
            # No free slots
            yet_to_try.pop(index)
        else:
            # Could not find a free slot in any worker
            return -1


class RoundRobin(Scheduler):
    """
    Pick machines in a circular order
    Assume that the list of workers passed is the same
    """
    name = "RoundRobin"
    def __init__(self):
        # What worker was picked the last time? Go on from there
        self.prev = -1

    def select(self, workers: list, lock: threading.Lock):
        n = len(workers)
        if self.prev == -1:
            start = 0
        else:
            start = (self.prev + 1) % n
        # How many nodes to search before giving up
        counter = n
        while counter > 0:
            if workers[start].free > 0:
                self.prev = start
                return start
            start = (start + 1) % n
            counter -= 1
        else:
            return -1


class LeastLoaded(Scheduler):
    """
    Pick the least loaded machine
    """
    name = "LeastLoaded"
    def select(self, workers: list, lock: threading.Lock):
        min_slots = 1 << 30
        max_idx = -1
        while True:
            for i in range(len(workers)):
                # Update max free slots available and the index of worker
                if workers[i].free > 0 and workers[i].used < min_slots:
                    min_slots = workers[i].used
                    max_idx = i

            # If no slots are free, sleep for one second
            if max_idx == -1:
                lock.release()
                time.sleep(1)
                lock.acquire()
            else:
                return max_idx
        
        return -1


"""
Helper functions
Does not affect fundamental workflow
Ported from the utility module from the previous setup
"""
BUF_LEN = 65535 # Buffer Size
class Utility:
    # Recevie a message through a socket
    def sock_recv(self, sock):
        # This socket is a server-side socket
        (clientsocket, _) = sock.accept()
        # print("CONNECTED from", address)
        # Must add true buffering later on
        data = clientsocket.recv(BUF_LEN)
        clientsocket.close()
        return data.decode()

    def sock_send(self, sock, data: str):
        # This socket is a client-side socket
        data = data.encode()
        # Must buffer this later on
        sock.send(data)
utility = Utility()


class Task:
    """
    A single independent task  
    Keep track of which job it represents  
    Can be a mapper or a reducer  
    Must know if it is the last mapper and/or the last task
    """
    def __init__(self, task_id, duration, job_id, **kwargs):
        self.task_id = task_id
        self.duration = duration
        self.job_id = job_id
        if 'mapper' in kwargs:
            self.type = MAPPER
        elif 'reducer'in kwargs:
            self.type = REDUCER

        # Unique identifier, extra protection with task_id
        self.hash = uuid.uuid4().hex + str(task_id)
    
    def dispatch(self):
        self.dispatch_time = time.time()
        # Completion time will be obtained once processed by worker
        self.completion_time = 0
    
    def arrive(self):
        self.arrival_time = time.time()
        self.completion_time = self.arrival_time - self.dispatch_time
    
    # For debugging
    def __repr__(self):
        return f"Task:{self.task_id} of Job{self.job_id} with duration:{self.duration}"

    # uuid is reliable
    def __hash__(self):
        return hash(self.hash)


class Worker:
    """
    A single worker, with:  
    Unique socket to send messages  
    Number of slots, the port number  
    Worker_id, hash for communication prefix  
    """
    def __init__(self, w_id, slots, port):
        self.w_id = w_id
        self.port = port
        # Max number of tasks allocatable
        self.capacity = slots
        # How many free slots
        self.free = slots
        # How many slots being used right now
        self.used = 0
        # Create a socket to send data to the worker
        # Hash to prefix all communications with
        self.hash = uuid.uuid4().hex
        self.WORK_SERV_ADDR = (HOST, port)
    
    def allocate(self, task: Task):
        # Send a message to the worker asking it to process this task
        # Set dispatch time for task
        task.dispatch()
        # Reduce one slot on the worker machine
        self.free -= 1
        self.used += 1
        # The worker only needs to know the hash and duration of the task
        # Attach a hash to all messages sent to this worker
        # This way, when the message comes back in the taskmaster we know which
        # Worker sent the message. Useful if all worker programs are identical
        data = {"identifier":self.hash, "task_hash":task.hash, "duration":task.duration}
        payload = json.dumps(data)

        # Private communication channel
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.WORK_SERV_ADDR)
        utility.sock_send(sock, payload)
        sock.close()
    
    def __repr__(self):
        # For debugging
        return f"Worker:{self.w_id} at {self.port} with {self.free} slots"


class TaskMaster:
    """
    Handles three threads for the master  
    Properties of one master: 
    - The scheduling algorithm it's using
    - The task queues it maintains
    - The sockets it's using for listening
    - The workers it has in its configuration
    """
    def __init__(self, scheduler: Scheduler, config):
        # An object of a sublclass of the Scheduler class
        self.scheduler = scheduler

        # Execution queues
        # Lock while using
        self.ready_q = []
        self.wait_q = []
        # k:v -> task_hash : task object
        self.running_q = {}

        # Lock while using
        # k:v -> worker_id : worker object
        self.workers = {}
        # Fetch the workers from the config file
        for worker in config["workers"]:
            new_worker = Worker(worker["worker_id"], worker["slots"], worker["port"])
            self.workers[new_worker.w_id] = new_worker
        
        # Reverse map workers, useful when listening for updates
        # k:v -> worker_hash : worker id
        # Updates must be performed on self.workers
        # Read only object, lock not necessary
        self.worker_r_index = {w.hash: w.w_id for w in self.workers.values()}

        # Lock while using
        self.jobs = {}
        # Store id: arrival time

        # Locks for all variables which can be accessed in different threads
        lockable_items = [
            "wait_q", "ready_q", "running_q", "workers", "jobs",
            "stdin", "stdout"
        ]
        self.lock = {item: threading.Lock() for item in lockable_items}

        # The worker-load log file
        self.w_log = scheduler.name + "Worker.log"
        with open(self.w_log, 'w') as wire:
            # Write the log file format
            # Worker-id - number of tasks
            wire.write(f"{len(worker)}\n")
            wire.write("# Worker-id, Number of tasks running, Timestamp\n")
        
        # TM timer
        self.timer = time.time()
    
    #NOTE: Separate thread
    def serve(self):
        # The client facing server
        # Any job it receives is split between the ready_q and wait_q
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(CLIENT_SIDE_ADDR)
        server_sock.listen()

        with self.lock["stdout"]:
            print("LISTENING FOR REQUESTS")
        
        while True:
            # Receive a request
            # Update queues and jobs
            data = utility.sock_recv(server_sock)
            job = json.loads(data)
            job_id = job['job_id']

            # Note down the time the job came to the system
            # Note down how many mapper and reducers are there
            # Once all mappers are done, this job's reducers can start
            # Once all reducers are done, this job can finish
            self.lock["jobs"].acquire()
            # ==================================================================
            self.jobs[job_id] = {"arrival": time.time()}
            self.jobs[job_id]["no_mappers"] = len(job['map_tasks'])
            self.jobs[job_id]["no_reducers"] = len(job['reduce_tasks'])

            mapper_not_exist = (len(job['map_tasks']) == 0)

            # Indicate if the reducers have been started
            self.jobs[job_id]["started_reducers"] = mapper_not_exist
            # ==================================================================
            self.lock["jobs"].release()
            
            # Add each task to the ready queue
            self.lock["ready_q"].acquire()
            for task in job['map_tasks']:
                new_task = Task(task['task_id'], task['duration'], job_id, mapper=True)
                self.ready_q.append(new_task)
            self.lock["ready_q"].release()
            
            # Add each reduce task to the ready queue if there are no map tasks
            # else add them to the wait queue
            if mapper_not_exist:
                self.lock["ready_q"].acquire()
                for task in job['reduce_tasks']:
                    new_task = Task(task['task_id'], task['duration'], job_id, reducer=True)
                    self.ready_q.append(new_task)
                self.lock["ready_q"].release()
            else:
                self.lock["wait_q"].acquire()
                for task in job['reduce_tasks']:
                    new_task = Task(task['task_id'], task['duration'], job_id, reducer=True)
                    self.wait_q.append(new_task)
                self.lock["wait_q"].release()

    #NOTE: Separate thread
    def listen(self):
        # Worker facing server
        # Check for completion of tasks
        # Also update worker slots
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(WORKER_SIDE_ADDR)
        # Should be able to have connections to all workers
        server_sock.listen(len(self.workers))

        while True:
            # Update queues, some special conditions:
            # Last map -> Move reducers to waiting
            # Last reducer -> Log job completion
            
            payload = utility.sock_recv(server_sock)
            # Get the completed task's details
            task_info = json.loads(payload)
            
            # Update it's worker's slots
            worker_id = self.worker_r_index[task_info["identifier"]]
            with self.lock["workers"]:
                self.workers[worker_id].free += 1
                self.workers[worker_id].used -= 1

            # Remove it from the running queue
            self.lock["running_q"].acquire()
            task = self.running_q[task_info["task_hash"]]
            task.arrive()
            del self.running_q[task.hash]
            self.lock["running_q"].release()
            # Task is read-only from here on

            # Debug log info about task
            with self.lock["stdout"]:
                print("Task complete", task.hash)

            # Log task completion
            with open("task" + scheduling_algorithm + ".log", 'a') as wire:
                print(f"{task.task_id} - {task.completion_time}", file=wire)

            # Update job
            job = task.job_id
            self.lock["jobs"].acquire()
            if task.type == MAPPER:
                self.jobs[job]["no_mappers"] -= 1
            elif task.type == REDUCER:
                self.jobs[job]["no_reducers"] -= 1
            
            # If all mappers have finished
            map_done = (self.jobs[job]["no_mappers"] == 0)
            started_reducers = self.jobs[job]["started_reducers"]
            self.lock["jobs"].release()

            if map_done and not started_reducers:
                # Move all reducers from the waiting queue to the ready queue
                with self.lock["wait_q"]:
                    reducers = list(filter(lambda x: x.job_id == job, self.wait_q))
                    self.wait_q = [x for x in self.wait_q if x not in reducers]
                
                with self.lock["ready_q"]:
                    self.ready_q.extend(reducers)
                
                with self.lock["jobs"]:
                    self.jobs[job]["started_reducers"] = True
            
            # If all the reducers have finished
            self.lock["jobs"].acquire()
            reduce_done = (self.jobs[job]["no_reducers"] == 0)
            if reduce_done:
                self.jobs[job]["completed"] = time.time()
                # Debug log info about job completion
                print("Job complete")
                print(self.jobs[job])

                # Log job completion
                total = self.jobs[job]["completed"] - self.jobs[job]["arrival"] 
                with open('job' + scheduling_algorithm + '.log', 'a') as wire:
                    print(f"{job} - {total}", file=wire)
                
                # Don't need the job anymore
                del self.jobs[job]

            self.lock["jobs"].release()
    
    #NOTE: Separate thread
    def schedule(self):
        # Send tasks to workers
        # Move tasks from ready to running
        # Debug logs are performed in this file as there is no socket blocking
        
        while True:
            # Wait if no tasks are ready
            self.lock["ready_q"].acquire()
            if not self.ready_q:
                self.lock["ready_q"].release()
                time.sleep(0.2)
                continue
            # This lock will be released only once
            self.lock["ready_q"].release()

            # Try to allocate a task to a worker
            
            # Once taken from the dictionary, are these values copies?
            # If they are copies, the later locks on workers is not required
            self.lock["workers"].acquire()
            workers_list = list(self.workers.values())
            workers_list.sort(key=lambda x: x.w_id)
            index = self.scheduler.select(workers_list, self.lock["workers"])
            self.lock["workers"].release()

            if index == -1:
                time.sleep(0.2)
                continue
            # Update variables if succesful
            with self.lock["ready_q"]:
                task = self.ready_q.pop()

            # Verify no lock required for this
            with self.lock["workers"]:
                worker = workers_list[index]

            # Move the task to the running queue
            with self.lock["running_q"]:
                self.running_q[task.hash] = task

            # Allocate the task to the worker, let it do the comms
            with self.lock["workers"]:
                self.workers[worker.w_id].allocate(task)

            with self.lock["stdout"]:
                print("ALLOCATED", task.task_id, "TO", worker.w_id)

    #NOTE: Separate thread
    def worker_logger(self, frequency=1):
        # Log worker loads
        while True:
            # Log only while jobs still in progress
            self.lock["jobs"].acquire()
            if not self.jobs:
                self.lock["jobs"].release()
                continue
            self.lock["jobs"].release()

            # Local timer
            local_timer = round(time.time() - self.timer, 5)

            # log worker load information
            self.lock["workers"].acquire()
            # No lock for this as no other thread accesses it
            with open(self.w_log, "a") as wire:
                for worker in self.workers.values():
                    print(f"{worker.w_id},{worker.used},{local_timer}", file=wire)
            self.lock["workers"].release()

            # Wait for provided frequency
            # Reduce default parameter for more accurate logs
            time.sleep(frequency)

    #NOTE: Separate thread
    def debug_logger(self):
        # Log state of queues for debugging
        while True:
            # Don't log when there are no jobs
            self.lock["jobs"].acquire()
            if not self.jobs:
                self.lock["jobs"].release()
                continue
            self.lock["jobs"].release()

            # Not required for log analysis
            with open('main.log', 'a') as wire:
                print("========================================", file=wire)
                self.lock["ready_q"].acquire()
                print("READY_QUEUE", file=wire)
                for task in self.ready_q:
                    print(task, file=wire)
                self.lock["ready_q"].release()

                self.lock["wait_q"].acquire()
                print("WAIT_QUEUE", file=wire)
                for task in self.wait_q:
                    print(task, file=wire)
                self.lock["wait_q"].release()
                
                self.lock["running_q"].acquire()
                print("RUNNING_QUEUE", file=wire)
                for v in self.running_q.values():
                    print(v, file=wire)
                self.lock["running_q"].release()
                print("========================================", file=wire)
            # Don't print too often
            time.sleep(1)
    # End logger
# End class


def main():
    # Create a taskmaster object
    # Parse the config and pass that as an option
    # Run the three threads of taskmaster

    # Clear the debug log file
    with open('main.log', 'w'):
        pass

    with open(path_to_config_file) as red:
        config = json.load(red)
    
    # Clear the general log files
    with open('task' + scheduling_algorithm + '.log', 'w') as wire:
        # Write down the log file format
        wire.write("# task-id - completion time in seconds (float)\n")

    with open('job' + scheduling_algorithm + '.log', 'w') as wire:
        # Write down the log file format
        wire.write("# job-id - completion time in seconds (float)\n")
    
    # Choosing the scheduler to use
    if scheduling_algorithm == "RR":
        spider_man = TaskMaster(RoundRobin(), config)
    elif scheduling_algorithm == "LL":
        spider_man = TaskMaster(LeastLoaded(), config)
    else:
         spider_man = TaskMaster(Random(), config)
       
    for worker in spider_man.workers.values():
        print(worker)

    # Create separate threads
    client_side_thread = threading.Thread(target=spider_man.serve)
    scheduler_thread = threading.Thread(target=spider_man.schedule)
    worker_side_thread = threading.Thread(target=spider_man.listen)
    worker_logger_thread = threading.Thread(target=spider_man.worker_logger)
    debug_logger_thread = threading.Thread(target=spider_man.debug_logger)

    # Collect them in a pool like structure
    threads = [
        client_side_thread, scheduler_thread, worker_side_thread,
        worker_logger_thread, debug_logger_thread
    ]

    for thread in threads:
        thread.start()
    
    # Loop till finish
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
