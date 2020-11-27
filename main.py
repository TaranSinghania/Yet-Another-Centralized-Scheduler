"""
The master program
Functions: 
* Listen for requests from the client
* Send tasks to workers and track them
* Schedule tasks to the worker  
Communications between workers happens through JSON  
Use the utility module for all communications  

! Start all workers before running this file
"""
import uuid
import json
import socket
import time

import scheduler
import utility


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
        
        self.is_last_map = ('last_mapper' in kwargs)
        self.is_last_task = ('last_task' in kwargs)
    
    def dispatch(self):
        self.dispatch_time = time.time()
        # Completion time will be obtained once processed by worker
        self.completion_time = 0
    
    # For debugging
    def __repr__(self):
        return f"Task:{self.task_id} of Job{self.job_id} with duration:{self.duration}"


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
        # How many free slots
        self.free = slots
        # Create a socket to send data to the worker
        # Hash to prefix all communications with
        self.hash = uuid.uuid4().hex
        # Private communication channel
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        WORK_SERV_ADDR = (HOST, port)
        # Maintain connection all the time, BUG possible
        self.sock.connect(WORK_SERV_ADDR)
    
    def allocate(self, task: Task):
        # Send a message to the worker asking it to process this task
        # Set dispatch time for task
        task.dispatch()
        # Reduce one slot on the worker machine
        self.free -= 1
        # The worker only needs to know the hash and duration of the task
        # Attach a hash to all messages sent to this worker
        # This way, when the message comes back in the taskmaster we know which
        # Worker sent the message. Useful if all worker programs are identical
        data = {"identifier":self.hash, "task_hash":hash(task), "duration":task.duration}
        payload = json.dumps(data)
        utility.sock_send(self.sock, payload)
    
    def __repr__(self):
        # For debugging
        return f"Worker:{self.w_id} at {self.port} with {self.free}"


class TaskMaster:
    """
    Handles three threads for the master  
    Properties of one master: 
    - The scheduling algorithm it's using
    - The task queues it maintains
    - The sockets it's using for listening
    - The workers it has
    """
    def __init__(self, scheduler: scheduler.Scheduler, config):
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

        # Lock while using
        self.jobs = {}
        # Store id: arrival time
    
    #NOTE: Separate thread
    def serve(self):
        # The client facing server
        # Any job it receives is split between the ready_q and wait_q
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(CLIENT_SIDE_ADDR)
        server_sock.listen()
        print("ALL GOOD")
        
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
            self.jobs[job_id] = {"arrival": time.time()}
            self.jobs[job_id]["no_mappers"] = len(job['map_tasks'])
            self.jobs[job_id]["no_reducers"] = len(job['reduce_tasks'])
            
            # Add each task to the ready queue
            for task in job['map_tasks']:
                new_task = Task(task['task_id'], task['duration'], job_id, mapper=True)
                self.ready_q.append(new_task)
            
            no_mappers = (len(job['map_tasks']) == 0)
            
            # Add each reduce task to the ready queue if there are no map tasks
            # else add them to the wait queue
            for task in job['reduce_tasks']:
                new_task = Task(task['task_id'], task['duration'], job_id, reducer=True)
                if no_mappers:
                    self.ready_q.append(new_task)
                else:
                    self.wait_q.append(new_task)

            break

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
            pass
    
    #NOTE: Separate thread
    def schedule(self):
        # Send tasks to workers
        # Move tasks from ready to running
        
        while True:
            # Wait a second
            time.sleep(1)
            # Wait if no tasks are ready
            if not self.ready_q:
                continue
            # Try to allocate a task to a worker
            workers_list = list(self.workers.values())
            index = self.scheduler.select(workers_list, lock=None)
            if index == -1:
                continue
            # Update variables if succesful
            task = self.ready_q.pop()
            worker = workers_list[index]
            # Move the task to the running queue
            self.running_q[hash(task)] = task
            # Allocate the task to the worker, let it do the comms
            self.workers[worker.w_id].allocate(task)
            print("ALLOCATED", task.task_id, "TO", worker.w_id)


def main():
    # Create a taskmaster object
    # Parse the config and pass that as an option
    # Run the three threads of taskmaster

    with open('min-config.json') as red:
        config = json.load(red)
    spider_man = TaskMaster(scheduler.Random(), config)
    for k, v in spider_man.workers.items():
        print(k, v)
    
    spider_man.serve()

    print("READY")
    for task in spider_man.ready_q:
        print(task)
    
    print("WAITING")
    for task in spider_man.wait_q:
        print(task)
    
    spider_man.schedule()


if __name__ == "__main__":
    main()