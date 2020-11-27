"""
The master program
Functions: 
* Listen for requests from the client
* Send tasks to workers and track them
* Schedule tasks to the worker
"""
import uuid
import json

import scheduler


class Task:
    """
    A single independent task
    Keep track of which job it represents
    Can be a mapper or a reducer
    Must know if it is the last mapper or reducer
    """
    pass


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
    def __init__(self, scheduler, config):
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
        
        while True:
            # Receive a request
            # Update queues and jobs
            pass
    
    #NOTE: Separate thread
    def listen(self):
        # Worker facing listener
        # Check for completion of tasks
        # Also update worker slots

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
            # Wait if no tasks are ready
            # Try to allocate a task to a worker
            # Update variables if succesful
            # Wait a second
            pass


def main():
    # Create a taskmaster object
    # Parse the config and pass that as an option
    # Run the three threads of taskmaster

    with open('config.json') as red:
        config = json.load(red)
    spider_man = TaskMaster(scheduler.Random(), config)
    for k, v in spider_man.workers.items():
        print(k, v)


if __name__ == "__main__":
    main()