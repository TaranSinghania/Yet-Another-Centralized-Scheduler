"""
An independent worker process
Task execution request received as: 
{
    "identifier": communication token with master,
    "task_hash" : unique identifier for tasks - use this as key
    "duration"  : execution time for the task
}

Execution process: 
Add tasks to pool as they arrive
At each second, reduce the duration of all tasks in the pool
When any task has finished execution relay info to master
"""
import socket
import json
import time
import threading

import utility


HOST = "localhost"
MASTER_PORT = 5001


class Worker:
    """  
    Two communication sockets:  
    - Server: listen for task requests from master
    - Client: send notification to master on task completion
    """
    def __init__(self, listen_port: int):
        # The execution pool
        # Each task is a dictionary, check top for format
        self.pool = []
        
        self.port = listen_port

        # Locks for all variables which can be accessed in different threads
        lockable_items = [
            "pool", "stdout"
        ]
        self.lock = {item: threading.Lock() for item in lockable_items}

    # NOTE: separate thread
    def receive(self):
        address = (HOST, self.port)
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(address)
        server_sock.listen()

        with self.lock["stdout"]:
            print("READY FOR REQUESTS")

        while True:
            # Listen for requests from master
            data = utility.sock_recv(server_sock)
            task = json.loads(data)
            # Possible BUG - case when task has 0 duration
            # Add task to execution pool
            with self.lock["pool"]:
                self.pool.append(task)

            with self.lock["stdout"]:
                print("received task")
                print(task)

    # NOTE: separate thread
    def execute(self):
        # Run forever
        while True:
            # Give time for changes, fake execution
            time.sleep(1)

            self.lock["pool"].acquire()
            with open('worker.log', 'a') as wire:
                print(self.pool, file=wire)
                print("============================================", file=wire)
            self.lock["pool"].release()

            # Wait if pool is empty
            self.lock["pool"].acquire()
            if not self.pool:
                self.lock["pool"].release()
                continue
            self.lock["pool"].release()

            # Reduce duration for all tasks by 1
            self.lock["pool"].acquire()
            for i in range(len(self.pool)):
                remain = self.pool[i]["duration"]
                # Handle case of float duration
                remain = max(0, remain-1)
                self.pool[i]["duration"] = remain
            self.lock["pool"].release()
        
            # Remove all tasks that have finished from the pool
            # Possible alternative, use a set, set hash as task hash
            # Set approach is more readable, but it is a hack
            # Linear time algorithm, ignore specifics
            first_free_position = 0
            self.lock["pool"].acquire()
            for i in range(len(self.pool)):
                if self.pool[i]["duration"] == 0:
                    # Notify master of completion
                    self.notify(self.pool[i])
                    # This position is free
                    continue
                # Move the task to the first free position in the list
                self.pool[first_free_position] = self.pool[i]
                # That position is now occupied, move on
                first_free_position += 1
            # Only want to keep occupied positions
            self.pool = self.pool[:first_free_position]
            self.lock["pool"].release()

    def notify(self, task: dict):
        # Prepare payload to send
        # Decide where to print this statement, is not protected by a lock
        print("Done with task", task["task_hash"])
        payload = json.dumps(task)

        # Connect to master
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        address = (HOST, MASTER_PORT)
        client_sock.connect(address)
        
        # Send data
        utility.sock_send(client_sock, payload)
        client_sock.close()


def main():
    # Clear the log file
    with open('worker.log', 'w') as _:
        pass
    bob = Worker(4000)
    receiver_thread = threading.Thread(target=bob.receive)
    executor_thread = threading.Thread(target=bob.execute)

    threads = [receiver_thread, executor_thread]

    for thread in threads:
        thread.start()
    
    # Service requests continuosly
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()