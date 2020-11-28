"""
Scheduler classes
Subclassed to maintain common interface
"""
import time
import random


class Scheduler:
    """
    A single function to pick a worker from a list of worker
    """
    # Useful for log file naming
    name = "Default-Scheduler"
    def select(self, workers: list, lock):
        # Acquire lock while searching
        # Return the worker_id of a single worker
        # Return -1 if no slot is available
        raise NotImplementedError


class Random(Scheduler):
    """
    Randomly select a worker with a free slot
    """
    name = "RandomScheduler"
    def select(self, workers: list):
        # If a worker is not free remove from random selection
        yet_to_try = workers.copy()
        while yet_to_try:
            index = random.randrange(0, len(yet_to_try))
            if yet_to_try[index].free > 0:
                # Found a worker with free slots
                return index
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

    def select(self, workers: list):
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
    def select(self, workers: list):
        max_slots = -1
        max_idx = -1
        while 1:
            for i in range(len(workers)):
                # Update max free slots available and the index of worker
                if workers[i].free > 0 and workers[i].free > max_slots:
                    max_slots = workers[i].free
                    max_idx = i

            # If no slots are free sleep for one second
            if max_idx == -1:
                return -1
            else:
                return max_idx
