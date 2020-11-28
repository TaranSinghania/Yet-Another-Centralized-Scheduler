"""
Scheduler classes
Subclassed to maintain common interface
"""
import random


class Scheduler:
    """
    A single function to pick a worker from a list of worker
    """
    def select(self, workers: list, lock):
        # Acquire lock while searching
        # Return the worker_id of a single worker
        # Return -1 if no slot is available
        raise NotImplementedError


class Random(Scheduler):
    """
    Randomly selects a worker with a free slot
    """
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