# Yet Another Centralized Scheduler

What's been tried:
* A single request and a single worker, all multithreaded
* Multiple requests, single worker
* Logging info for jobs and tasks
* Logging at the worker level

Next on list:
* Place multiple workers, use a shell script
* Add RR and LL schedulers

How to use:
1. Run `python3 worker.py`
2. Run `python3 main.py`
3. Run `python3 requests.py`

## Notes
- Because the scheduler allocates only one task at a time the worker slots are
being under-utilized