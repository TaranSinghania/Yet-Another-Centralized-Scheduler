# Yet Another Centralized Scheduler

What's been tried:
* A single request and a single worker, all multithreaded
* Multiple requests, single worker
* Logging info for jobs and tasks

Next on list:
* Logging at the worker level
* Place multiple workers, use a shell script

How to use:
1. Run `python3 worker.py`
2. Run `python3 main.py`
3. Run `python3 requests.py`