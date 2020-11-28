# Yet Another Centralized Scheduler

What's been tried:
* A single request and a single worker, all multithreaded
* Multiple requests, single worker
* Multiple requests, multiple workers
* Logging info for jobs and tasks
* Logging at the worker level
* Finding mean, median mode of logs
* Place multiple workers, use a shell script
* Add LL scheduler
* Finished Log Analysis

Next on list:
* Add selective waiting for scheduling

How to use:
1. Run `python3 worker.py 4000 1`
2. Run `python3 worker.py 4001 2`
2. Run `python3 worker.py 4002 3`
2. Run `python3 master.py`
3. Run `python3 requests.py 10`
4. Run `python3 log_analysis.py 3`

## Notes
- Because the scheduler allocates only one task at a time the worker slots are
being under-utilized  
