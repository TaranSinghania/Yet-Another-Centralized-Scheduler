# Yet Another Centralized Scheduler

What's been tried:
* A single request and a single worker, all multithreaded
* Multiple requests, single worker
* Multiple requests, multiple workers
* Logging info for jobs and tasks
* Logging at the worker level
* Finding mean, median mode of logs

Next on list:
* Place multiple workers, use a shell script
* Add LL scheduler

How to use:
1. Run `python3 worker.py 4000 1`
2. Run `python3 worker.py 4001 2`
2. Run `python3 main.py`
3. Run `python3 requests.py 1`

## Notes
- Because the scheduler allocates only one task at a time the worker slots are
being under-utilized  
- All logs go to the logs folder where they are ignored  
- For logs which need to be analyzed use the perma-logs foler