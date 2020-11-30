# Yet Another Centralized Scheduler

What's been tried:
* A single request and a single worker, all multithreaded
* Multiple requests, single worker
* Multiple requests, multiple workers
* Logging info for jobs and tasks
* Logging at the worker level
* Finding mean, median of task and job times
* Place multiple workers, use a shell script
* Add LL scheduler
* Finished Log Analysis
* Add selective waiting for scheduling

## Usage
Clone this repo and move to the root of the repo
`git clone https://github.com/TaranSinghania/BigDataYACS_SpaceMonkeyz`

How to run: (Use separate terminal for each one)
1. Run `python3 worker.py 4000 1`
2. Run `python3 worker.py 4001 2`
3. Run `python3 worker.py 4002 3`
4. Run `python3 master.py config.json RR` (Acceptable options: RR - Round Robin | R - Random | LL - Least Loaded)
5. Run `python3 requests.py 10`
6. Run `python3 log_analysis.py 3`
