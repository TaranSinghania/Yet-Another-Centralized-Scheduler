# Yet Another Centralized Scheduler
Team ID: BD_0333_0764_1655_1958

## Requirements
To install the requirements, run the following command
`python3 -m pip install -r requirements.txt`

## Usage
Clone this repo and move to the root of the repo
`git clone https://github.com/TaranSinghania/BigDataYACS_SpaceMonkeyz`

How to run: (Use separate terminal for each one)
1. Run `python3 worker.py 4000 1`
2. Run `python3 worker.py 4001 2`
3. Run `python3 worker.py 4002 3`
4. Run `python3 master.py config.json RR` (Acceptable options: RR - Round Robin | R - Random | LL - Least Loaded)
5. Run `python3 requests.py 30`
6. Run `python3 analysis.py`
