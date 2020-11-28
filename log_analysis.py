"""
Performing Log Analysis
Functions: 
* Parses log file specified
* Calculates mean and median task completion time
* Calculates mean and median job completion time
* Plots number of tasks scheduled on each machine vs time
Use the utility module for all communications  

! Start all workers and master before running this file
"""
import sys


class stats:
    def __init__(self, times):
        self.times = times
        self.len_times = len(times)

    def mean(self):
        return sum(self.times)/self.len_times

    def median(self):
        self.times.sort()

        if(self.len_times % 2 == 0):
            median = (self.times[self.len_times//2] + self.times[self.len_times//2 - 1])/2
        else:
            median = self.times[self.len_times//2]

        return median

class calc_time:
    def __init__(self, parse_file):
        self.file = parse_file
        self.times = []

    def parseFile(self):
        with open(self.file, 'r') as f:
            logs = f.readlines()

        for item in logs:
            if item[0] == '#':
                continue
            time = float(item.split(" ")[2])
            self.times.append(time)
    
    def calc_mean_median(self):
        calc = stats(self.times)
        mean = calc.mean()
        median = calc.median()

        print("The mean ", self.file[:-4], " completion time is: ", mean, " seconds")
        print("The median ", self.file[:-4], " completion time is: ", median, " seconds")
        print("\n")


def main():

    task = calc_time('task.log')
    task.parseFile()
    task.calc_mean_median()

    job = calc_time('job.log')
    job.parseFile()
    job.calc_mean_median()

    #Plot here


if __name__ == "__main__":
    main()