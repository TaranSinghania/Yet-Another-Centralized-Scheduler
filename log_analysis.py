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
import matplotlib.pyplot as plt


class stats:
    """
    Handles the following statistical calculations:
    1. Mean
    2, Median
    """
    def __init__(self, times):
        # List of execution times
        self.times = times
        self.len_times = len(times)

    def mean(self):
        # Average = Sum/Number of Occurences
        return sum(self.times)/self.len_times

    def median(self):
        # Need sorted list for median
        self.times.sort()

        # Check if even
        if(self.len_times % 2 == 0):
            # Take average of n/2 and n+1/2
            median = (self.times[self.len_times//2] + self.times[self.len_times//2 + 1])/2
        else:
            # If odd, median is n/2
            median = self.times[self.len_times//2]

        return median



class calc_time:
    """
    Calculates the mean and median time needed to run the tasks and jobs.
    Contains a parser to parse the log file
    Contains a function to calculate the mean and median time
    """
    def __init__(self, parse_file):
        # Name of file to be parsed
        self.file = parse_file
        # List to store the execution times after parsing log file.
        self.times = []

    def parse(self):
        with open(self.file, 'r') as f:
            logs = f.readlines()

        for item in logs:
            # Ignore header
            if item[0] == '#':
                continue
            # Converting the time to float
            time = float(item.split(" ")[2])
            self.times.append(time)
    
    def calc_mean_median(self):
        # Using stats class to calculate mean and median
        calc = stats(self.times)
        mean = calc.mean()
        median = calc.median()

        print("The mean ", self.file[5:-4], " completion time is: ", mean, " seconds")
        print("The median ", self.file[5:-4], " completion time is: ", median, " seconds")
        print("\n")



class plotTime:
    """
    Plots the Timestamp vs Number of slots used graph.
    Contains a parser to parse the log file
    Contains a function to plot the line graph.
    """
    def __init__(self, parse_file, num_workers):
        self.parse_file = parse_file
        self.num_workers = num_workers
        self.time_dict = {}
        self.timestamps = []
        self.key = "Worker "

    def parse(self):
        with open(self.parse_file, 'r') as f:
            # Ignore header
            logs = f.readlines()[1:]
        
        # Creating dictionary Ex: "Worker 1": [slots used]
        for i in range(self.num_workers):
            self.time_dict[self.key + str(i+1)] = []
        
        # Skip every interval of size num_workers
        for log_idx in range(0, len(logs), self.num_workers):
            # Retrieve slice of one specific timestamp
            log_items = logs[log_idx: log_idx + self.num_workers]
            for log_item in log_items:
                worker_id, slots, timestamp = log_item.split(",")
                self.time_dict[self.key + worker_id].append(int(slots))
                # Add to timestamp list if not already present. Timestamps are unique.
                timestamp = round(float(timestamp.strip()), 2)
                if timestamp not in self.timestamps:
                    self.timestamps.append(timestamp)

    def plotGraph(self):
        # Parse time_dict to get X Axis (timestamps) and Y Axis (Number of slots for specifc timestamp).
        for worker in self.time_dict:
            y_ax = self.time_dict[worker]
            x_ax = self.timestamps
            plt.plot(x_ax, y_ax, label=worker)
        
        plt.xlabel("Timestamp")
        plt.ylabel("Number of slots being used")
        plt.title("Slots vs Time for " + self.parse_file[5:-10]) # [:-10] for removing "Worker.log" in the title.
        plt.legend()
        plt.show()  



def main():

    # Check if arguments passed satisfy the requirements
    if(len(sys.argv) < 2):
        print("Incorrect Usage. Correct Usage: python3 log_analysis.py <number of workers>")
        sys.exit(0)

    # Set number of workers. Needed to parse the log file
    num_workers = int(sys.argv[1])

    # Round Robin Calculation time
    task = calc_time('logs/taskRR.log')
    task.parse()
    task.calc_mean_median()

    job = calc_time('logs/jobRR.log')
    job.parse()
    job.calc_mean_median()


    #Random Calculation time
    task = calc_time('logs/taskR.log')
    task.parse()
    task.calc_mean_median()

    job = calc_time('logs/jobR.log')
    job.parse()
    job.calc_mean_median()

    #Least Loaded Calculation time
    task = calc_time('logs/taskLL.log')
    task.parse()
    task.calc_mean_median()

    job = calc_time('logs/jobLL.log')
    job.parse()
    job.calc_mean_median()


    #Round Robin plot
    rr = plotTime('logs/RoundRobinWorker.log', num_workers)
    rr.parse()
    rr.plotGraph()

    #Random plot
    rd = plotTime('logs/RandomWorker.log', num_workers)
    rd.parse()
    rd.plotGraph()

    #Least Loaded plot
    ll = plotTime('logs/LeastLoadedWorker.log', num_workers)
    ll.parse()
    ll.plotGraph()    



if __name__ == "__main__":
    main()
