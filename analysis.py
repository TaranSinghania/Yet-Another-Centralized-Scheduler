"""
Performing Log Analysis
Functions: 
* Parses log file specified
* Calculates mean and median task completion time
* Calculates mean and median job completion time
* Plots number of tasks scheduled on each machine vs time
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
            logs = logs[1:]
        

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

        print("The mean ", self.file[:-4], " completion time is: ", mean, " seconds")
        print("The median ", self.file[:-4], " completion time is: ", median, " seconds")
        print("\n")

        return mean, median


class plotTime:
    """
    Plots the Timestamp vs Number of slots used graph.
    Contains a parser to parse the log file
    Contains a function to plot the line graph.
    """
    def __init__(self, parse_file):
        self.parse_file = parse_file
        self.time_dict = {} # Dictionary containing: Worker_ID -> Slots Used
        self.timestamps = [] # Timestamps in a list
        self.key = "Worker "

    def parse(self):
        with open(self.parse_file, 'r') as f:
            # Ignore header
            logs = f.readlines()
            self.num_workers = int(logs[0].strip())
            logs = logs[2:]
        
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
                timestamp = float(timestamp.strip())
                if timestamp not in self.timestamps:
                    self.timestamps.append(timestamp)

    def plotGraph(self):
        # Parse time_dict to get X Axis (timestamps) and Y Axis (Number of slots for specifc timestamp).
        for worker in self.time_dict:
            y_ax = self.time_dict[worker]
            x_ax = self.timestamps
            plt.plot(x_ax, y_ax, label=worker, marker='o')
        
        plt.xlabel("Timestamp")
        plt.ylabel("Number of slots being used")
        plt.title("Slots vs Time for " + self.parse_file[:-10]) # [:-10] for removing "Worker.log" in the title.
        plt.legend()
        plt.show()  


def main():
    # Round Robin Calculation time
    task = calc_time('taskRR.log')
    task.parse()
    task.calc_mean_median()

    job = calc_time('jobRR.log')
    job.parse()
    rr_job_mean, rr_job_median = job.calc_mean_median()


    # Random Calculation time
    task = calc_time('taskR.log')
    task.parse()
    task.calc_mean_median()

    job = calc_time('jobR.log')
    job.parse()
    r_job_mean, r_job_median = job.calc_mean_median()

    # Least Loaded Calculation time
    task = calc_time('taskLL.log')
    task.parse()
    task.calc_mean_median()

    job = calc_time('jobLL.log')
    job.parse()
    ll_job_mean, ll_job_median = job.calc_mean_median()


    # Plotting bar chart for comparison [JOBS]
    labels = ['RoundRobin', 'Random', 'LeastLoaded']
    job_mean = [rr_job_mean, r_job_mean, ll_job_mean]
    job_median = [rr_job_median, r_job_median, ll_job_median]

    # Plotting mean job completion time for different algorithms
    plt.bar(labels, job_mean)
    plt.xlabel("Scheduling Algorithms")
    plt.ylabel("Time")
    plt.title("Mean Job completion time")
    plt.show()

    # Plotting median job completion time for different algorithms
    plt.bar(labels, job_median)
    plt.xlabel("Scheduling Algorithms")
    plt.ylabel("Time")
    plt.title("Median Job completion time")
    plt.show()

    # Round Robin plot
    rr = plotTime('RoundRobinWorker.log')
    rr.parse()
    rr.plotGraph()

    # Random plot
    rd = plotTime('RandomWorker.log')
    rd.parse()
    rd.plotGraph()

    # Least Loaded plot
    ll = plotTime('LeastLoadedWorker.log')
    ll.parse()
    ll.plotGraph()    


if __name__ == "__main__":
    main()
