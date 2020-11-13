import os
import sys
import json
import time
import socket
import random
import threading


def randomScheduling(config):
    workerStatus = [0 for i in range(len(config))]

    while(1):
        temp = random.randint(1, len(config)+1)

        if(workerStatus[temp-1] or config[temp-1]["slots"] == 0):
          continue

        return temp - 1

def roundRobinScheduling(config):
    while(1):
        for i in range(0, len(config)):
            if(config[i]["slots"] == 0):
                continue
            return i

def leastLoadedScheduling(config):
    while(1):
        max = -1
        maxIdx = -1

        for i in range(0, len(config)):
            if(config[i]["slots"] > max):
                max = config[i]["slots"]
                maxIdx = i

        if max == -1:
            time.sleep(1)
        else:
            return maxIdx

def jobRequests(config, schedulingAlgorithm): #TODO: Add concurrency, locking and updating the json config for new scheduling 
    HOST = 'localhost'
    PORT = 5000

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, PORT))
    except socket.error as err:
        #print("Binding failed. Error Code: " + str(err[0]) + "Message" +  err[1])
        print("Failed to bind.", err)
        sys.exit()
    
    
    while(1):
        s.listen(1)
        connectionSocket, address = s.accept()
        job = connectionSocket.recv(1024)

        if job == None:
            connectionSocket.close()
            print("No more jobs. All jobs completed!")
            sys.exit()        

        
        for i in range(0, len(job["map_tasks"])):
            dictToSend = {}
            dictToSend = {"job_id": job["job_id"]}

            dictToSend["task_id"] = i["task_id"]
            dictToSend["duration"] = i["duration"]
            dictToSend["type"] = "M"

            if schedulingAlgorithm == "R":
                idx = randomScheduling(config)
            elif schedulingAlgorithm == "RR":
                idx = roundRobinScheduling(config)
            else:
                idx = leastLoadedScheduling(config)

            workerPort = config[idx]["port"]
            s.connect((HOST, workerPort))
            
            s.send(dictToSend)
            receive = s.recv(1024)

            #Update config here


        for i in range(0, len(job["reduce_tasks"])):
            dictToSend = {}
            dictToSend = {"job_id": job["job_id"]}

            dictToSend["task_id"] = i["task_id"]
            dictToSend["duration"] = i["duration"]
            dictToSend["type"] = "R"

            s.send(dictToSend)
            receive = s.recv(1024) 
        
            if schedulingAlgorithm == "R":
                idx = randomScheduling(config)
            elif schedulingAlgorithm == "RR":
                idx = roundRobinScheduling(config)
            else:
                idx = leastLoadedScheduling(config)

            workerPort = config[idx]["port"]
            s.connect((HOST, workerPort))
            
            s.send(dictToSend)
            receive = s.recv(1024)

            #Update config here

        

def workerUpdates(config, schedulingAlgorithm): #TODO: Well, the entire thing
    pass


if __name__ == "__main__":
    if(len(sys.argv) < 3):
        print("Usage: Master.py <path to config file> <Scheduling algorithm - R|RR|LL>")
        sys.exit()

    pathToConfigFile = sys.argv[1]
    schedulingAlgorithm = sys.argv[2]

    with open(pathToConfigFile) as f:
        workerConfig = json.load(f)
        config = workerConfig['workers']

    threadJR = threading.Thread(target=jobRequests, args=(config, schedulingAlgorithm,))
    threadWU = threading.Thread(target=workerUpdates, args=(config, schedulingAlgorithm, ))

    threadJR.start()
    threadWU.start()

    threadJR.join()
    threadWU.join()

    print("Completed all jobs!")