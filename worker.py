import json
import threading
import socket
import sys

def listener(args):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', args))
    while True:
        s.listen()
        conn, addr = s.accept()
        data = conn.recv(1024)
        task_info = json.loads(data)
        threading.Thread(target=task,args=[task_info,s]).start()
        


def task(args):
    sys.sleep(args[0]["duration"])
    args[0]["status"] = 'done'
    args[1].connect(('127.0.0.1', 5001))
    args[1].sendall(bytes(json.dumps(args[0])))
    args[1].recv(1024)

threading.Thread(target=listener,args=sys.argv[1]).start()

"""
{
	"job_id":"--",
	"type":"m/r",
	"task_id":"--",
	"duration":"--",
}
"""
