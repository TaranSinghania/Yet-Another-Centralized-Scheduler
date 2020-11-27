"""
Helper functions
Does not affect fundamental workflow
"""
BUF_LEN = 4096

# Recevie a message through a socket
def sock_recv(sock):
    # This socket is a server-side socket
    (clientsocket, address) = sock.accept()
    print("CONNECTED from", address)
    data = clientsocket.recv(BUF_LEN)
    clientsocket.close()
    return data.decode()

def sock_send(sock, data: str):
    # This socket is a client-side socket
    data = data.encode()
    # Must buffer this later on
    sock.send(data)