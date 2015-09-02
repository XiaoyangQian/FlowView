import socket

class Graphite_Manager():

    def __init__(self):
        self.CARBON_SERVER = 'oprdmetas303'
        self.CARBON_PORT = 2003


    def send(self,message):
        sock = socket.socket()
        sock.connect((self.CARBON_SERVER,self.CARBON_PORT))
        sock.sendall(message)
        sock.close()

