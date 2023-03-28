import socket

SERVER = "localhost"
PORT = 9999

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((SERVER, PORT))

print(client.recv(1024).decode())
client.send("Test from client".encode())

