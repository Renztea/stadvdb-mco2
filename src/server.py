import socket

SERVER = "localhost"
PORT = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((SERVER, PORT))
server.listen()
print(f"Server listening at {SERVER}:{PORT}")

while True:
	client, addr = server.accept()
	client.send("Test from server".encode())
	print(client.recv(64).decode())
	client.close()
