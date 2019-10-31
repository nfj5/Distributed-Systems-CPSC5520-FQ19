# Nicholas Jones
# CPSC 5520 - FQ19
# Professor Lundeen
# Lab 1

import socket
import sys
import pickle

# alert for incorrect usage
if len(sys.argv) != 3:
    print("Usage:\tpython lab1.py [host] [port]")
    exit()

host = sys.argv[1]
port = int(sys.argv[2])

# ask the gcd for members via protocol
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((host, port))
    sock.send(pickle.dumps("JOIN"))
    
    print("JOIN ('" + host + "', " + str(port) + ")")
    
    response = pickle.loads(sock.recv(1024))
    sock.disconnect()

members = list(response)

for member in members:
	
	m_host = member['host']
	m_port = int(member['port'])

	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
		
		sock.settimeout(1.5)

		# make sure we are able to connect
		try:
			sock.connect((m_host, m_port))
		except Exception as e:
			print("Failed to connect: " + str(e))
		
		# connecting went smoothly
		else:
			sock.send(pickle.dumps("HELLO"))

			print("HELLO to " + str(member))
			m_response = pickle.loads(sock.recv(1024))
			print(m_response)
			sock.disconnect()