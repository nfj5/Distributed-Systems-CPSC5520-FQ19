"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Nicholas Jones
:Version: f19-01
"""

from datetime import datetime
from enum import Enum
import random
import socket
import sys
import pickle
import time
import threading
import traceback

BUFFER_SIZE = 1024	# how many bytes to accept
MAX_NUM_CONNECTIONS = 5 # how many people to handle at once
TIMEOUT_LIMIT = 1.5	# seconds to wait for a socket to send/recv
WAIT_FOR_COORD = 2	# seconds to wait for a COORDINATOR message

class State(Enum):
	"""
	Enum class to keep track of various peer states.
	"""
	
	IDLE = 'IDLE'	# when nothing is going on
	WAITING_FOR_OK = 'WAIT_OK'	# after an ELECTION message is sent
	WAITING_FOR_VICTOR = 'WAIT_VICTOR'	# once an OK is received

class Lab2(object):
	
	def __init__(self, gcd_host, gcd_port, days_to_birthday, su_id):
		"""
		Constructs a Lab2 object to communicate with the Group Coordinator Daemon.
		:param gcd_host: GCD hostname
		:param gcd_port: GCD port
		:param days_to_birthday: number of days until next birthday
		:param su_id: SeattleU student ID
		"""
		self.gcd_host = gcd_host
		self.gcd_port = int(gcd_port)
		self.identity = (int(days_to_birthday), int(su_id))
		self.peers = []
		self.connections = {}
		self.leader = None
		self.state = State.IDLE
		
	def run(self):
		"""
		Set up the listener thread, then join peers via the GCD.
		After we have a list of peers, start an election.
		"""
		self.start_listener()
		self.join_peers()
		self.start_election()
		
	def start_listener(self):
		"""
		Start a listener thread on a random port (1025-2025)
		"""
		self.host = "localhost"
		self.port = random.randint(1025,2026)
		
		listener = threading.Thread(target=self.thr_listener)
		listener.start()
		
	def join_peers(self):
		"""
		Send a JOIN request to the GCD.
		"""
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
			address = (self.gcd_host, self.gcd_port)
			self.pr_time("JOIN {}".format(address))
			gcd.connect(address)
			self.peers = self.message(gcd, 'JOIN', (self.host, self.port))
			self.pr_time("Recieved list of {} peers from the GCD".format(len(self.peers)))
	
	def start_election(self):
		"""
		Start an election among known peers who are "stronger", let them know who we know
		by sending our list of known peers.
		"""
		self.state = State.WAITING_FOR_OK # put ourself into an election state
		the_greatest = True
		
		for peer in self.peers:
			identity = peer
			address = self.peers[peer]
			if identity[0] < self.identity[0] or (identity[0] == self.identity[0] and identity[1] < self.identity[1]):
				the_greatest = False
				sender = threading.Thread(target=self.thr_send, args=((identity, address), 'ELECTION', self.peers))
				sender.start()
		
		# nobody is a bigger bully than we are, send a message to all peers that we are in charge
		if the_greatest:
			self.declare_victory()
			
	def declare_victory(self):
		"""
		Method to send a coordinator method to all known peers, tell myself that I am the leader
		"""
		for peer in self.peers:
			if not peer == self.identity:
				if peer in self.connections:
					self.connections[peer][0].close()
					del self.connections[peer]
				identity = peer
				address = self.peers[peer]
				sender = threading.Thread(target=self.thr_send, args=((identity, address), 'COORDINATOR', self.peers))
				sender.start()
		self.leader = self.identity
		self.state = State.IDLE
	
	def message(self, sock, protocol, message, buffer_size=BUFFER_SIZE, care_for_response=True):
		"""
		Package the message, send it over the socket and return the response.
		:param sock: the socket to send the message over
		:param protocol: the protocol to indicate in the message header
		:param message: the actual message data
		:param buffer_size: the size of message to expect/accept as a response
		:param care_for_response: indicator of whether or not to wait for a response (default: True)
		"""
		data = (protocol, (self.identity, message))
		
		try:
			sock.sendall(pickle.dumps(data))
			if care_for_response:
				return pickle.loads(sock.recv(buffer_size))
		except Exception as e:
			self.pr_time(e, "msg error")
			return 500
		
	def thr_send(self, peer, protocol, msg):
		"""
		Helper function for multi-threading the delivery of messages.
		:param sock: socket to send data through
		:param msg: the message to be sent
		"""
		sock = self.get_connection(peer)[0]
		
		print("Sending", protocol)
		send_resp = self.message(sock, protocol, msg, care_for_response=(protocol != 'COORDINATOR'))
		
		if send_resp and send_resp == 500: # if we get a connection error, set the socket flag
			self.connections[peer[0]][1] = True
		
		elif protocol == 'ELECTION' and send_resp == 500:
			done = True
			for conn in self.connections:
				if not self.connections[conn][1]:
					done = False
			
			if done:
				for conn in self.connections:
					self.connections[conn][1] = False
				self.declare_victory()
		
		elif protocol == 'ELECTION' and send_resp and send_resp[0] == 'OK':
			self.pr_time("Received OK from a peer. Waiting for victor.")
			self.state = State.WAITING_FOR_VICTOR
				
	def thr_listener(self):
		"""
		Method for listener to run in a separate thread.
		"""
		host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		host_socket.bind((self.host, self.port))
		host_socket.listen(MAX_NUM_CONNECTIONS)
		
		self.pr_time("Listening on port {}".format(self.port), "listener")
		
		# constantly wait for connections
		while True:
			try:
				peer_sock, peer_addr = host_socket.accept()
				self.pr_time("Accepted connection", "listener")
				peer_thread = threading.Thread(target=self.thr_peer, args=(peer_sock, peer_addr))
				peer_thread.start()
			except Exception as e:
				self.pr_time(e, "error")
	
	def thr_peer(self, socket, address):
		"""
		A method for handling an incoming message from a peer node.
		:param socket: the socket object to use for communication
		:param address: the address identifier for the socket
		"""
		msg = pickle.loads(socket.recv(BUFFER_SIZE))
		
		protocol = msg[0]
		data = msg[1]
		
		self.update_peers(data[1])
		
		# someone else has started an election
		if protocol == 'ELECTION':
			self.pr_time("ELECTION message from {}. Sending OK.".format(address))
			self.message(socket, 'OK', None, care_for_response=False)
			
			# if we aren't currently in an election, let's start one
			print(self.state)
			if self.state != State.WAITING_FOR_OK:
				self.start_election()
				
		# someone has declared themselves the winner
		elif protocol == 'COORDINATOR':
			self.state = State.IDLE
			self.pr_time("I am the leader now.", data[0])
			self.leader = data[0]
			
		socket.close()
	
	def update_peers(self, new_peers):
		"""
		Add or update our peers list with one we have received from another peer.
		:param new_peers: the peer list that was received
		"""
		for peer in new_peers:
			self.peers[peer] = new_peers[peer]
	
	def get_connection(self, peer):
		"""
		Keep track of open sockets, open a new socket if one does not exist
		:param peer: tuple of identity and address
		"""
		if not peer[0] in self.connections or self.connections[peer[0]][1]:
			peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			peer_sock.settimeout(TIMEOUT_LIMIT)
			
			try:
				peer_sock.connect(peer[1])
			except Exception as e:
				self.pr_time(e, "conn error")
				pass
			
			self.connections[peer[0]] = [peer_sock, False]
		
		return self.connections[peer[0]]
	
	@staticmethod
	def pr_time(msg, label="main"):
		"""
		Helper method for printing with timestamp.
		:param msg: the content to print
		:param label: who/what is sending the message
		"""
		time_now = datetime.now().strftime("[%H:%M:%S.%f]")
		print(time_now, "<"+str(label)+">", msg)

if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("Usage:\tpython3 lab2.py [host] [port] [days_to_birthday] [su_id]")
		exit(1)
		
	host, port = sys.argv[1:3]
	days_to_birthday = sys.argv[3]
	su_id = sys.argv[4]
    
	lab2 = Lab2(host, port, days_to_birthday, su_id)
	lab2.run()
