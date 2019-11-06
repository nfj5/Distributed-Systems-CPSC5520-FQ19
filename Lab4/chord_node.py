"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
Collaborated with Pabi
:Authors: Nicholas Jones
:Version: fq19-01
"""

# rewrite message to call_rpc

import sys
from enum import Enum
import random
import threading
import socket
import pickle
import hashlib

BUFFER_SIZE = 4096
HASH_BITS = 160  # SHA-1 is a 160-bit hashing algorithm
MAX_KEY = 360

FINGER_SIZE = 3
NODES = 2**FINGER_SIZE


class Protocol(Enum):
	JOIN = 'JOIN'
	POPULATE = 'POPULATE'
	QUERY = 'QUERY'
	SUCCESSOR = 'SUCCESSOR'


class ModRange(object):
	"""
	Range-like object that wraps around 0 at some divisor using modulo arithmetic.

	>>> mr = ModRange(1, 4, 100)
	>>> mr

	>>> 1 in mr and 2 in mr and 4 not in mr
	True
	>>> [i for i in mr]
	[1, 2, 3]
	>>> mr = ModRange(97, 2, 100)
	>>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
	True
	>>> [i for i in mr]
	[97, 98, 99, 0, 1]
	"""

	def __init__(self, start, stop, divisor):
		self.divisor = divisor
		self.start = start % self.divisor
		self.stop = stop % self.divisor
		# we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
		if self.start < self.stop:
			self.intervals = (range(self.start, self.stop),)
		else:
			self.intervals = (range(self.start, self.divisor), range(0, self.stop))

	def __repr__(self):
		""" Something like the interval|node charts in the paper """
		return ''.format(self.start, self.stop, self.divisor)

	def __contains__(self, id):
		""" Is the given id within this finger's interval? """
		for interval in self.intervals:
			if id in interval:
				return True
		return False

	def __len__(self):
		total = 0
		for interval in self.intervals:
			total += len(interval)
		return total

	def __iter__(self):
		return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
	""" Iterator class for ModRange """

	def __init__(self, mr, i, j):
		self.mr, self.i, self.j = mr, i, j

	def __iter__(self):
		return ModRangeIter(self.mr, self.i, self.j)

	def __next__(self):
		if self.j == len(self.mr.intervals[self.i]) - 1:
			if self.i == len(self.mr.intervals) - 1:
				raise StopIteration()
			else:
				self.i += 1
				self.j = 0
		else:
			self.j += 1
		return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
	"""
	Row in a finger table.

	>>> fe = FingerEntry(0, 1)
	>>> fe

	>>> fe.node = 1
	>>> fe

	>>> 1 in fe, 2 in fe
	(True, False)
	>>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
	(, )
	>>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
	(, , )
	>>> fe = FingerEntry(3, 3, 0)
	>>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
	True
	"""

	def __init__(self, n, k, node=None):
		if not (0 <= n < NODES and 0 < k <= FINGER_SIZE):
			raise ValueError('invalid finger entry values')
		self.start = (n + 2 ** (k - 1)) % NODES
		self.next_start = (n + 2 ** k) % NODES if k < FINGER_SIZE else n
		self.interval = ModRange(self.start, self.next_start, NODES)
		self.node = node

	def __repr__(self):
		""" Something like the interval|node charts in the paper """
		return ''.format(self.start, self.next_start, self.node)

	def __contains__(self, id):
		""" Is the given id within this finger's interval? """
		return id in self.interval


class ChordNode(object):
	def __init__(self):
		"""
		Initialize the chord node with an empty finger table and random listening port
		"""
		self.listen_addr = ('localhost', random.randint(1025, 2025))
		self.finger = []
		self.predecessor = None
		self.successor = None
		self.node_id = self.get_node_id(self.listen_addr)

		print("Generated Node ID: {}".format(self.node_id))

		# default interval = [node_id, node_id)
		self.start = self.node_id
		self.end = self.node_id

		self.finger_id = {'start': self.start, 'end': self.end, 'node_id': self.node_id, 'addr': self.listen_addr}
		# finger_id = {start, interval, node}
		# interval = [finger[k].start, finger[k+1]) - if k+1 > MAX_KEY, replace k+1 with me
		# put garbage data in finger[0] - one-based table
		# node = location of the first node in the interval

		# ChordNode objects
		# successor = finger[1].node
		# predecessor

	def run(self, node_addr):
		"""
		Start the listening thread and join the network if the connection port isn't 0
		:param node_addr: the address of another node
		"""
		listen_thr = threading.Thread(target=self.listener)
		listen_thr.start()

		# join the network through a node, if we know of one
		if node_addr[1] == 0:
			print("Starting a new network")

			self.predecessor = self.finger_id
			self.successor = self.finger_id

			# initialize all elements of the finger table to me
			for i in range(HASH_BITS):
				self.finger.append(self.finger_id)

		else:
			self.join_network(node_addr)

	def get_node_id(self, addr):
		return int.from_bytes(hashlib.sha1(pickle.dumps(addr)).digest(), byteorder='little') % MAX_KEY

	def find_successor(self, node_id):
		node = self.find_predecessor(node_id)
		return node['end']  # return successor

	def find_predecessor(self, node_id):
		"""
		Given a node_id, find the predecessor for that node
		:param node_id: the node to find the predecessor of
		"""
		other_node = self.finger_id

		while not (other_node['start'] < node_id or (node_id <= other_node['end']) or other_node['start'] > other_node['end']):
			if other_node['node_id'] != self.node_id:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
					sock.connect(other_node['addr'])
					other_node = self.message(sock, Protocol.SUCCESSOR, None)
					sock.close()
			else:
				other_node = self.closest_preceding_finger(node_id)
				print(other_node['node_id'])
		# other_node = other_node.closest_preceding_finger(id)

		return other_node

	def closest_preceding_finger(self, node_id):
		"""
		Find the closest finger node preceding the given node
		:param node_id: the node to find closest preceding finger of
		"""
		for i in range(HASH_BITS - 1, 0, -1):
			if self.node_id < self.finger[i]['node_id'] < node_id:
				return self.finger[i]

		return self.finger_id

	def init_finger_table(self, node_addr):
		pass

	def listener(self):
		"""
		Threaded helper method for handling incoming connections
		"""
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.bind(self.listen_addr)
		sock.listen()

		print("Started listening at {}".format(self.listen_addr))
		while True:
			conn, addr = sock.accept()
			protocol, msg = pickle.loads(conn.recv(BUFFER_SIZE))

			# someone is asking me to help them join the network
			if protocol == Protocol.JOIN:
				print("JOIN request from {}".format(msg))

				# finger[0].start of requester = requester.node_id + 1
				node_id = self.get_node_id(msg) + 1
				print("Sent SUCCESSOR as", self.find_successor(node_id))
				conn.sendall(pickle.dumps(self.find_successor(node_id)))

			elif protocol == Protocol.SUCCESSOR:
				node_id = self.get_node_id(msg)
				conn.sendall(pickle.dumps(self.closest_preceding_finger(node_id)))

			elif protocol == Protocol.POPULATE:
				pass

			elif protocol == Protocol.QUERY:
				pass

			else:
				print("Unrecognized message type ({}) from {}".format(protocol, addr))

	def join_network(self, node_addr):
		"""
		Send a JOIN request to the node that we know of
		:param node_addr: the address of the node to send the request to
		"""
		print("Joining a network using {}".format(node_addr))

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.connect(node_addr)
			self.finger.append(self.message(sock, Protocol.JOIN, self.listen_addr, cfr=True))  # TODO update response to return the successor
			print("Received SUCCESSOR as", self.finger[0])

	def message(self, conn, protocol, msg, buffer_size=BUFFER_SIZE, cfr=True):
		"""
		Helper method for packaging and sending a message over a socket
		:param conn: the socket to send the message over
		:param protocol: the protocol to use when sending the message
		:param msg: the data to be sent
		:param buffer_size: how much information to listen for in response to the sent message
		:param cfr: boolean indicating whether or not we care if there is a response
		:return: the response to the message that was sent, if there is one
		"""
		try:
			print("Sending {} to {}".format(protocol.value, conn.getsockname()))

			conn.sendall(pickle.dumps((protocol, msg)))
			if cfr:
				return pickle.loads(conn.recv(buffer_size))
		except Exception as e:
			print(e)


if __name__ == '__main__':
	if len(sys.argv) != 3:
		print("Usage: python chord_node.py [node_host] [node_port]")
		exit(1)

	address = (sys.argv[1], int(sys.argv[2]))
	node = ChordNode()
	node.run(address)
