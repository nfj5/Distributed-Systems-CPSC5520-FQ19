"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
Collaborated with Pabi and Joe
:Authors: Nicholas Jones
:Version: fq19-01
"""

import sys
from enum import Enum
import random
import threading
import socket
import pickle
import hashlib

BUFFER_SIZE = 1024
HASH_BITS = 160 # SHA-1 is a 160-bit hashing algorithm
MAX_KEY = 360


class Protocol(Enum):
    JOIN = 'JOIN'
    POPULATE = 'POPULATE'
    QUERY = 'QUERY'
    SUCCESSOR = 'SUCCESSOR'
		

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
        
        # default interval = [node_id, node_id)
        self.start = self.node_id
        self.end = self.node_id
        
        self.finger_id = {'start': self.start, 'end': self.end, 'node_id': self.node_id, 'addr': self.listen_addr}

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
    	return int(hashlib.sha1(pickle.dumps(addr)).hexdigest(), 16) % MAX_KEY
    
    def find_successor(self, node_id):
    	node = self.find_predecessor(node_id)
    	return node['end'] # return successor
    	
    def find_predecessor(self, node_id):
    	"""
    	Given a node_id, find the predecessor for that node
    	:param node_id: the node to find the predecessor of
    	"""
    	other_node = self.finger_id
    	
    	while not (other_node['start'] < node_id or node_id <= other_node['end']):
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
    	for i in range(HASH_BITS-1, 0, -1):
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
                print ("succ", self.find_successor(node_id))
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
            self.finger.append(self.message(sock, Protocol.JOIN, self.listen_addr, cfr=False))  # TODO update response to return the successor
            print (self.finger[0])

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
