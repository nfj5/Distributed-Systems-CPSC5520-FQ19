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

BUFFER_SIZE = 1024


class Protocol(Enum):
    JOIN = 'JOIN'
    POPULATE = 'POPULATE'
    QUERY = 'QUERY'


class ChordNode(object):
    def __init__(self):
        """
        Initialize the chord node with an empty finger table and random listening port
        """
        self.listen_addr = ('localhost', random.randint(1025, 2025))
        self.finger_table = []

    def run(self, node_addr):
        """
        Start the listening thread and join the network if the connection port isn't 0
        :param node_addr: the address of another node
        """
        listen_thr = threading.Thread(target=self.listener)
        listen_thr.start()

        # join the network through a node, if we know of one
        if node_addr[1] != 0:
            self.join_network(node_addr)
        else:
            print("Starting a new network")

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
                print("JOIN request from {}".format(addr))

    def join_network(self, node_addr):
        """
        Send a JOIN request to the node that we know of
        :param node_addr: the address of the node to send the request to
        """
        print("Joining a network using {}".format(node_addr))

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(node_addr)
            self.message(sock, Protocol.JOIN, self.listen_addr, cfr=False)  # TODO update so that a response is cared for

    def message(self, conn, protocol, msg, buffer_size=BUFFER_SIZE, cfr=True):
        """
        Helper method for packaging and sending a message over a socket
        :param conn: the socket to send the message over
        :param protocol: the protocol to use when sending the message
        :param msg: the data to be sent
        :param buffer_size: how much information to listen for in response to the sent message
        :param cfr: boolean indicating whether or not we care if there is a response
        """
        try:
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
