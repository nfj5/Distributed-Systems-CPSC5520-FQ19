"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Kevin Lundeen
:Version: f19-02
"""
import pickle
import socketserver
import sys

BUF_SZ = 1024  # tcp receive buffer size


class GroupCoordinatorDaemon(socketserver.BaseRequestHandler):
    """
    A Group Coordinator Daemon (GCD) which will respond with a list of potential group members to a text message JOIN
    with list of group members to contact.

    For Lab1, we just respond with a fixed list of two servers.
    """
    JOIN_RESPONSE = [{'host': 'cs1.seattleu.edu', 'port': 21313},
                     {'host': 'cs2.seattleu.edu', 'port': 33313},
                     {'host': 'localhost', 'port': 23015}]

    def handle(self):
        """
        Handles the incoming messages - expects only 'JOIN' messages
        """
        raw = self.request.recv(BUF_SZ)  # self.request is the TCP socket connected to the client
        print(self.client_address)
        try:
            message = pickle.loads(raw)
        except (pickle.PickleError, KeyError):
            response = bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8')
        else:
            if message != 'JOIN':
                response = pickle.dumps('Unexpected message: ' + str(message))
            else:
                response = pickle.dumps(self.JOIN_RESPONSE)
        self.request.sendall(response)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python gcd.py GCDPORT")
        exit(1)
    port = int(sys.argv[1])
    with socketserver.TCPServer(('', port), GroupCoordinatorDaemon) as server:
        server.serve_forever()
