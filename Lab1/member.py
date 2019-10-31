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

class GroupMember(socketserver.BaseRequestHandler):
    """
    A Group Member that acts as a server and responds to peers' messages

    For Lab1, we respond only to HELLO messages.
    """

    def handle(self):
        """
        Handles the incoming messages - expects only 'HELLO' messages
        """
        raw = self.request.recv(BUF_SZ)  # self.request is the TCP socket connected to the client
        try:
            message = pickle.loads(raw)
        except (pickle.PickleError, KeyError):
            response = bytes('Expected a pickled message, got ' + str(raw)[:100] + '\n', 'utf-8')
        else:
            if message != 'HELLO':
                response = pickle.dumps('Unexpected message: ' + str(message))
            else:
                message = ('OK', 'Happy to meet you, {}'.format(self.client_address))
                response = pickle.dumps(message)
        self.request.sendall(response)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python member.py PORT")
        exit(1)
    port = int(sys.argv[1])
    with socketserver.TCPServer(('', port), GroupMember) as server:
        server.serve_forever()
