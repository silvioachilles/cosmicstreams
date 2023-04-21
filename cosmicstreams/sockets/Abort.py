import json

import cosmicstreams.sockets.Defaults as D
from cosmicstreams.sockets.SocketPub import SocketPub
from cosmicstreams.sockets.SocketSub import SocketSub


class AbortSocketPub(SocketPub):
    DEFAULT_PORT = D.ABORT_PORT
    DEFAULT_TOPIC = D.ABORT_TOPIC

    def __init__(self, pub_port=None, pub_topic=None, socket=None):
        super().__init__(pub_port, pub_topic, socket)

    def send_abort(self, metadata):
        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq
        ])


class AbortSocketSub(SocketSub):
    DEFAULT_PORT = D.ABORT_PORT
    DEFAULT_TOPIC = D.ABORT_TOPIC

    def __init__(self, sub_host, sub_port=None, sub_topic=None):
        super().__init__(sub_host, sub_port, sub_topic)

    def recv_abort(self):
        topic, metadata_zmq = self.sub_socket.recv_multipart()

        return json.loads(metadata_zmq.decode())
