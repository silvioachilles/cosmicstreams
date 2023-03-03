import zmq
import json


SCAN_BEGIN_TOPIC = b'b'
SCAN_BEGIN_PORT = 7014


class ScanBeginSocketPub:
    def __init__(self, pub_port=None, pub_topic=None):
        self.context = zmq.Context()

        self.pub_port = pub_port
        if self.pub_port is None:
            self.pub_port = SCAN_BEGIN_PORT

        self.pub_topic = pub_topic
        if self.pub_topic is None:
            self.pub_topic = SCAN_BEGIN_TOPIC

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://*:{self.pub_port}")

    def send_metadata(self, metadata):
        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq
        ])


class ScanBeginSocketSub:
    def __init__(self, sub_host, sub_port=None, sub_topic=None):
        self.context = zmq.Context()

        self.sub_host = sub_host
        self.sub_port = sub_port
        if self.sub_port is None:
            self.sub_port = SCAN_BEGIN_PORT

        self.sub_topic = sub_topic
        if self.sub_topic is None:
            self.sub_topic = SCAN_BEGIN_TOPIC

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{self.sub_host}:{self.sub_port}")
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, self.sub_topic)

    def recv_metadata(self):
        topic, metadata_zmq = self.sub_socket.recv_multipart()

        return json.loads(metadata_zmq.decode())
