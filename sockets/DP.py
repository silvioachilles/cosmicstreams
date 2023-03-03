import numpy as np
import json
import zmq


DP_TOPIC = b'd'
DP_PORT = 7015

KEY_POSX = 'posx'
KEY_POSY = 'posy'
KEY_INDEX = 'index'
KEY_SHAPE = 'shape'
KEY_DTYPE = 'dtype'
KEY_IDENT = 'ident'


class DPSocketPub:
    def __init__(self, pub_port=None, pub_topic=None):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)

        self.pub_port = pub_port
        if self.pub_port is None:
            pub_port = DP_PORT

        self.pub_socket.bind(f"tcp://*:{pub_port}")

        self.pub_topic = pub_topic
        if self.pub_topic is None:
            self.pub_topic = DP_TOPIC

    def send_dp(self, identifier, dp, index, posy, posx, metadata=None):
        if metadata is None:
            metadata = {}

        metadata[KEY_IDENT] = identifier
        metadata[KEY_SHAPE] = int(dp.shape[0]), int(dp.shape[1])
        metadata[KEY_DTYPE] = dp.dtype.name
        metadata[KEY_INDEX] = int(index)
        metadata[KEY_POSY] = int(posy)
        metadata[KEY_POSX] = int(posx)

        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq,
            dp.data
        ])


class DPSocketSub:
    def __init__(self, sub_host, sub_port=None, sub_topic=None):
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)

        self.sub_host = sub_host
        self.sub_port = sub_port
        if self.sub_port is None:
            self.sub_port = DP_PORT

        self.sub_socket.connect(f"tcp://{self.sub_host}:{self.sub_port}")

        self.sub_topic = sub_topic
        if self.sub_topic is None:
            self.sub_topic = DP_TOPIC

        self.sub_socket.setsockopt(zmq.SUBSCRIBE, self.sub_topic)

    def recv_dp(self):
        zmq_topic, metadata_bytes, dp_bytes = self.sub_socket.recv_multipart()
        metadata = json.loads(metadata_bytes.decode())

        dp = np.frombuffer(dp_bytes, dtype=metadata[KEY_DTYPE])
        dp = dp.reshape(metadata[KEY_SHAPE])

        return metadata[KEY_IDENT], dp, metadata[KEY_INDEX], metadata[KEY_POSY], metadata[KEY_POSX], metadata
