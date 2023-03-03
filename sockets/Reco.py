import numpy as np
import json
import zmq


RECO_PORT = 7017
RECO_TOPIC = b'r'


class RecoSocketPub:
    def __init__(self, pub_port=None, pub_topic=None):
        self.context = zmq.Context()

        self.pub_port = pub_port
        if self.pub_port is None:
            self.pub_port = RECO_PORT

        self.pub_topic = pub_topic
        if self.pub_topic is None:
            self.pub_topic = RECO_TOPIC

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://*:{}".format(self.pub_port))

    def send_reco(self, reco):
        metadata = {
            'shape': reco.shape,
            'dtype': reco.dtype.name
        }
        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq,
            reco.data
        ])


class RecoSocketSub:
    def __init__(self, host, sub_port=None, sub_topic=None):
        self.context = zmq.Context()

        self.sub_host = host
        self.sub_port = sub_port
        if self.sub_port is None:
            self.sub_port = RECO_PORT

        self.sub_topic = sub_topic
        if self.sub_topic is None:
            self.sub_topic = RECO_TOPIC

        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect("tcp://{}:{}".format(self.sub_host, self.sub_port))
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, self.sub_topic)

    def recv_reco(self, flags=0):
        topic, metadata_zmq, reco = self.sub_socket.recv_multipart(flags)

        metadata = json.loads(metadata_zmq.decode())
        reco = np.frombuffer(reco, dtype=metadata['dtype']).reshape(metadata['shape'])

        return reco
