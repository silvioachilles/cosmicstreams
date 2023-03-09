import numpy as np
import json
import zmq

from cosmicstreams.utils import utils


RECO_PORT = 37016
RECO_TOPIC = b'rec'


KEY_SHAPE_Y = 'shape_y'
KEY_SHAPE_X = 'shape_x'
KEY_DTYPE = 'dtype'
KEY_BYTEORDER = 'byteorder'
KEY_ORDER = 'order'
KEY_OBJ_PIXELSIZE_Y = 'obj_pixelsize_y'
KEY_OBJ_PIXELSIZE_X = 'obj_pixelsize_x'


class RecSocketPub:
    def __init__(self, pub_port=None, pub_topic=None, socket=None):
        self.pub_port = pub_port
        if self.pub_port is None:
            self.pub_port = RECO_PORT

        self.pub_topic = pub_topic
        if self.pub_topic is None:
            self.pub_topic = RECO_TOPIC

        if socket is None:
            self.context = zmq.Context()
            self.pub_socket = self.context.socket(zmq.PUB)
            self.pub_socket.bind("tcp://*:{}".format(self.pub_port))
        else:
            self.pub_socket = socket

    def send_reco(self, reco, pixelsize_y=0.0, pixelsize_x=0.0):
        metadata = utils.get_array_metadata(reco)
        metadata[KEY_OBJ_PIXELSIZE_Y] = pixelsize_y
        metadata[KEY_OBJ_PIXELSIZE_X] = pixelsize_x

        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq,
            reco.data
        ])


class RecSocketSub:
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
