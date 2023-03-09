import json

import cosmicstreams.sockets.Defaults as D
import cosmicstreams.sockets.Keys as K
from cosmicstreams.sockets.SocketPub import SocketPub
from cosmicstreams.sockets.SocketSub import SocketSub
from cosmicstreams.utils import utils


class RecSocketPub(SocketPub):
    DEFAULT_PORT = D.REC_PORT
    DEFAULT_TOPIC = D.REC_TOPIC

    def __init__(self, pub_port=None, pub_topic=None, socket=None):
        super().__init__(pub_port, pub_topic, socket)

    def send_rec(self, reco, pixelsize_y=0.0, pixelsize_x=0.0):
        metadata = utils.get_array_metadata(reco)
        metadata[K.KEY_OBJ_PIXELSIZE_Y] = pixelsize_y
        metadata[K.KEY_OBJ_PIXELSIZE_X] = pixelsize_x

        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq,
            reco.data
        ])


class RecSocketSub(SocketSub):
    DEFAULT_PORT = D.REC_PORT
    DEFAULT_TOPIC = D.REC_TOPIC

    def __init__(self, host, sub_port=None, sub_topic=None):
        super().__init__(host, sub_port, sub_topic)

    def recv_rec(self, flags=0):
        topic, metadata_bytes, rec_bytes = self.sub_socket.recv_multipart(flags)

        metadata = json.loads(metadata_bytes.decode())

        obj_pixelsize_y = metadata[K.KEY_OBJ_PIXELSIZE_Y]
        obj_pixelsize_x = metadata[K.KEY_OBJ_PIXELSIZE_X]

        rec = utils.get_array(
            rec_bytes,
            metadata[K.KEY_DTYPE],
            metadata[K.KEY_SHAPE_Y],
            metadata[K.KEY_SHAPE_X],
            metadata[K.KEY_BYTEORDER],
            metadata[K.KEY_ORDER]
        )

        return rec, obj_pixelsize_y, obj_pixelsize_x, metadata
