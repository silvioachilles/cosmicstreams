import json

import cosmicstreams.sockets.Defaults as D
import cosmicstreams.sockets.Keys as K
from cosmicstreams.sockets.SocketPub import SocketPub
from cosmicstreams.sockets.SocketSub import SocketSub
from cosmicstreams.utils import utils


class FrameSocketPub(SocketPub):
    DEFAULT_PORT = D.FRAME_PORT
    DEFAULT_TOPIC = D.FRAME_TOPIC

    def __init__(self, pub_port=None, pub_topic=None, socket=None):
        super().__init__(pub_port, pub_topic, socket)

    def send_frame(self, identifier, frame, index, posy, posx, metadata=None):
        if metadata is None:
            metadata = {}

        metadata = utils.get_array_metadata(frame, metadata)

        metadata[K.KEY_IDENT] = identifier
        metadata[K.KEY_INDEX] = int(index)
        metadata[K.KEY_POSY] = int(posy)
        metadata[K.KEY_POSX] = int(posx)

        metadata_zmq = json.dumps(metadata).encode()

        self.pub_socket.send_multipart([
            self.pub_topic,
            metadata_zmq,
            frame.data
        ])


class FrameSocketSub(SocketSub):
    DEFAULT_PORT = D.FRAME_PORT
    DEFAULT_TOPIC = D.FRAME_TOPIC

    def __init__(self, sub_host, sub_port=None, sub_topic=None):
        super().__init__(sub_host, sub_port, sub_topic)

    def recv_frame(self):
        zmq_topic, metadata_bytes, frame_bytes = self.sub_socket.recv_multipart()
        metadata = json.loads(metadata_bytes.decode())

        frame = utils.get_array(
            frame_bytes,
            metadata[K.KEY_DTYPE],
            metadata[K.KEY_SHAPE_Y],
            metadata[K.KEY_SHAPE_X],
            metadata[K.KEY_BYTEORDER],
            metadata[K.KEY_ORDER]
        )

        return metadata[K.KEY_IDENT], frame, metadata[K.KEY_INDEX], metadata[K.KEY_POSY], metadata[K.KEY_POSX], metadata
