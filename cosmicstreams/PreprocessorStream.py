from cosmicstreams.sockets.Frame import FrameSocketPub
from cosmicstreams.sockets.Start import StartSocketPub
from cosmicstreams.sockets.Stop import StopSocketPub


class PreprocessorStream:
    def __init__(
            self,
            port_start=None,
            topic_start=None,
            port_dp=None,
            topic_dp=None,
            port_end=None,
            topic_end=None,
    ):
        self.port_start = port_start
        self.topic_start = topic_start
        self.port_dp = port_dp
        self.topic_dp = topic_dp
        self.port_end = port_end
        self.topic_end = topic_end

        self.socket_start: StartSocketPub = None
        self.socket_frame: FrameSocketPub = None
        self.socket_stop: StopSocketPub = None

        self.bind_sockets()

    def bind_sockets(self):
        self.socket_start = StartSocketPub(
            self.port_start,
            self.topic_start,
        )

        self.socket_frame = FrameSocketPub(
            self.port_dp,
            self.topic_dp,
            self.socket_start.pub_socket,
        )

        self.socket_stop = StopSocketPub(
            self.port_end,
            self.topic_end,
            self.socket_start.pub_socket,
        )

    def close_sockets(self):
        self.socket_start.pub_socket.close()
        self.socket_frame.pub_socket.close()
        self.socket_stop.pub_socket.close()

    def send_start(self, metadata: {}):
        self.socket_start.send_start(metadata)

    def send_frame(
            self,
            identifier,
            data,
            index,
            posy,
            posx,
            metadata=None
    ):
        self.socket_frame.send_frame(identifier, data, index, posy, posx, metadata)

    def send_stop(self, metadata: dict = dict()):
        self.socket_stop.send_stop(metadata)
