from cosmicstreams.sockets.frame import FrameSocketPub
from cosmicstreams.sockets.start import StartSocketPub
from cosmicstreams.sockets.stop import StopSocketPub


PORT = 37013


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

        self.socket_scan_begin: StartSocketPub = None
        self.socket_dp: FrameSocketPub = None
        self.socket_scan_end: StopSocketPub = None

        self.bind_sockets()

    def bind_sockets(self):
        self.socket_scan_begin = StartSocketPub(
            self.port_start,
            self.topic_start,
        )

        self.socket_dp = FrameSocketPub(
            self.port_dp,
            self.topic_dp,
        )

        self.socket_scan_end = StopSocketPub(
            self.port_end,
            self.topic_end,
        )

    def close_sockets(self):
        self.socket_scan_begin.pub_socket.close()
        self.socket_dp.pub_socket.close()
        self.socket_scan_end.pub_socket.close()

    def send_scan_begin(self, metadata: {}):
        self.socket_scan_begin.send_metadata(metadata)

    def send_dp(
            self,
            identifier,
            data,
            index,
            posy,
            posx,
            metadata=None
    ):
        self.socket_dp.send_dp(identifier, data, index, posy, posx, metadata)

    def send_scan_end(self, metadata: dict = dict()):
        self.socket_scan_end.send_metadata(metadata)
