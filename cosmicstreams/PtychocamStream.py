import zmq

from cosmicstreams.sockets.frame import FrameSocketSub
from cosmicstreams.sockets.start import StartSocketSub
from cosmicstreams.sockets.stop import StopSocketSub
from cosmicstreams.sockets.rec import RecSocketPub


class PtychocamStream:
    def __init__(
            self,
            host_start,
            port_start=None,
            topic_start=None,
            host_dp=None,
            port_dp=None,
            topic_dp=None,
            host_end=None,
            port_end=None,
            topic_end=None,
            use_out=False,
            port_out=None,
            topic_out=None,
    ):
        if host_dp is None:
            host_dp = host_start
        if host_end is None:
            host_end = host_start

        self.socket_scan_begin = StartSocketSub(
            host_start,
            port_start,
            topic_start,
        )

        self.socket_dp = FrameSocketSub(
            host_dp,
            port_dp,
            topic_dp,
        )

        self.socket_scan_end = StartSocketSub(
            host_end,
            port_end,
            topic_end,
        )

        self.use_out = use_out
        if self.use_out:
            self.socket_reco = RecSocketPub(
                port_out,
                topic_out,
            )

        self.poller = zmq.Poller()
        self.poller.register(self.socket_scan_begin.sub_socket, zmq.POLLIN)
        self.poller.register(self.socket_dp.sub_socket, zmq.POLLIN)
        self.poller.register(self.socket_scan_end.sub_socket, zmq.POLLIN)

        self.poll_time_ms = 0

    def poll(self):
        return dict(self.poller.poll(self.poll_time_ms))

    def has_scan_started(self):
        sockets = self.poll()
        if self.socket_scan_begin.sub_socket in sockets:
            return True
        else:
            return False

    def get_scan_start_metadata(self):
        return self.socket_scan_begin.recv_metadata()

    def has_scan_finished(self):
        sockets = self.poll()
        if self.socket_scan_end.sub_socket in sockets:
            return True
        else:
            return False

    def get_scan_end_metadata(self):
        return self.socket_scan_end.recv_metadata()

    def has_dp_arrived(self):
        sockets = self.poll()
        if self.socket_dp.sub_socket in sockets:
            return True
        else:
            return False

    def get_dp(self):
        return self.socket_dp.recv_dp()

    def send_reco(self, reco):
        if self.use_out:
            self.socket_reco.send_reco(reco)

    def something_in_queue(self):
        sockets = self.poll()
        return len(sockets) > 0
