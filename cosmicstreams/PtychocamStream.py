import zmq

from cosmicstreams.sockets.Frame import FrameSocketSub
from cosmicstreams.sockets.Start import StartSocketSub
from cosmicstreams.sockets.Stop import StopSocketSub
from cosmicstreams.sockets.Rec import RecSocketPub
from cosmicstreams.sockets.Abort import AbortSocketSub


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
            host_abort=None,
            port_abort=None,
            topic_abort=None,
            use_out=False,
            port_out=None,
            topic_out=None,
    ):
        if host_dp is None:
            host_dp = host_start
        if host_end is None:
            host_end = host_start
        if host_abort is None:
            host_abort = host_start

        self.socket_start = StartSocketSub(
            host_start,
            port_start,
            topic_start,
        )

        self.socket_frame = FrameSocketSub(
            host_dp,
            port_dp,
            topic_dp,
        )

        self.socket_stop = StopSocketSub(
            host_end,
            port_end,
            topic_end,
        )

        self.socket_abort = AbortSocketSub(
            host_abort,
            port_abort,
            topic_abort,
        )

        self.use_out = use_out
        if self.use_out:
            self.socket_rec = RecSocketPub(
                port_out,
                topic_out,
            )

        self.poller = zmq.Poller()
        self.poller.register(self.socket_start.sub_socket, zmq.POLLIN)
        self.poller.register(self.socket_frame.sub_socket, zmq.POLLIN)
        self.poller.register(self.socket_stop.sub_socket, zmq.POLLIN)
        self.poller.register(self.socket_abort.sub_socket, zmq.POLLIN)

        self.poll_time_ms = 0

    def poll(self):
        return dict(self.poller.poll(self.poll_time_ms))

    def has_scan_started(self):
        sockets = self.poll()
        if self.socket_start.sub_socket in sockets:
            return True
        else:
            return False

    def recv_start(self):
        return self.socket_start.recv_start()

    def has_scan_stopped(self):
        sockets = self.poll()
        if self.socket_stop.sub_socket in sockets:
            return True
        else:
            return False

    def recv_stop(self):
        return self.socket_stop.recv_stop()

    def has_frame_arrived(self):
        sockets = self.poll()
        if self.socket_frame.sub_socket in sockets:
            return True
        else:
            return False

    def recv_frame(self):
        return self.socket_frame.recv_frame()

    def send_rec(self, rec, pixelsize_x=0.0, pixelsize_y=0.0):
        if self.use_out:
            self.socket_rec.send_rec(rec, pixelsize_x=pixelsize_x, pixelsize_y=pixelsize_y)

    def has_scan_aborted(self):
        sockets = self.poll()
        if self.socket_abort.sub_socket in sockets:
            return True
        else:
            return False

    def recv_abort(self):
        return self.socket_abort.recv_abort()

    def something_in_queue(self):
        sockets = self.poll()
        return len(sockets) > 0
