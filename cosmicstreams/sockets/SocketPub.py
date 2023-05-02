import zmq


class SocketPub:
    DEFAULT_PORT = 38000
    DEFAULT_TOPIC = b'topic'

    def __init__(self, pub_port=None, pub_topic=None, socket=None, hwm=10000):
        self.pub_port = pub_port
        if self.pub_port is None:
            self.pub_port = self.DEFAULT_PORT

        self.pub_topic = pub_topic
        if self.pub_topic is None:
            self.pub_topic = self.DEFAULT_TOPIC

        if socket is None:
            self.context = zmq.Context()
            self.pub_socket = self.context.socket(zmq.PUB)
            self.pub_socket.set_hwm(hwm)
            self.pub_socket.bind("tcp://*:{}".format(self.pub_port))
        else:
            self.pub_socket = socket
