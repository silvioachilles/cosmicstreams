import zmq


class SocketSub:
    DEFAULT_PORT = 38000
    DEFAULT_TOPIC = b'topic'

    def __init__(self, sub_host, sub_port=None, sub_topic=None):
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)

        self.sub_host = sub_host
        self.sub_port = sub_port
        if self.sub_port is None:
            self.sub_port = self.DEFAULT_PORT

        self.sub_topic = sub_topic
        if self.sub_topic is None:
            self.sub_topic = self.DEFAULT_TOPIC

        self.sub_socket.connect(f"tcp://{self.sub_host}:{self.sub_port}")
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, self.sub_topic)

        print("Listening on {}:{} for topic: {}".format(self.sub_host, self.sub_port, self.sub_topic))
