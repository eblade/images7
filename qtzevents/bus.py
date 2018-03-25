#!/usr/bin/env python3


import zmq


class Pub:
    def __init__(self, context, name):
        self.context = context
        self.name = name
        self.socket = context.socket(zmq.PUB)
        self.address = 'inproc://' + name
        self.socket.bind(self.address)

    def send(self, topic, message):
        if hasattr(topic, 'encode'):
            topic = topic.encode()
        if hasattr(message, 'encode'):
            message = message.encode()
        self.socket.send_multipart([topic, message])

    def subscriber(self, *topics):
        return Sub(self, *topics)


class Sub:
    def __init__(self, publisher, *topics):
        self.socket = publisher.context.socket(zmq.SUB)
        self.socket.connect(publisher.address)
        for topic in topics:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def recv(self):
        topic, message = map(lambda x: x.decode(), self.socket.recv_multipart())
        return topic, message


class Push:
    def __init__(self, context, name):
        self.context = context
        self.name = name
        self.socket = context.socket(zmq.PUSH)
        self.address = 'inproc://' + name
        self.socket.bind(self.address)

    def send(self, obj):
        self.socket.send_json(obj)

    def puller(self):
        return Pull(self)


class Pull:
    def __init__(self, pusher):
        self.socket = pusher.context.socket(zmq.PULL)
        self.socket.connect(pusher.address)

    def recv(self):
        return self.socket.recv_json()
