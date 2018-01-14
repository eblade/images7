import threading
import time
import logging
import zmq
import uuid
from collections import OrderedDict


class QueueClient(threading.Thread):
    def __init__(self, address, id):
        self.id = id
        self.address = address
        self.requests = 0
        threading.Thread.__init__(self)

    def run(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        identity = 'client-%d' % self.id
        self.socket.identity = identity.encode('ascii')
        self.socket.connect(self.address)
        logging.debug("Client %s started at %s", identity, self.address)
        poll = zmq.Poller()
        poll.register(self.socket, zmq.POLLIN)

        for request in self.do():
            self.requests += 1
            self.socket.send_string(request)

        self.socket.close()
        self.context.term()

    def do(self):
        pass # override this!


class QueueServer(threading.Thread):
    def __init__(self, address, Worker, workers=4):
        self.address = address
        self.Worker = Worker
        self.workers = workers
        threading.Thread.__init__(self)

    def run(self):
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(self.address)

        backend = context.socket(zmq.DEALER)
        backend.bind('inproc://backend')

        workers = []
        for i in range(self.workers):
            worker = self.Worker(context)
            worker.start()
            workers.append(worker)

        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()
        context.term()


class QueueWorker(threading.Thread):
    def __init__(self, context):
        threading.Thread.__init__(self)
        self.context = context

    def run(self):
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        logging.debug('Worker started')

        while True:
            ident, msg = worker.recv_multipart()
            logging.debug('Message from %s:\n%s', ident.decode('ascii'), msg.decode('utf8'))
            self.work(msg)

        worker.close()

    def work(self, message):
        pass # override this!
