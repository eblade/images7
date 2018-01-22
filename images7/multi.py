import threading
import time
import logging
import zmq
import uuid
from collections import OrderedDict


class QueueClient(threading.Thread):
    def __init__(self, address, id=None):
        self.id = id or uuid.uuid4().hex
        self.address = address
        threading.Thread.__init__(self)

    def _setup(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        identity = 'client-%s' % str(self.id)
        self.socket.identity = identity.encode('ascii')
        self.socket.connect(self.address)
        logging.debug("Client %s started at %s", identity, self.address)
        poll = zmq.Poller()
        poll.register(self.socket, zmq.POLLIN)

    def _close(self):
        self.socket.close()
        self.context.term()

    def run(self):
        self._setup()

        for request in self.do():
            self.send(request)

        self._close()

    def __exit__(self, type, value, traceback):
        self._close()

    def do(self):
        pass # override this!

    def send(self, request):
        if hasattr(request, 'to_json'):
            request = request.to_json()
        self.socket.send_string(request)

    def __enter__(self):
        self._setup()
        return self


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
