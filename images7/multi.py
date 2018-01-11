import threading
import time
import logging
import zmq
import uuid
from collections import OrderedDict


HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1.0
INTERVAL_INIT = 1
INTERVAL_MAX = 32

#  Paranoid Pirate Protocol constants
PPP_READY = b"\x01"      # Signals worker is ready
PPP_HEARTBEAT = b"\x02"  # Signals worker heartbeat


class Worker:
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS


class WorkerQueue:
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        # Look for and kill expired workers
        t = time.time()
        expired = []
        for address, worker in self.queue.items():
            if t > worker.expiry:
                expired.append(address)
        for address in expired:
            logging.warning("Idle worker expired: %s", address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address


def start_queue(context, channel):
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    frontend.bind(channel + '_queue')
    backend.bind(channel + '_workers')

    poll_workers = zmq.Poller()
    poll_workers.register(backend, zmq.POLLIN)

    poll_both = zmq.Poller()
    poll_both.register(frontend, zmq.POLLIN)
    poll_both.register(backend, zmq.POLLIN)

    workers = WorkerQueue()

    logging.info('Job queue available at %s_queue', channel)

    heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    while True:
        if len(workers.queue) > 0:
            poller = poll_both
        else:
            poller = poll_workers
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            frames = backend.recv_multipart()
            if not frames:
                break

            address = frames[0]
            workers.ready(Worker(address))

            # Validate control message or return reply to client
            msg = frames[1:]
            if len(msg) == 1:
                if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                    logging.error('Invalid message from worker: %s', msg)
            else:
                frontend.send_multipart(msg)

            # Send heartbeats to idle workers if it's time
            if time.time() >= heartbeat_at:
                for worker in workers.queue:
                    msg = [worker, PPP_HEARTBEAT]
                    backend.send_multipart(msg)
                heartbeat_at = time.time() + HEARTBEAT_INTERVAL

        if socks.get(frontend) == zmq.POLLIN:
            frames = frontend.recv_multipart()
            if not frames:
                break

            frames.insert(0, workers.next())
            backend.send_multipart(frames)

        workers.purge()

    logging.info('Queue died.')


def worker_socket(context, poller, channel):
    """Helper function that returns a new configured socket
       connected to the Paranoid Pirate queue"""
    worker = context.socket(zmq.DEALER)
    identity = uuid.uuid4()
    worker.setsockopt_string(zmq.IDENTITY, identity.hex)
    poller.register(worker, zmq.POLLIN)
    worker.connect(channel + '_workers')
    logging.info('Worker %s reporting ready', identity.hex)
    worker.send(PPP_READY)
    return worker


def spawn_worker(context, channel):
    poller = zmq.Poller()
    liveness = HEARTBEAT_LIVENESS
    interval = INTERVAL_INIT

    heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    worker = worker_socket(context, poller, channel)

    while True:
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

        # Handle worker activity on backend
        if socks.get(worker) == zmq.POLLIN:
            #  Get message
            #  - 3-part envelope + content -> request
            #  - 1-part HEARTBEAT -> heartbeat
            frames = worker.recv_multipart()
            if not frames:
                break # Interupted

            if len(frames) == 3:
                addr, a, b = frames
                logging.debug(addr)
                logging.debug(a)
                logging.debug(b)

                worker.send_multipart(frames)
                liveness = HEARTBEAT_LIVENESS

                # work
                time.sleep(1)

            elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                #logging.debug('Queue heartbeat')
                liveness = HEARTBEAT_LIVENESS
            else:
                logging.error('Invalid message %s', frames)

            interval = INTERVAL_INIT

        else:
            liveness -= 1
            if liveness == 0:
                logging.warning('Heartbeat failure, cannot reach queue')
                logging.warning('Reconnecting in %0.2fs…', interval)
                time.sleep(interval)

                if interval < INTERVAL_MAX:
                    interval *= 2

                poller.unregister(worker)
                worker.setsockopt(zmq.LINGER, 0)
                worker.close()
                worker = worker_socket(context, poller, channel)
                liveness = HEARTBEAT_LIVENESS

        if time.time() > heartbeat_at:
            heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            #logging.debug('Worker heartbeat')
            worker.send(PPP_HEARTBEAT)
