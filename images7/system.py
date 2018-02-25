import logging
import configparser
import os
import threading
import json
import jsondb
import socket
import zmq

from images7.config import Config, resolve_path, StorageType


def current_system() -> 'System':
    return current_system.system
current_system.system = None


class System:
    def __init__(self, config: Config):
        self.config = config
        self.hostname = socket.getfqdn()
        self.name = config.system.name
        self.close_hooks = []

        self.setup_filesystem()
        self.setup_database()
        self.setup_http()
        self.setup_zmq()

        current_system.system = self
        logging.debug("System registered.")

    def select(self, database):
        return self.db[database]

    def setup_filesystem(self):
        self.server = next((x for x in self.config.servers if x.hostname == self.hostname), None)
        assert self.server is not None, 'Missing server config for %s' % self.hostname
        self.main_storage = next((x for x in self.config.storages if x.server == self.hostname and x.type == StorageType.main), None)
        self.cut_storage = next((x for x in self.config.storages if x.server == self.hostname and x.type == StorageType.cut), None)
        self.media_root = resolve_path(self.main_storage.root_path)
        assert self.media_root is not None, 'Missing media root!'
        os.makedirs(self.media_root, exist_ok=True)
        logging.debug("Media root path: %s", self.media_root)

    def setup_zmq(self):
        logging.debug("Setting up ZeroMQ context...")
        self.zmq_config = next((x for x in self.config.zmqs if x.server == self.hostname), None)
        assert self.zmq_config is not None, 'Missing zmq config for %s' % self.hostname
        self.zmq_context = zmq.Context(1)
        logging.debug("ZeroMQ context setup")

    def zmq_req_lazy_pirate(self, channel, timeout=2500, retries=3):
        endpoint = 'ipc://' + channel

        logging.debug('Connecting to server...')
        client = self.zmq_context.socket(zmq.REQ)
        client.bind(endpoint)

        poll = zmq.Poller()
        poll.register(client, zmq.POLLIN)

        reply = None
        sequence = 0
        retries_left = retries
        while retries_left:
            sequence += 1
            request = str(sequence).encode('utf8')
            logging.debug('Sending %i...', sequence)
            client.send(request)

            expect_reply = True
            while expect_reply:
                socks = dict(poll.poll(timeout))
                if socks.get(client) == zmq.POLLIN:
                    reply = client.recv()
                    if not reply:
                        break
                    if int(reply) == sequence:
                        # Server replied OK
                        logging.debug('Server replied %i (good)', int(reply))
                        retries_left = 0
                        expect_reply = False
                    else:
                        logging.debug('Server replied %i (bad)', int(reply))
                        #pass # bad server reply

                else:
                    # No reponse from server, retry...
                    logging.debug('Closing down...')
                    client.setsockopt(zmq.LINGER, 0)
                    client.close()
                    poll.unregister(client)
                    retries_left -= 1
                    if retries_left == 0:
                        logging.debug('Give up!')
                        break
                    # Reconnecting
                    logging.debug('Reconnect...')
                    client = self.zmq_context.socket(zmq.REQ)
                    client.connect(endpoint)
                    poll.register(client, zmq.POLLIN)
                    client.send(request)

        return reply

    def zmq_rep_lazy_pirate(self, channel, handler):
        endpoint = 'ipc://' + channel

        server = self.zmq_context.socket(zmq.REP)
        server.connect(endpoint)

        while True:
            request = server.recv()
            handler()
            server.send(request)

    def zmq_req_payload(self, channel, payload, timeout=2500, retries=3):
        endpoint = 'ipc://' + channel

        logging.debug('Connecting to %s...', endpoint)
        client = self.zmq_context.socket(zmq.REQ)
        client.bind(endpoint)

        poll = zmq.Poller()
        poll.register(client, zmq.POLLIN)

        sequence = 0
        retries_left = retries
        while retries_left:
            sequence += 1
            request = [str(sequence).encode('utf8'), payload.to_json().encode('utf8')]
            logging.debug('Sending %i...', sequence)
            client.send_multipart(request)
            logging.debug('Sent %i. Waiting.', sequence)

            expect_reply = True
            while expect_reply:
                socks = dict(poll.poll(timeout))
                if socks.get(client) == zmq.POLLIN:
                    reply = client.recv_multipart()
                    if not reply:
                        break
                    if int(reply[0]) == sequence:
                        # Server replied OK
                        logging.debug('Server replied %i (good)', int(reply))
                        retries_left = 0
                        expect_reply = False
                    else:
                        logging.debug('Server replied %i (bad)', int(reply))
                        #pass # bad server reply

                else:
                    # No reponse from server, retry...
                    logging.debug('Closing down...')
                    client.setsockopt(zmq.LINGER, 0)
                    client.close()
                    poll.unregister(client)
                    retries_left -= 1
                    if retries_left == 0:
                        logging.debug('Give up!')
                        break
                    # Reconnecting
                    logging.debug('Reconnect...')
                    client = context.socket(zmq.REQ)
                    client.connect(endpoint)
                    poll.register(client, zmq.POLLIN)
                    client.send(request)

        return reply

    def setup_database(self):
        db_config = next((x for x in self.config.databases if x.server == self.hostname), None)
        assert db_config is not None, 'Missing database config!'
        db_root = resolve_path(db_config.path)

        self.db = dict()

        def get_taken_ts(o):
            metadata = o.get('metadata')
            if metadata is None: return None
            return metadata.get('taken_ts')

        def get_taken_ts_tuple(o):
            t = get_taken_ts(o)
            if t is None: return None
            return tuple(int(x) for x in t[:10].split('-')) + (t[11:],), None

        def get_taken_date_tuple(o):
            t = get_taken_ts(o)
            if t is None: return None
            return tuple(int(x) for x in t[:10].split('-')), None

        def get_taken_date(o, get_value):
            t = get_taken_ts(o)
            if t is None: return None
            return t[:10], get_value(o)

        def get_source(o):
            metadata = o.get('metadata')
            if metadata is None: return None
            source = metadata.get('source')
            original_filename = metadata.get('original_filename')
            if not all([source, original_filename]): return None
            return (source, original_filename), None

        def sum_per(field, values):
            result = {}
            for value in values:
                v = value.get(field)
                if v in result:
                    result[v] += 1
                else:
                    result[v] = 1
            result['total'] = len(values)
            return result

        def each_tag(value):
            for subvalue in value.get('tags', []):
                yield (subvalue, None)

        def each_tag_with_taken_ts(value):
            for subvalue in value.get('tags', []):
                yield ((subvalue, get_taken_ts(value)), None)

        def each_file_reference(value):
            for file in value.get('files', []):
                yield file.get('reference'), None

        entry = jsondb.Database(os.path.join(db_root, 'entry'))
        entry.define('by_taken_ts', get_taken_ts_tuple)
        entry.define(
            'state_by_date',
            lambda o: get_taken_date(o, lambda oo: {'state': oo['state']}),
            lambda keys, values, rereduce: sum_per('state', values)
        )
        entry.define('by_date', get_taken_date_tuple)
        entry.define(
            'by_state',
            lambda o: (o['state'], None)
        )
        entry.define(
            'by_state_and_taken_ts',
            lambda o: ((o['state'], get_taken_ts(o)), None)
        )
        entry.define('by_source', get_source)
        entry.define(
            'by_tag',
            each_tag,
            lambda keys, values, rereduce: len(values),
        )
        entry.define('by_tag_and_taken_ts', each_tag_with_taken_ts)
        entry.define('by_file_reference', each_file_reference)
        self.db['entry'] = entry

        file = jsondb.Database(os.path.join(db_root, 'file'))
        file.define(
            'by_reference',
            lambda o: (o['reference'], None)
        )
        self.db['file'] = file

        date = jsondb.Database(os.path.join(db_root, 'date'))
        date.define(
            'by_date',
            lambda o: (o['_id'], None)
        )
        self.db['date'] = date

        job = jsondb.Database(os.path.join(db_root, 'job'))
        job.define(
            'by_state',
            lambda o: ((o['state'], o['release'], o['priority']), None),
        )
        job.define(
            'by_updated',
            lambda o: (10000000000 - int(o['updated']), None),
        )
        job.define(
            'stats',
            lambda o: (None, {'state': o['state']}),
            lambda keys, values, rereduce: sum_per('state', values),
        )
        self.db['job'] = job

    def setup_http(self):
        self.http = next((x for x in self.config.https if x.server == self.hostname), None)
        assert self.http is not None, 'Missing HTTP config for %s' % self.hostname

    def close(self):
        for close_hook in self.close_hooks:
            close_hook()
        for db in self.db.values():
            #db.close()
            pass
