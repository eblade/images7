"""Take care of import jobs and copying files. Keep track of import modules"""

import logging
import mimetypes
import os
import re
import base64
import bottle

from jsonobject import wrap_raw_json
from threading import Thread, Event, Lock
from time import sleep

from images7.web import ResourceBusy
from images7.system import current_system
from images7.localfile import FolderScanner
from images7.job import Job, Step
from images7.job.register import Register, RegisterPart

from images7.multi import QueueClient

re_clean = re.compile(r'[^A-Za-z0-9_\-\.]')


# WEB
#####


class App:
    BASE = '/importer'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        app.route(
            path='/trig',
            method='POST',
            callback=trig_import,
        )

        return app

    @classmethod
    def run(cls, **kwargs):
        importer = Importer()
        t = Thread(target=importer.run, name='Importer')
        t.daemon = True
        t.start()


def trig_import():
    logging.info("Start")
    current_system().zmq_req_lazy_pirate('trig_import')
    logging.info("Stop")
    return {'result': 'ok'}


def get_trig_url():
    return '/trig' % (App.BASE)


# IMPORT MODULE HANDLING
########################

mime_map = {}


def register_import_module(mime_type, module):
    mime_map[mime_type] = module


def get_import_module(mime_type):
    import_module = mime_map.get(mime_type, None)
    return import_module


class GenericImportModule(object):
    def __init__(self, folder, file_path, mime_type):
        self.folder = folder
        self.file_path = file_path
        self.full_path = folder.get_full_path(file_path)
        self.mime_type = mime_type


# IMPORT MANAGER
################


class Importer:
    def __init__(self):
        system = current_system()

        cards = [config for config in system.config.cards]
        drops = [config for config in system.config.drops if config.server == system.hostname]
        self.sources = cards + drops

    def run(self):
        system = current_system()
        system.zmq_rep_lazy_pirate('trig_import', self.trig_import)

    def trig_import(self):
        logging.info('Received trig_import')
        t = Scanner('ipc://job_queue', 1)
        t.sources = self.sources
        t.start()
        t.join()


class Scanner(QueueClient):
    def do(self):
        logging.info('Started scanning...')

        # Look for any device mounted under mount root, having a file <system>.images6
        pre_scanner = FolderScanner(current_system().server.mount_root, extensions=['images6'])
        wanted_filename = '.'.join([current_system().name, 'images6'])
        for file_path in pre_scanner.scan():
            file_path = os.path.join(current_system().server.mount_root, file_path)
            logging.debug("Found file '%s'", file_path)
            filename = os.path.basename(file_path)
            if filename == wanted_filename:
                with open(file_path) as f:
                    name = f.readlines()[0].strip()
                path = os.path.dirname(file_path)
                logging.info('Importing from %s (%s)', path, name)
                for request in self.run_scan(name, path):
                    yield request

    def run_scan(self, name, root_path):
        # Scan the root path for files matching the filter
        system = current_system()
        source = next((t for t in self.sources if t.name == name), None)
        if source is None:
            logging.debug("No source for '%s'", None)
            return

        prios = {x: n for (n, x) in enumerate(source.extension)}
        def prio(x): return prios[os.path.splitext(x)[1][1:].lower()]

        scanner = FolderScanner(root_path, extensions=source.extension)
        collected = {}
        for file_path in scanner.scan():
            if not '.' in file_path:
                continue
            if True:  # TODO check for file in files db here!
                stem, _ = os.path.splitext(file_path)
                if stem in collected.keys():
                    collected[stem].append(file_path)
                else:
                    collected[stem] = [file_path]
                #logging.debug('To import: %s', file_path)
                if len(collected) > 10: break

        # Create entries and import jobs for each found file
        for _, file_paths in sorted(collected.items(), key=lambda x: x[0]):
            logging.debug("Importing %s", ' + '.join(file_paths))

            parts = []
            for file_path in sorted(file_paths, key=prio):
                full_path = os.path.join(root_path, file_path)
                mime_type, is_raw = guess_mime_type(full_path)

                parts.append(RegisterPart(
                    server=system.hostname,
                    source=source.name,
                    root_path=root_path,
                    path=file_path,
                    is_raw=is_raw,
                    mime_type=mime_type,
                ))

            yield Job(
                steps=[
                    Register.AsStep(
                        parts=parts,
                    )
                ]
            )


def guess_mime_type(file_path):
    ext = os.path.splitext(file_path)[1][1:].lower()
    if ext in ['dng', 'raf', 'cr2']:
        return 'image/' + ext, True
    else:
        return mimetypes.guess_type(file_path)[0], False
