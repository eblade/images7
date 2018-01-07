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

from .web import ResourceBusy
from .system import current_system
from .localfile import FolderScanner
#from .entry import (
#    Entry,
#    State,
#    get_entry_by_id,
#    create_entry,
#    update_entry_by_id,
#)
#from .job import Job, create_job


re_clean = re.compile(r'[^A-Za-z0-9_\-\.]')


# WEB
#####


class App:
    BASE = '/importer'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        app.route(
            path='/',
            callback=get_importers_dict,
        )
        app.route(
            path='/trig',
            method='POST',
            callback=trig_import,
        )
        app.route(
            path='/status',
            method='GET',
            callback=get_status,
        )

        return app

    @classmethod
    def run(cls, **kwargs):
        importer = Importer()
        t = Thread(target=importer.run, name='Importer')
        t.daemon = True
        t.start()


def get_importers_dict():
    entries = []
    for name in sorted(current_system().import_folders.keys()):
        entries.append({
            'name': name,
            'trig_url': get_trig_url(name),
        })

    return {
        '*schema': 'ImportFeed',
        'count': len(entries),
        'entries': entries,
    }


def trig_import():
    logging.info("Start")
    current_system().zmq_req_lazy_pirate('trig_import')
    logging.info("Stop")
    return {'result': 'ok'}


def get_status():
    if ImportJob.files == 0:
        progress = 100
    else:
        progress = int((ImportJob.imported + ImportJob.failed) / ImportJob.files * 100)
    return {
        '*schema': 'ImportStatus',
        'folder_name': ImportJob.folder_name,
        'status': ImportJob.status,
        'files': ImportJob.files,
        'imported': ImportJob.imported,
        'failed': ImportJob.failed,
        'progress': progress,
    }


def get_trig_url(name):
    return '%s/trig' % (App.BASE, name)


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
        self.status = 'idle'
        self.files = 0
        self.imported = 0
        self.failed = 0

        system = current_system()

        card_trackers = [ImportTracker(config, system.media_root) for config in system.config.cards]
        drop_trackers = [ImportTracker(config, system.media_root) for config in system.config.drops
                                                                  if config.server == system.hostname]
        self.trackers = card_trackers + drop_trackers

    def run(self):
        system = current_system()
        system.zmq_rep_lazy_pirate('trig_import', self.trig_import)

    def trig_import(self):
        logging.info('Received trig_import')
        if self.status == 'idle':
            self.status = 'acquired'
            t = Thread(target=self.run_scans, name='import_worker')
            t.daemon = True
            t.start()

    def run_scans(self):
        try:
            logging.info('Started scanning...')
            self.status = 'scanning'
            self.files = 0
            self.imported = 0
            self.failed = 0

            sources = {}

            # Look for any device mounted under mount root, having a file <system>.images6
            pre_scanner = FolderScanner(current_system().server.mount_root, extensions=['images6'])
            wanted_filename = '.'.join([current_system().name, 'images6'])
            for filepath in pre_scanner.scan():
                filepath = os.path.join(current_system().server.mount_root, filepath)
                logging.debug("Found file '%s'", filepath)
                filename = os.path.basename(filepath)
                if filename == wanted_filename:
                    with open(filepath) as f:
                        name = f.readlines()[0].strip()
                    path = os.path.dirname(filepath)
                    logging.info('Importing from %s (%s)', path, name)
                    self.run_scan(name, path)

        except Exception as e:
            logging.error('Import thread failed: %s', str(e))
            raise e

        finally:
            self.status = 'idle'

    def run_scan(self, name, path):
        # Scan the root path for files matching the filter
        tracker = next((t for t in self.trackers if t.name == name), None)
        if tracker is None:
            logging.debug("No tracker for '%s'", None)
            return

        scanner = FolderScanner(path, extensions=tracker.config.extension)
        collected = {}
        for filepath in scanner.scan():
            if not tracker.is_known(filepath):
                if not '.' in filepath: continue
                stem, ext = os.path.splitext(filepath)
                if stem in collected.keys():
                    collected[stem].append(filepath)
                else:
                    collected[stem] = [filepath]
                logging.debug('To import: %s', filepath)

        # Create entries and import jobs for each found file
        for file_path in tracker:
            logging.debug("Importing %s", file_path)
            full_path = folder.get_full_path(file_path)

            # Try to obtain an import module (a job that can import the file)
            mime_type = guess_mime_type(full_path, raw=(folder.mode in ('raw', 'raw+jpg')))
            ImportModule = get_import_module(mime_type)

            if ImportModule is None:
                logging.error('Could not find an import module for %s', mime_type)
                ImportJob.failed += 1
                folder.add_failed(file_path)
                continue

            # Create new entry or attach to existing one
            entry = None
            new = None
            if folder.derivatives:
                # Try to see if there is an entry to match it with
                file_name = os.path.basename(file_path)
                m = re.search(r'^[0-9a-f]{8}', file_name)
                if m is not None:
                    hex_id = m.group(0)
                    logging.debug('Converting hex %s into decimal', hex_id)
                    entry_id = int(hex_id, 16)
                    logging.debug('Trying to use entry %s (%d)', hex_id, entry_id)
                    try:
                        entry = get_entry_by_id(entry_id)
                        new = False
                    except KeyError:
                        logging.warn('There was no such entry %s (%d)', hex_id, entry_id)

            if entry is None:
                logging.debug('Creating entry...')
                entry = create_entry(Entry(
                    original_filename=os.path.basename(file_path),
                    state=State.new,
                    import_folder=folder.name,
                    mime_type=mime_type,
                ))
                logging.debug('Created entry %d.', entry.id)
                new = True

            options = ImportModule.Options(
                entry_id=entry.id,
                source_path=file_path,
                mime_type=mime_type,
                is_derivative=not new,
                folder=folder.name,
            )
            job = create_job(Job(
                method=ImportModule.method,
                options=options,
            ))

            ImportJob.imported += 1
            folder.add_imported(file_path)
            logging.info("Created job %d for %s", job.id, full_path)

            ImportJob.status = 'done'



def guess_mime_type(file_path, raw=False):
    if raw:
        mime_type = 'image/' + os.path.splitext(file_path)[1].lower().replace('.', '')
    else:
        mime_type = mimetypes.guess_type(file_path)[0]
    logging.debug("Guessed MIME Type '%s' for '%s'", mime_type, file_path)
    return mime_type


class ImportTracker:
    def __init__(self, config, system_root):
        self.config = config
        self.name = str(config.name)

        self.imported_file = os.path.join(
                system_root, self.name + '_imported.index')

        self.failed_file = os.path.join(
                system_root, self.name + '_failed.index')

        try:
            with open(self.imported_file) as f:
                self.imported = set([
                    line.strip() for line in f.readlines() if line
                ])
        except IOError:
            self.imported = set()
        try:
            with open(self.failed_file) as f:
                self.failed = set([
                    line.strip() for line in f.readlines() if line
                ])
        except IOError:
            self.failed = set()

        self.to_import = set()

    def __repr__(self):
        return '<ImportTracker %s>' % (self.name)

    def is_imported(self, path):
        return path in self.imported

    def is_known(self, path):
        return any((
            path in self.imported,
            path in self.failed,
        ))

    def add_imported(self, path):
        self.imported.add(path)
        with open(self.imported_file, 'a') as f:
            f.write(path + '\n')

    def add_to_import(self, path):
        self.to_import.add(path)

    def add_failed(self, path):
        self.failed.add(path)
        with open(self.failed_file, 'a') as f:
            f.write(path + '\n')

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.to_import.pop()
        except KeyError:
            raise StopIteration
