import os
import logging
from jsonobject import register_schema, PropertySet, Property

from . import Job, JobHandler, register_job_handler, create_job
from .jpg import JPEGImportJobHandler
from ..system import current_system
from ..importer import register_import_module
from ..entry import get_entry_by_id, update_entry_by_id, Purpose, Variant, State
from ..localfile import FileCopy, FolderScanner


############
# RAW IMPORT


class RawImportOptions(PropertySet):
    entry_id = Property(int)
    source_path = Property()
    folder = Property()
    mime_type = Property()


register_schema(RawImportOptions)


class RawImportJobHandler(JobHandler):
    Options = RawImportOptions
    method = 'raw_import'

    def run(self, job):
        logging.info('Starting raw import.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        self.system = current_system()
        self.options = job.options
        self.folder = self.system.import_folders[self.options.folder]

        self.full_source_file_path = self.folder.get_full_path(self.options.source_path)
        logging.debug('Full source path is %s', self.full_source_file_path)

        self.entry = get_entry_by_id(self.options.entry_id)
        if self.entry.state is State.new:
            self.entry.state = State.pending

        variant = self.create_variant()
        logging.debug('Created variant.')

        self.entry = update_entry_by_id(self.entry.id, self.entry)
        logging.debug('Updated entry.\n%s', self.entry.to_json())

        # Create job for adding JPG
        if self.folder.mode == 'raw+jpg':
            stripped_path, _ = os.path.splitext(self.options.source_path)
            jpg_path = stripped_path + '.JPG' 
            if os.path.exists(self.folder.get_full_path(jpg_path)):
                jpg_job = Job(
                    method=JPEGImportJobHandler.method,
                    options=JPEGImportJobHandler.Options(
                        entry_id=self.entry.id,
                        source_path=jpg_path,
                        folder=self.folder.name,
                        mime_type='image/jpeg',
                        analyse=True,
                        is_derivative=True,
                        source_purpose=variant.purpose,
                        source_version=variant.version,
                    )
                )
                jpg_job = create_job(jpg_job)
                logging.info('Created jpg job %d.', jpg_job.id)
            else:
                logging.warn('Found no jpg %s.', jpg_path)

        logging.info('Raw import job %d done.', job.id)

    def create_variant(self):
        variant = Variant(
            store='raw',
            mime_type=self.options.mime_type,
            purpose=Purpose.raw,
            version=self.entry.get_next_version(Purpose.raw),
        )

        filecopy = FileCopy(
            source=self.full_source_file_path,
            destination=os.path.join(
                self.system.media_root,
                variant.get_filename(self.entry.id)
            ),
            link=True,
            remove_source=self.folder.auto_remove,
        )
        filecopy.run()
        self.full_destination_file_path = filecopy.destination
        variant.size = os.path.getsize(filecopy.destination)
        self.entry.variants.append(variant)
        return variant


register_job_handler(RawImportJobHandler)
register_import_module('image/raf', RawImportJobHandler)
register_import_module('image/dng', RawImportJobHandler)
register_import_module('image/cr2', RawImportJobHandler)



###########
# RAW FETCH


class RawFetchOptions(PropertySet):
    entry_id = Property(int)


register_schema(RawFetchOptions)


class RawFetchJobHandler(JobHandler):
    Options = RawFetchOptions
    method = 'raw_fetch'

    def run(self, job):
        logging.info('Starting raw fetching.')
        logging.info('Options\n%s', job.to_json())

        options = job.options
        entry = get_entry_by_id(options.entry_id)
        logging.info('Original filename is %s', entry.original_filename)

        full_raw_file_path = self.find_raw(entry.original_filename)
        if full_raw_file_path is None:
            logging.info('Could not find a raw file for %s', entry.original_filename)
            return

        _, extension = os.path.splitext(full_raw_file_path)
        raw = Variant(
            store='raw',
            mime_type='image/' + extension.lower().replace('.', ''),
            purpose=Purpose.raw,
        )
        filecopy = FileCopy(
            source=full_raw_file_path,
            destination=os.path.join(
                current_system().media_root,
                raw.get_filename(entry.id)
            ),
            link=True,
            remove_source=False,
        )
        filecopy.run()
        raw.size = os.path.getsize(filecopy.destination)
        entry.variants.append(raw)

        update_entry_by_id(entry.id, entry)
        logging.info('Done with raw fetching.')

    def find_raw(self, original_filename):
        raw_extensions = [e.strip() for e in self.extensions.split() if e.strip()]
        raw_extensions = [e if e.startswith('.') else ('.' + e) for e in raw_extensions]
        raw_locations = [l.strip() for l in self.locations.split('\n') if l.strip()]

        wanted_without_extension, _ = os.path.splitext(original_filename)
        logging.info('Without extension: %s', wanted_without_extension)

        for location in raw_locations:
            logging.info('Scanning for raws in %s', location)
            scanner = FolderScanner(location, extensions=raw_extensions)
            for filepath in scanner.scan():
                filename = os.path.basename(filepath)
                logging.info('Found one raw %s', filename)
                without_extension, _ = os.path.splitext(filename)
                if without_extension == wanted_without_extension:
                    logging.info('Found raw %s', filepath)
                    return os.path.join(location, filepath)



register_job_handler(RawFetchJobHandler)
