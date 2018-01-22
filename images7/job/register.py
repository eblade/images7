import os
import logging
from jsonobject import register_schema, PropertySet, Property
from jsondb import Conflict

from . import Job, JobHandler, register_job_handler, create_job
from ..system import current_system
from ..files import File, FileStatus, get_file_by_url, create_file
from ..entry import Entry, FileReference, FilePurpose, EntryType, DefaultEntryMetadata, get_entries_by_reference, create_entry
from ..job import Job, create_job
from ..multi import QueueClient


class RegisterPart(PropertySet):
    server = Property()
    source = Property()
    path = Property()
    is_raw = Property(bool)
    mime_type = Property()

    def get_url(self, system):
        source = system.config.get_source_by_name(self.source)
        assert source is not None, "Source with name %s not found" % self.source
        return source.get_part_url(self)


class RegisterImportOptions(PropertySet):
    parts = Property(type=RegisterPart, is_list=True)


register_schema(RegisterImportOptions)


class RegisterImportJobHandler(JobHandler):
    Options = RegisterImportOptions
    method = 'register'

    def run(self, job):
        logging.info('Starting register import.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        self.system = current_system()
        self.options = job.options

        self.register_parts()

    def register_parts(self):
        raw = None
        original = None
        derivative = None
        entry = None
        source = None

        for part in self.options.parts:
            url = part.get_url(self.system)
            f = File(url=url, reference=url, status=FileStatus.new, mime_type=part.mime_type)

            try:
                f = create_file(f)
            except Conflict:
                f = get_file_by_url(url)
                if f.reference is not None:
                    entry = next(iter(get_entries_by_reference(f.reference).entries), None)

            if f is None:
                logging.error('Bad file:', f.to_json())

            if part.is_raw:
                raw = f
                source = part.source
            elif raw is None:
                original = f
                source = part.source
            else:
                derivative = f

        primary = raw or original or derivative
        
        if primary is None:
            logging.error('No valid file!', self.job.to_json())
            return

        if entry is None:
            entry = Entry(
                type=EntryType.image,
                metadata=DefaultEntryMetadata(
                    original_filename=os.path.basename(primary.url),
                    source=source,
                ),
            )
            with QueueClient('ipc://job_queue') as q:
                for f, p in ((raw, FilePurpose.raw), (original, FilePurpose.original), (derivative, FilePurpose.derivative)):
                    if f is None:
                        continue

                    entry.files.append(FileReference(
                        reference=f.reference,
                        purpose=p,
                        version=0,
                        extension=os.path.splitext(os.path.basename(f.url))[1],
                    ))

                    q.send(Job(
                        method='transfer',
                        options=TransferOptions(
                            entry_id=entry.id,
                            source_url=f.url,
                            steps=[
                                'to_cut',
                                'analyse',
                                'extract_metadata',
                                'to_main'
                            ],
                        )
                    ))

            entry = create_entry(entry)


register_job_handler(RegisterImportJobHandler)
