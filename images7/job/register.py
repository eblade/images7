import os
import logging
from jsonobject import register_schema, PropertySet, Property
from jsondb import Conflict

from images7.job import Job, JobHandler, Step, StepStatus, register_job_handler
from images7.job.to_cut import ToCut
from images7.job.calculate_hash import CalculateHash
from images7.job.read_metadata import ReadMetadata
from images7.job.to_main import ToMain
from images7.job.create_proxy import CreateProxy
from images7.job.clean_cut import CleanCut
from images7.system import current_system
from images7.files import File, FileStatus, get_file_by_url, create_file
from images7.entry import Entry, FileReference, FilePurpose, EntryType, DefaultEntryMetadata, get_entries_by_reference, create_entry, update_entry_by_id
from images7.multi import QueueClient


class RegisterPart(PropertySet):
    server = Property()
    source = Property()
    root_path = Property()
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


class Register(JobHandler):
    Options = RegisterImportOptions
    method = 'register'

    def run(self, job):
        logging.info('Starting register import.')
        assert job is not None, "Job can't be None"
        logging.info('Job\n%s', job.to_json())
        self.step = job.get_current_step()
        self.system = current_system()
        self.options = self.step.options

        self.register_parts()

        self.step.status = StepStatus.done
        return self.step

    def register_parts(self):
        raw = None
        original = None
        derivative = None
        entry = None
        source = None
        root_path = None

        for part in self.options.parts:
            url = part.get_url(self.system)
            root_path = root_path or part.root_path
            f = File(url=url, reference=url, status=FileStatus.new, mime_type=part.mime_type)

            try:
                f = create_file(f)
            except Conflict:
                f = get_file_by_url(url)
                if f.reference is not None:
                    entry = next(iter(get_entries_by_reference(f.reference).entries), None)

            if f is None:
                logging.error('Bad file: %s', f.to_json())

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
            logging.error('No valid file!\n%s', self.step.to_json())
            return

        if entry is None:
            entry = Entry(
                type=EntryType.image,
                metadata=DefaultEntryMetadata(
                    original_filename=os.path.basename(primary.url),
                    source=source,
                ),
            )
            entry = create_entry(entry)
            jobs = []
            for f, p in ((raw, FilePurpose.raw), (original, FilePurpose.original), (derivative, FilePurpose.derivative)):
                if f is None:
                    continue

                entry.files.append(FileReference(
                    reference=f.reference,
                    purpose=p,
                    version=0,
                    mime_type=f.mime_type,
                ))

                jobs.append(Job(
                    steps=[
                        ToCut.AsStep(
                            source_root_path=root_path,
                            source_url=f.url,
                        ),
                        CalculateHash.AsStep(),
                        ReadMetadata.AsStep(
                            entry_id=entry.id,
                            mime_type=f.mime_type,
                        ),
                        ToMain.AsStep(
                            entry_id=entry.id,
                            source_url=f.url,
                        ),
                        CreateProxy.AsStep(
                            entry_id=entry.id,
                            source_url=f.url,
                        ),
                        CleanCut.AsStep(),
                    ]
                ))

            update_entry_by_id(entry.id, entry)

            with QueueClient('ipc://job_queue') as q:
                for job in jobs:
                    q.send(job)


register_job_handler(Register)
