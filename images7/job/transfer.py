#!/usr/bin/env python3

import os
import logging
import uuid
import datetime

from jsonobject import PropertySet, Property, register_schema

from images7.system import current_system
from images7.config import resolve_path
from images7.job import JobHandler, Job, register_job_handler
from images7.job.transcode import get_transcoder
from images7.localfile import FileCopy, mangle, calculate_hash
from images7.files import get_file_by_url, create_file, File, FileStatus
from images7.analyse import get_analyser
from images7.entry import get_entry_by_id, update_entry_by_id, FilePurpose, FileReference
from images7.retry import retry, GiveUp


class TransferOptions(PropertySet):
    entry_id = Property()
    source_url = Property()
    source_root_path = Property()
    destination = Property()
    steps = Property(type=list)


register_schema(TransferOptions)


class TransferJobHandler(JobHandler):
    Options = TransferOptions
    method = 'transfer'

    def run(self, job):
        logging.info('Starting transfer')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        self.system = current_system()
        self.options = job.options
        self.source = get_file_by_url(self.options.source_url)

        self.cut_path = None
        self.reference = None

        router = {
            'extract_hash': self.extract_hash,
            'extract_metadata': self.extract_metadata,
            'to_main': self.to_main,
            'create_proxy': self.create_proxy,
        }
        for step in self.options.steps:
            if step not in router:
                logging.error('No such step: %s', step)
                break
            try:
                router[step]()
            except GiveUp:
                logging.error('Gave up during step: %s', step)
                break

    def extract_hash(self):
        if self.reference is not None:
            return

        self.to_cut()
        self.reference = calculate_hash(self.cut_path)

    def extract_metadata(self):
        self.to_cut()
        
        analyse = get_analyser(self.source.mime_type)

        if analyse is None:
            return

        metadata = analyse(self.cut_path)
        if metadata is None:
            return
        
        @retry()
        def push():
            entry = get_entry_by_id(self.options.entry_id)
            if entry.metadata is None:
                entry.metadata = metadata
            else:
                entry.metadata.merge(metadata)
            update_entry_by_id(entry.id, entry)
        
        push()


    def to_main(self):
        self.to_cut()
        self.extract_hash()
        
        entry = get_entry_by_id(self.options.entry_id)
        source_url = self.source.parsed_url

        file_ref = next((fr for fr in entry.files if fr.reference == self.source.reference), None)
        purpose = file_ref.purpose if file_ref is not None else FilePurpose.unknown

        if entry.metadata is not None and hasattr(entry.metadata, 'taken_ts') and entry.metadata.taken_ts is not None:
            taken_ts = entry.metadata.taken_ts[:10]
        else:
            taken_ts = datetime.datetime.fromtimestamp(os.path.getmtime(self.cut_path)).strftime('%Y-%m-%d')

        main_root = resolve_path(self.system.main_storage.root_path)
        filename = os.path.basename(self.options.source_url)
        parts = [main_root, entry.type.value, purpose.value, taken_ts, filename]
        main_path = os.path.join(*parts)

        logging.info("Source: %s", str(source_url))
        logging.info("Main path: %s", str(main_path))

        filecopy = FileCopy(
            source=self.cut_path,
            destination=main_path,
            link=True,
            remove_source=True,
        )
        filecopy.run()

        @retry()
        def push():
            entry = get_entry_by_id(self.options.entry_id)
            new_file_ref = FileReference(
                purpose=file_ref.purpose,
                version=file_ref.version,
                reference=self.reference,
                mime_type=file_ref.mime_type,
            )
            new_file = File(
                reference=self.reference,
                url=self.system.main_storage.get_file_url(main_path),
                mime_type=self.source.mime_type,
                status=FileStatus.managed,
            )
            create_file(new_file)

            entry.files.append(new_file_ref)
            update_entry_by_id(entry.id, entry)

        push()


    def to_cut(self):
        if self.cut_path is not None:
            return

        source = self.source
        source_url = source.parsed_url
        source_path = source_url.path
        cut = self.system.cut_storage
        cut_filename = uuid.uuid4().hex
        cut_path = os.path.join(resolve_path(cut.root_path), cut_filename)

        if source_url.scheme == 'card':
            if self.options.source_root_path is not None:
                source_path = os.path.join(self.options.source_root_path, source_path[1:])
        else:
            raise Exception("Only support card here")

        filecopy = FileCopy(
            source=source_path,
            destination=cut_path,
            link=False,
            remove_source=False,
        )
        filecopy.run()

        self.cut_path = cut_path
    

    def create_proxy(self):
        self.to_cut() # Main har tagit bort den...
        entry = get_entry_by_id(self.options.entry_id)

        transcoder = get_transcoder(entry.type.value + '-proxy')

        targets = [
            transcoder.Options(entry=entry, cut_source=self.cut_path, purpose=FilePurpose.proxy),
            transcoder.Options(entry=entry, cut_source=self.cut_path, purpose=FilePurpose.thumb),
            transcoder.Options(entry=entry, cut_source=self.cut_path, purpose=FilePurpose.check), # clean_up=True
        ]

        filerefs = []
        for target in targets:
            f = transcoder.run(target)
            filerefs.append(FileReference(
                purpose=target.purpose,
                version=self.source.version,
                reference=f.reference,
                mime_type=f.mime_type,
            ))
        
        logging.info(filerefs)
        # Sen kanske bra att spara dem...


register_job_handler(TransferJobHandler)
