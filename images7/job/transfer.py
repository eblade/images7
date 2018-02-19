#!/usr/bin/env python3

import os
import logging
import uuid
import hashlib
import datetime

from jsonobject import PropertySet, Property, register_schema

from images7.system import current_system
from images7.config import resolve_path
from images7.job import JobHandler, Job, create_job, register_job_handler
#from ..job.imageproxy import ImageProxyOptions, ImageProxyJobHandler
from images7.localfile import FileCopy, mangle
from images7.files import get_file_by_url, create_file, File, FileStatus
from images7.analyse import get_analyser
from images7.entry import get_entry_by_id, update_entry_by_id, FilePurpose, FileReference


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
        self.metadata = None

        router = {
            'extract_hash': self.extract_hash,
            'extract_metadata': self.extract_metadata,
            'to_main': self.to_main,
        }
        for step in self.options.steps:
            router[step]()

    def extract_hash(self):
        if self.reference is not None:
            return

        self.to_cut()

        BLOCKSIZE = 65636
        sha = hashlib.sha256()
        with open(self.cut_path, 'rb') as f:
            buf = f.read(BLOCKSIZE)
            while len(buf) > 0:
                sha.update(buf)
                buf = f.read(BLOCKSIZE)

        self.reference = sha.hexdigest()

    def extract_metadata(self):
        self.to_cut()
        
        # TODO What's wrong with getting the analyser???
        analyse = get_analyser(self.source.mime_type)

        if analyse is None:
            return

        self.metadata = analyse(self.cut_path)

    def to_main(self):
        self.to_cut()
        self.extract_hash()
        
        entry = get_entry_by_id(self.options.entry_id)
        source_url = self.source.parsed_url

        file_ref = next((fr for fr in entry.files if fr.reference == self.source.reference), None)
        purpose = file_ref.purpose if file_ref is not None else FilePurpose.unknown

        metadata = None
        if self.metadata is not None:
            metadata = self.metadata
        else:
            metadata = entry.metadata

        if metadata is not None and hasattr(metadata, 'taken_ts') and metadata.taken_ts is not None:
            taken_ts = metadata.taken_ts[:10]
        else:
            taken_ts = datetime.datetime.fromtimestamp(os.path.getmtime(self.cut_path)).strftime('%Y-%m-%d')

        entry.metadata.merge(metadata)
        main_root = resolve_path(self.system.main_storage.root_path)
        filename = os.path.basename(self.options.source_url)
        parts = [main_root, entry.type.value, purpose.value, taken_ts, filename]
        print(parts)
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

        new_file_ref = FileReference(
            purpose=file_ref.purpose,
            version=file_ref.version,
            reference=self.reference,
            extension=file_ref.extension,
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


register_job_handler(TransferJobHandler)
