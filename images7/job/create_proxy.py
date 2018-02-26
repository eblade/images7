#!/usr/bin/env python3

import os
import logging
import uuid
import datetime

from jsonobject import PropertySet, Property, register_schema

from images7.system import current_system
from images7.config import resolve_path
from images7.job import JobHandler, Job, StepStatus, register_job_handler
from images7.retry import retry
from images7.entry import get_entry_by_id, update_entry_by_id, FilePurpose, FileReference
from images7.files import get_file_by_url, create_file, File, FileStatus
from images7.localfile import FileCopy
from images7.job.transcode import get_transcoder


class CreateProxyOptions(PropertySet):
    path = Property()
    entry_id = Property()
    source_url = Property()


register_schema(CreateProxyOptions)


class CreateProxyResult(PropertySet):
    pass


register_schema(CreateProxyResult)


class CreateProxy(JobHandler):
    method = 'create_proxy'
    Options = CreateProxyOptions

    def run(self, job: Job):
        logging.debug(job.to_json())

        step = job.get_current_step()
        entry = get_entry_by_id(step.options.entry_id)
        source = get_file_by_url(step.options.source_url)
        source_ref = entry.get_file_reference(source.reference)

        if step.options.path is not None:
            source_path = step.options.path
        else:
            cut = job.get_step('to_cut')
            source_path = cut.result.path

        transcoder = get_transcoder(source.mime_type, 'proxy')

        if transcoder is None:
            step.status = StepStatus.done
            return

        targets = [
            transcoder.Options(entry=entry, cut_source=source_path, purpose=FilePurpose.proxy),
            transcoder.Options(entry=entry, cut_source=source_path, purpose=FilePurpose.thumb),
            transcoder.Options(entry=entry, cut_source=source_path, purpose=FilePurpose.check),
        ]

        filerefs = []
        for target in targets:
            f = transcoder.run(target)
            filerefs.append(FileReference(
                purpose=target.purpose,
                version=source_ref.version,
                reference=f.reference,
                mime_type=f.mime_type,
            ))
        
        logging.info(filerefs)

        @retry()
        def push():
            entry = get_entry_by_id(step.options.entry_id)

            for file_ref in filerefs:
                new_file_ref = FileReference(
                    purpose=file_ref.purpose,
                    version=file_ref.version,
                    reference=file_ref.reference,
                    mime_type=file_ref.mime_type,
                )
                entry.files.append(new_file_ref)

            update_entry_by_id(entry.id, entry)

        push()

        step.result = CreateProxyResult()
        step.status = StepStatus.done


register_job_handler(CreateProxy)
