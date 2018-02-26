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


class ToMainOptions(PropertySet):
    path = Property()
    entry_id = Property()
    source_url = Property()
    reference = Property()


register_schema(ToMainOptions)


class ToMainResult(PropertySet):
    pass


register_schema(ToMainResult)


class ToMain(JobHandler):
    method = 'to_main'
    Options = ToMainOptions

    def run(self, job: Job):
        logging.debug(job.to_json())

        step = job.get_current_step()
        entry = get_entry_by_id(step.options.entry_id)
        source = get_file_by_url(step.options.source_url)

        if step.options.path is not None:
            source_path = step.options.path
        else:
            cut = job.get_step('to_cut')
            source_path = cut.result.path
        
        assert source_path, "Missing source path (forgot to_cut step?)"

        if step.options.reference is not None:
            reference = step.options.reference
        else:
            calculate_hash = job.get_step('calculate_hash')
            reference = calculate_hash.result.calculated_hash

        assert source_path, "Missing reference (forgot calculate_reference step?)"

        metadata_step = job.get_step('read_metadata')
        if metadata_step.result is not None and metadata_step.result.metadata is not None:
            metadata = metadata_step.result.metadata
        else:
            metadata = entry.metadata
        
        if metadata is not None and hasattr(metadata, 'taken_ts') and metadata.taken_ts is not None:
            taken_ts = metadata.taken_ts[:10]
        else:
            taken_ts = datetime.datetime.fromtimestamp(os.path.getmtime(source_path)).strftime('%Y-%m-%d')

        system = current_system()

        file_ref = next((fr for fr in entry.files if fr.reference == source.reference), None)
        purpose = file_ref.purpose if file_ref is not None else FilePurpose.unknown

        main_root = resolve_path(system.main_storage.root_path)
        filename = os.path.basename(step.options.source_url)
        parts = [main_root, entry.type.value, purpose.value, taken_ts, filename]
        main_path = os.path.join(*parts)

        logging.info("Source: %s", str(source.reference))
        logging.info("Main path: %s", str(main_path))

        filecopy = FileCopy(
            source=source_path,
            destination=main_path,
            link=True,
            remove_source=False,
        )
        filecopy.run()

        @retry()
        def push():
            entry = get_entry_by_id(step.options.entry_id)
            new_file_ref = FileReference(
                purpose=file_ref.purpose,
                version=file_ref.version,
                reference=reference,
                mime_type=file_ref.mime_type,
            )
            new_file = File(
                reference=reference,
                url=system.main_storage.get_file_url(main_path),
                mime_type=source.mime_type,
                status=FileStatus.managed,
            )
            create_file(new_file)

            entry.files.append(new_file_ref)
            update_entry_by_id(entry.id, entry)

        push()

        step.result = ToMainResult()
        step.status = StepStatus.done


register_job_handler(ToMain)

