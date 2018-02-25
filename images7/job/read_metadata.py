#!/usr/bin/env python3

import os
import logging
import uuid
import datetime

from jsonobject import PropertySet, Property, register_schema

from images7.job import JobHandler, Job, StepStatus, register_job_handler
from images7.analyse import get_analyser
from images7.retry import retry
from images7.entry import get_entry_by_id, update_entry_by_id


class ReadMetadataOptions(PropertySet):
    path = Property()
    mime_type = Property()
    entry_id = Property()


register_schema(ReadMetadataOptions)


class ReadMetadataResult(PropertySet):
    metadata = Property(wrap=True)


register_schema(ReadMetadataResult)


class ReadMetadata(JobHandler):
    method = 'read_metadata'
    Options = ReadMetadataOptions

    def run(self, job: Job):
        logging.debug(job.to_json())

        step = job.get_current_step()

        if step.options.path is not None:
            path = step.options.path
        else:
            cut = job.get_step('to_cut')
            path = cut.result.path

        analyse = get_analyser(step.options.mime_type)

        if analyse is None:
            logging.info("Found no metadata analyser for %s", step.options.mime_type)
            step.status = StepStatus.done
            return

        metadata = analyse(path)

        if metadata is not None and step.options.entry_id is not None:
            @retry()
            def update():
                entry = get_entry_by_id(step.options.entry_id)
                if entry.metadata is None:
                    entry.metadata = metadata
                else:
                    entry.metadata.merge(metadata)
                update_entry_by_id(entry.id, entry)
            
            update()

        step.result = ReadMetadataResult(metadata=metadata)
        step.status = StepStatus.done


register_job_handler(ReadMetadata)
