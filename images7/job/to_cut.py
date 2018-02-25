#!/usr/bin/env python3

import os
import logging
import uuid

from jsonobject import PropertySet, Property, register_schema

from images7.system import current_system
from images7.config import resolve_path
from images7.job import JobHandler, Job, register_job_handler, StepStatus
from images7.files import get_file_by_url
from images7.localfile import FileCopy


class ToCutOptions(PropertySet):
    source_root_path = Property()
    source_url = Property()


register_schema(ToCutOptions)


class ToCutResult(PropertySet):
    path = Property()


register_schema(ToCutResult)


class ToCut(JobHandler):
    method = 'to_cut'
    Options = ToCutOptions

    def run(self, job: Job):
        logging.debug(job.to_json())

        step = job.get_current_step()
        system = current_system()

        source = get_file_by_url(step.options.source_url)
        source_url = source.parsed_url
        source_path = source_url.path

        cut = system.cut_storage
        cut_filename = uuid.uuid4().hex
        cut_path = os.path.join(resolve_path(cut.root_path), cut_filename)

        if source_url.scheme == 'card':
            if step.options.source_root_path is not None:
                source_path = os.path.join(step.options.source_root_path, source_path[1:])
        else:
            raise Exception("Only support card here")

        filecopy = FileCopy(
            source=source_path,
            destination=cut_path,
            link=False,
            remove_source=False,
        )
        filecopy.run()

        step.result = ToCutResult(path=cut_path)
        step.status = StepStatus.done


register_job_handler(ToCut)