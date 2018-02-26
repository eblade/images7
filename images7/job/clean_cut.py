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


class CleanCutOptions(PropertySet):
    pass


register_schema(CleanCutOptions)


class CleanCutResult(PropertySet):
    pass


register_schema(CleanCutResult)


class CleanCut(JobHandler):
    method = 'clean_cut'
    Options = CleanCutOptions

    def run(self, job: Job):
        logging.debug(job.to_json())

        step = job.get_current_step()

        cut = job.get_step('to_cut')

        if cut is not None:
            source_path = cut.result.path
            if os.path.exists(source_path):
                os.remove(source_path)

        step.result = CleanCutResult()
        step.status = StepStatus.done


register_job_handler(CleanCut)
