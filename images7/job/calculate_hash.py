
#!/usr/bin/env python3

import os
import logging
import uuid
import datetime

from jsonobject import PropertySet, Property, register_schema

from images7.job import JobHandler, Job, StepStatus, register_job_handler
from images7.localfile import calculate_hash


class CalculateHashOptions(PropertySet):
    path = Property()


register_schema(CalculateHashOptions)


class CalculateHashResult(PropertySet):
    calculated_hash = Property()


register_schema(CalculateHashResult)


class CalculateHash(JobHandler):
    method = 'calculate_hash'
    Options = CalculateHashOptions

    def run(self, job: Job):
        logging.debug(job.to_json())

        step = job.get_current_step()
        
        if step.options.path is not None:
            path = step.options.path
        else:
            cut = job.get_step('to_cut')
            path = cut.result.path

        ref = calculate_hash(path)

        step.result = CalculateHashResult(calculated_hash=ref)
        step.status = StepStatus.done


register_job_handler(CalculateHash)