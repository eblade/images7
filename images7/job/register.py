import os
import logging
from jsonobject import register_schema, PropertySet, Property

from . import Job, JobHandler, register_job_handler, create_job
from ..system import current_system


class RegisterPart(PropertySet):
    server = Property()
    source = Property()
    path = Property()
    is_raw = Property(bool)
    mime_type = Property()


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


register_job_handler(RegisterImportJobHandler)
