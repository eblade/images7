import os
import logging
from jsonobject import register_schema, PropertySet, Property
from jsondb import Conflict

from . import Job, JobHandler, register_job_handler, create_job
from ..system import current_system
from ..files import File, get_file_by_url, create_file


class RegisterPart(PropertySet):
    server = Property()
    source = Property()
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

        self.register_parts()

    def register_parts(self):
        for part in self.options.parts:
            url = part.get_url(self.system)
            f = File(url=url)

            try:
                new = create_file(f)
            except Confict:
                existing = get_file_by_url(url)


register_job_handler(RegisterImportJobHandler)
