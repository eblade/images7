import logging
import os
import re
import exifread
from jsonobject import PropertySet, Property, register_schema

from ..system import current_system
from ..localfile import FileCopy, mangle
from ..job import JobHandler, Job, register_job_handler
from ..entry import Variant


LOCAL_STORES = ('original', 'derivative', 'thumb', 'proxy', 'check', 'raw')

class DeleteOptions(PropertySet):
    entry_id = Property(int)
    variant = Property(Variant)


register_schema(DeleteOptions)


class DeleteJobHandler(JobHandler):
    Options = DeleteOptions
    method = 'delete'

    def run(self, job):
        logging.info('Starting delete job.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        self.system = current_system()
        self.options = job.options
        self.variant = job.options.variant

        if self.variant.store in LOCAL_STORES:
            self.delete_local()
        else:
            raise Exception("Files on store %s cannot be deleted." % self.variant.store)

        logging.info('Done with delete job.')

    def delete_local(self):
        logging.info('Deleting %s', self.variant)
        filename = self.variant.get_filename(self.options.entry_id)
        filepath = os.path.join(self.system.media_root, filename)
        logging.info('Filepath is %s', filepath)
        os.remove(filepath)
        logging.info('Deleted %s', self.variant)


register_job_handler(DeleteJobHandler)
