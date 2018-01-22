#!/usr/bin/env python3

import logging
from jsonobject import PropertySet, Property, register_schema
import uuid

from ..job import JobHandler, Job, create_job, register_job_handler
from ..job.imageproxy import ImageProxyOptions, ImageProxyJobHandler
from ..localfile import FileCopy, mangle
from ..files import get_file_by_url


class TransferOptions(PropertySet):
    entry_id = Property()
    source_url = Property()
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


        router = {
            'to_cut': self.to_cut,
        }
        for step in self.options.steps:
            router[step]()

    def to_cut(self):
        source_url = self.source.parsed_url
        cut = self.system.cut_storage
        cut_filename = uuid.uuid4().hex

        if source_url.scheme == 'card':
            card = next((x for x in self.system.cards if x.name == source_url.netloc), None)

        filecopy = FileCopy(
            source



register_job_handler(TransferJobHandler)
