import os
from PIL import Image
import logging
from jsonobject import register_schema, PropertySet, Property

from ..system import current_system
from ..job import JobHandler, register_job_handler
from ..entry import get_entry_by_id, update_entry_by_id, Purpose, Variant
from ..localfile import FileCopy, FolderScanner


class AmendOptions(PropertySet):
    entry_id = Property(int)
    amend_metadata = Property(bool, default=True)
    amend_variants = Property(bool, default=True)


register_schema(AmendOptions)


class AmendJobHandler(JobHandler):
    method = 'amend'
    Options = AmendOptions

    def run(self, job):
        logging.info('Starting amending.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())

        options = job.options
        entry = get_entry_by_id(options.entry_id)
        before = entry.to_json()
        logging.info('Original entry is\n%s', before)

        if options.amend_metadata:
            if entry.metadata.Copyright == '[]':
                entry.metadata.Copyright = None

        if options.amend_variants:
            latest_source = None
            for variant in entry.variants:
                full_path = os.path.join(
                    current_system().media_root,
                    variant.get_filename(entry.id),
                )

                if variant.mime_type == 'image/jpeg':
                    img = Image.open(full_path)
                    variant.width, variant.height = img.size
                    img.close()

                if variant.purpose in (Purpose.original, Purpose.derivative):
                    latest_source = variant

                if variant.purpose in (Purpose.original, Purpose.raw):
                    variant.source_purpose = None
                    variant.source_version = None

                elif variant.purpose in (Purpose.proxy, Purpose.check, Purpose.thumb):
                    variant.source_purpose = latest_source.purpose
                    variant.source_version = latest_source.version

        after = entry.to_json()
        logging.info('Amended entry is\n%s', after)

        if after != before:
            update_entry_by_id(entry.id, entry)

        logging.info('Done amending.')


register_job_handler(AmendJobHandler)
