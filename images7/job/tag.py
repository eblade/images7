import logging
from jsonobject import register_schema, PropertySet, Property

from . import JobHandler, register_job_handler
from ..system import current_system
from ..entry import get_entry_by_id, update_entry_by_id, Purpose, Variant, State


#################
# TAG BULK UPDATE


class TagBulkUpdateOptions(PropertySet):
    entry_ids = Property(list)
    add_tags = Property(list)
    remove_tags = Property(list)


register_schema(TagBulkUpdateOptions)


class TagBulkUpdateJobHandler(JobHandler):
    Options = TagBulkUpdateOptions
    method = 'tag_update'

    def run(self, job):
        logging.info('Starting tag update.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        system = current_system()
        options = job.options

        if len(options.add_tags) == 0 and len(options.remove_tags) == 0:
            logging.info('Nothing to do!')
            return

        for entry_id in options.entry_ids:
            entry = get_entry_by_id(entry_id)
            changed = False

            for tag in options.add_tags:
                if tag not in entry.tags:
                    entry.tags.append(tag)
                    changed = True

            for tag in options.remove_tags:
                if tag in entry.tags:
                    entry.tags.remove(tag)
                    changed = True

            if changed:
                logging.info('Updating entry %d.', entry.id)
                update_entry_by_id(entry.id, entry)

        logging.info('Done with tag update.')


register_job_handler(TagBulkUpdateJobHandler)
