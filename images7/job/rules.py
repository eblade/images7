import logging
from datetime import datetime
from jsonobject import PropertySet, Property, register_schema

from ..system import current_system
from ..entry import (
    Variant,
    get_entry_by_id,
    update_entry_by_id,
)
from ..job import JobHandler, Job, create_job, register_job_handler
from .remote import RemoteOptions, RemoteJobHandler


class RulesOptions(PropertySet):
    entry_id = Property(int)
    entry_ids = Property(list)


register_schema(RulesOptions)


class RulesJobHandler(JobHandler):
    Options = RulesOptions
    method = 'rules'

    def run(self, job):
        logging.info('Starting rules check.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        system = current_system()
        options = job.options
        rules = system.rules.values()

        if options.entry_id is None:
            entry_ids = options.entry_ids
        else:
            entry_ids = [options.entry_id]

        if len(entry_ids) == 0:
            logging.info('Nothing to do')
            return

        for entry_id in entry_ids:
            logging.info('Checking entry %i', entry_id)
            entry = get_entry_by_id(entry_id)

            for rule in rules:
                if rule.when_tag is not None:
                    if not set(rule.when_tag).issubset(entry.tags):
                        continue

                if rule.then_remote is not None:
                    for remote_name in rule.then_remote:
                        remote = system.remotes[remote_name]

                        for variant in entry.variants:
                            if not variant.purpose in remote.media:
                                continue
                            if entry.has_backup(variant, remote.method):
                                continue
                            logging.info("Rule [%s]: Need to push [%s] to [%s]",
                                rule.name, variant, remote_name)

                            create_job(Job(
                                method=RemoteJobHandler.method,
                                message='Transfer %s/%s to %s' % (
                                    variant.purpose.value,
                                    variant.version,
                                    remote_name
                                ),
                                options=RemoteOptions(
                                    entry_id=entry_id,
                                    remote=remote_name,
                                    variant=variant,
                                )
                            ))


register_job_handler(RulesJobHandler)

