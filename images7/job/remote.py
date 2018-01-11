import logging
import hashlib
import os
import subprocess
from datetime import datetime
from jsonobject import PropertySet, Property, register_schema

from ..system import current_system
from ..entry import (
    Variant,
    Backup,
    get_entry_by_id,
    update_entry_by_id,
)
from ..job import JobHandler, Job, create_job, register_job_handler


class RemoteOptions(PropertySet):
    entry_id = Property(int)
    variant = Property(type=Variant)
    remote = Property()


register_schema(RemoteOptions)


class RemoteJobHandler(JobHandler):
    Options = RemoteOptions
    method = 'remote'

    def run(self, job):
        logging.info('Starting remote transfer.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        system = current_system()
        options = job.options
        remote = system.remotes[options.remote]
        rules = system.rules.values()

        local_filepath = options.variant.get_filename(options.entry_id)
        full_local_filepath = os.path.join(system.media_root, local_filepath)
        md5sum_for_file = md5sum(full_local_filepath)

        logging.info("MD5 sum is %s", md5sum_for_file)

        exists = check_if_exists(remote, md5sum_for_file)

        if exists:
            logging.info("Object already exists in store %s", remote.name)
        else:
            upload_ssh(remote, full_local_filepath, md5sum_for_file)

        entry = get_entry_by_id(options.entry_id)
        entry.backups.append(
            Backup(
                method=remote.method,
                key=md5sum_for_file,
                source_purpose=options.variant.purpose,
                source_version=options.variant.version,
            )
        )
        update_entry_by_id(options.entry_id, entry)


register_job_handler(RemoteJobHandler)


# http://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


def check_if_exists(remote, md5hash):
    code = subprocess.call(['ssh', remote.host, 'cao', remote.path, 'check', md5hash])
    return code == 0


def upload_ssh(remote, source_path, remote_filaname):
    destination_path = os.path.join(remote.path, remote_filaname)
    scp_path = '%s:%s' % (remote.host, destination_path)
    code = subprocess.call(['scp', source_path, scp_path])
    if code != 0:
        raise IOError("Could not upload file %s to %s" % (source_path, scp_path))
    code = subprocess.call(['ssh', remote.host, 'cao', remote.path, 'move', destination_path])
    if code != 0:
        raise IOError("Could not move file %s into object store" % scp_path)
