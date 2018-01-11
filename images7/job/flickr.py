import os
import logging
from jsonobject import register_schema, PropertySet, Property

from ..job import JobHandler, register_job_handler
from ..system import current_system
from ..entry import get_entry_by_id, update_entry_by_id, Purpose, Backup


class FlickrOptions(PropertySet):
    entry_id = Property(int)
    source_purpose = Property(enum=Purpose, default=Purpose.original)
    source_version = Property(int)
    title = Property()
    description = Property()
    tags = Property(list)
    is_public = Property(bool, default=True)


register_schema(FlickrOptions)


class FlickrJobHandler(JobHandler):
    method = 'flickr'
    Options = FlickrOptions

    def run(self, job):
        logging.info('Starting flickr export.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())

        options = job.options
        entry = get_entry_by_id(options.entry_id)

        flickr_backup = None
        for backup in entry.backups:
            if backup.method == 'flickr':
                flickr_backup = backup

        import flickrapi
        flickr = flickrapi.FlickrAPI(api_key=self.key, secret=self.secret)
        flickr.authenticate_via_browser(perms='write')

        self.source = entry.get_variant(options.source_purpose, version=options.source_version)

        if flickr_backup is None:
            logging.debug('Uploading to flickr')
            response = flickr.upload(
                filename=os.path.join(current_system().media_root, self.source.get_filename(entry.id)),
                title=options.title or entry.title or '',
                description=options.description or entry.description or '',
                is_public=options.is_public,
                format='etree',
            )
            photo_id = response.find('photoid').text

            flickr_backup = Backup(
                method='flickr',
                key=photo_id,
                source_purpose=self.source.purpose,
                source_version=self.source.version,
            )
            logging.info('Backup\n%s', flickr_backup.to_json())

            entry.backups.append(flickr_backup)

            if entry.title is None:
                entry.title = options.title
            if entry.description is None:
                entry.description = options.description

            update_entry_by_id(entry.id, entry)

        else:
            logging.debug('Replacing image on flickr')
            logging.info('Backup\n%s', flickr_backup.to_json())
            response = flickr.replace(
                filename=os.path.join(current_system().media_root, entry.get_filename(Purpose.original)),
                photo_id=flickr_backup.key,
                format='etree',
            )

            flickr_backup.source_purpose = self.source.purpose
            flickr_backup.source_version = self.source.version

            update_entry_by_id(entry.id, entry)

        logging.info('Done with flickr export.')


register_job_handler(FlickrJobHandler)
