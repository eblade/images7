import logging
import os
import re
import exifread
from datetime import datetime
from jsonobject import PropertySet, Property, register_schema
from PIL import Image

from ..system import current_system
from ..importer import register_import_module
from ..localfile import FileCopy, mangle
from ..entry import (
    Entry,
    Variant,
    State,
    Access,
    Purpose,
    create_entry,
    get_entry_by_id,
    update_entry_by_id,
    delete_entry_by_id,
)
from ..exif import(
    exif_position,
    exif_orientation,
    exif_string,
    exif_int,
    exif_ratio,
)
from ..job import JobHandler, Job, create_job, register_job_handler
from ..job.imageproxy import ImageProxyOptions, ImageProxyJobHandler


class JPEGImportOptions(PropertySet):
    entry_id = Property(int)
    source_path = Property()
    folder = Property()
    mime_type = Property()
    analyse = Property(bool)
    is_derivative = Property(bool, default=False)
    source_purpose = Property(enum=Purpose, default=Purpose.raw)
    source_version = Property(int)


register_schema(JPEGImportOptions)


class JPEGImportJobHandler(JobHandler):
    Options = JPEGImportOptions
    method = 'jpeg_import'

    def run(self, job):
        logging.info('Starting jpg generation.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        self.system = current_system()
        self.options = job.options
        self.folder = self.system.import_folders[self.options.folder]

        if self.options.analyse is None:
            self.options.analyse = not self.options.is_derivative

        self.full_source_file_path = self.folder.get_full_path(self.options.source_path)
        logging.debug('Full source path is %s', self.full_source_file_path)

        self.entry = get_entry_by_id(self.options.entry_id)

        variant = self.create_variant()
        logging.debug('Created variant.')

        if self.options.analyse:
            metadata = JPEGMetadata(**(self.analyse()))
            if metadata.Copyright == '[]':
                metadata.Copyright = None
            self.fix_taken_ts(metadata)
            logging.debug('Read metadata.')

            self.entry.metadata = metadata
            variant.angle = self.entry.metadata.Angle
            variant.mirror = self.entry.metadata.Mirror
            if not self.options.is_derivative:
                self.entry.state = State.pending

        self.entry = update_entry_by_id(self.entry.id, self.entry)
        logging.debug('Updated entry.\n%s', self.entry.to_json())

        proxy_job = Job(
            method=ImageProxyJobHandler.method,
            options=ImageProxyOptions(
                entry_id=self.entry.id,
                source_purpose=variant.purpose,
                source_version=variant.version,
            )
        )
        proxy_job = create_job(proxy_job)
        logging.debug('Created image proxy job %d.', proxy_job.id)

    def clean_up(self):
        logging.debug('Cleaning up...')
        if self.new:
            delete_entry_by_id(self.entry.id)
        logging.debug('Cleaned up.')

    def create_variant(self):
        if self.options.is_derivative:
            variant = Variant(
                store='derivative',
                mime_type=self.options.mime_type,
                purpose=Purpose.derivative,
                version=self.entry.get_next_version(Purpose.derivative),
                source_purpose=self.options.source_purpose,
                source_version=self.options.source_version,
            )
            if self.options.source_path.startswith('from_raw/'):
                raw = self.entry.get_variant(Purpose.raw)
                if raw is not None:
                    derivative.source_purpose = Purpose.raw
                    derivative.source_version = raw.version
                    derivative.angle = 0
            else:
                original = self.entry.get_variant(Purpose.original)
                if original is not None:
                    original.source_purpose = Purpose.original
                    original.source_version = original.version
                    original.angle = 0
        else:
            variant = Variant(
                store='original',
                mime_type=self.options.mime_type,
                purpose=Purpose.original,
                version=self.entry.get_next_version(Purpose.original),
            )

        filecopy = FileCopy(
            source=self.full_source_file_path,
            destination=os.path.join(
                self.system.media_root,
                variant.get_filename(self.entry.id)
            ),
            link=True,
            remove_source=self.folder.auto_remove,
        )
        filecopy.run()
        self.full_destination_file_path = filecopy.destination
        variant.size = os.path.getsize(filecopy.destination)
        img = Image.open(filecopy.destination)
        variant.width, variant.height = img.size
        img.close()
        self.entry.variants.append(variant)
        return variant

    def fix_taken_ts(self, metadata):
        real_date = metadata.DateTimeOriginal
        if not real_date:
            return

        self.entry.taken_ts = (datetime.strptime(
                real_date, '%Y:%m:%d %H:%M:%S').replace(microsecond=0)
                .strftime('%Y-%m-%d %H:%M:%S')
        )

    def analyse(self):
        infile = self.full_destination_file_path

        exif = None
        with open(infile, 'rb') as f:
            exif = exifread.process_file(f)

        # Orientation (rotation)
        orientation, mirror, angle = exif_orientation(exif)

        # GPS Position
        lon, lat = exif_position(exif)

        return {
            "Artist": exif_string(exif, "Image Artist"),
            "ColorSpace": exif_string(exif, "EXIF ColorSpace"),
            "Copyright": exif_string(exif, "Image Copyright"),
            "Geometry": (exif_int(exif, "EXIF ExifImageWidth"), exif_int(exif, "EXIF ExifImageLength")),
            "DateTime": exif_string(exif, "EXIF DateTime"),
            "DateTimeDigitized": exif_string(exif, "EXIF DateTimeDigitized"),
            "DateTimeOriginal": exif_string(exif, "EXIF DateTimeOriginal"),
            "ExposureTime": exif_ratio(exif, "EXIF ExposureTime"),
            "FNumber": exif_ratio(exif, "EXIF FNumber"),
            "Flash": exif_string(exif, "EXIF Flash"),
            "FocalLength": exif_ratio(exif, "EXIF FocalLength"),
            "FocalLengthIn35mmFilm": exif_int(exif, "EXIF FocalLengthIn35mmFilm"),
            "ISOSpeedRatings": exif_int(exif, "EXIF ISOSpeedRatings"),
            "Make": exif_string(exif, "Image Make"),
            "Model": exif_string(exif, "Image Model"),
            "Orientation": orientation,
            "Mirror": mirror,
            "Angle": angle,
            "Saturation": exif_string(exif, "EXIF Saturation"),
            "Software": exif_string(exif, "Software"),
            "SubjectDistanceRange": exif_int(exif, "EXIF SubjectDistanceRange"),
            "WhiteBalance": exif_string(exif, "WhiteBalance"),
            "Latitude": lat,
            "Longitude": lon,
        }


register_job_handler(JPEGImportJobHandler)
register_import_module('image/jpeg', JPEGImportJobHandler)
register_import_module('image/tiff', JPEGImportJobHandler)
register_import_module('image/png', JPEGImportJobHandler)


class JPEGExportOptions(PropertySet):
    entry_id = Property(int)
    entry_ids = Property(list)
    purpose = Property(enum=Purpose)
    version = Property(int)
    date = Property()
    longest_side = Property(int)
    folder = Property()
    filename = Property()


register_schema(JPEGExportOptions)


class JPEGExportJobHandler(JobHandler):
    Options = JPEGExportOptions
    method = 'jpeg_export'

    def run(self, job):
        logging.info('Starting jpg generation.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        assert job.options.folder, "Need folder in job options"
        assert job.options.entry_id or job.options.entry_ids or job.options.date, "Need either entry_id or date"
        logging.info('Job\n%s', job.to_json())
        self.system = current_system()
        self.options = job.options
        self.folder = self.system.export_folders[self.options.folder]

        # If we get multiple entry ids, create a new job for each and finish
        if self.options.entry_ids:
            for entry_id in self.options.entry_ids:
                create_job(Job(
                    method=self.method,
                    options=self.Options(
                        entry_id=entry_id,
                        purpose=self.options.purpose,
                        folder=self.options.folder,
                        filename=self.options.filename,
                        longest_side=self.options.longest_side,
                    )
                ))
            return

        if self.options.date is not None:
            self.explode()
            return

        self.entry = get_entry_by_id(self.options.entry_id)
        self.select_source()
        self.select_path()

        if self.options.longest_side == None:
            self.export_plain()

    def explode(self):
        pass
        # Create one new job for each entry in from that date

    def select_source(self):
        purposes = (
            (self.options.purpose,)
            if self.options.purpose is not None
            else (Purpose.original, Purpose.derivative)
        )
        for purpose in purposes:
            self.variant = self.entry.get_variant(purpose, self.options.version)
            if self.variant is not None:
                break
        else:
            raise ValueError('Could not find a suitable variant for %s/%s' % (
                 self.options.purpose.value if self.options.purpose else '*',
                 self.options.version if self.options.version is not None else '*'
            ))

    def select_path(self):
        params = dict(
            id=str(self.entry.id),
            extension=self.variant.get_extension(),
            title=mangle(self.entry.title) if self.entry.title else str(self.entry.id),
            date=self.entry.taken_ts[:10],
            original=self.entry.original_filename,
        )
        self.destination_path = self.folder.get_full_path(**params)

    def export_plain(self):
        filecopy = FileCopy(
            source=os.path.join(
                self.system.media_root,
                self.variant.get_filename(self.entry.id),
            ),
            destination=self.destination_path,
            link=False,
            remove_source=False,
        )
        filecopy.run()
        self.options.filename = filecopy.destination


register_job_handler(JPEGExportJobHandler)


class JPEGMetadata(PropertySet):
    Artist = Property()
    ColorSpace = Property()
    Copyright = Property()
    DateTime = Property()
    DateTimeDigitized = Property()
    DateTimeOriginal = Property()
    ExposureTime = Property(tuple)
    FNumber = Property(tuple)
    Flash = Property()
    FocalLength = Property(tuple)
    FocalLengthIn35mmFilm = Property(int)
    Geometry = Property(tuple)
    ISOSpeedRatings = Property(int)
    Make = Property()
    Model = Property()
    Orientation = Property()
    Mirror = Property()
    Angle = Property(int, default=0)
    Saturation = Property()
    Software = Property()
    SubjectDistanceRange = Property(int)
    WhiteBalance = Property()
    Latitude = Property()
    Longitude = Property()


register_schema(JPEGMetadata)
