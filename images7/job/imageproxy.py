import os
from PIL import Image
import logging
from jsonobject import register_schema, PropertySet, Property

from ..system import current_system
from ..job import JobHandler, register_job_handler, Job, create_job
from ..job.delete import DeleteJobHandler
from ..job.rules import RulesJobHandler, RulesOptions
from ..entry import (
    Entry,
    Variant,
    Purpose,
    get_entry_by_id,
    update_entry_by_id,
    delete_entry_by_id,
)


class ImageProxyOptions(PropertySet):
    entry_id = Property(int)
    angle = Property(int)
    mirror = Property(int)
    source_purpose = Property(enum=Purpose, default=Purpose.original)
    source_version = Property(int)


register_schema(ImageProxyOptions)


class ImageProxyJobHandler(JobHandler):
    Options = ImageProxyOptions
    method = 'imageproxy'

    def run(self, job):
        logging.info('Starting image proxy generation.')
        assert job is not None, "Job can't be None"
        assert job.options is not None, "Job Options can't be None"
        logging.info('Job\n%s', job.to_json())
        options = job.options

        self.entry = get_entry_by_id(options.entry_id)
        logging.info('Entry\n%s', self.entry.to_json())
        self.system = current_system()

        self.source = self.entry.get_variant(options.source_purpose, version=options.source_version)

        self.full_original_file_path = os.path.join(
            self.system.media_root,
            self.source.get_filename(self.entry.id),
        )

        logging.info('Full original file path is %s.',
                    self.full_original_file_path)

        if options.angle is not None:
            angle = options.angle
        elif self.source.angle is not None:
            angle = self.source.angle
        elif self.source is Purpose.original and self.entry.metadata:
            angle = self.entry.metadata.Angle
        else:
            angle = 0

        if options.mirror is not None:
            mirror = options.mirror
        elif self.source.mirror is not None:
            mirror = self.source.mirror
        elif self.entry.metadata:
            mirror = self.entry.metadata.Mirror
        else:
            mirror = 0

        thumb = self.create_variant(
            'thumb',
            Purpose.thumb,
            self.system.thumb_size,
            angle,
            mirror,
        )

        proxy = self.create_variant('proxy',
            Purpose.proxy,
            self.system.proxy_size,
            angle,
            mirror,
        )

        check = self.create_check(angle, mirror)

        logging.debug('Created proxy files.')

        self.delete_deprecated()
        self.entry.variants += [thumb, proxy, check]
        self.entry = update_entry_by_id(self.entry.id, self.entry)
        print(self.entry.to_json())

        logging.info('Done with image proxy generation.')

    def delete_deprecated(self):
        logging.debug('Deleting deprecated variants...')
        keep = []
        while len(self.entry.variants):
            variant = self.entry.variants.pop(0)
            if variant.source_purpose is self.source.purpose \
                    and variant.source_version == self.source.version:

                logging.info("Creating delete job for %s.", variant)
                delete_job = Job(
                    method=DeleteJobHandler.method,
                    options=DeleteJobHandler.Options(
                        entry_id=self.entry.id,
                        variant=variant,
                    )
                )
                delete_job = create_job(delete_job)
                logging.info("Created delete job %d.", delete_job.id)

            else:
                keep.append(variant)

        self.entry.variants = keep

        logging.debug('Done deleting deprecated variants.')


    def create_variant(self, store, purpose, longest_edge, angle, mirror):
        variant = Variant(
            store=store,
            mime_type='image/jpeg',
            purpose=purpose,
            version=self.entry.get_next_version(purpose),
            source_purpose=self.source.purpose,
            source_version=self.source.version,
        )
        full_path = os.path.join(
            self.system.media_root,
            variant.get_filename(self.entry.id),
        )
        variant.width, variant.height = _convert(
            self.full_original_file_path,
            full_path,
            longest_edge=longest_edge,
            angle=angle,
            mirror=mirror,
        )
        s = os.stat(full_path)
        variant.size=s.st_size
        return variant

    def create_check(self, angle, mirror):
        variant = Variant(
            store='check',
            mime_type='image/jpeg',
            purpose=Purpose.check,
            version=self.entry.get_next_version(Purpose.check),
            source_purpose=self.source.purpose,
            source_version=self.source.version,
        )
        full_path = os.path.join(
            self.system.media_root,
            variant.get_filename(self.entry.id),
        )
        variant.width, variant.height = _create_check(
            self.full_original_file_path,
            full_path,
            angle=angle,
            mirror=mirror,
            size=self.system.check_size,
        )
        s = os.stat(full_path)
        variant.size=s.st_size
        return variant


register_job_handler(ImageProxyJobHandler)



def _create_check(path_in, path_out, size=200, angle=None, mirror=None):
    os.makedirs(os.path.dirname(path_out), exist_ok=True)

    with open(path_out, 'w') as out:
        img = Image.open(path_in)
        width, height = img.size

        left = int((width - size) / 2)
        top = int((height - size) / 2)
        right = int((width + size) / 2)
        bottom = int((height + size) / 2)

        logging.debug('Cropping %i %i %i %i', left, top, right, bottom)
        cropped = img.crop((left, top, right, bottom))
        img.close()

        if mirror == 'H':
            cropped = cropped.transpose(Image.FLIP_RIGHT_LEFT)
        elif mirror == 'V':
            cropped = cropped.transpose(Image.FLIP_TOP_BOTTOM)
        if angle:
            logging.debug('Rotating by %i degrees', angle)
            cropped = cropped.rotate(angle)

        cropped.save(out, "JPEG", quality=98)
        cropped.close()
        logging.debug("Created check %s", path_out)
        return size, size


def _convert(path_in, path_out, longest_edge=1280, angle=None, mirror=None):
    os.makedirs(os.path.dirname(path_out), exist_ok=True)

    with open(path_out, 'w') as out:
        img = Image.open(path_in)
        width, height = img.size
        if width > height:
            scale = float(longest_edge) / float(width)
        else:
            scale = float(longest_edge) / float(height)
        w = int(width * scale)
        h = int(height * scale)
        logging.debug('_resize %i %i %i', h, w, angle)
        _resize(img, (w, h), out, angle, mirror)
        logging.info("Created image %s", path_out)
        return w, h


def _resize(img, box, out, angle, mirror):
    '''Downsample the image.
    @param img: Image -  an Image-object
    @param box: tuple(x, y) - the bounding box of the result image
    @param out: file-like-object - save the image into the output stream
    @param angle: int - rotate with this angle
    @param mirror: str - mirror in this direction, None, "H" or "V"
    '''
    # Preresize image with factor 2, 4, 8 and fast algorithm
    factor = 1
    bw, bh = box
    iw, ih = img.size
    while (iw * 2 / factor > 2 * bw) and (ih * 2 / factor > 2 * bh):
        factor *= 2
    factor /= 2
    if factor > 1:
        logging.debug('factor = %d: Scale down to %ix%i', factor, int(iw / factor), int(ih / factor))
        img.thumbnail((iw / factor, ih / factor), Image.NEAREST)

    # Resize the image with best quality algorithm ANTI-ALIAS
    logging.debug('Final scale down to %ix%i', box[0], box[1])
    img.thumbnail(box, Image.ANTIALIAS)
    if mirror == 'H':
        img = img.transpose(Image.FLIP_RIGHT_LEFT)
    elif mirror == 'V':
        img = img.transpose(Image.FLIP_TOP_BOTTOM)
    if angle:
        logging.debug('Rotating by %i degrees', angle)
        img = img.rotate(angle, resample=Image.BICUBIC, expand=True)

    # Save it into a file-like object
    img.save(out, "JPEG", quality=90)
