import os
from PIL import Image
import logging
from jsonobject import register_schema, PropertySet, Property

from images7.config import resolve_path
from images7.system import current_system
from images7.files import File, FileStatus, create_file
from images7.entry import (
    Entry,
    FilePurpose,
)
from images7.job.transcode import Transcoder, DefaultTranscodeOptions, register_transcoder
from images7.localfile import FileCopy, calculate_hash
from images7.retry import retry


class ImageProxyOptions(DefaultTranscodeOptions):
    entry = Property(type=Entry)


register_schema(ImageProxyOptions)


class ImageTranscoder(Transcoder):
    Options = ImageProxyOptions

    def run(self, options: ImageProxyOptions):
        logging.info('Starting image transaction.')
        assert options is not None, "Options can't be None"
        logging.info('Options\n%s', options.to_json())

        entry = options.entry
        logging.info('Entry\n%s', entry.to_json())
        self.system = current_system()

        self.full_original_file_path = options.cut_source

        logging.info('Full original file path is %s.',
                    self.full_original_file_path)

        if hasattr(entry.metadata, 'angle'):
            angle = entry.metadata.angle
        else:
            angle = 0

        if hasattr(entry.metadata, 'mirror'):
            mirror =entry.metadata.mirror
        else:
            mirror = 0

        product = None
        if options.purpose == FilePurpose.thumb:
            product = self.generate_rescaled(
                entry.type.value,
                FilePurpose.thumb,
                self.system.config.get_job_settings('import').thumb_size,
                angle,
                mirror,
            )

        elif options.purpose == FilePurpose.proxy:
            product = self.generate_rescaled(
                entry.type.value,
                FilePurpose.proxy,
                self.system.config.get_job_settings('import').proxy_size,
                angle,
                mirror,
            )

        elif options.purpose == FilePurpose.check:
            product = self.create_check(angle, mirror)
        
        if product is None:
            logging.info("Nothing to do.")
            return

        @retry()
        def create():
            create_file(product)
        
        create()

        logging.info("Generated:\n" + product.to_json())

        return product


    #def delete_deprecated(self):
    #    logging.debug('Deleting deprecated variants...')
    #    keep = []
    #    while len(self.entry.variants):
    #        variant = self.entry.variants.pop(0)
    #        if variant.source_purpose is self.source.purpose \
    #                and variant.source_version == self.source.version:

    #            logging.info("Creating delete job for %s.", variant)
    #            delete_job = Job(
    #                method=DeleteJobHandler.method,
    #                options=DeleteJobHandler.Options(
    #                    entry_id=self.entry.id,
    #                    variant=variant,
    #                )
    #            )
    #            delete_job = create_job(delete_job)
    #            logging.info("Created delete job %d.", delete_job.id)

    #        else:
    #            keep.append(variant)

    #    self.entry.variants = keep

    #    logging.debug('Done deleting deprecated variants.')


    def generate_rescaled(self, store, purpose, longest_edge, angle, mirror):
        cut_target = self.full_original_file_path + '_' + purpose.value
        _convert(
            self.full_original_file_path,
            cut_target,
            longest_edge=longest_edge,
            angle=angle,
            mirror=mirror,
        )
        reference = calculate_hash(cut_target)

        main_root = resolve_path(self.system.main_storage.root_path)
        parts = [main_root, store, purpose.value, taken_ts, reference + '.jpg']
        main_path = os.path.join(*parts)
        FileCopy(
            source=cut_target,
            destination=main_path,
            link=True,
            remove_source=True,
        ).run()
        f = File(
            reference=reference,
            url=self.system.main_storage.get_file_url(main_path),
            mime_type='image/jpeg',
            status=FileStatus.managed,
        )
        return f

    def create_check(self, angle, mirror):
        cut_target = self.full_original_file_path + '_check'
        _create_check(
            self.full_original_file_path,
            cut_target,
            angle=angle,
            mirror=mirror,
            size=self.system.config.get_job_settings('import').check_size,
        )

        main_root = resolve_path(self.system.main_storage.root_path)
        parts = [main_root, store, 'check', taken_ts, reference + '.jpg']
        main_path = os.path.join(*parts)
        FileCopy(
            source=cut_target,
            destination=main_path,
            link=True,
            remove_source=True,
        ).run()
        f = File(
            reference=reference,
            url=self.system.main_storage.get_file_url(main_path),
            mime_type='image/jpeg',
            status=FileStatus.managed,
        )
        return f


register_transcoder('image/jpg', 'proxy', ImageTranscoder)
register_transcoder('image/png', 'proxy', ImageTranscoder)
register_transcoder('image/bmp', 'proxy', ImageTranscoder)
register_transcoder('image/gif', 'proxy', ImageTranscoder)


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
