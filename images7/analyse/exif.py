from jsonobject import PropertySet, Property, register_schema
import exifread

from images7.analyse import register_analyser
from images7.entry import DefaultEntryMetadata


def read_exif(path):
    exif = None
    with open(path, 'rb') as f:
        exif = exifread.process_file(f)

    # Orientation (rotation)
    orientation, mirror, angle = exif_orientation(exif)

    # GPS Position
    lon, lat = exif_position(exif)

    return ExifMetadata(**{
        "taken_ts": exif_timestamp(exif, "EXIF DateTimeOriginal"),
        "author": exif_string(exif, "Image Artist"),
        "copyright": exif_string(exif, "Image Copyright"),
        "Orientation": orientation,
        "mirror": mirror,
        "angle": angle,

        "color_space": exif_string(exif, "EXIF ColorSpace"),
        "geometry": (exif_int(exif, "EXIF ExifImageWidth"), exif_int(exif, "EXIF ExifImageLength")),
        "datetime": exif_timestamp(exif, "EXIF DateTime"),
        "datetime_digitized": exif_timestamp(exif, "EXIF DateTimeDigitized"),
        "exposure_time": exif_ratio(exif, "EXIF ExposureTime"),
        "f_number": exif_ratio(exif, "EXIF FNumber"),
        "flash": exif_string(exif, "EXIF Flash"),
        "focal_length": exif_ratio(exif, "EXIF FocalLength"),
        "focal_length_in_35_mm_film": exif_int(exif, "EXIF FocalLengthIn35mmFilm"),
        "iso_speed_ratings": exif_int(exif, "EXIF ISOSpeedRatings"),
        "make": exif_string(exif, "Image Make"),
        "model": exif_string(exif, "Image Model"),
        "saturation": exif_string(exif, "EXIF Saturation"),
        "software": exif_string(exif, "Software"),
        "subject_distance_range": exif_int(exif, "EXIF SubjectDistanceRange"),
        "white_balance": exif_string(exif, "WhiteBalance"),
        "latitude": lat,
        "longitude": lon,
    })


register_analyser('image/jpeg', read_exif)
register_analyser('image/tiff', read_exif)
register_analyser('image/png', read_exif)


class ExifMetadata(DefaultEntryMetadata):
    color_space = Property()
    date_time = Property()
    datetime_digitized = Property()
    exposure_time = Property(tuple)
    f_number = Property(tuple)
    flash = Property()
    focal_length = Property(tuple)
    focal_length_in_35_mm_film = Property(int)
    geometry = Property(tuple)
    iso_speed_ratings = Property(int)
    make = Property()
    model = Property()
    saturation = Property()
    software = Property()
    subject_distance_range = Property(int)
    white_balance = Property()
    latitude = Property()
    longitude = Property()


register_schema(ExifMetadata)


# Helper functions to convert exif data into better formats

orientation2angle = {
   'Horizontal (normal)': (None, 0),
   'Mirrored horizontal': ('H', 0),
   'Rotated 180': (None, 180),
   'Mirrored vertical': ('V', 0),
   'Mirrored horizontal then rotated 90 CCW': ('H', -90),
   'Rotated 90 CCW': (None, -90),
   'Mirrored horizontal then rotated 90 CW': ('H', 90),
   'Rotated 90 CW': (None, 90),
}


def exif_position(exif):
    """Reads exifread tags and extracts a float tuple (lat, lon)"""
    lat = exif.get("GPS GPSLatitude")
    lon = exif.get("GPS GPSLongitude")
    if None in (lat, lon):
        return None, None

    lat = dms_to_float(lat)
    lon = dms_to_float(lon)
    if None in (lat, lon):
        return None, None

    if exif.get('GPS GPSLatitudeRef').printable == 'S':
        lat *= -1
    if exif.get('GPS GPSLongitudeRef').printable == 'S':
        lon *= -1

    return lat, lon


def dms_to_float(p):
    """Converts exifread data points to decimal GPX floats"""
    try:
        degree = p.values[0]
        minute = p.values[1]
        second = p.values[2]
        return (
            float(degree.num)/float(degree.den) +
            float(minute.num)/float(minute.den)/60 +
            float(second.num)/float(second.den)/3600
        )
    except AttributeError:
        return None


def exif_timestamp(exif, key):
    p = exif.get(key)
    if p:
        s = p.printable.strip()
        parts = s.split()
        return parts[0].replace(':', '-') + ' ' + parts[1]


def exif_string(exif, key):
    p = exif.get(key)
    if p:
        return p.printable.strip()


def exif_int(exif, key):
    p = exif.get(key)
    if p:
        return int(p.printable or 0)


def exif_ratio(exif, key):
    p = exif.get(key)
    try:
        if p:
            p = p.values[0]
            return int(p.num), int(p.den)
    except AttributeError:
        if isinstance(p, int):
            return p


def exif_orientation(exif):
    orientation = exif.get("Image Orientation")
    if orientation is None:
        return None, None, 0

    mirror, angle = orientation2angle.get(orientation.printable)
    return orientation.printable, mirror, angle
