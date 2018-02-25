#!/usr/bin/python

from jsonobject import PropertySet, Property
from images7.entry import FilePurpose


_transcoders = {}


def register_transcoder(mime_type, purpose, transcoder):
    _transcoders[(mime_type, purpose)] = transcoder


def get_transcoder(mime_type, purpose):
    transcoder = _transcoders.get((mime_type, purpose))
    if transcoder is None:
        return None
    else:
        return transcoder()


class DefaultTranscodeOptions(PropertySet):
    cut_source = Property()
    size = Property(type=int)
    purpose = Property(enum=FilePurpose)


class Transcoder:
    Options = DefaultTranscodeOptions

    def run(self, options):
        raise NotImplementedError