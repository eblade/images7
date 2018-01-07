#!/usr/bin/python

import bottle
from urllib.parse import urlparse


from jsonobject import (
    PropertySet,
    Property,
    EnumProperty,
    Query,
    register_schema,
    get_schema,
)

from .system import current_system
from .config import resolve_path


# WEB
#####

class App:
    BASE = '/file'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        app.route(
            path='/<reference>.<extension>',
            callback=download,
        )
        app.route(
            path='/<reference>',
            callback=download,
        )

        return app


# DESCRIPTOR
############


class File(PropertySet):
    reference = Property()
    url = Property()


# API
#####


def get_urls_by_reference(reference):
    files = current_system() \
        .select('file') \
        .view('by_reference', include_docs=True, key=reference)

    return (urlparse(file.url) for file in files)


def download(id, reference, extension=None):
    system = current_system()
    as_download = bottle.request.query.download == 'yes'
    for url in get_urls_by_reference(reference):
        if scheme == 'local' and url.netloc == system.hostname:
            return bottle.static_file(
                resolve_path(url.path),
                download=as_download,
                root=current_system().media_root
            )

    raise HTTPError(404)
