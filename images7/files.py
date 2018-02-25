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

from images7.system import current_system
from images7.config import resolve_path


# WEB
#####

class App:
    BASE = '/file'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        #app.route(
        #    path='/<reference>.<extension>',
        #    callback=download,
        #)
        #app.route(
        #    path='/<reference>',
        #    callback=download,
        #)

        return app


# DESCRIPTOR
############


class FileStatus(EnumProperty):
    new = 'new'
    managed = 'managed'


class File(PropertySet):
    reference = Property()
    revision = Property(int, name='_rev')
    url = Property(name='_id')
    mime_type = Property()
    status = Property(enum=FileStatus)

    @property
    def parsed_url(self):
        return urlparse(self.url)



# API
#####


def get_file_by_url(url):
    return File.FromDict(
        current_system()
            .select('file')[url])


def get_urls_by_reference(reference):
    files = current_system() \
        .select('file') \
        .view('by_reference', include_docs=False, key=reference)

    return (urlparse(file['_id']) for file in files)


def create_file(f):
    return File.FromDict(
        current_system()
            .select('file')
            .save(f.to_dict()))


#def download(id, reference, extension=None):
#    system = current_system()
#    as_download = bottle.request.query.download == 'yes'
#    for url in get_urls_by_reference(reference):
#        if scheme == 'local' and url.netloc == system.hostname:
#            return bottle.static_file(
#                resolve_path(url.path),
#                download=as_download,
#                root=current_system().media_root
#            )
#
#    raise HTTPError(404)
