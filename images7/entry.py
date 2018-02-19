#!/usr/bin/python

import bottle
import uuid
import logging
import datetime

from jsonobject import (
    PropertySet,
    Property,
    EnumProperty,
    Query,
    register_schema,
    get_schema,
)

from .system import current_system
from .files import App as FileApp
from .web import (
    Create,
    FetchById,
    FetchByQuery,
    UpdateById,
    UpdateByIdAndQuery,
    PatchById,
    DeleteById,
)


# WEB
#####

class App:
    BASE = '/entry'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        app.route(
            path='/',
            callback=FetchByQuery(get_entries, QueryClass=EntryQuery),
        )
        app.route(
            path='/<id>',
            callback=FetchById(get_entry_by_id),
        )
        app.route(
            path='/<id:int>',
            method='PUT',
            callback=UpdateById(update_entry_by_id, Entry),
        )
        app.route(
            path='/<id:int>/metadata',
            method='PATCH',
            callback=PatchById(patch_entry_metadata_by_id),
        )
        app.route(
            path='/<id:int>',
            method='PATCH',
            callback=PatchById(patch_entry_by_id),
        )
        app.route(
            path='/',
            method='POST',
            callback=Create(create_entry, Entry),
        )
        app.route(
            path='/<id:int>',
            method='DELETE',
            callback=DeleteById(delete_entry_by_id),
        )
        app.route(
            path='/<id:int>/state',
            method='PUT',
            callback=UpdateByIdAndQuery(update_entry_state, QueryClass=StateQuery),
        )
        app.route(
            path='/<id:int>/dl/<store>/<version:int>.<extension>',
            callback=download,
        )

        return app


# DESCRIPTOR
############

class State(EnumProperty):
    new = 'new'
    pending = 'pending'
    keep = 'keep'
    todo = 'todo'
    final = 'final'
    wip = 'wip'
    purge = 'purge'


class FilePurpose(EnumProperty):
    raw = 'raw'
    derivative = 'derivative'
    original = 'original'
    proxy = 'proxy'
    thumb = 'thumb'
    check = 'check'
    unknown = 'unknown'


class EntryType(EnumProperty):
    image = 'image'
    video = 'video'
    audio = 'audio'
    document = 'document'
    other = 'other'


class FileReference(PropertySet):
    purpose = Property(enum=FilePurpose)
    version = Property(int)
    reference = Property()
    extension = Property()


class Entry(PropertySet):
    id = Property(name='_id')
    revision = Property(name='_rev')
    type = Property(enum=EntryType, default=EntryType.image)
    state = Property(enum=State, default=State.new)
    files = Property(type=FileReference, is_list=True)
    metadata = Property(wrap=True)
    tags = Property(list)

    urls = Property(dict, calculated=True)
    state_url = Property(calculated=True)

    self_url = Property(calculated=True)
    original_url = Property(calculated=True)
    thumb_url = Property(calculated=True)
    proxy_url = Property(calculated=True)
    check_url = Property(calculated=True)
    raw_url = Property(calculated=True)
    derivative_url = Property(calculated=True)

    def calculate_urls(self):
        self.self_url = '%s/%s' % (App.BASE, self.id)
        self.state_url = '%s/%s/state' % (App.BASE, self.id)
        self.urls = {}
        for reference in self.files:
            if not reference.purpose.value in self.urls.keys():
                self.urls[reference.purpose.value] = {}
            url = '%s/%s%s' % (
                FileApp.BASE,
                reference.reference,
                reference.extension,
            )
            self.urls[reference.purpose.value][reference.version] = url
            if reference.purpose is FilePurpose.original:
                self.original_url = url
            elif reference.purpose is FilePurpose.proxy:
                self.proxy_url = url
            elif reference.purpose is FilePurpose.thumb:
                self.thumb_url = url
            elif reference.purpose is FilePurpose.check:
                self.check_url = url
            elif reference.purpose is FilePurpose.raw:
                self.raw_url = url
            elif reference.purpose is FilePurpose.derivative:
                self.derivative_url = url


class DefaultEntryMetadata(PropertySet):
    title = Property()
    description = Property()
    author = Property()
    copyright = Property()
    source = Property()
    original_filename = Property()
    taken_ts = Property()
    orientation = Property()
    mirror = Property()
    angle = Property(int, default=0)

    def merge(self, other):
        for k, v in other.to_dict().items():
            if hasattr(self, k):
                setattr(self, k, v)

register_schema(DefaultEntryMetadata)


class EntryFeed(PropertySet):
    count = Property(int)
    total_count = Property(int)
    offset = Property(int)
    date = Property()
    state = Property(enum=State)
    entries = Property(Entry, is_list=True)


class EntryQuery(Query):
    prev_offset = Property(int)
    offset = Property(int, default=0)
    page_size = Property(int, default=25, required=True)
    date = Property()
    state = Property(enum=State)
    delta = Property(int, default=0)
    reverse = Property(bool, default=False)


class StateQuery(Query):
    state = Property()
    soft = Property(bool, default=False)


#####
# API

def get_entries(query=None):
    if query is None:
        offset = 0
        page_size = 500
        date = None
        state = None
        delta = 0
        reverse = False

    else:
        offset = query.offset
        page_size = query.page_size
        date = query.date
        delta = query.delta
        reverse = query.reverse
        state = query.state

    if state is not None:
        entry_data = current_system().db['entry'].view(
            'by_state_and_taken_ts',
            startkey=(state.value, None),
            endkey=(state.value, any),
            include_docs=True,
            skip=offset,
            limit=page_size,
        )

    elif date is not None:
        if date == 'today':
            date = datetime.date.today()
        else:
            date = (int(part) for part in date.split('-', 2))
            date = datetime.date(*date)
        date += datetime.timedelta(days=delta)
        entry_data = current_system().db['entry'].view(
            'by_taken_ts',
            startkey=(date.year, date.month, date.day),
            endkey=(date.year, date.month, date.day, any),
            include_docs=True,
            skip=offset,
            limit=page_size,
        )

    else:
        entry_data = current_system().db['entry'].view(
            'by_taken_ts',
            include_docs=True,
            skip=offset,
            limit=page_size,
        )


    entries = [Entry.FromDict(entry.get('doc')) for entry in entry_data]
    for entry in entries:
        entry.calculate_urls()
    return EntryFeed(
        date=(date.isoformat() if date else None),
        state=state,
        count=len(entries),
        offset=offset,
        entries=entries if not reverse else list(reversed(entries)),
    )


def get_entry_by_id(id):
    entry = Entry.FromDict(current_system().db['entry'][id])
    entry.calculate_urls()
    return entry


def get_entry_by_source(folder, filename):
    entry_data = list(current_system().db['entry'].view(
        'by_source',
        key=(folder, filename),
        include_docs=True
    ))
    if len(entry_data) > 0:
        return Entry.FromDict(entry_data[0]['doc'])
    else:
        return None


def get_entries_by_reference(reference):
    entry_data = current_system() \
        .select('entry') \
        .view('by_file_reference', reference, include_docs=True)

    entries = [Entry.FromDict(entry.get('doc')) for entry in entry_data]
    for entry in entries:
        entry.calculate_urls()
    return EntryFeed(
        count=len(entries),
        entries=entries,
    )


def update_entry_by_id(id, entry):
    entry.id = id
    logging.debug('Updating entry to\n%s', entry.to_json())
    entry = Entry.FromDict(current_system().db['entry'].save(entry.to_dict()))
    return entry


def update_entry_state(id, query):
    try:
        state = getattr(State, query.state)
    except AttributeError:
        raise bottle.HTTPError(400)

    entry = get_entry_by_id(id)

    if query.soft and entry.state != State.pending:
        return entry

    entry.state = state
    return update_entry_by_id(id, entry)


def patch_entry_metadata_by_id(id, patch):
    entry = get_entry_by_id(id)
    logging.debug('Metadata Patch for %d: \n%s', id, json.dumps(patch, indent=2))
    metadata_dict = entry.metadata.to_dict()
    metadata_dict.update(patch)
    metadata = wrap_dict(metadata_dict)
    entry.metadata = metadata
    logging.debug(entry.to_json())
    current_system().db['entry'].save(entry.to_dict())

    if 'Angle' in patch:
        options = get_schema('ImageProxyOptions')()
        options.entry_id = id
        #trig_plugin('imageproxy', options)

    return get_entry_by_id(id)


def patch_entry_by_id(id, patch):
    logging.debug('Patch for %d: \n%s', id, json.dumps(patch, indent=2))
    entry = get_entry_by_id(id)

    for key, value in patch.items():
        if key in Entry._patchable:
            setattr(entry, key, value)
        elif key == 'variants':
            purpose = value['purpose']
            version = value['version']
            variant = entry.get_variant(purpose, version=version)
            for key, value in value.items():
                if key in Variant._patchable:
                    setattr(variant, key, value)

    logging.debug(entry.to_json())
    current_system().db['entry'].save(entry.to_dict())
    return get_entry_by_id(id)


def create_entry(ed):
    if ed.id is None:
        ed.id = uuid.uuid4().hex
    logging.debug('Create entry\n%s', ed.to_json())
    return Entry.FromDict(current_system().select('entry').save(ed.to_dict()))


def delete_entry_by_id(id):
    current_system().db['entry'].delete(id)


def download(id, store, version, extension):
    download = bottle.request.query.download == 'yes'
    entry = get_entry_by_id(id)
    for variant in entry.variants:
        if variant.store == store and variant.version == version and variant.get_extension() == '.' + extension:
            return bottle.static_file(
                variant.get_filename(id),
                download=download,
                root=current_system().media_root
            )

    raise bottle.HTTPError(404)
