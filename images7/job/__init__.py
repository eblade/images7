import logging
import bottle
import threading
import time
import jsondb
import urllib
import traceback

from jsonobject import (
    PropertySet,
    Property,
    EnumProperty,
    wrap_dict,
    get_schema,
    register_schema,
)


from ..multi import start_queue, spawn_worker
from ..system import current_system

from ..web import (
    Create,
    FetchById,
    FetchByQuery,
    PatchById,
    Delete,
)


# WEB
#####


class App:
    BASE = '/job'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        app.route(
            path='/',
            callback=FetchByQuery(get_jobs, QueryClass=JobQuery)
        )
        app.route(
            path='/<id:int>',
            callback=FetchById(get_job_by_id)
        )
        app.route(
            path='/<id:int>',
            method='PATCH',
            callback=PatchById(patch_job_by_id),
        )
        app.route(
            path='/',
            method='POST',
            callback=Create(create_job, Job),
        )
        app.route(
            path='/',
            method='DELETE',
            callback=Delete(delete_jobs),
        )

        return app

    @classmethod
    def run(self, workers=1):
        context = current_system().zmq_context
        channel = 'ipc://job'
        queue_thread = threading.Thread(
            target=start_queue,
            name='job_queue',
            args=(context, channel),
        )
        queue_thread.daemon = True
        queue_thread.start()

        for n in range(workers):
            worker_thread = threading.Thread(
                target=spawn_worker,
                name='worker_%i' % n,
                args=(context, channel),
            )
            worker_thread.daemon = True
            worker_thread.start()


def dispatch(job):
    try:
        Handler = get_job_handler_for_method(job.method)
    except KeyError:
        logging.error('Method %s is not supported', str(job.method))
        job.state = State.failed
        job.message = 'Method %s is not supported' % str(job.method)
        job.updated = time.time()
        job.stopped = time.time()
        current_system().db['job'].save(job.to_dict())
        return

    try:
        config = current_system().job_config.get(job.method, dict())
        Handler(**config).run(job)
        job.state = State.done
        job.updated = time.time()
        job.stopped = time.time()
        current_system().db['job'].save(job.to_dict())
    except Exception as e:
        logging.exception('Job of type %s failed with %s: %s', str(job.method), e.__class__.__name__, str(e))
        job.state = State.failed
        job.message = 'Job of type %s failed with %s: %s' % (str(job.method), e.__class__.__name__, str(e))
        job.stopped = time.time()
        job.updated = time.time()
        current_system().db['job'].save(job.to_dict())


# DESCRIPTOR
############


class State(EnumProperty):
    new = 'new'
    acquired = 'acquired'
    active = 'active'
    done = 'done'
    held = 'held'
    failed = 'failed'


class Job(PropertySet):
    id = Property(int, name='_id')
    revision = Property(name='_rev')
    method = Property(required=True)
    state = Property(enum=State, default=State.new)
    priority = Property(int, default=1000)
    message = Property()
    release = Property(float)
    options = Property(wrap=True)
    created = Property(float)
    updated = Property(float)
    started = Property(float)
    stopped = Property(float)

    self_url = Property(calculated=True)

    _patchable = ('state', )

    def calculate_urls(self):
        self.self_url = '%s/%i' % (App.BASE, self.id)


class JobStats(PropertySet):
    new = Property(int, none=0)
    acquired = Property(int, none=0)
    active = Property(int, none=0)
    done = Property(int, none=0)
    held = Property(int, none=0)
    failed = Property(int, none=0)
    total = Property(int, none=0)


class JobFeed(PropertySet):
    count = Property(int)
    total_count = Property(int)
    offset = Property(int)
    stats = Property(JobStats)
    entries = Property(Job, is_list=True)


class JobQuery(PropertySet):
    prev_offset = Property(int)
    offset = Property(int, default=0)
    page_size = Property(int, default=25, required=True)
    state = Property(enum=State)

    @classmethod
    def FromRequest(self):
        eq = JobQuery()

        if bottle.request.query.prev_offset not in (None, ''):
            eq.prev_offset = bottle.request.query.prev_offset
        if bottle.request.query.offset not in (None, ''):
            eq.offset = bottle.request.query.offset
        if bottle.request.query.page_size not in (None, ''):
            eq.page_size = bottle.request.query.page_size
        if bottle.request.query.state not in (None, ''):
            eq.state = getattr(State, bottle.request.query.state)

        return eq

    def to_query_string(self):
        return urllib.parse.urlencode(
            (
                ('prev_offset', self.prev_offset or ''),
                ('offset', self.offset),
                ('page_size', self.page_size),
                ('state', self.state.value if self.state is not None else ''),
            )
        )


# API
#####


def get_jobs(query):
    if query is None:
        offset = 0
        page_size = 25
        state = None

    else:
        offset = query.offset
        page_size = query.page_size
        state = None if query.state is None else query.state.value

    if state is None:
        data = current_system().db['job'].view(
            'by_updated',
            startkey=None,
            endkey=any,
            include_docs=True,
        )
    else:
        data = current_system().db['job'].view(
            'by_state',
            startkey=(state, None, None),
            endkey=(state, any, any),
            include_docs=True,
        )

    stats = list(current_system().db['job'].view('stats', group=True))
    stats = JobStats.FromDict(stats[0]['value']) if len(stats) else JobStats()

    entries = []
    for i, d in enumerate(data):
        if i < offset:
            continue
        elif i >= offset + page_size:
            break
        entries.append(Job.FromDict(d['doc']))

    return JobFeed(
        offset=offset,
        count=len(entries),
        total_count=stats.total,
        page_size=page_size,
        stats=stats,
        entries=entries,
    )


def get_job_by_id(id):
    job = Job.FromDict(current_system().db['job'][id])
    job.calculate_urls()
    return job


def create_job(job):
    if job.method is None:
        raise bottle.HTTPError(400, 'Missing parameter "method".')
    job.state = State.new
    job.created = time.time()
    job.updated = time.time()
    job._id = None
    job._rev = None
    job = Job.FromDict(current_system().db['job'].save(job.to_dict()))
    logging.debug('Created job\n%s', job.to_json())
    return job


def patch_job_by_id(id, patch):
    logging.debug('Patch job %d: \n%s', id, json.dumps(patch, indent=2))
    job = get_job_by_id(id)
    for key, value in patch.items():
        if key in Job._patchable:
            setattr(job, key, value)

    job.updated = time.time()
    job = Job.FromDict(current_system().db['entry'].save(entry.to_dict()))
    return job


def delete_jobs():
    logging.debug("Delete jobs.")
    current_system().db['job'].clear()



# JOB HANDLING
##############

_handlers = {}


def register_job_handler(module):
    if not hasattr(module, 'method'):
        raise ValueError("Handler must support the method attribute")
    if not hasattr(module, 'run') or not callable(module.run):
        raise ValueError("Handler must support the run method")
    if module.method in _handlers:
        raise KeyError("Handler for method %s already registred" % module.method)
    _handlers[module.method] = module


def get_job_handler_for_method(method):
    return _handlers[method]


class JobHandler(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key.replace(' ', '_'), value)

    def run(self, *args, **kwargs):
        raise NotImplemented


class DummyOptions(PropertySet):
    time = Property(float, default=1.0)


class DummyJobHandler(JobHandler):
    method = 'dummy'
    Options = DummyOptions

    def run(self, job):
        options = job.options
        time.sleep(options.time)


register_job_handler(DummyJobHandler)
register_schema(DummyOptions)
