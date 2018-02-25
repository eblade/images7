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

from images7.multi import QueueServer, QueueWorker, QueueClient
from images7.system import current_system


# WEB
#####


class App:
    BASE = '/job'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        return app

    @classmethod
    def run(self, workers=1):
        queue_thread = QueueServer('ipc://job_queue', Dispatcher, workers=workers)
        queue_thread.start()


class Dispatcher(QueueWorker):
    def work(self, message):
        job = Job.FromJSON(message)
        job.status = JobStatus.running

        while job.status == JobStatus.running:
            step = job.get_current_step()

            Handler = get_job_handler_for_method(step.method)

            if Handler is None:
                logging.error('Method %s is not supported', str(step.method))
                return

            Handler().run(job)
            job.get_on_with_it()


# DESCRIPTOR
############


class StepStatus(EnumProperty):
    new = "new"
    done = "done"
    running = "running"
    failed = "failed"


class Step(PropertySet):
    name = Property()
    method = Property(required=True)
    options = Property(wrap=True)
    result = Property(wrap=True)
    status = Property(enum=StepStatus, default=StepStatus.new)


class JobStatus(EnumProperty):
    new = "new"
    done = "done"
    running = "running"
    failed = "failed"


class Job(PropertySet):
    current_step = Property(type=int, default=0)
    steps = Property(type=Step, is_list=True)
    status = Property(enum=JobStatus, default=JobStatus.new)

    def get_current_step(self) -> Step:
        return self.steps[self.current_step]
    
    def get_on_with_it(self):
        step = self.get_current_step()
        if step.status != StepStatus.done:
            self.status = JobStatus.failed
            step.status = StepStatus.failed
        else:
            self.current_step += 1
            if self.current_step < len(self.steps):
                self.status = JobStatus.running
            else:
                self.status = JobStatus.done
    
    def get_step(self, method) -> Step:
        return next(filter(lambda x: x.method == method, self.steps), None)


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
    return _handlers.get(method)


class JobHandler(object):
    method = None
    Options = lambda: None

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key.replace(' ', '_'), value)

    def run(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def AsStep(cls, **kwargs):
        return Step(method=cls.method, options=cls.Options(**kwargs))


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
