import os
import bottle
import urllib

from .system import current_system


class App:
    BASE = '/'
    HTML = 'web'
    JS = os.path.join(HTML, 'js')
    CSS = os.path.join(HTML, 'css')
    GRAPHICS = os.path.join(HTML, 'graphics')


    @classmethod
    def create(self):
        app = bottle.Bottle()

        # Static files
        app.route(
            path='/',
            callback=lambda: bottle.static_file('index.html', root=self.HTML),
        )
        app.route(
            path='/favicon.png',
            callback=lambda: bottle.static_file('images6.png', root='.'),
        )
        app.route(
            path='/js/<fn>.js',
            callback=lambda fn: bottle.static_file(fn + '.js', root=self.JS),
        )
        app.route(
            path='/css/<fn>.css',
            callback=lambda fn: bottle.static_file(fn + '.css', root=self.CSS),
        )
        app.route(
            path='/graphics/<fn>.png',
            callback=lambda fn: bottle.static_file(fn + '.png', root=self.GRAPHICS),
        )
        app.route(
            path='/graphics/<fn>.json',
            callback=lambda fn: bottle.static_file(fn + '.json', root=self.GRAPHICS),
        )
        app.route(
            path='/view-date',
            callback=lambda: bottle.static_file('date.html', root=self.HTML),
        )
        app.route(
            path='/view-tag',
            callback=lambda: bottle.static_file('tag.html', root=self.HTML),
        )
        app.route(
            path='/view-state',
            callback=lambda: bottle.static_file('state.html', root=self.HTML),
        )

        app.route(
            path='/shutdown',
            callback=lambda: current_system().close(),
        )

        return app


class ResourceBusy(Exception):
    pass


def Create(function, InputClass, pre=None):
    def f():
        if callable(pre):
            pre()
        o = InputClass.FromDict(bottle.request.json)
        print(o.to_json())
        if o is None:
            raise bottle.HTTPError(400)
        result = function(o)
        return result.to_dict(include_calculated=True)
    return f


def Fetch(function, pre=None):
    def f():
        if callable(pre):
            pre()
        result = function()
        return result.to_dict(include_calculated=True)
    return f


def FetchById(function, pre=None):
    def f(id):
        if callable(pre):
            pre()
        if id is None:
            raise bottle.HTTPError(400)
        result = function(id)
        return result.to_dict(include_calculated=True)
    return f


def FetchByKey(function, pre=None):
    def f(key):
        if callable(pre):
            pre()
        if key is None:
            raise bottle.HTTPError(400)
        result = function(key)
        return result.to_dict(include_calculated=True)
    return f


def FetchByQuery(function, QueryClass=None, pre=None):
    def f():
        if callable(pre):
            pre()
        if QueryClass is None:
            result = function()
        else:
            q = make_query_from_request(QueryClass)
            result = function(q)
        return result.to_dict(include_calculated=True)
    return f


def UpdateById(function, InputClass, pre=None):
    def f(id):
        if callable(pre):
            pre()
        o = InputClass.FromDict(bottle.request.json)
        if o is None:
            raise bottle.HTTPError(400)
        result = function(id, o)
        return result.to_dict(include_calculated=True)
    return f


def PatchById(function, pre=None):
    def f(id):
        if callable(pre):
            pre()
        o = bottle.request.json
        if o is None:
            raise bottle.HTTPError(400)
        result = function(id, o)
        return result.to_dict(include_calculated=True)
    return f


def PatchByKey(function, pre=None):
    def f(key):
        if callable(pre):
            pre()
        o = bottle.request.json
        if o is None:
            raise bottle.HTTPError(400)
        result = function(key, o)
        return result.to_dict(include_calculated=True)
    return f


def UpdateByKey(function, InputClass, pre=None):
    def f(key):
        if callable(pre):
            pre()
        o = InputClass.FromDict(bottle.request.json)
        if o is None:
            raise bottle.HTTPError(400)
        result = function(key, o)
        return result.to_dict(include_calculated=True)
    return f


def UpdateByIdAndQuery(function, QueryClass=None, pre=None):
    def f(id):
        if callable(pre):
            pre()
        if QueryClass is None:
            result = function(id)
        else:
            q = make_query_from_request(QueryClass)
            result = function(id, q)
        return result.to_dict(include_calculated=True)
    return f


def DeleteById(function, pre=None):
    def f(id):
        if callable(pre):
            pre()
        if id is None:
            raise bottle.HTTPError(400)
        function(id)
        raise bottle.HTTPError(204)
    return f


def DeleteByKey(function, pre=None):
    def f(key):
        if callable(pre):
            pre()
        if key is None:
            raise bottle.HTTPError(400)
        function(key)
        raise bottle.HTTPError(204)
    return f


def Delete(function, pre=None):
    def f():
        if callable(pre):
            pre()
        if id is None:
            raise bottle.HTTPError(400)
        function()
        raise bottle.HTTPError(204)
    return f


def make_query_from_request(QueryClass):
    return QueryClass.FromDict(bottle.request.query.decode())
