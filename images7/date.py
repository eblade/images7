import logging
import bottle
import datetime
import urllib
import json
from jsonobject import PropertySet, Property, EnumProperty

from .system import current_system
from .web import (
    FetchByKey,
    FetchByQuery,
    UpdateByKey,
    PatchByKey,
    DeleteByKey,
)


# WEB
#####


class App:
    BASE = '/date'

    @classmethod
    def create(self):
        app = bottle.Bottle()

        app.route(
            path='/',
            callback=FetchByQuery(get_dates, QueryClass=DateQuery),
        )
        app.route(
            path='/<key>',
            callback=FetchByKey(get_date),
        )
        app.route(
            path='/<key>',
            method='PUT',
            callback=UpdateByKey(update_date, Date),
        )
        app.route(
            path='/<key>',
            method='PATCH',
            callback=PatchByKey(patch_date),
        )
        app.route(
            path='/<key>',
            method='DELETE',
            callback=DeleteByKey(delete_date),
        )

        return app


# DESCRIPTOR
############


class DateStats(PropertySet):
    new = Property(int, none=0)
    pending = Property(int, none=0)
    keep = Property(int, none=0)
    purge = Property(int, none=0)
    todo = Property(int, none=0)
    wip = Property(int, none=0)
    final = Property(int, none=0)
    total = Property(int, none=0)


class Date(PropertySet):
    id = Property(name='_id')
    revision = Property(name='_rev')
    date = Property(calculated=True)
    short = Property()
    full = Property()
    mimetype = Property(default='text/plain')

    count = Property(int, default=0, calculated=True)
    stats = Property(DateStats, calculated=True)
    entries = Property(dict, calculated=True)

    self_url = Property(calculated=True)
    date_url = Property(calculated=True)

    def calculate_urls(self):
        if self.date is None:
            self.date = self.id
        self.self_url = App.BASE + '/' + self.date
        self.date_url = '/entry?date=' + self.date

        if self.stats is not None:
            self.count = self.stats.total


class DateFeed(PropertySet):
    count = Property(int)
    entries = Property(Date, is_list=True)


class DateQuery(PropertySet):
    year = Property(int)
    month = Property(int)
    reverse = Property(bool, default=False)

    @classmethod
    def FromRequest(self):
        q = DateQuery()

        if bottle.request.query.year not in (None, ''):
            q.year = bottle.request.query.year
        if bottle.request.query.month not in (None, ''):
            q.month = bottle.request.query.month
        if bottle.request.query.reverse not in (None, ''):
            q.reverse = (bottle.request.query.reverse == 'yes')

        return q

    def to_query_string(self):
        return urllib.parse.urlencode(
            (
                ('year', str(self.year) or ''),
                ('month', str(self.month) or ''),
                ('reverse', 'yes' if self.reverse else 'no'),
            )
        )


# API
#####


def get_dates(query=None):
    if query is None:
        query = DateQuery()

    logging.info(query.to_query_string())
    if query.month is not None:
        sk = (query.year, query.month)
        ek = (query.year, query.month, any)
    elif query.year is not None:
        sk = (query.year, )
        ek = (query.year, any)
    else:
        sk = None
        ek = any
    reverse = query.reverse

    date_stats = [(date['key'], DateStats.FromDict(date['value'])) for date
                  in current_system().db['entry'].view('state_by_date', startkey=sk, endkey=ek, group=True)]
    date_infos = {date.get('key'): Date.FromDict(date['doc']) for date
                  in current_system().db['date'].view('by_date', startkey=sk, endkey=ek, include_docs=True)}
    dates = []
    for date_str, date_stat in date_stats:
        try:
            date = date_infos[date_str]
            date.stats = date_stat
            dates.append(date)
        except KeyError:
            date = Date(date=date_str, stats=date_stat)
            dates.append(date)

    [date.calculate_urls() for date in dates]
    return DateFeed(
        count=len(dates),
        entries=dates if not reverse else list(reversed(dates)),
    )


def get_date(date):
    logging.info(date)
    try:
        result = Date.FromDict(current_system().db['date'][date])
    except KeyError:
        result = Date(date=date)

    try:
        result.stats = DateStats.FromDict(list(current_system().db['entry'].view('state_by_date', key=date, group=True))[0]['value'])
    except IndexError:
        result.stats = DateStats()

    result.calculate_urls()
    return result


def update_date(date, date_info):
    date_info.id = date
    date_info.calculate_urls()
    date_info = date_info.to_dict()
    return current_system().db['date'].save(date_info)


def patch_date(date, patch):
    logging.debug('Patch for %s: \n%s', date, json.dumps(patch, indent=2))
    date_info = get_date(date)

    for key, value in patch.items():
        if key in ('short', 'full'):
            setattr(date_info, key, value)

    return Date.FromDict(update_date(date, date_info))


def delete_date(date):
    current_system().db['date'].delete(date)
