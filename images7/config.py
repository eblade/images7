#!/usr/bin/python

import sys
import os
from urllib.parse import urlparse, parse_qsl
from jsonobject import (
    PropertySet,
    Property,
    EnumProperty,
    register_schema,
    get_schema,
)


def get_path(path):
    return path[1:]


def resolve_path(path):
    if not path.startswith('/'):
        path = "$HOME/" + path
    return os.path.expanduser(os.path.expandvars(path))


class SystemConfig(PropertySet):
    name = Property()
    full_name = Property()

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.name = query.netloc
        for key, value in parse_qsl(query.query):
            if key == 'mount_root': value = get_path(value)
            setattr(inst, key, value)
        return inst


class DatabaseConfig(PropertySet):
    server = Property()
    path = Property()

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.server = query.netloc
        inst.path = get_path(query.path)
        for key, value in parse_qsl(query.query):
            setattr(inst, key, value)
        return inst


class HttpConfig(PropertySet):
    server = Property()
    interface = Property(default='localhost')
    port = Property(int, default=8080)
    adapter = Property(default='cheroot')
    debug = Property(bool, default=False)

    @classmethod
    def FromUrl(cls, query):
        print(query)
        inst = cls()
        inst.server = query.netloc
        for key, value in parse_qsl(query.query):
            setattr(inst, key, value)
        return inst


class ZeroMQConfig(PropertySet):
    server = Property()
    interface = Property(default='localhost')
    port = Property(int, default=5555)

    @classmethod
    def FromUrl(cls, query):
        print(query)
        inst = cls()
        inst.server = query.netloc
        for key, value in parse_qsl(query.query):
            setattr(inst, key, value)
        return inst


class ServerConfig(PropertySet):
    hostname = Property()
    mount_root = Property()
    workers = Property(int)

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.hostname = query.netloc
        for key, value in parse_qsl(query.query):
            setattr(inst, key, value)
        return inst


class StorageConfig(PropertySet):
    server = Property()
    root = Property()
    main = Property(bool)

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.server = query.netloc
        inst.root = get_path(query.path)
        for key, value in parse_qsl(query.query):
            setattr(inst, key, value)
        return inst


class ImportMode(EnumProperty):
    raw = 'raw'
    generic = 'generic'


class CardConfig(PropertySet):
    name = Property()
    mode = Property(enum=ImportMode)
    remove_source = Property(bool)
    extension = Property(list)

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.name = query.netloc
        for key, value in parse_qsl(query.query):
            if key == 'extension': value = value.split()
            setattr(inst, key, value)
        return inst


class DropConfig(PropertySet):
    name = Property()
    server = Property()
    path = Property()
    mode = Property(enum=ImportMode)
    remove_source = Property(bool)
    extension = Property(list)

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.server = query.netloc
        inst.path = get_path(query.path)
        for key, value in parse_qsl(query.query):
            if key == 'extension': value = value.split()
            setattr(inst, key, value)
        return inst


class ExportConfig(PropertySet):
    server = Property()
    path = Property()
    name = Property()
    filename = Property()
    longest_sise = Property(int)

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.server = query.netloc
        inst.path = get_path(query.path)
        for key, value in parse_qsl(query.query):
            setattr(inst, key, value)
        return inst


def get_schema_name(name):
    return name.capitalize() + "JobSettings"


class JobConfig(PropertySet):
    name = Property()
    settings = Property(wrap=True)

    @classmethod
    def FromUrl(cls, query):
        inst = cls()
        inst.name = query.netloc
        inst.settings = get_schema(get_schema_name(inst.name))()
        for key, value in parse_qsl(query.query):
            setattr(inst.settings, key, value)
        return inst


class ImportJobSettings(PropertySet):
    proxy_size = Property(int)
    thumb_size = Property(int)
    check_size = Property(int)


class FlickrJobSettings(PropertySet):
    username = Property()
    secret = Property()
    key = Property()


register_schema(ImportJobSettings)
register_schema(FlickrJobSettings)


class Config(PropertySet):
    system = Property(type=SystemConfig)
    https = Property(type=HttpConfig, is_list=True)
    zmqs = Property(type=ZeroMQConfig, is_list=True)
    databases = Property(type=DatabaseConfig, is_list=True)
    servers = Property(type=ServerConfig, is_list=True)
    storages = Property(type=StorageConfig, is_list=True)
    cards = Property(type=CardConfig, is_list=True)
    drops = Property(type=DropConfig, is_list=True)
    exports = Property(type=ExportConfig, is_list=True)
    jobs = Property(type=JobConfig, is_list=True)

    def __init__(self, filename):
        self.filename = filename

        with open(filename, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if line.startswith('#') or line == '': continue
                url = urlparse(line)
                {
                    "system": self.read_system,
                    "http": self.read_http,
                    "zmq": self.read_zmq,
                    "server": self.read_server,
                    "database": self.read_database,
                    "storage": self.read_storage,
                    "card": self.read_card,
                    "drop": self.read_drop,
                    "export": self.read_export,
                    "job": self.read_job,
                }.get(url.scheme, self.read_nothing)(url)

    def read_nothing(self, url):
        pass

    def read_system(self, url):
        self.system = SystemConfig.FromUrl(url)

    def read_http(self, url):
        self.https.append(HttpConfig.FromUrl(url))

    def read_zmq(self, url):
        self.zmqs.append(ZeroMQConfig.FromUrl(url))

    def read_server(self, url):
        self.servers.append(ServerConfig.FromUrl(url))

    def read_database(self, url):
        self.databases.append(DatabaseConfig.FromUrl(url))

    def read_storage(self, url):
        self.storages.append(StorageConfig.FromUrl(url))

    def read_card(self, url):
        self.cards.append(CardConfig.FromUrl(url))

    def read_drop(self, url):
        self.drops.append(DropConfig.FromUrl(url))

    def read_export(self, url):
        self.exports.append(ExportConfig.FromUrl(url))

    def read_job(self, url):
        self.jobs.append(JobConfig.FromUrl(url))


if __name__ == '__main__':
    filename = sys.argv.pop() if len(sys.argv) > 1 else 'images.ini'
    config = Config(filename)
    print(config.to_json())
