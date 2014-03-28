#-*- coding: utf-8 -*-

from redis import StrictRedis


class CacheHelper(object):
    """A thin wrapper on top of Redis."""
    def __init__(self, host="localhost", port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db

    def __enter__(self):
        self.r = StrictRedis(host=self.host, port=self.port, db=self.db)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if any((exc_type, exc_value, traceback,)):
                raise
        finally:
            pass

    def get(self, key):
        return self.r.get(key) if key in self.r.keys() else None

    def set(self, key, json):
        self.r.set(key, json)

    def keys(self):
        return self.r.keys()

    def reset(self):
        for key in self.keys():
            self.r.delete(key)
