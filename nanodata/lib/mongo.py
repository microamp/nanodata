#-*- coding: utf-8 -*-

from pymongo import MongoClient
from bson import ObjectId


class MongoHelper(object):
    """A thin wrapper on top of PyMongo."""
    def __init__(self, hosts, name):
        self.hosts = hosts
        self.name = name

    def __enter__(self):
        self.client = MongoClient(host=self.hosts)
        self.db = self.client[self.name]
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if any((exc_type, exc_value, traceback,)):
                raise
        finally:
            self.client.disconnect()

    def drop(self, coll):
        if hasattr(self.db, coll):
            getattr(self.db, coll).drop()

    def write(self, coll, doc):
        self.db[coll].insert(doc)

    def read(self, coll, query=None):
        return (self.db[coll].find(query)
                if query is not None else
                self.db[coll].find())

    def read_one(self, coll, query=None):
        return (self.db[coll].find_one(query)
                if query is not None else
                self.db[coll].find_one())

    def read_by_id(self, coll, id_):
        return self.read_one(coll, query={"_id": ObjectId(id_)})


if __name__ == "__main__":
    dbname, collname = "mydb", "mycoll"
    with MongoHelper(("localhost",), dbname) as db:
        db.drop(collname)

        db.write(collname, {"name": "a"})
        db.write(collname, {"name": "b"})

        assert sorted([d["name"] for d in db.read(collname)]) == ["a", "b"]
        assert [d["name"] for d in db.read(collname,
                                           query={"name": "b"})] == ["b"]
        assert db.read_one(collname, query={"name": "b"})["name"] == "b"
        a = db.read_one(collname, query={"name": "a"})
        assert db.read_by_id(collname, unicode(a["_id"]))["_id"] == a["_id"]

        db.drop(collname)
