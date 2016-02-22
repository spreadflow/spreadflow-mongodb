from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import pymongo
import threading
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

from twisted.internet import defer
from twisted.python import failure

class WorkerStop(object):
    def __init__(self, d, result=None):
        self.d = d
        self.result = result

    def run(self, reactor):
        reactor.callFromThread(self.d.callback, self.result)
        return False

class WorkerJob(object):
    def __init__(self, d, f, *args, **kwargs):
        self.d = d
        self.f = f
        self.args = args
        self.kwargs = kwargs

    def run(self, reactor):
        try:
            result = self.f(*self.args, **self.kwargs)
        except Exception:
            reactor.callFromThread(self.d.errback, failure.Failure())
        else:
            reactor.callFromThread(self.d.callback, result)
        return True

class MongoService(object):

    reactor = None
    _client = None
    _connection = None
    _queue = None
    _thread = None

    def __init__(self, db = 'test_database', uri = 'mongodb://localhost:27017/'):
        self.uri = uri
        self.db = db

    def attach(self, dispatcher, reactor):
        self.reactor = reactor

    @defer.inlineCallbacks
    def start(self):
        self._queue = Queue()
        self._thread = threading.Thread(target=self._mongo_worker, args=(self.reactor, self._queue))
        self._thread.start()

        self._client = yield self.cmd(pymongo.MongoClient, self.uri)

    @defer.inlineCallbacks
    def join(self):
        if self._thread:
            if self._thread.is_alive():
                yield self._worker_stop()
            self._thread.join()

        if self._client:
            self._client.close()

        self._client = None
        self._queue = None
        self._thread = None

    def detach(self):
        self.reactor = None

    def cmd(self, f, *args, **kwargs):
        d = defer.Deferred()
        self._queue.put(WorkerJob(d, f, *args, **kwargs))
        return d

    def collection(self, collection):
        return self._client[self.db][collection]

    def _worker_stop(self):
        d = defer.Deferred()
        self._queue.put(WorkerStop(d))
        return d

    @staticmethod
    def _mongo_worker(reactor, queue):
        alive = True
        while alive:
            job = queue.get()
            alive = job.run(reactor)
            queue.task_done()

class MongoDestination(object):

    _collection = None

    def __init__(self, service, collection='test_collection', reset=False):
        self.service = service
        self.collection = collection
        self.reset = reset

    @defer.inlineCallbacks
    def start(self):
        self._collection = self.service.collection(self.collection)
        if self.reset:
            yield self.service.cmd(self._collection.remove, {})

    def join(self):
        self._collection = None

    @defer.inlineCallbacks
    def __call__(self, item, send):
        deferreds = []

        if len(item['deletes']):
            deletes = item['deletes'][:]
            deferreds.append(self.service.cmd(self._collection.remove, {'_id':{'$in':deletes}}))

        if len(item['inserts']):
            docs = [dict(item['data'][oid].items() + [('_id', oid)]) for oid in item['inserts']]
            deferreds.append(self.service.cmd(self._collection.insert, docs))

        if len(deferreds):
            yield defer.DeferredList(deferreds, fireOnOneErrback=True)

        send(item, self)

    @property
    def dependencies(self):
        yield (self, self.service)
