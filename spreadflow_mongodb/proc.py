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

class MongoConnection(object):

    reactor = None
    _client = None
    _connection = None
    _queue = None
    _thread = None

    def __init__(self, uri='mongodb://localhost:27017/'):
        self.uri = uri

    @defer.inlineCallbacks
    def attach(self, dispatcher, reactor):
        self.reactor = reactor

        self._queue = Queue()
        self._thread = threading.Thread(target=self._mongo_worker, args=(self.reactor, self._queue))
        self._thread.start()

        self._client = yield self.cmd(pymongo.MongoClient, self.uri)

    @defer.inlineCallbacks
    def detach(self):
        if self._thread:
            if self._thread.is_alive():
                yield self._worker_stop()
            self._thread.join()

        if self._client:
            self._client.close()

        self._client = None
        self._queue = None
        self._thread = None
        self.reactor = None

    @defer.inlineCallbacks
    def __call__(self, item, send):
        if item[0] == 'collection':
            _, database, collection, name, args, kwargs = item
            func = getattr(self._client[database][collection], name)
            result = yield self.cmd(func, *args, **kwargs)
            send(result, self)
        else:
            raise RuntimeError('Cannot handle command ' + item[0])

    def cmd(self, f, *args, **kwargs):
        d = defer.Deferred()
        self._queue.put(WorkerJob(d, f, *args, **kwargs))
        return d

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

class MongoCollectionDeltaSync(object):

    def __init__(self, database='test_database', collection='test_collection', reset=False):
        self.database = database
        self.collection = collection
        self.reset = reset

    def __call__(self, item, send):
        if (self.reset):
            cmd = self._format_cmd('delete_many', {})
            send(cmd, self)
            self.reset = False

        if len(item['deletes']):
            query = {'_id': {'$in': item['deletes'][:]}}
            cmd = self._format_cmd('delete_many', query)
            send(cmd, self)

        if len(item['inserts']):
            docs = [dict(item['data'][oid].items() + [('_id', oid)]) for oid in item['inserts']]
            cmd = self._format_cmd('insert_many', docs)
            send(cmd, self)

    def _format_cmd(self, name, *args):
        return "collection", self.database, self.collection, name, args, {}
