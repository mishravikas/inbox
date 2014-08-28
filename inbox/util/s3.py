import gevent
from gevent.queue import Queue
from gevent.event import Event
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from inbox.config import config


class S3Worker(gevent.Greenlet):
    def __init__(self, queue):
        self.queue = queue
        gevent.Greenlet.__init__(self)

    def _run(self):
        self.conn = S3Connection(config.get('AWS_ACCESS_KEY_ID'),
                                 config.get('AWS_SECRET_ACCESS_KEY'))
        self.bucket = self.conn.get_bucket('MESSAGE_STORE_BUCKET_NAME')
        while True:
            data, data_sha256, callback = self.queue.get()
            self._save(data, data_sha256, callback)

    def _save(self, data, data_sha256, callback_event):
        # See if data object already exists on S3 and has the same hash
        data_obj = self.bucket.get_key(data_sha256)
        if data_obj:
            assert data_obj.get_metadata('data_sha256') == data_sha256, \
                "Block hash doesn't match what we previously stored on s3!"
            return

        # If it doesn't already exist, save it.
        data_obj = Key(self.bucket)
        data_obj.set_metadata('data_sha256', self.data_sha256)
        data_obj.key = self.data_sha256
        data_obj.set_contents_from_string(data)
        callback_event.set()


class S3WorkerPool(object):
    def __init__(self, pool_size=22):
        self.queue = Queue()
        self.workers = []
        for _ in range(pool_size):
            worker = S3Worker(self.queue)
            worker.start()
            self.workers.append(worker)

    def save(self, data, data_sha256):
        callback_event = Event()
        self.queue.put((data, data_sha256, callback_event))
        callback_event.wait()


__pool = None


def get_s3_pool():
    global __pool
    if __pool is None:
        __pool = S3WorkerPool()
        return __pool
