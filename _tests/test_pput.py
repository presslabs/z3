from cStringIO import StringIO
from datetime import datetime
from Queue import Queue
from uuid import uuid4
import hashlib

import boto
import pytest

from z3.pput import (UploadSupervisor, UploadWorker, StreamHandler,
                     Result, WorkerCrashed, multipart_etag, parse_metadata,
                     retry, UploadException)
from z3.config import get_config


cfg = get_config()
_cached_sample_data = None


class ReadOnlyFile(object):
    """A read-only file like object.
    Helps ensure we don't accidentally mutate the fixture between test runs.
    """
    def __init__(self, fd, allowed=('read', 'seek')):
        self._fd = fd
        self._allowed = set(allowed)

    def __getattr__(self, name):
        if name in self._allowed:
            return getattr(self._fd, name)
        raise AssertionError("this file-like-object is readonly, {} is now allowed".format(name))


@pytest.fixture()
def sample_data():
    """Sets up a file-like-object with 6 Mbytes of data
    Since this is expensive to do, we share this object across test runs and just
    seek the file back to the start after each use.
    """
    global _cached_sample_data
    if _cached_sample_data is None:
        data = StringIO()
        chars = "".join(chr(i) for i in xrange(256))
        for count in xrange(6):
            cc = chr(count)
            for _ in xrange(2 * 1024):
                # each iteration adds 1MB
                # each 1MB chunk is made up of an alternation of the block's index (zero based)
                # and an incrementing counter (overflows to 0 several times)
                # the first block will be: 00 00 00 01 00 02 ... 00 ff 00 00 ... 00 ff
                data.write(
                    "".join(cc+chars[i] for i in xrange(256))
                )
        print "wrote {} MB" .format(data.tell() / 1024.0 / 1024.0)
        # give the test a read-only file to avoid accidentally modifying the data between tests
        _cached_sample_data = ReadOnlyFile(data)
    _cached_sample_data.seek(0)
    return _cached_sample_data


def test_multipart_etag(sample_data):
    digests = []
    while True:
        chunk = sample_data.read(5 * 1024 * 1024)
        if len(chunk) == 0:
            break
        digest = hashlib.md5()
        digest.update(chunk)
        digests.append(digest.hexdigest())
    assert multipart_etag(digests) == '"d229c1fc0e509475afe56426c89d2724-2"'


def test_stream_handler():
    stream_handler = StreamHandler(StringIO("aabbccdde"), chunk_size=2)
    chunks = []
    while not stream_handler.finished:
        chunk = stream_handler.get_chunk()
        chunks.append(chunk)
    assert chunks == ['aa', 'bb', 'cc', 'dd', 'e']


def test_handle_results():
    sup = UploadSupervisor(None, None, None)
    sup.inbox = Queue()
    sup._pending_chunks = 3
    sup.inbox.put(Result(success=True, traceback=None, index=1, md5='a'))
    sup.inbox.put(Result(success=True, traceback=None, index=3, md5='c'))
    sup.inbox.put(Result(success=True, traceback=None, index=2, md5='b'))
    sup._handle_results()
    assert sorted(sup.results) == [(1, 'a'), (2, 'b'), (3, 'c')]
    assert sup._pending_chunks == 0


class FakeMultipart(object):
    def __init__(self, name):
        self._name = name
        self.id = str(uuid4())
        self.key_name = str(uuid4())
        self._completed = False
        self._canceled = False

    def complete_upload(self):
        if self._completed:
            raise AssertionError('multipart already completed')
        self._completed = True

    def cancel_upload(self):
        if self._canceled:
            raise AssertionError('multipart already canceled')
        self._canceled = True


class FakeBucket(object):
    def __init__(self):
        self._multipart = None

    def initiate_multipart_upload(self, name, headers):
        self._multipart = FakeMultipart(name)
        return self._multipart


class DummyWorker(UploadWorker):
    def upload_part(self, index, chunk):
        return hashlib.md5(chunk).hexdigest()


def test_supervisor_loop(sample_data):
    stream_handler = StreamHandler(sample_data)
    bucket = FakeBucket()
    sup = UploadSupervisor(stream_handler, 'test', bucket=bucket)
    etag = sup.main_loop(worker_class=DummyWorker)
    assert etag == '"d229c1fc0e509475afe56426c89d2724-2"'
    assert bucket._multipart._completed


def test_zero_data(sample_data):
    stream_handler = StreamHandler(StringIO())
    bucket = FakeBucket()
    sup = UploadSupervisor(stream_handler, 'test', bucket=bucket)
    with pytest.raises(UploadException):
        sup.main_loop(worker_class=DummyWorker)
    assert bucket._multipart._canceled is True


class ErrorWorker(UploadWorker):
    def upload_part(self, index, chunk):
        if index == 2:
            raise Exception("Testing worker crash")
        return hashlib.md5(chunk).hexdigest()


def test_supervisor_loop_with_worker_crash(sample_data):
    stream_handler = StreamHandler(sample_data)
    bucket = FakeBucket()
    sup = UploadSupervisor(stream_handler, 'test', bucket=bucket)
    with pytest.raises(WorkerCrashed):
        sup.main_loop(worker_class=ErrorWorker)


class BoomException(Exception):
    pass


class Boom(object):
    def __init__(self):
        self.count = 0

    @retry(3)
    def call(self):
        self.count += 1
        raise BoomException("Boom!")


def test_retry_decorator():
    boom = Boom()
    with pytest.raises(BoomException) as excp_info:
        for _ in xrange(3):
            boom.call()
    assert boom.count == 3


@pytest.mark.with_s3
def test_integration(sample_data):
    cfg = get_config()
    stream_handler = StreamHandler(sample_data)
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    key_name = "z3_test_" + datetime.now().strftime("%Y%m%d_%H-%M-%S")
    sup = UploadSupervisor(
        stream_handler,
        key_name,
        bucket=bucket,
        headers=parse_metadata(["ana=are+mere", "dana=are=pere"])
    )
    etag = sup.main_loop()
    uploaded = bucket.get_key(key_name)
    assert etag == '"d229c1fc0e509475afe56426c89d2724-2"'
    assert etag == uploaded.etag
    assert uploaded.metadata == {"ana": "are+mere", "dana": "are=pere"}
