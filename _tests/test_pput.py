from cStringIO import StringIO
from Queue import Queue
from uuid import uuid4
import hashlib

import pytest

from z3.pput import (UploadSupervisor, UploadWorker, StreamHandler,
                     Result, WorkerCrashed, multipart_etag)
from z3.config import get_config
from .fixtures import sample_data


cfg = get_config()


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

    def complete_upload(self):
        if self._completed:
            raise AssertionError('multipart already completed')
        self._completed = True


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
