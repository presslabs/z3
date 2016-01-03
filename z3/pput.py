"""Multipart parallel s3 upload.

usage
pput bucket_name/filename
"""

import binascii
import hashlib
import functools
from collections import namedtuple
from cStringIO import StringIO
from Queue import Queue
from logging import getLogger
from threading import Thread

import boto.s3.multipart


from z3.config import get_config


Result = namedtuple('Result', ['success', 'traceback', 'index', 'md5'])
CFG = get_config()


def multipart_etag(digests):
    """
    Computes etag for multipart uploads
    :type digests: list of hex-encoded md5 sums (string)
    :param digests: The list of digests for each individual chunk.

    :rtype: string
    :returns: The etag computed from the individual chunks.
    """
    etag = hashlib.md5()
    count = 0
    for dig in digests:
        count += 1
        etag.update(binascii.a2b_hex(dig))
    return '"{}-{}"'.format(etag.hexdigest(), count)


class StreamHandler(object):
    def __init__(self, input_stream, chunk_size=5*1024*1024):
        self.input_stream = input_stream
        self.chunk_size = chunk_size
        self._partial_chunk = ""
        self._eof_reached = False

    @property
    def finished(self):
        return self._eof_reached and len(self._partial_chunk) == 0

    def get_chunk(self):
        """Return complete chunks or None if EOF reached"""
        while not self._eof_reached:
            read = self.input_stream.read(self.chunk_size - len(self._partial_chunk))
            if len(read) == 0:
                self._eof_reached = True
            self._partial_chunk += read
            if len(self._partial_chunk) == self.chunk_size or self._eof_reached:
                chunk = self._partial_chunk
                self._partial_chunk = ""
                return chunk


@functools.wraps
def retry(func):
    def wrapped(*a, **kwa):
        times = int(CFG['max_retries'])
        for attempt in xrange(times):
            try:
                return func(*a, **kwa)
            except: # pylint: disable=bare-except
                if attempt+1 >= times:
                    raise
    return wrapped


class UploadWorker(object):
    def __init__(self, bucket, multipart, inbox, outbox):
        self.bucket = bucket
        self.inbox = inbox
        self.outbox = outbox
        self.multipart = multipart
        self._thread = None
        self.log = getLogger('UploadWorker')

    @retry
    def upload_part(self, index, chunk):
        part = boto.s3.multipart.MultiPartUpload(self.bucket)
        part.id = self.multipart.id
        part.key_name = self.multipart.key_name
        return part.upload_part_from_file(
            StringIO(chunk), index, replace=True).md5

    def start(self):
        self._thread = Thread(target=self.main_loop)
        self._thread.daemon = True
        self._thread.start()
        return self

    def main_loop(self):
        while True:
            index, chunk = self.inbox.get()
            md5 = self.upload_part(index, chunk)
            # print "worker loop i:{} md5:{}".format(index, md5)
            self.outbox.put(Result(
                success=True,
                md5=md5,
                traceback=None,
                index=index,
            ))


class UploadSupervisor(object):
    '''Reads chunks and dispatches them to UploadWorkers'''

    def __init__(self, stream_handler, name, bucket):
        self.stream_handler = stream_handler
        self.name = name
        self.bucket = bucket
        self.inbox = None
        self.outbox = None
        self.multipart = None
        self.results = []  # beware s3 multipart indexes are 1 based
        self._pending_chunks = 0

    def _start_workers(self, concurrency, worker_class):
        work_queue = Queue(maxsize=concurrency)
        result_queue = Queue()
        self.outbox = work_queue
        self.inbox = result_queue
        workers = [
            worker_class(
                bucket=self.bucket,
                multipart=self.multipart,
                inbox=work_queue,
                outbox=result_queue,
            ).start()
            for _ in xrange(concurrency)]
        return workers

    def _begin_upload(self):
        if self.multipart is not None:
            raise AssertionError("multipart upload already started")
        self.multipart = self.bucket.initiate_multipart_upload(
            self.name,
            headers={
                "x-amz-acl": "bucket-owner-full-control",
            }
        )

    def _finish_upload(self):
        return self.multipart.complete_upload()

    def _handle_result(self):
        """Process one result. Block untill one is available
        """
        result = self.inbox.get()
        if result.success:
            self.results.append((result.index, result.md5))
            self._pending_chunks -= 1
        else:
            raise result.traceback

    def _handle_results(self):
        """Process any available result
        Doesn't block.
        """
        while not self.inbox.empty():
            self._handle_result()

    def _send_chunk(self, index, chunk):
        """Send the current chunk to the workers for processing.
        Called when the _partial_chunk is complete.

        Blocks when the outbox is full.
        """
        self._pending_chunks += 1
        self.outbox.put((index, chunk))

    def main_loop(self, concurrency=4, worker_class=UploadWorker):
        chunk_index = 0
        self._begin_upload()
        self._start_workers(concurrency, worker_class=worker_class)
        while self._pending_chunks or not self.stream_handler.finished:
            # print "main_loop p:{} o:{} i:{}".format(
            #     self._pending_chunks, self.outbox.qsize(), self.inbox.qsize())
            # consume results first as this is a quick operation
            self._handle_results()
            chunk = self.stream_handler.get_chunk()
            if chunk:
                # s3 multipart index is 1 based, increment before sending
                chunk_index += 1
                self._send_chunk(chunk_index, chunk)
        self._finish_upload()
        self.results.sort()
        return multipart_etag(r[1] for r in self.results)
