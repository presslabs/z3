"""Multipart parallel s3 upload.

usage
pput bucket_name/filename
"""

from Queue import Queue
from cStringIO import StringIO
from collections import namedtuple
from threading import Thread
import argparse
import binascii
import functools
import hashlib
import logging
import json
import os
import sys

import boto.s3.multipart

from z3.config import get_config


Result = namedtuple('Result', ['success', 'traceback', 'index', 'md5'])
CFG = get_config()
VERB_QUIET = 0
VERB_NORMAL = 1
VERB_PROGRESS = 2


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


def parse_size(size):
    if isinstance(size, (int, long)):
        return size
    size = size.strip().upper()
    last = size[-1]
    if last == 'T':
        return int(size[:-1]) * 1024 * 1024 * 1024 * 1024
    if last == 'G':
        return int(size[:-1]) * 1024 * 1024 * 1024
    if last == 'M':
        return int(size[:-1]) * 1024 * 1024
    if last == 'K':
        return int(size[:-1]) * 1024
    return int(size)


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
            # else:
            #     print "partial", len(self._partial_chunk)


def retry(times=int(CFG['MAX_RETRIES'])):
    def decorator(func):
        @functools.wraps(func)
        def wrapped(*a, **kwa):
            for attempt in xrange(1, times+1):
                try:
                    return func(*a, **kwa)
                except:  # pylint: disable=bare-except
                    if attempt >= times:
                        raise
                    logging.exception('Failed to upload part attempt {} of {}'.format(
                        attempt, times))
        return wrapped
    return decorator


class WorkerCrashed(Exception):
    pass


class UploadWorker(object):
    def __init__(self, bucket, multipart, inbox, outbox):
        self.bucket = bucket
        self.inbox = inbox
        self.outbox = outbox
        self.multipart = multipart
        self._thread = None
        self.log = logging.getLogger('UploadWorker')

    @retry()
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

    def is_alive(self):
        return self._thread.is_alive()

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


class UploadException(Exception):
    pass


class UploadSupervisor(object):
    '''Reads chunks and dispatches them to UploadWorkers'''

    def __init__(self, stream_handler, name, bucket, headers=None, verbosity=1):
        self.stream_handler = stream_handler
        self.name = name
        self.bucket = bucket
        self.inbox = None
        self.outbox = None
        self.multipart = None
        self.results = []  # beware s3 multipart indexes are 1 based
        self._pending_chunks = 0
        self._verbosity = verbosity
        self._workers = None
        self._headers = headers

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
        headers = {
            "x-amz-acl": "bucket-owner-full-control",
        }
        if self._headers:
            headers.update(self._headers)
        self.multipart = self.bucket.initiate_multipart_upload(self.name, headers=headers)

    def _finish_upload(self):
        if len(self.results) == 0:
            self.multipart.cancel_upload()
            raise UploadException("Error: Can't upload zero bytes!")
        return self.multipart.complete_upload()

    def _handle_result(self):
        """Process one result. Block untill one is available
        """
        result = self.inbox.get()
        if result.success:
            if self._verbosity >= VERB_PROGRESS:
                sys.stderr.write("\nuploaded chunk {} \n".format(result.index))
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

    def _check_workers(self):
        """Check workers are alive, raise exception if any is dead."""
        for worker in self._workers:
            if not worker.is_alive():
                raise WorkerCrashed()

    def main_loop(self, concurrency=4, worker_class=UploadWorker):
        chunk_index = 0
        self._begin_upload()
        self._workers = self._start_workers(concurrency, worker_class=worker_class)
        while self._pending_chunks or not self.stream_handler.finished:
            self._check_workers()  # raise exception and stop everything if any worker has crashed
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


def parse_metadata(metadata):
    headers = {}
    for meta in metadata:
        try:
            key, val = meta.split('=', 1)
        except ValueError:
            sys.stderr.write(
                "malformed metadata '{}'; should be key=value\n".format(meta))
            sys.exit(1)
        headers['x-amz-meta-' + key] = val
    return headers


def optimize_chunksize(estimated):
    max_parts = 9999  # S3 requires part indexes to be between 1 and 10000
    # part size has to be at least 5MB
    estimated = estimated * 1.05  # just to be on the safe side overesimate the total size to upload
    min_part_size = max(estimated / max_parts, 5*1024*1024)
    return int(min_part_size)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Read data from stdin and upload it to s3',
        epilog=('All optional args have a configurable default. '
                'Order of precedence is command line args then '
                'environment variables then user config ~/.z3.cfg'
                ' then default config.'),
    )
    parser.add_argument('name', help='name of S3 key')
    chunk_group = parser.add_mutually_exclusive_group()
    chunk_group.add_argument('-s', '--chunk-size',
                             dest='chunk_size',
                             default=CFG['CHUNK_SIZE'],
                             help='multipart chunk size, eg: 10M, 1G')
    chunk_group.add_argument('--estimated',
                             help='Estimated upload size')
    parser.add_argument('--file-descriptor',
                        dest='file_descriptor',
                        type=int,
                        help=('read data from this fd instead of stdin; '
                              'useful if you want an [i]pdb session to use stdin\n'
                              '`pput --file-descriptor 3 3<./file`'))
    parser.add_argument('--concurrency',
                        dest='concurrency',
                        type=int,
                        default=int(CFG['CONCURRENCY']),
                        help='number of worker threads to use')
    parser.add_argument('--metadata',
                        action='append',
                        dest='metadata',
                        default=list(),
                        help='Metatada in key=value format')
    parser.add_argument('--storage-class', default=CFG['S3_STORAGE_CLASS'],
                        dest='storage_class', help='The S3 storage class. Defaults to STANDARD_IA.')
    quiet_group = parser.add_mutually_exclusive_group()
    quiet_group.add_argument('--progress',
                             dest='progress',
                             action='store_true',
                             help=('show progress report'))
    quiet_group.add_argument('--quiet',
                             dest='quiet',
                             action='store_true',
                             help=('don\'t emit any output at all'))
    return parser.parse_args()


def main():
    args = parse_args()
    input_fd = os.fdopen(args.file_descriptor, 'r') if args.file_descriptor else sys.stdin
    if args.estimated is not None:
        chunk_size = optimize_chunksize(parse_size(args.estimated))
    else:
        chunk_size = parse_size(args.chunk_size)
    stream_handler = StreamHandler(input_fd, chunk_size=chunk_size)

    extra_config = {}
    if 'HOST' in CFG:
        extra_config['host'] = CFG['HOST']

    bucket = boto.connect_s3(
        CFG['S3_KEY_ID'], CFG['S3_SECRET'], **extra_config).get_bucket(CFG['BUCKET'])

    # verbosity: 0 totally silent, 1 default, 2 show progress
    verbosity = 0 if args.quiet else 1 + int(args.progress)
    headers = parse_metadata(args.metadata)
    headers["x-amz-storage-class"] = args.storage_class
    sup = UploadSupervisor(
        stream_handler,
        args.name,
        bucket=bucket,
        verbosity=verbosity,
        headers=headers,
    )
    if verbosity >= VERB_NORMAL:
        sys.stderr.write("starting upload to {}/{} with chunksize {}M using {} workers\n".format(
            CFG['BUCKET'], args.name, (chunk_size/(1024*1024.0)), args.concurrency))
    try:
        etag = sup.main_loop(concurrency=args.concurrency)
    except UploadException as excp:
        sys.stderr.write("{}\n".format(excp))
        return 1
    if verbosity >= VERB_NORMAL:
        print json.dumps({'status': 'success', 'etag': etag})


if __name__ == '__main__':
    main()
