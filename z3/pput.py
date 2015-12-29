"""Multipart parallel s3 upload.

usage
pput bucket_name/filename
"""


from cStringIO import StringIO

import boto


class StreamHandler(object):
    '''Reads chunks and dispatches them to an Uploader'''
    def __init__(self, uploader, chunk_size=512, concurrency=5):
        self.uploader = uploader
        self.chunk_size = chunk_size
        self.concurrency = concurrency

    def upload_stream(self, stream):
        while stream.read(self.chunk_size):
            pass


class Uploader(object):
    def __init__(self, bucket_name, key_id, secret):
        self.bucket_name = bucket_name
        self.key_id = key_id
        self.secret = secret
        self.connection = boto.connect_s3(self.key_id, self.secret)
        self.bucket = self.connection.get_bucket(self.bucket_name, validate=True)
        self.multipart = None

    def begin_upload(self, name):
        if self.multipart is not None:
            raise AssertionError("multipart upload already started")
        self.multipart = self.bucket.initiate_multipart_upload(
            name,
            headers={
                "x-amz-acl": "bucket-owner-full-control",
            }
        )

    @staticmethod
    def upload_part(bucket, part_id, key_name, chunk, index):
        part = boto.s3.multipart.MultiPartUpload(bucket)
        part.id = part_id
        part.key_name = key_name
        return part.upload_part_from_file(StringIO(chunk), index, replace=True)

    def finish_upload(self):
        return self.multipart.complete_upload()
