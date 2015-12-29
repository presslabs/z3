from cStringIO import StringIO
from datetime import datetime

import pytest

from z3.pput import Uploader
from z3.config import get_config


cfg = get_config()


@pytest.fixture(scope='session')
def sample_data():
    "returns a StringIO with 6 Mbytes"
    data = StringIO()
    for count in xrange(6):
        for _ in xrange(2 * 1024):
            # each iteration adds 1MB
            # each 1MB chunk is made up of an alternation of the block's index (zero based)
            # and an incrementing counter (overflows to 0 several times)
            # the first block will be: 00 00 00 01 00 02 ... 00 ff 00 00 ... 00 ff
            data.write(
                "".join(chr(count)+chr(i) for i in xrange(256))
            )
    print "wrote {} MB" .format(data.tell() / 1024.0 / 1024.0)
    data.seek(0)
    return data


def test_multipart_upload(sample_data):
    uploader = Uploader('presslabstest', cfg['S3_KEY_ID'], cfg['S3_SECRET'])
    uploader.begin_upload("z3_test_"+datetime.now().strftime("%Y%m%d_%H-%M-%S"))
    index = 1
    while True:
        chunk = sample_data.read(5 * 1024 * 1024)
        print "read {} KB".format(len(chunk) / 1024.0)
        if len(chunk) == 0:
            break
        # since this is a stringIO there's no way this can return less than 512 bytes
        # unless we're at the end of the stream
        print "uploading chunk", index
        uploader.upload_part(
            uploader.bucket,
            uploader.multipart.id,
            uploader.multipart.key_name,
            chunk,
            index)
        index += 1
    uploader.finish_upload()
