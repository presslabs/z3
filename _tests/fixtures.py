from cStringIO import StringIO

import pytest


_cached_sample_data = None


@pytest.fixture()
def sample_data():
    """Sets up a StringIO with 6 Mbytes of data"""
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
        _cached_sample_data = data
    _cached_sample_data.seek(0)
    return _cached_sample_data

