from cStringIO import StringIO

import pytest


_cached_sample_data = None


class ReadOnlyFile(object):
    def __init__(self, fd, allowed):
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
        _cached_sample_data = ReadOnlyFile(data, allowed=['read', 'seek'])
    _cached_sample_data.seek(0)
    return _cached_sample_data

