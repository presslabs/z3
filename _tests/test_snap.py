import os.path

import boto
import pytest

from z3.config import get_config
from z3.snap import list_snapshots, SnapshotManager, LocalZFS


class FakeKey(object):
    def __init__(self, name, metadata=None):
        self.name = name
        self.key = name
        self.metadata = metadata


class FakeBucket(object):
    fake_data = {
        "pool/fs@snap_1": FakeKey('pool/fs@snap_1', {'is_full': 'true'}),
        "pool/fs@snap_2": FakeKey('pool/fs@snap_2', {'parent': 'pool/fs@snap_1'}),
        "pool/fs@snap_3": FakeKey('pool/fs@snap_3', {'parent': 'pool/fs@snap_2', 'is_full': 'false'}),
        "pool/fs@snap_4": FakeKey('pool/fs@snap_4', {'parent': 'missing_parent'}),  # missing parent
        "pool/fs@snap_5": FakeKey('pool/fs@snap_5', {'parent': 'pool/fs@snap_4'}),
        "pool/fs@snap_6": FakeKey('pool/fs@snap_6', {'parent': 'pool/fs@snap_7'}),  # cycle
        "pool/fs@snap_7": FakeKey('pool/fs@snap_7', {'parent': 'pool/fs@snap_6'}),  # cycle
    }

    def list(self, *a, **kwa):
        # boto bucket.list gives you keys without metadata, let's emulate that
        return (FakeKey(name) for name in self.fake_data.iterkeys())

    def get_key(self, name):
        return self.fake_data.get(name)




class FakeZFS(LocalZFS):
    _expected = (
        'pool@p1\t0\t19K\t-\t19K\n'
        'pool@p2\t0\t19K\t-\t0\n'
        'pool/fs@snap_1\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_3\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_9\t10.0M\t10.0M\t-\t10.0M\n'
    )

    def __init__(self, expected=None):
        if expected is not None:
            self._expected = expected

    def _list_snapshots(self):
        return self._expected
        # import _tests
        # with open(os.path.join(_tests.__path__[0], 'zfs_list.txt')) as fd:
        #     return fd.read()


def write_s3_data():
    """Takes the default data from FakeBucket and writes it to S3.
    Allows running the same tests against fakes and the boto api.
    """
    cfg = get_config()
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    for fake_key in FakeBucket.fake_data.itervalues():
        key = bucket.new_key(fake_key.name)
        headers = {("x-amz-meta-" + k): v for k, v in fake_key.metadata.iteritems()}
        key.set_contents_from_string("spam", headers=headers)
    return bucket



@pytest.fixture(
    scope='module',
    params=[
        FakeBucket,
        pytest.mark.with_s3(write_s3_data),
    ],
    ids=['fake_bucket', 'with_s3']
)
def manager(request):
    """This parametrized fixture will cause any test using it to execute twice,
    once using fakes and again using boto and hittint s3.
    """
    return SnapshotManager(request.param(), FakeZFS(), prefix="pool/fs@snap_")


def test_list_snapshots(manager):
    snapshots = sorted(manager.list(), key=lambda el: el.name)
    expected = ["pool/fs@snap_1", "pool/fs@snap_2", "pool/fs@snap_3", "pool/fs@snap_4",
                "pool/fs@snap_5", "pool/fs@snap_6", "pool/fs@snap_7"]
    assert [s.name for s in snapshots] == expected


def test_healthy_full(manager):
    snap = manager.get('pool/fs@snap_1')
    assert snap.is_full
    assert snap.is_healthy


def test_healthy_incremental(manager):
    snap = manager.get('pool/fs@snap_3')
    assert snap.is_full is False
    assert snap.is_healthy


def test_unhealthy_incremental(manager):
    snap = manager.get('pool/fs@snap_5')
    assert snap.is_full is False
    assert snap.is_healthy is False
    assert snap.reason_broken == 'parent broken'
    assert snap.parent.reason_broken == 'missing parent'


def test_unhealthy_cycle(manager):
    snap = manager.get('pool/fs@snap_7')
    assert snap.is_full is False
    assert snap.is_healthy is False
    assert snap.reason_broken == 'cycle detected'
    assert snap.parent.reason_broken == 'cycle detected'


def test_list_local_snapshots():
    zfs = FakeZFS()
    expected = {
        'pool': {
            'p1': {
                'mountpoint': '-',
                'name': 'pool@p1',
                'refer': '19K',
                'used': '0',
                'written': '19K',
            },
            'p2': {
                'mountpoint': '-',
                'name': 'pool@p2',
                'refer': '19K',
                'used': '0',
                'written': '0',
            },
        },
        'pool/fs': {
            'snap_1': {
                'mountpoint': '-',
                'name': 'pool/fs@snap_1',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M',
            },
            'snap_2': {
                'mountpoint': '-',
                'name': 'pool/fs@snap_2',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M',
            },
            'snap_3': {
                'mountpoint': '-',
                'name': 'pool/fs@snap_3',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M'
            },
            'snap_9': {
                'mountpoint': '-',
                'name': 'pool/fs@snap_9',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M'
            },
        }
    }
    assert zfs.list_snapshots() == expected


def test_local_state(manager):
    snap_1 = manager.get('pool/fs@snap_1')
    snap_2 = manager.get('pool/fs@snap_2')
    snap_3 = manager.get('pool/fs@snap_3')
    snap_4 = manager.get('pool/fs@snap_4')
    assert snap_1.local_state == "OK"
    assert snap_2.local_state == "OK"
    assert snap_3.local_state == "OK"
    assert snap_4.local_state == "missing locally"
