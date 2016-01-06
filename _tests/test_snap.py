import boto
import pytest

from z3.config import get_config
from z3.snap import list_snapshots, SnapshotManager


class FakeKey(object):
    def __init__(self, name, metadata=None):
        self.name = name
        self.key = name
        self.metadata = metadata


class FakeBucket(object):
    def __init__(self):
        self._keys = {
            "snap_1": FakeKey('snap_1', {'is_full': 'true'}),
            "snap_2": FakeKey('snap_2', {'parent': 'snap_1'}),
            "snap_3": FakeKey('snap_3', {'parent': 'snap_2', 'is_full': 'false'}),
            "snap_4": FakeKey('snap_4', {'parent': 'missing_parent'}),  # missing parent
            "snap_5": FakeKey('snap_5', {'parent': 'snap_4'}),
            "snap_6": FakeKey('snap_6', {'parent': 'snap_7'}),  # cycle
            "snap_7": FakeKey('snap_7', {'parent': 'snap_6'}),  # cycle
        }

    def list(self, *a, **kwa):
        # boto bucket.list gives you keys without metadata, let's emulate that
        return (FakeKey("snap_{}".format(i)) for i in xrange(1, 8))

    def get_key(self, name):
        return self._keys.get(name)


@pytest.fixture
def manager():
    return SnapshotManager(FakeBucket())


def test_list_snapshots(manager):
    snapshots = sorted(manager.list(), key=lambda el: el.name)
    expected = ["snap_1", "snap_2", "snap_3", "snap_4", "snap_5", "snap_6", "snap_7"]
    assert [s.name for s in snapshots] == expected


def test_healthy_full(manager):
    snap = manager.get('snap_1')
    assert snap.is_full
    assert snap.is_healthy


def test_healthy_incremental(manager):
    snap = manager.get('snap_3')
    assert snap.is_full is False
    assert snap.is_healthy


def test_unhealthy_incremental(manager):
    snap = manager.get('snap_5')
    assert snap.is_full is False
    assert snap.is_healthy is False


def test_unhealthy_cycle(manager):
    snap = manager.get('snap_7')
    assert snap.is_full is False
    assert snap.is_healthy is False


# def test_integration():
#     cfg = get_config()
#     bucket = boto.connect_s3(
#         cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
#     import ipdb; ipdb.set_trace()
