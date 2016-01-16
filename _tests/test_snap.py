from collections import OrderedDict

import boto
import pytest

from z3.config import get_config
from z3.snap import (list_snapshots, S3SnapshotManager, ZFSSnapshotManager,
                     PairManager, CommandExecutor)


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


class FakeZFSManager(ZFSSnapshotManager):
    _expected = (
        'pool@p1\t0\t19K\t-\t19K\n'
        'pool@p2\t0\t19K\t-\t0\n'
        'pool/fs@snap_1\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_3\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_9\t10.0M\t10.0M\t-\t10.0M\n'
    )

    def __init__(self, expected=None, *a, **kwa):
        if expected is not None:
            self._expected = expected
        super(FakeZFSManager, self).__init__(*a, **kwa)

    def _list_snapshots(self):
        return self._expected


class FakeCommandExecutor(CommandExecutor):
    has_pv = False  # disable pv for consistent test output

    def __init__(self, *a, **kwa):
        super(FakeCommandExecutor, self).__init__(*a, **kwa)
        self._called_commands = []

    def shell(self, cmd):  # pylint: disable=arguments-differ
        self._called_commands.append(cmd)


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
def s3_manager(request):
    """This parametrized fixture will cause any test using it to execute twice,
    once using fakes and again using boto and hitting s3.
    """
    return S3SnapshotManager(request.param(), prefix="pool/fs@snap_")


def test_list_snapshots(s3_manager):
    snapshots = sorted(s3_manager.list(), key=lambda el: el.name)
    expected = ["pool/fs@snap_1", "pool/fs@snap_2", "pool/fs@snap_3", "pool/fs@snap_4",
                "pool/fs@snap_5", "pool/fs@snap_6", "pool/fs@snap_7"]
    assert [s.name for s in snapshots] == expected


def test_healthy_full(s3_manager):
    snap = s3_manager.get('pool/fs@snap_1')
    assert snap.is_full
    assert snap.is_healthy


def test_healthy_incremental(s3_manager):
    snap = s3_manager.get('pool/fs@snap_3')
    assert snap.is_full is False
    assert snap.is_healthy


def test_unhealthy_incremental(s3_manager):
    snap = s3_manager.get('pool/fs@snap_5')
    assert snap.is_full is False
    assert snap.is_healthy is False
    assert snap.reason_broken == 'parent broken'
    assert snap.parent.reason_broken == 'missing parent'


def test_unhealthy_cycle(s3_manager):
    snap = s3_manager.get('pool/fs@snap_7')
    assert snap.is_full is False
    assert snap.is_healthy is False
    assert snap.reason_broken == 'cycle detected'
    assert snap.parent.reason_broken == 'cycle detected'


def test_list_local_snapshots():
    zfs = FakeZFSManager(fs_name='pool/fs')
    expected = {
        'pool': OrderedDict([
            ('p1', {
                'mountpoint': '-',
                'name': 'pool@p1',
                'refer': '19K',
                'used': '0',
                'written': '19K',
            }),
            ('p2', {
                'mountpoint': '-',
                'name': 'pool@p2',
                'refer': '19K',
                'used': '0',
                'written': '0',
            }),
        ]),
        'pool/fs': OrderedDict([
            ('snap_1', {
                'mountpoint': '-',
                'name': 'pool/fs@snap_1',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M',
            }),
            ('snap_2', {
                'mountpoint': '-',
                'name': 'pool/fs@snap_2',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M',
            }),
            ('snap_3', {
                'mountpoint': '-',
                'name': 'pool/fs@snap_3',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M'
            }),
            ('snap_9', {
                'mountpoint': '-',
                'name': 'pool/fs@snap_9',
                'refer': '10.0M',
                'used': '10.0M',
                'written': '10.0M'
            }),
        ])
    }
    snapshots = zfs._parse_snapshots()
    # checking items because we care about order
    assert snapshots['pool'].items() == expected['pool'].items()
    assert snapshots['pool/fs'].items() == expected['pool/fs'].items()


@pytest.mark.parametrize("fs_name, expected", [
    ('pool/fs', [('pool/fs@snap_1', None),
                 ('pool/fs@snap_2', 'pool/fs@snap_1'),
                 ('pool/fs@snap_3', 'pool/fs@snap_2'),
                 ('pool/fs@snap_9', 'pool/fs@snap_3')]),
    ('pool', [('pool@p1', None),
              ('pool@p2', 'pool@p1')]),
])
def test_zfs_list(fs_name, expected):
    zfs = FakeZFSManager(fs_name=fs_name)
    actual = [
        (snap.name, snap.parent.name if snap.parent else None)
        for snap in zfs.list()]
    assert actual == expected


def test_pair_list(s3_manager):
    zfs_manager = FakeZFSManager(fs_name='pool/fs')
    pair_manager = PairManager(s3_manager, zfs_manager)
    pairs = pair_manager.list()
    name = lambda snap: snap.name if snap is not None else None
    names = [
        (name(s3_snap), name(z_snap))
        for (s3_snap, z_snap) in pairs]
    expected = [
        ('pool/fs@snap_1', 'pool/fs@snap_1'),
        ('pool/fs@snap_2', 'pool/fs@snap_2'),
        ('pool/fs@snap_3', 'pool/fs@snap_3'),
        (None, 'pool/fs@snap_9'),  # snap_9 doesn't exist in the s3 fixture
        # s3-only snapshot pairs are listed last
        ('pool/fs@snap_4', None),
        ('pool/fs@snap_5', None),
        ('pool/fs@snap_6', None),
        ('pool/fs@snap_7', None),
    ]
    assert names == expected


def test_backup_latest_full(s3_manager):
    zfs_manager = FakeZFSManager(fs_name='pool/fs')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.backup_full()
    assert fake_cmd._called_commands == [
        "zfs send 'pool/fs@snap_9' | pput --meta is_full=true pool/fs@snap_9"]


def test_backup_full(s3_manager):
    zfs_manager = FakeZFSManager(fs_name='pool/fs')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.backup_full('pool/fs@snap_3')
    assert fake_cmd._called_commands == [
        "zfs send 'pool/fs@snap_3' | pput --meta is_full=true pool/fs@snap_3"]


def test_backup_incremental_latest(s3_manager):
    zfs_manager = FakeZFSManager(fs_name='pool/fs')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.backup_incremental()
    assert fake_cmd._called_commands == [
        ("zfs send -i 'pool/fs@snap_3' 'pool/fs@snap_9' | "
         "pput --meta parent=pool/fs@snap_3 pool/fs@snap_9")
    ]


# def test_local_state(s3_manager):
#     snap_1 = s3_manager.get('pool/fs@snap_1')
#     snap_2 = s3_manager.get('pool/fs@snap_2')
#     snap_3 = s3_manager.get('pool/fs@snap_3')
#     snap_4 = s3_manager.get('pool/fs@snap_4')
#     assert snap_1.local_state == "OK"
#     assert snap_2.local_state == "OK"
#     assert snap_3.local_state == "OK"
#     assert snap_4.local_state == "missing locally"


# def test_list_missing_remote(s3_manager):
#     snap_9 = s3_manager.get('pool/fs@snap_9')
#     assert snap_9.local_state == 'OK'
#     assert snap_9.reason_broken == 'missing'
