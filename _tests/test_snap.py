# pylint: disable=redefined-outer-name,protected-access
from collections import OrderedDict
from cStringIO import StringIO
import contextlib
import string
import sys
import random
import os.path

import boto
import pytest

from z3.config import get_config
from z3.snap import (list_snapshots, S3SnapshotManager, ZFSSnapshotManager,
                     PairManager, CommandExecutor, IntegrityError, SoftError,
                     _humanize, handle_soft_errors)


MEGA = 1024 ** 2
GIGA = 1024 ** 3
TERA = 1024 ** 4


class FakeKey(object):
    def __init__(self, name, metadata=None):
        self.name = name
        self.key = name
        self.metadata = metadata
        self.size = 1234


class FakeBucket(object):
    rand_prefix = 'test-' + ''.join([random.choice(string.ascii_letters) for _ in xrange(8)]) + '/'
    fake_data = {
        "pool/fs@snap_0": {'parent': 'pool/fs@snap_expired'},
        "pool/fs@snap_1_f": {'isfull': 'true', 'compressor': 'pigz1'},
        "pool/fs@snap_2": {'parent': 'pool/fs@snap_1_f'},
        "pool/fs@snap_3": {'parent': 'pool/fs@snap_2', 'isfull': 'false'},
        "pool/fs@snap_4_mp": {'parent': 'missing_parent'},  # missing parent
        "pool/fs@snap_5": {'parent': 'pool/fs@snap_4_mp'},
        "pool/fs@snap_6_cycle": {'parent': 'pool/fs@snap_7_cycle'},  # cycle
        "pool/fs@snap_7_cycle": {'parent': 'pool/fs@snap_6_cycle'},  # cycle
    }

    def list(self, *a, **kwa):
        # boto bucket.list gives you keys without metadata, let's emulate that
        return (FakeKey(os.path.join(self.rand_prefix, name)) for name in self.fake_data.iterkeys())

    def get_key(self, key):
        name = key[len(self.rand_prefix):]
        return FakeKey(
            name=key,
            metadata=self.fake_data[name])


def write_s3_data():
    """Takes the default data from FakeBucket and writes it to S3.
    Allows running the same tests against fakes and the boto api.
    """
    cfg = get_config()
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    for name, metadata in FakeBucket.fake_data.iteritems():
        key = bucket.new_key(os.path.join(FakeBucket.rand_prefix, name))
        headers = {("x-amz-meta-" + k): v for k, v in metadata.iteritems()}
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

    The tests don't actually write to the bucket so we can share the same S3SnapshotManager
    across all tests.
    """
    return S3SnapshotManager(
        request.param(), s3_prefix=FakeBucket.rand_prefix, snapshot_prefix="pool/fs@snap_")


@pytest.mark.parametrize("size, expected", [
    (43 * MEGA, "43 M"),
    (50 * GIGA, "50 G"),
    (50.512 * GIGA, "50.51 G"),
    (2.724 * TERA, "2.72 T")])
def test_humanize(size, expected):
    assert _humanize(size) == expected


def test_list_snapshots(s3_manager):
    snapshots = sorted(s3_manager.list(), key=lambda el: el.name)
    expected = sorted(
        ["pool/fs@snap_0", "pool/fs@snap_1_f", "pool/fs@snap_2", "pool/fs@snap_3",
         "pool/fs@snap_4_mp", "pool/fs@snap_5", "pool/fs@snap_6_cycle",
         "pool/fs@snap_7_cycle"])
    assert [s.name for s in snapshots] == expected


def test_healthy_full(s3_manager):
    snap = s3_manager.get('pool/fs@snap_1_f')
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
    snap = s3_manager.get('pool/fs@snap_7_cycle')
    assert snap.is_full is False
    assert snap.is_healthy is False
    assert snap.reason_broken == 'cycle detected'
    assert snap.parent.reason_broken == 'cycle detected'


class FakeZFSManager(ZFSSnapshotManager):
    _expected = (
        # pool is a different zfs dataset, the s3 fixtures don't include it
        'pool@snap_p1\t0\t19K\t-\t19K\n'
        'pool@snap_p2\t0\t19K\t-\t0\n'

        'pool/fs@snap_0\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@funky_name\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_3\t10.0M\t10.0M\t-\t10.0M\n'

        # local only snapshots, not in s3 fixtures
        'pool/fs@snap_8\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_9\t10.0M\t10.0M\t-\t10.0M\n'
    )

    def __init__(self, expected=None, *a, **kwa):
        if expected is not None:
            self._expected = expected
        super(FakeZFSManager, self).__init__(*a, **kwa)

    def _list_snapshots(self):
        return self._expected


def test_list_local_snapshots():
    zfs = FakeZFSManager(fs_name='pool/fs', snapshot_prefix='snap_')
    expected = {
        'pool': OrderedDict([  # _parse_snapshots returns snapshots for ALL filesystems
            ('snap_p1', {
                'name': 'pool@snap_p1',
                'mountpoint': '-', 'refer': '19K', 'used': '0', 'written': '19K',
            }),
            ('snap_p2', {
                'name': 'pool@snap_p2',
                'mountpoint': '-', 'refer': '19K', 'used': '0', 'written': '0',
            }),
        ]),
        'pool/fs': OrderedDict([
            ('snap_0', {
                'name': 'pool/fs@snap_0',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M',
            }),
            ('snap_1_f', {
                'name': 'pool/fs@snap_1_f',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M',
            }),
            ('funky_name', {
                'name': 'pool/fs@funky_name',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M',
            }),
            ('snap_2', {
                'name': 'pool/fs@snap_2',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M',
            }),
            ('snap_3', {
                'name': 'pool/fs@snap_3',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M'
            }),
            ('snap_8', {
                'name': 'pool/fs@snap_8',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M'
            }),
            ('snap_9', {
                'name': 'pool/fs@snap_9',
                'mountpoint': '-', 'refer': '10.0M', 'used': '10.0M', 'written': '10.0M'
            }),
        ])
    }
    snapshots = zfs._parse_snapshots()
    # comparing .items() because we care about the sorting in the OrderedDict's
    assert snapshots['pool'].items() == expected['pool'].items()
    assert snapshots['pool/fs'].items() == expected['pool/fs'].items()


@pytest.mark.parametrize("fs_name, expected", [
    # (dataset [(snapshot_name, parent_snapshot_name), ...])
    ('pool/fs', [('pool/fs@snap_0', None),
                 ('pool/fs@snap_1_f', 'pool/fs@snap_0'),
                 ('pool/fs@snap_2', 'pool/fs@snap_1_f'),
                 ('pool/fs@snap_3', 'pool/fs@snap_2'),
                 ('pool/fs@snap_8', 'pool/fs@snap_3'),
                 ('pool/fs@snap_9', 'pool/fs@snap_8')]),
    ('pool', [('pool@snap_p1', None),
              ('pool@snap_p2', 'pool@snap_p1')]),
])
def test_zfs_list(fs_name, expected):
    zfs = FakeZFSManager(fs_name=fs_name, snapshot_prefix='snap_')
    actual = [
        (snap.name, snap.parent.name if snap.parent else None)
        for snap in zfs.list()]
    assert actual == expected


class FakeCommandExecutor(CommandExecutor):
    has_pv = False  # disable pv for consistent test output

    def __init__(self, *a, **kwa):
        super(FakeCommandExecutor, self).__init__(*a, **kwa)
        self._called_commands = []
        # currently we only need the output of 'zfs send -nvP' in the tests
        self._expected = "\nsize 1234"

    def shell(self, cmd, dry_run=None, capture=None):  # pylint: disable=arguments-differ
        self._called_commands.append(cmd)
        return self._expected


@pytest.fixture
def pair_manager(s3_manager):
    zfs_manager = FakeZFSManager(fs_name='pool/fs', snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    return PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)


def test_pair_list(pair_manager):
    pairs = pair_manager.list()
    name = lambda snap: snap.name if snap is not None else None
    names = [
        (name(s3_snap), name(z_snap))
        for (s3_snap, z_snap) in pairs]
    expected = [
        ('pool/fs@snap_0', 'pool/fs@snap_0'),
        ('pool/fs@snap_1_f', 'pool/fs@snap_1_f'),
        ('pool/fs@snap_2', 'pool/fs@snap_2'),
        ('pool/fs@snap_3', 'pool/fs@snap_3'),
        (None, 'pool/fs@snap_8'),  # snap_8 doesn't exist in the s3 fixture
        (None, 'pool/fs@snap_9'),  # snap_9 doesn't exist in the s3 fixture
        # s3-only snapshot pairs are listed last
        ('pool/fs@snap_4_mp', None),
        ('pool/fs@snap_5', None),
        ('pool/fs@snap_6_cycle', None),
        ('pool/fs@snap_7_cycle', None),
    ]
    assert names == expected


def test_backup_latest_full(pair_manager):
    pair_manager.backup_full()
    expected = [
        "zfs send -nvP 'pool/fs@snap_9'",
        ("zfs send 'pool/fs@snap_9' | "
         "pput --quiet --estimated 1234 --meta size=1234 "
         "--meta isfull=true {}pool/fs@snap_9")]
    assert pair_manager._cmd._called_commands == [
        e.format(FakeBucket.rand_prefix)
        for e in expected]


def test_backup_full(pair_manager):
    pair_manager.backup_full('pool/fs@snap_3')
    expected = [
        "zfs send -nvP 'pool/fs@snap_3'",
        ("zfs send 'pool/fs@snap_3' | "
         "pput --quiet --estimated 1234 --meta size=1234 "
         "--meta isfull=true {}pool/fs@snap_3")]
    assert pair_manager._cmd._called_commands == [
        e.format(FakeBucket.rand_prefix)
        for e in expected]


def test_backup_incremental_latest(pair_manager):
    pair_manager.backup_incremental()
    # snap_8 and snap_9 exist locally but not in s3
    # snap_8 comes after snap_3
    commands = [
        "zfs send -nvP -i 'pool/fs@snap_3' 'pool/fs@snap_8'",
        ("zfs send -i 'pool/fs@snap_3' 'pool/fs@snap_8' | "
         "pput --quiet --estimated 1234 --meta size=1234 "
         "--meta parent=pool/fs@snap_3 {}pool/fs@snap_8"),
        "zfs send -nvP -i 'pool/fs@snap_8' 'pool/fs@snap_9'",
        ("zfs send -i 'pool/fs@snap_8' 'pool/fs@snap_9' | "
         "pput --quiet --estimated 1234 --meta size=1234 "
         "--meta parent=pool/fs@snap_8 {}pool/fs@snap_9")
    ]
    expected = [e.format(FakeBucket.rand_prefix) for e in commands]
    assert pair_manager._cmd._called_commands == expected


def test_backup_incremental_missing_parent(s3_manager):
    expected = (
        'pool@p1\t0\t19K\t-\t19K\n'
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_4_mp\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_5\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=expected, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    with pytest.raises(IntegrityError) as excp_info:
        pair_manager.backup_incremental()
    assert excp_info.value.message == \
        "Broken snapshot detected pool/fs@snap_5, reason: 'parent broken'"
    assert fake_cmd._called_commands == []


def test_backup_incremental_cycle(s3_manager):
    zfs_list = (
        'pool@p1\t0\t19K\t-\t19K\n'
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_3\t10.0M\t10.0M\t-\t10.0M\n'
        # the next 2 have bad metadata in the s3 fixture
        'pool/fs@snap_6_cycle\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_7_cycle\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_8\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    with pytest.raises(IntegrityError) as excp_info:
        pair_manager.backup_incremental()
    assert excp_info.value.message == \
        "Broken snapshot detected pool/fs@snap_7_cycle, reason: 'cycle detected'"
    assert fake_cmd._called_commands == []


def test_backup_incremental_compressed(s3_manager):
    zfs_list = (
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_3\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_8\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(
        s3_manager, zfs_manager, command_executor=fake_cmd, compressor='pigz1')
    pair_manager.backup_incremental()
    commands = [
        "zfs send -nvP -i 'pool/fs@snap_3' 'pool/fs@snap_8'",
        ("zfs send -i 'pool/fs@snap_3' 'pool/fs@snap_8' | "
         "pigz -1 --blocksize 4096 | "
         "pput --quiet --estimated 1234 --meta size=1234 --meta parent=pool/fs@snap_3 "
         "--meta compressor=pigz1 {}pool/fs@snap_8"),
    ]
    expected = [e.format(FakeBucket.rand_prefix) for e in commands]
    assert fake_cmd._called_commands == expected


def test_backup_full_compressed(s3_manager):
    zfs_list = (
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_3\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_8\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(
        s3_manager, zfs_manager, command_executor=fake_cmd, compressor='pigz1')
    pair_manager.backup_full()
    commands = [
        "zfs send -nvP 'pool/fs@snap_8'",
        ("zfs send 'pool/fs@snap_8' | "
         "pigz -1 --blocksize 4096 | "
         "pput --quiet --estimated 1234 --meta size=1234 --meta isfull=true "
         "--meta compressor=pigz1 {}pool/fs@snap_8"),
    ]
    expected = [e.format(FakeBucket.rand_prefix) for e in commands]
    assert fake_cmd._called_commands == expected


def test_restore_full(s3_manager):
    """Test full restore on empty zfs dataset"""
    zfs_list = 'pool@p1\t0\t19K\t-\t19K\n'  # we have no pool/fs snapshots locally
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.restore('pool/fs@snap_1_f')
    expected = "z3_get {}pool/fs@snap_1_f | pigz -d | zfs recv pool/fs@snap_1_f".format(
        FakeBucket.rand_prefix)
    assert fake_cmd._called_commands == [expected]


def test_restore_incremental_empty_dataset(s3_manager):
    """Tests incremental restore on a zfs dataset with no snapshots"""
    zfs_list = 'pool@p1\t0\t19K\t-\t19K\n'  # we have no pool/fs snapshots locally
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.restore('pool/fs@snap_3')  # ask for an incremental snapshot
    # all incremental snapshots until we hit a full snapshot are expected
    expected = [
        "z3_get {}pool/fs@snap_1_f | pigz -d | zfs recv pool/fs@snap_1_f",
        "z3_get {}pool/fs@snap_2 | zfs recv pool/fs@snap_2",
        "z3_get {}pool/fs@snap_3 | zfs recv pool/fs@snap_3",
    ]
    expected = [e.format(FakeBucket.rand_prefix) for e in expected]
    assert fake_cmd._called_commands == expected


def test_restore_incremental(s3_manager):
    """Tests incremental restore on a zfs dataset with existing snapshots"""
    zfs_list = (
        'pool@p1\t0\t19K\t-\t19K\n'  # we have no pool/fs snapshots locally
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.restore('pool/fs@snap_3')  # ask for an incremental snapshot
    # all incremental snapshots until we hit a full snapshot are expected
    expected = [
        "z3_get {}pool/fs@snap_3 | zfs recv pool/fs@snap_3",
    ]
    expected = [e.format(FakeBucket.rand_prefix) for e in expected]
    assert fake_cmd._called_commands == expected


def test_restore_broken(s3_manager):
    """Tests restoring a broken snapshot raises integrity error"""
    zfs_list = (
        'pool@p1\t0\t19K\t-\t19K\n'  # we have no pool/fs snapshots locally
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    with pytest.raises(IntegrityError) as excp_info:
        pair_manager.restore('pool/fs@snap_4_mp')
    assert excp_info.value.message == \
        "Broken snapshot detected pool/fs@snap_4_mp, reason: 'missing parent'"


def test_restore_noop(s3_manager):
    """Test restore does nothing when snapshot already exist locally"""
    expected = (
        'pool@p1\t0\t19K\t-\t19K\n'  # we have no pool/fs snapshots locally
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=expected, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.restore('pool/fs@snap_2')
    assert fake_cmd._called_commands == []


def test_restore_force(s3_manager):
    """Tests incremental restore with forced rollback"""
    zfs_list = (
        'pool@p1\t0\t19K\t-\t19K\n'  # we have no pool/fs snapshots locally
        'pool/fs@snap_1_f\t10.0M\t10.0M\t-\t10.0M\n'
        'pool/fs@snap_2\t10.0M\t10.0M\t-\t10.0M\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=zfs_list, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    pair_manager = PairManager(s3_manager, zfs_manager, command_executor=fake_cmd)
    pair_manager.restore('pool/fs@snap_3', force=True)  # ask for an incremental snapshot
    # all incremental snapshots until we hit a full snapshot are expected
    expected = [
        "z3_get {}pool/fs@snap_3 | zfs recv -F pool/fs@snap_3",
    ]
    expected = [e.format(FakeBucket.rand_prefix) for e in expected]
    assert fake_cmd._called_commands == expected


def test_get_latest():
    expected = (
        'pool@p1\t0\t19K\t-\t19K\n'
    )
    zfs_manager = FakeZFSManager(fs_name='pool/fs', expected=expected, snapshot_prefix='snap_')
    fake_cmd = FakeCommandExecutor()
    with pytest.raises(SoftError) as excp_info:
        zfs_manager.get_latest()
    assert excp_info.value.message == \
        'Nothing to backup for filesystem "None". Are you sure ' \
        'SNAPSHOT_PREFIX="zfs-auto-snap:daily" is correct?'
    assert fake_cmd._called_commands == []


@contextlib.contextmanager
def capture_output():
    old_fds = sys.stdout, sys.stderr
    try:
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_fds


def test_handle_errors():
    @handle_soft_errors
    def func():
        raise SoftError('Ana are mere')
    with capture_output() as (stdout, stderr):
        func()
    assert (stdout.getvalue(), stderr.getvalue()) == ("", "Ana are mere\n")
