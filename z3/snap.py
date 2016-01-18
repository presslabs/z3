import argparse
import functools
import logging
import operator
import os
import subprocess
from collections import OrderedDict

import boto

from z3.config import get_config


def cached(func):
    @functools.wraps(func)
    def wrapper(self, *a, **kwa):
        # ARGUMENTS AREN'T PART OF CACHE KEY
        # that's safe in this case, but might cause surprising bugs if you reuse it as is
        cache_key = func.__name__ + '_cached_value'
        if not hasattr(self, cache_key):
            val = func(self, *a, **kwa)
            setattr(self, cache_key, val)
        return getattr(self, cache_key)
    return wrapper


class IntegrityError(Exception):
    pass


class S3Snapshot(object):
    CYCLE = 'cycle detected'
    MISSING_PARENT = 'missing parent'
    PARENT_BROKEN = 'parent broken'

    def __init__(self, key, manager):
        self.name = key.key
        self._metadata = key.metadata
        self._mgr = manager
        self._reason_broken = None

    def __repr__(self):
        if self.is_full:
            return "<Snapshot {} [full]>".format(self.name)
        else:
            return "<Snapshot {} [{}]>".format(self.name, self.parent_name)

    @property
    def is_full(self):
        return self._metadata.get('is_full') == 'true'

    @property
    def parent(self):
        parent_name = self._metadata.get('parent')
        return self._mgr.get(parent_name)

    @property
    def parent_name(self):
        return self._metadata.get("parent")

    @cached
    def _is_healthy(self, visited=frozenset()):
        if self.is_full:
            return True
        if self in visited:
            self._reason_broken = self.CYCLE
            return False  # we ended up with a cycle, abort
        if self.parent is None:
            self._reason_broken = self.MISSING_PARENT
            return False  # missing parent
        if not self.parent._is_healthy(visited.union([self])):
            if self.parent.reason_broken == self.CYCLE:
                self._reason_broken = self.CYCLE
            else:
                self._reason_broken = self.PARENT_BROKEN
            return False
        return True

    @property
    def is_healthy(self):
        return self._is_healthy()

    @property
    def reason_broken(self):
        if self.is_healthy:
            return
        return self._reason_broken


class S3SnapshotManager(object):
    def __init__(self, bucket, prefix=""):
        self.bucket = bucket
        self._prefix = prefix

    def _get_snapshots(self, prefix):
        snapshots = {}
        for key in self.bucket.list(prefix):
            key = self.bucket.get_key(key.key)
            snapshots[key.name] = S3Snapshot(key, manager=self)
        return snapshots

    @property
    @cached
    def _snapshots(self):
        return self._get_snapshots(prefix=self._prefix)

    def list(self):
        return sorted(self._snapshots.values(), key=operator.attrgetter('name'))

    def get(self, name):
        return self._snapshots.get(name)


class ZFSSnapshot(object):
    def __init__(self, name, metadata, parent=None, manager=None):
        self.name = name
        self.parent = parent

    def __repr__(self):
        return "<Snapshot {} [{}]>".format(self.name, self.parent.name if self.parent else '')


class ZFSSnapshotManager(object):
    def __init__(self, fs_name):
        self._fs_name = fs_name
        self._sorted = None

    def _list_snapshots(self):
        # This is overridden in tests
        # see FakeZFSManager
        return subprocess.check_output(
            ['zfs', 'list', '-Ht', 'snap', '-o',
             'name,used,refer,mountpoint,written'])

    def _parse_snapshots(self):
        """Returns all snapshots grouped by filesystem, a dict of OrderedDict's
        The order of snapshots matters when determining parents for incremental send,
        so it's preserved.
        Data is indexed by filesystem then for each filesystem we have an OrderedDict
        of snapshots.
        """
        try:
            snap = self._list_snapshots()
        except OSError as err:
            logging.error("unable to list local snapshots!")
            return {}
        vols = {}
        for line in snap.splitlines():
            name, used, refer, mountpoint, written = line.split('\t')
            vol_name, snap_name = name.split('@', 1)
            snapshots = vols.setdefault(vol_name, OrderedDict())
            snapshots[snap_name] = {
                'name': name,
                'used': used,
                'refer': refer,
                'mountpoint': mountpoint,
                'written': written,
            }
        return vols

    def _build_snapshots(self, fs_name):
        snapshots = OrderedDict()
        # for fs_name, fs_snaps in self._parse_snapshots().iteritems():
        fs_snaps = self._parse_snapshots()[fs_name]
        parent = None
        for snap_name, data in fs_snaps.iteritems():
            full_name = '{}@{}'.format(fs_name, snap_name)
            zfs_snap = ZFSSnapshot(
                full_name,
                metadata=data,
                parent=parent,
                manager=self,
            )
            snapshots[full_name] = zfs_snap
            parent = zfs_snap
        return snapshots

    @property
    @cached
    def _snapshots(self):
        return self._build_snapshots(self._fs_name)

    def list(self):
        return self._snapshots.values()

    def get_latest(self):
        return self._snapshots.values()[-1]

    def get(self, name):
        return self._snapshots.get(name)


class CommandExecutor(object):
    @staticmethod
    def shell(cmd, die=True, encoding='utf8'):
        try:
            print cmd
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            # print(out)
            # print("")
            return unicode(out, encoding=encoding)
        except subprocess.CalledProcessError as err:
            # print(err.output)
            # print("")
            if die is True:
                raise
            else:
                return str(err.output, encoding=encoding)

    @property
    @cached
    def has_pv(self):
        return subprocess.call(['which', 'pv']) == 0

    def pipe(self, cmd1, cmd2):
        """Executes commands"""
        if self.has_pv:
            return self.shell("{} | pv | {}".format(cmd1, cmd2))
        else:
            return self.shell("{} | {}".format(cmd1, cmd2))


class PairManager(object):
    def __init__(self, s3_manager, zfs_manager, command_executor=None):
        self.s3_manager = s3_manager
        self.zfs_manager = zfs_manager
        self._cmd = command_executor or CommandExecutor()

    def list(self):
        pairs = []
        seen = set([])
        for z_snap in self.zfs_manager.list():
            seen.add(z_snap.name)
            pairs.append(
                (self.s3_manager.get(z_snap.name), z_snap))
        for s3_snap in self.s3_manager.list():
            if s3_snap.name not in seen:
                pairs.append((s3_snap, None))
        return pairs

    def _snapshot_to_backup(self, snap_name):
        if snap_name is None:
            z_snap = self.zfs_manager.get_latest()
        else:
            z_snap = self.zfs_manager.get(snap_name)
            if z_snap is None:
                raise Exception('Failed to get the snapshot {}'.format(snap_name))
        return z_snap

    def backup_full(self, snap_name=None):
        """Do a full backup of a snapshot. By default latest local snapshot"""
        z_snap = self._snapshot_to_backup(snap_name)
        self._cmd.pipe(
            "zfs send '{}'".format(z_snap.name),
            "pput --meta is_full=true {}".format(z_snap.name)
        )

    def backup_incremental(self, snap_name=None):
        z_snap = self._snapshot_to_backup(snap_name)
        to_upload = []
        current = z_snap
        while True:
            s3_snap = self.s3_manager.get(current.name)
            if s3_snap is not None:
                if not s3_snap.is_healthy:
                    # abort everything if we run in to unhealthy snapshots
                    raise IntegrityError(
                        "Broken snapshot detected {}, reason: '{}'".format(
                            s3_snap.name, s3_snap.reason_broken
                        ))
                break
            to_upload.append(current)
            if current.parent is None:
                break
            current = current.parent
        for z_snap in reversed(to_upload):
            self._cmd.pipe(
                "zfs send -i '{}' '{}'".format(
                    z_snap.parent.name, z_snap.name),
                "pput --meta parent={} {}".format(
                    z_snap.parent.name, z_snap.name)
            )


def list_snapshots(bucket, s3_prefix, snapshot_prefix, filesystem):
    prefix = "{}@{}".format(os.path.join(s3_prefix, filesystem), snapshot_prefix)
    s3_mgr = S3SnapshotManager(bucket, prefix=prefix)
    zfs_mgr = ZFSSnapshotManager(fs_name=filesystem)
    pair_manager = PairManager(s3_mgr, zfs_mgr)
    fmt = "{:20} | {:20} | {:15} | {:16} | {:10}"
    print fmt.format("NAME", "PARENT", "TYPE", "HEALTH", "LOCAL STATE")
    for s3_snap, z_snap in pair_manager.list():
        if s3_snap is None:
            snap_type = 'missing'
            health = '-'
            name = z_snap.name
            parent_name = '-'
            local_state = 'ok'
        else:
            snap_type = 'full' if s3_snap.is_full else 'incremental'
            health = s3_snap.reason_broken or 'ok'
            parent_name = '' if s3_snap.is_full else s3_snap.parent_name
            name = s3_snap.name
            local_state = 'ok' if z_snap is not None else 'missing'
        print fmt.format(name, parent_name, snap_type, health, local_state)


def do_backup(bucket, s3_prefix, snapshot_prefix, filesystem, full, snapshot):
    prefix = "{}@{}".format(os.path.join(s3_prefix, filesystem), snapshot_prefix)
    s3_mgr = S3SnapshotManager(bucket, prefix=prefix)
    zfs_mgr = ZFSSnapshotManager(fs_name=filesystem)
    pair_manager = PairManager(s3_mgr, zfs_mgr)
    snap_name = "{}@{}".format(filesystem, snapshot) if snapshot else None
    if full is True:
        pair_manager.backup_full(snap_name=snap_name)
    else:
        pair_manager.backup_incremental(snap_name=snap_name)


def main():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='list z3 snapshots',
    )
    parser.add_argument('--s3-prefix',
                        dest='s3_prefix',
                        default=cfg.get('S3_PREFIX', 'z3-backup/'),
                        help='S3 key prefix, defaults to z3-backup')
    parser.add_argument('--filesystem', '--dataset',
                        dest='filesystem',
                        default=cfg.get('FILESYSTEM'),
                        help='the zfs dataset/filesystem to operate on')
    parser.add_argument('--snapshot-prefix',
                        dest='snapshot_prefix',
                        default=cfg.get('SNAPSHOT_PREFIX', 'zfs-auto-snap:daily'),
                        help='only look at snapshots that start with this prefix')
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')

    backup_parser = subparsers.add_parser(
        'backup', help='backup local zfs snapshots to an s3 bucket')
    backup_parser.add_argument('--snapshot', dest='snapshot', default=None,
                               help='Snapshot to backup. Defaults to latest.')
    incremental_group = backup_parser.add_mutually_exclusive_group()
    incremental_group.add_argument(
        '--full', dest='full', action='store_true', help='Perform full backup')
    incremental_group.add_argument(
        '--incremental', dest='incremental', default=True, action='store_true',
        help='Perform incremental backup; this is the default')

    restore_parser = subparsers.add_parser('restore', help='not implemented')
    status_parser = subparsers.add_parser('status', help='show status of current backups')
    args = parser.parse_args()
    print args
    bucket = boto.connect_s3(
        cfg['S3_KEY_ID'], cfg['S3_SECRET']).get_bucket(cfg['BUCKET'])
    if args.subcommand == 'status':
        list_snapshots(bucket, s3_prefix=args.s3_prefix, snapshot_prefix=args.snapshot_prefix,
                       filesystem=args.filesystem)
    elif args.subcommand == 'backup':
        do_backup(bucket, s3_prefix=args.s3_prefix, snapshot_prefix=args.snapshot_prefix,
                  filesystem=args.filesystem, full=args.full, snapshot=args.snapshot)


if __name__ == '__main__':
    main()
