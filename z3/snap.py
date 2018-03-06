from __future__ import print_function

import argparse
import functools
import logging
import operator
import os
import subprocess
import sys
from collections import OrderedDict

import boto

from z3.config import get_config


def cached(func):
    @functools.wraps(func)
    def cacheing_wrapper(self, *a, **kwa):
        cache_key = func.__name__ + '_cached_value'
        if len(a) or len(kwa):
            # make sure we don't shoot ourselves in the foot by calling this on a method with args
            raise AssertionError("'cached' decorator called on method with arguments!")
        if not hasattr(self, cache_key):
            val = func(self, *a, **kwa)
            setattr(self, cache_key, val)
        return getattr(self, cache_key)
    return cacheing_wrapper


COMPRESSORS = {
    'pigz1': {
        'compress': 'pigz -1 --blocksize 4096',
        'decompress': 'pigz -d'},
    'pigz4': {
        'compress': 'pigz -4 --blocksize 4096',
        'decompress': 'pigz -d'},
}


class IntegrityError(Exception):
    pass


class SoftError(Exception):
    pass


def handle_soft_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SoftError as err:
            sys.stderr.write(str(err) + os.linesep)
            sys.stderr.flush()
    return wrapper


class S3Snapshot(object):
    CYCLE = 'cycle detected'
    MISSING_PARENT = 'missing parent'
    PARENT_BROKEN = 'parent broken'

    def __init__(self, name, metadata, manager, size):
        self.name = name
        self._metadata = metadata
        self._mgr = manager
        self._reason_broken = None
        self.size = size

    def __repr__(self):
        if self.is_full:
            return "<Snapshot {} [full]>".format(self.name)
        else:
            return "<Snapshot {} [{}]>".format(self.name, self.parent_name)

    @property
    def is_full(self):
        # keep backwards compatibility for underscore metadata
        return 'true' in [self._metadata.get('is_full'), self._metadata.get('isfull')]

    @property
    def parent(self):
        parent_name = self._metadata.get('parent')
        return self._mgr.get(parent_name)

    @property
    def parent_name(self):
        return self._metadata.get("parent")

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
            if self.parent._reason_broken == self.CYCLE:
                self._reason_broken = self.CYCLE
            else:
                self._reason_broken = self.PARENT_BROKEN
            return False
        return True

    @property
    @cached
    def is_healthy(self):
        return self._is_healthy()

    @property
    def reason_broken(self):
        if self.is_healthy:
            return
        return self._reason_broken

    @property
    def compressor(self):
        return self._metadata.get('compressor')

    @property
    def uncompressed_size(self):
        return self._metadata.get('size')


class S3SnapshotManager(object):
    def __init__(self, bucket, s3_prefix, snapshot_prefix):
        self.bucket = bucket
        self.s3_prefix = s3_prefix.rstrip('/') + '/'  # make sure we always have a trailing /
        self.snapshot_prefix = snapshot_prefix

    @property
    @cached
    def _snapshots(self):
        prefix = os.path.join(self.s3_prefix, self.snapshot_prefix)
        snapshots = {}
        strip_chars = len(self.s3_prefix)
        for key in self.bucket.list(prefix):
            key = self.bucket.get_key(key.key)
            name = key.key[strip_chars:]
            snapshots[name] = S3Snapshot(name, metadata=key.metadata, manager=self, size=key.size)
        return snapshots

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
    def __init__(self, fs_name, snapshot_prefix):
        self._fs_name = fs_name
        self._snapshot_prefix = snapshot_prefix
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
            if len(line) == 0:
                continue
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
        fs_snaps = self._parse_snapshots().get(fs_name, {})
        parent = None
        for snap_name, data in fs_snaps.iteritems():
            if not snap_name.startswith(self._snapshot_prefix):
                continue
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
        if len(self._snapshots) == 0:
            cfg = get_config()
            raise SoftError(
                'Nothing to backup for filesystem "{}". Are you sure '
                'SNAPSHOT_PREFIX="{}" is correct?'.format(
                    cfg.get('FILESYSTEM'), cfg.get('SNAPSHOT_PREFIX')))
        return self._snapshots.values()[-1]

    def get(self, name):
        return self._snapshots.get(name)


class CommandExecutor(object):
    @staticmethod
    def shell(cmd, dry_run=False, capture=False):
        if dry_run:
            print(cmd)
        else:
            if capture:
                return subprocess.check_output(
                    cmd, shell=True, stderr=subprocess.STDOUT)
            else:
                return subprocess.check_call(
                    cmd, shell=True)

    @property
    @cached
    def has_pv(self):
        return subprocess.call(
            ['which', 'pv'],
            stderr=subprocess.STDOUT, stdout=subprocess.PIPE) == 0

    def pipe(self, cmd1, cmd2, quiet=False, estimated_size=None, **kwa):
        """Executes commands"""
        if self.has_pv and not quiet:
            pv = "pv" if estimated_size is None else "pv --size {}".format(estimated_size)
            return self.shell("{} | {}| {}".format(cmd1, pv, cmd2), **kwa)
        else:
            return self.shell("{} | {}".format(cmd1, cmd2), **kwa)


class PairManager(object):
    def __init__(self, s3_manager, zfs_manager, command_executor=None, compressor=None):
        self.s3_manager = s3_manager
        self.zfs_manager = zfs_manager
        self._cmd = command_executor or CommandExecutor()
        self.compressor = compressor

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

    @staticmethod
    def _parse_estimated_size(output):
        try:
            size_line = [line for line in output.splitlines() if len(line)][-1]
            _, size = size_line.split()
            return int(size)
        except:
            logging.error("failed to parse output '%s'", output)
            raise

    def _compress(self, cmd):
        """Adds the appropriate command to compress the zfs stream"""
        compressor = COMPRESSORS.get(self.compressor)
        if compressor is None:
            return cmd
        compress_cmd = compressor['compress']
        return "{} | {}".format(compress_cmd, cmd)

    def _decompress(self, cmd, s3_snap):
        """Adds the appropriate command to decompress the zfs stream
        This is determined from the metadata of the s3_snap.
        """
        compressor = COMPRESSORS.get(s3_snap.compressor)
        if compressor is None:
            return cmd
        decompress_cmd = compressor['decompress']
        return "{} | {}".format(decompress_cmd, cmd)

    def _pput_cmd(self, estimated, s3_prefix, snap_name, parent=None):
        meta = ['size={}'.format(estimated)]
        if parent is None:
            meta.append("isfull=true")
        else:
            meta.append("parent={}".format(parent))
        if self.compressor is not None:
            meta.append("compressor={}".format(self.compressor))
        return "pput --quiet --estimated {estimated} {meta} {prefix}{name}".format(
            estimated=estimated, prefix=s3_prefix, name=snap_name,
            meta=" ".join(("--meta " + m) for m in meta))

    def backup_full(self, snap_name=None, dry_run=False):
        """Do a full backup of a snapshot. By default latest local snapshot"""
        z_snap = self._snapshot_to_backup(snap_name)
        estimated_size = self._parse_estimated_size(
            self._cmd.shell(
                "zfs send -nvP '{}'".format(z_snap.name),
                capture=True))
        self._cmd.pipe(
            "zfs send '{}'".format(z_snap.name),
            self._compress(
                self._pput_cmd(
                    estimated=estimated_size,
                    s3_prefix=self.s3_manager.s3_prefix,
                    snap_name=z_snap.name)
            ),
            dry_run=dry_run,
            estimated_size=estimated_size,
        )
        return [{'snap_name': z_snap.name, 'size': estimated_size}]

    def backup_incremental(self, snap_name=None, dry_run=False):
        """Uploads named snapshot or latest, along with any other snapshots
        required for an incremental backup.
        """
        z_snap = self._snapshot_to_backup(snap_name)
        to_upload = []
        current = z_snap
        uploaded_meta = []
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
            estimated_size = self._parse_estimated_size(
                self._cmd.shell(
                    "zfs send -nvP -i '{}' '{}'".format(
                        z_snap.parent.name, z_snap.name),
                    capture=True))
            self._cmd.pipe(
                "zfs send -i '{}' '{}'".format(
                    z_snap.parent.name, z_snap.name),
                self._compress(
                    self._pput_cmd(
                        estimated=estimated_size,
                        parent=z_snap.parent.name,
                        s3_prefix=self.s3_manager.s3_prefix,
                        snap_name=z_snap.name)
                ),
                dry_run=dry_run,
                estimated_size=estimated_size,
            )
            uploaded_meta.append({'snap_name': z_snap.name, 'size': estimated_size})
        return uploaded_meta

    def restore(self, snap_name, dry_run=False, force=False):
        current_snap = self.s3_manager.get(snap_name)
        if current_snap is None:
            raise Exception('no such snapshot "{}"'.format(snap_name))
        to_restore = []
        while True:
            z_snap = self.zfs_manager.get(current_snap.name)
            if z_snap is not None:
                break
            if not current_snap.is_healthy:
                raise IntegrityError(
                    "Broken snapshot detected {}, reason: '{}'".format(
                        current_snap.name, current_snap.reason_broken
                    ))
            to_restore.append(current_snap)
            if current_snap.is_full:
                break
            else:
                current_snap = current_snap.parent
        force = '-F ' if force is True else ''
        for s3_snap in reversed(to_restore):
            self._cmd.pipe(
                "z3_get {}".format(
                    os.path.join(self.s3_manager.s3_prefix, s3_snap.name)),
                self._decompress(
                    cmd="zfs recv {force}{snap}".format(
                        force=force, snap=s3_snap.name),
                    s3_snap=s3_snap,
                ),
                dry_run=dry_run,
                estimated_size=s3_snap.size,
            )


def _humanize(size):
    units = ('M', 'G', 'T')
    unit_index = 0
    size = float(size) / (1024**2)  # Mega
    while size > 1024 and unit_index < (len(units) - 1):
        size = size / 1024
        unit_index += 1
    size = "{:.2f}".format(size)
    size = size.rstrip('0').rstrip('.')
    return "{} {}".format(size, units[unit_index])


def _get_widths(widths, line):
    for index, value in enumerate(line):
        widths[index] = max(widths[index], len("{}".format(value)))
    return widths


def _prepare_line(s3_snap, z_snap):
    if s3_snap is None:
        snap_type = 'missing'
        health = '-'
        name = z_snap.name.split('@', 1)[1]
        parent_name = '-'
        local_state = 'ok'
        size = ''
    else:
        snap_type = 'full' if s3_snap.is_full else 'incremental'
        health = s3_snap.reason_broken or 'ok'
        parent_name = '' if s3_snap.is_full else s3_snap.parent_name.split('@', 1)[1]
        name = s3_snap.name.split('@', 1)[1]
        local_state = 'ok' if z_snap is not None else 'missing'
        size = _humanize(s3_snap.uncompressed_size) if s3_snap.uncompressed_size is not None else ''
    return (name, parent_name, snap_type, health, local_state, size)


def list_snapshots(bucket, s3_prefix, filesystem, snapshot_prefix):
    print("backup status for {}@{}* on {}/{}".format(
        filesystem, snapshot_prefix, bucket.name, s3_prefix))
    prefix = "{}@{}".format(filesystem, snapshot_prefix)
    pair_manager = PairManager(
        S3SnapshotManager(bucket, s3_prefix=s3_prefix, snapshot_prefix=prefix),
        ZFSSnapshotManager(fs_name=filesystem, snapshot_prefix=snapshot_prefix))
    header = ("NAME", "PARENT", "TYPE", "HEALTH", "LOCAL STATE", "SIZE")
    widths = [len(col) for col in header]
    listing = []
    for s3_snap, z_snap in pair_manager.list():
        line = _prepare_line(s3_snap, z_snap)
        listing.append(line)
        widths = _get_widths(widths, line)
    fmt = " | ".join("{{:{w}}}".format(w=w) for w in widths)
    print(fmt.format(*header))
    for line in sorted(listing):
        print(fmt.format(*line))


def do_backup(bucket, s3_prefix, filesystem, snapshot_prefix, full, snapshot, compressor, dry, parseable):
    prefix = "{}@{}".format(filesystem, snapshot_prefix)
    s3_mgr = S3SnapshotManager(bucket, s3_prefix=s3_prefix, snapshot_prefix=prefix)
    zfs_mgr = ZFSSnapshotManager(fs_name=filesystem, snapshot_prefix=snapshot_prefix)
    pair_manager = PairManager(s3_mgr, zfs_mgr, compressor=compressor)
    snap_name = "{}@{}".format(filesystem, snapshot) if snapshot else None
    if full is True:
        uploaded = pair_manager.backup_full(snap_name=snap_name, dry_run=dry)
    else:
        uploaded = pair_manager.backup_incremental(snap_name=snap_name, dry_run=dry)
    for meta in uploaded:
        if parseable:
            print("{snap_name}\x00{size}".format(**meta))
        else:
            print("Successfuly backed up {}: {}.".format(
                meta['snap_name'], _humanize(meta['size'])))


def restore(bucket, s3_prefix, filesystem, snapshot_prefix, snapshot, dry, force):
    prefix = "{}@{}".format(filesystem, snapshot_prefix)
    s3_mgr = S3SnapshotManager(bucket, s3_prefix=s3_prefix, snapshot_prefix=prefix)
    zfs_mgr = ZFSSnapshotManager(fs_name=filesystem, snapshot_prefix=snapshot_prefix)
    pair_manager = PairManager(s3_mgr, zfs_mgr)
    snap_name = "{}@{}".format(filesystem, snapshot)
    pair_manager.restore(snap_name, dry_run=dry, force=force)


def parse_args():
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='list z3 snapshots',
    )
    parser.add_argument('--s3-prefix',
                        dest='s3_prefix',
                        default=cfg.get('S3_PREFIX', 'z3-backup/'),
                        help='S3 key prefix, defaults to z3-backup/')
    parser.add_argument('--filesystem', '--dataset',
                        dest='filesystem',
                        default=cfg.get('FILESYSTEM'),
                        help='the zfs dataset/filesystem to operate on')
    parser.add_argument('--snapshot-prefix',
                        dest='snapshot_prefix',
                        default=None,
                        help=('Only operate on snapshots that start with this prefix. '
                              'Defaults to zfs-auto-snap:daily.'))
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')

    backup_parser = subparsers.add_parser(
        'backup', help='backup local zfs snapshots to an s3 bucket')
    backup_parser.add_argument('--snapshot', dest='snapshot', default=None,
                               help='Snapshot to backup. Defaults to latest.')
    backup_parser.add_argument('--dry-run', dest='dry', default=False, action='store_true',
                               help='Dry run.')
    backup_parser.add_argument('--compressor', dest='compressor', default=None,
                               choices=(['none'] + sorted(COMPRESSORS.keys())),
                               help=('Specify the compressor. Defaults to pigz1. '
                                     'Use "none" to disable.'))
    backup_parser.add_argument('--parseable', dest='parseable', action='store_true',
                               help='Machine readable output')
    incremental_group = backup_parser.add_mutually_exclusive_group()
    incremental_group.add_argument(
        '--full', dest='full', action='store_true', help='Perform full backup')
    incremental_group.add_argument(
        '--incremental', dest='incremental', default=True, action='store_true',
        help='Perform incremental backup; this is the default')

    restore_parser = subparsers.add_parser('restore', help='not implemented')
    restore_parser.add_argument(
        'snapshot', help='Snapshot to backup. Defaults to latest.')
    restore_parser.add_argument('--dry-run', dest='dry', default=False, action='store_true',
                                help='Dry run.')
    restore_parser.add_argument('--force', dest='force', default=False, action='store_true',
                                help='Force rollback of the filesystem (zfs recv -F).')
    subparsers.add_parser('status', help='show status of current backups')
    return parser.parse_args()


@handle_soft_errors
def main():
    cfg = get_config()
    args = parse_args()

    try:
        s3_key_id, s3_secret, bucket = cfg['S3_KEY_ID'], cfg['S3_SECRET'], cfg['BUCKET']

        extra_config = {}
        if 'HOST' in cfg:
            extra_config['host'] = cfg['HOST']
    except KeyError as err:
        sys.stderr.write("Configuration error! {} is not set.\n".format(err))
        sys.exit(1)

    bucket = boto.connect_s3(s3_key_id, s3_secret, **extra_config).get_bucket(bucket)

    fs_section = "fs:{}".format(args.filesystem)
    if args.snapshot_prefix is None:
        snapshot_prefix = cfg.get("SNAPSHOT_PREFIX", section=fs_section)
    else:
        snapshot_prefix = args.snapshot_prefix
    if args.subcommand == 'status':
        list_snapshots(bucket, s3_prefix=args.s3_prefix, snapshot_prefix=snapshot_prefix,
                       filesystem=args.filesystem)
    elif args.subcommand == 'backup':
        if args.compressor is None:
            compressor = cfg.get('COMPRESSOR', section=fs_section)
        else:
            compressor = args.compressor
        if compressor.lower() == 'none':
            compressor = None

        do_backup(bucket, s3_prefix=args.s3_prefix, snapshot_prefix=snapshot_prefix,
                  filesystem=args.filesystem, full=args.full, snapshot=args.snapshot,
                  dry=args.dry, compressor=compressor, parseable=args.parseable)
    elif args.subcommand == 'restore':
        restore(bucket, s3_prefix=args.s3_prefix, snapshot_prefix=snapshot_prefix,
                filesystem=args.filesystem, snapshot=args.snapshot, dry=args.dry,
                force=args.force)


if __name__ == '__main__':
    main()
