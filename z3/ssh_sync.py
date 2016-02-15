from __future__ import print_function

import argparse
import subprocess
import os.path

from z3.config import get_config
from z3.snap import ZFSSnapshotManager, CommandExecutor


quiet = False


class RemoteZFSSnapshotManager(ZFSSnapshotManager):
    def __init__(self, remote_addr, *a, **kwa):
        super(RemoteZFSSnapshotManager, self).__init__(*a, **kwa)
        self.remote_addr = remote_addr

    def _list_snapshots(self):
        return subprocess.check_output(
            ['ssh', self.remote_addr, '-C',
             'sudo zfs list -Ht snap -o name,used,refer,mountpoint,written'])


def snapshots_to_send(local_snaps, remote_snaps):
    """return pair of snapshots"""
    if len(local_snaps) == 0:
        raise AssertionError("No snapshots exist locally!")
    if len(remote_snaps) == 0:
        # nothing on the remote side, send everything
        return None, local_snaps[-1]
    last_remote = remote_snaps[-1]
    for snap in reversed(local_snaps):
        if snap == last_remote:
            # found a common snapshot
            return last_remote, local_snaps[-1]
    # no common snapshots exist; panic!
    raise AssertionError("Could not find common between local and remote!")


def send_snapshots(from_snap, to_snap, remote_addr, remote_fs, executor=None, dry_run=False):
    executor = executor if executor is not None else CommandExecutor()
    if from_snap == to_snap:
        if not quiet:
            print("Nothing to do here.")
        return
    if from_snap is None:
        local_cmd = "zfs send '{}'".format(to_snap)
    else:
        local_cmd = "zfs send -I '{}' '{}'".format(from_snap, to_snap)
    dry = 'nv' if dry_run else ''
    remote_cmd = "ssh {} -C 'mbuffer -s 128k -m 200m -q | sudo zfs recv -d{dry} {}'".format(
        remote_addr, remote_fs, dry=dry)
    executor.pipe(local_cmd, remote_cmd, quiet=quiet)


def main():
    global quiet
    cfg = get_config()
    parser = argparse.ArgumentParser(
        description='send z3 snapshots over ssh',
    )
    parser.add_argument('--filesystem', '--dataset',
                        dest='filesystem',
                        default=cfg.get('FILESYSTEM'),
                        help='the zfs dataset/filesystem to operate on')
    parser.add_argument('--remote-filesystem', '--remote-dataset',
                        dest='remote_filesystem',
                        default=None,
                        help='the target zfs dataset/filesystem to send snapshots to')
    parser.add_argument('--snapshot-prefix',
                        dest='snapshot_prefix',
                        default=cfg.get('SNAPSHOT_PREFIX', 'zfs-auto-snap:daily'),
                        help='only operate on snapshots that start with this prefix')
    parser.add_argument('--quiet',
                        dest='quiet',
                        default=False,
                        action='store_true',
                        help='suppress output')
    parser.add_argument('--dry-run',
                        dest='dry_run',
                        default=False,
                        action='store_true',
                        help='call zfs recv with -nv flags to test if snapshot can be sent')
    parser.add_argument('remote', help='hostname/address of remote server')
    args = parser.parse_args()
    quiet = args.quiet
    local_mgr = ZFSSnapshotManager(args.filesystem, args.snapshot_prefix)
    remote_fs = args.remote_filesystem or args.filesystem
    remote_mgr = RemoteZFSSnapshotManager(args.remote, remote_fs, args.snapshot_prefix)
    local_snaps = [s.name[len(args.filesystem)+1:]  # strip fs name
                   for s in local_mgr.list()]
    remote_snaps = [s.name[len(remote_fs)+1:]  # strip fs name
                    for s in remote_mgr.list()]
    from_snap, to_snap = snapshots_to_send(local_snaps, remote_snaps)
    local_fs = args.filesystem
    send_snapshots(
        "{}@{}".format(local_fs, from_snap) if from_snap is not None else None,
        "{}@{}".format(local_fs, to_snap),
        remote_addr=args.remote,
        remote_fs=remote_fs,
        dry_run=args.dry_run,
    )


if __name__ == '__main__':
    main()
