from __future__ import print_function

import argparse
import subprocess
import sys

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


def snapshots_to_send(source_snaps, dest_snaps):
    """return pair of snapshots"""
    if len(source_snaps) == 0:
        raise AssertionError("No snapshots exist locally!")
    if len(dest_snaps) == 0:
        # nothing on the remote side, send everything
        return None, source_snaps[-1]
    last_remote = dest_snaps[-1]
    for snap in reversed(source_snaps):
        if snap == last_remote:
            # found a common snapshot
            return last_remote, source_snaps[-1]
    # sys.stderr.write("source:'{}', dest:'{}'".format(source_snaps, dest_snaps))
    raise AssertionError("Latest snapshot on destination doesn't exist on source!")


def prepare_commands(from_snap, to_snap, filesystem, dry_run=False):
    if from_snap == to_snap:
        if not quiet:
            print("Nothing to do here.")
        return
    if from_snap is None:
        send_cmd = "zfs send {}".format(to_snap)
    else:
        send_cmd = "zfs send -I {} {}".format(from_snap, to_snap)
    dry = 'nv' if dry_run else ''
    recv_cmd = "zfs recv -d{dry} {}".format(
        filesystem, dry=dry)
    return send_cmd, recv_cmd


def send_snapshots(send_cmd, recv_cmd, remote_addr):
    recv_cmd = "ssh {} -C 'mbuffer -s 128k -m 200m -q | sudo {}'".format(
        remote_addr, recv_cmd)
    return send_cmd, recv_cmd


def pull_snapshots(send_cmd, recv_cmd, remote_addr):
    send_cmd = "ssh {} -C 'sudo {}'".format(
        remote_addr, send_cmd)
    recv_cmd = "mbuffer -s 128k -m 200m -q | {}".format(recv_cmd)
    return send_cmd, recv_cmd


def sync_snapshots(pair, local_fs, remote_fs, remote_addr, pull, dry_run):
    from_snap, to_snap = pair
    target_fs = local_fs if pull else remote_fs
    source_fs = remote_fs if pull else local_fs
    from_snap = "{}@{}".format(source_fs, from_snap) if from_snap is not None else None
    to_snap = "{}@{}".format(source_fs, to_snap)
    cmd_pair = prepare_commands(
        from_snap,
        to_snap,
        filesystem=target_fs,
        dry_run=dry_run,
    )
    if cmd_pair is None:
        return
    send_cmd, recv_cmd = cmd_pair
    if pull:
        return pull_snapshots(send_cmd, recv_cmd, remote_addr)
    else:
        return send_snapshots(send_cmd, recv_cmd, remote_addr)


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
    parser.add_argument('--pull',
                        dest='pull',
                        default=False,
                        action='store_true',
                        help='pull snapshots from remote')
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
    if args.pull:
        pair = snapshots_to_send(source_snaps=remote_snaps, dest_snaps=local_snaps)
    else:
        pair = snapshots_to_send(source_snaps=local_snaps, dest_snaps=remote_snaps)
    cmd_pair = sync_snapshots(
        pair, args.filesystem, remote_fs, args.remote, args.pull, dry_run=args.dry_run)
    if cmd_pair is None:
        return
    send_cmd, recv_cmd = cmd_pair
    executor = CommandExecutor()
    executor.pipe(send_cmd, recv_cmd, quiet=quiet)


if __name__ == '__main__':
    main()
