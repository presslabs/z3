# pylint: disable=redefined-outer-name,protected-access
import pytest

from z3.ssh_sync import snapshots_to_send, sync_snapshots


ALL = ['S_0', 'S_1', 'S_2', 'S_3', 'S_4']


HAPPY_PATH = dict(
    # empty remote, send latest; no base snapshot
    empty_remote=((ALL, []), (None, 'S_4')),

    # first snapshot common to both, send latest, based on S_0
    first_sent=((ALL, ['S_0']), ('S_0', 'S_4')),

    # send latest, based on S_3
    latest=((ALL, ['S_1', 'S_3']), ('S_3', 'S_4')),

    # nothing to do
    noop=((ALL[1:], ALL), ('S_4', 'S_4')),
)


@pytest.mark.parametrize("pair, expected", HAPPY_PATH.values(), ids=HAPPY_PATH.keys())
def test_snapshots_to_send(pair, expected):
    local, remote = pair
    assert snapshots_to_send(local, remote) == expected


ERRORS = dict(
    empty_local=(([], []),
                 "No snapshots exist locally!"),
    no_common_snapshots=((["S_0"], ["S_10"]),
                         "Latest snapshot on destination doesn't exist on source!"),
)


@pytest.mark.parametrize('pair, err_msg', ERRORS.values(), ids=ERRORS.keys())
def test_snapshots_to_send_error(pair, err_msg):
    local, remote = pair
    with pytest.raises(AssertionError) as err:
        snapshots_to_send(local, remote)
    assert err_msg == err.value.message


PULL_HAPPY_PATH = dict(
    incremental=(
        ('S_0', 'S_4'),
        ("ssh example.com -C 'sudo zfs send -I remote/fs@S_0 remote/fs@S_4'",
         'mbuffer -s 128k -m 200m -q | zfs recv -d local/fs')),
    empty_target=(
        (None, 'S_4'),
        ("ssh example.com -C 'sudo zfs send remote/fs@S_4'",
         'mbuffer -s 128k -m 200m -q | zfs recv -d local/fs')),
    noop=(
        ('S_4', 'S_4'),
        None),
)


@pytest.mark.parametrize('pair, expected', PULL_HAPPY_PATH.values(), ids=PULL_HAPPY_PATH.keys())
def test_pull_command(pair, expected):
    commands = sync_snapshots(
        pair,
        local_fs='local/fs',
        remote_fs='remote/fs',
        remote_addr='example.com',
        pull=True,
        dry_run=False,
    )
    assert commands == expected


PUSH_HAPPY_PATH = dict(
    incremental=(
        ('S_0', 'S_4'),
        ("zfs send -I local/fs@S_0 local/fs@S_4",
         "ssh example.com -C 'mbuffer -s 128k -m 200m -q | sudo zfs recv -d remote/fs'")),
    empty_target=(
        (None, 'S_4'),
        ("zfs send local/fs@S_4",
         "ssh example.com -C 'mbuffer -s 128k -m 200m -q | sudo zfs recv -d remote/fs'")),
    noop=(
        ('S_4', 'S_4'),
        None),
)


@pytest.mark.parametrize('pair, expected', PUSH_HAPPY_PATH.values(), ids=PUSH_HAPPY_PATH.keys())
def test_push_command(pair, expected):
    commands = sync_snapshots(
        pair,
        local_fs='local/fs',
        remote_fs='remote/fs',
        remote_addr='example.com',
        pull=False,
        dry_run=False,
    )
    assert commands == expected
