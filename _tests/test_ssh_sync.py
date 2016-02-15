# pylint: disable=redefined-outer-name,protected-access
import pytest

from z3.ssh_sync import snapshots_to_send


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
    empty_local=(([], []), "No snapshots exist locally!"),
    no_common_snapshots=((["S_0"], ["S_10"]), "Could not find common between local and remote!"),
)


@pytest.mark.parametrize('pair, err_msg', ERRORS.values(), ids=ERRORS.keys())
def test_snapshots_to_send_error(pair, err_msg):
    local, remote = pair
    with pytest.raises(AssertionError) as err:
        snapshots_to_send(local, remote)
    assert err_msg == err.value.message
