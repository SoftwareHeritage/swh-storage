# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hypothesis import given

from swh.model.identifiers import snapshot_identifier, identifier_to_bytes
from swh.model.hypothesis_strategies import \
    snapshots, branch_names, branch_targets

from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.storage.tests.test_in_memory import swh_storage_backend_config  # noqa


@given(snapshot=snapshots(min_size=0, max_size=10, only_objects=False))
def test_snapshot_small(swh_storage, snapshot):  # noqa
    snapshot = snapshot.to_dict()
    swh_storage.snapshot_add([snapshot])

    returned_snapshot = snapshot_get_all_branches(
        swh_storage, snapshot['id'])
    assert snapshot == returned_snapshot


@given(branch_name=branch_names(),
       branch_target=branch_targets(only_objects=True))
def test_snapshot_large(swh_storage, branch_name, branch_target):  # noqa
    branch_target = branch_target.to_dict()

    snapshot = {
        'branches': {
            b'%s%05d' % (branch_name, i): branch_target
            for i in range(10000)
        }
    }
    snapshot['id'] = identifier_to_bytes(snapshot_identifier(snapshot))

    swh_storage.snapshot_add([snapshot])

    returned_snapshot = snapshot_get_all_branches(
        swh_storage, snapshot['id'])
    assert snapshot == returned_snapshot
