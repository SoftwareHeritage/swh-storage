# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

import pytest

from hypothesis import given
from hypothesis.strategies import datetimes

from swh.model.identifiers import snapshot_identifier, identifier_to_bytes
from swh.model.hypothesis_strategies import \
    origins, snapshots, branch_names, branch_targets
from swh.storage.tests.storage_testing import StorageTestFixture

from swh.storage.algos.snapshot import snapshot_get_all_branches


@pytest.mark.db
@pytest.mark.property_based
class TestSnapshotAllBranches(StorageTestFixture, unittest.TestCase):
    @given(origins().map(lambda x: x.to_dict()),
           datetimes(),
           snapshots(min_size=0, max_size=10, only_objects=False))
    def test_snapshot_small(self, origin, ts, snapshot):
        snapshot = snapshot.to_dict()
        origin_id = self.storage.origin_add_one(origin)
        visit = self.storage.origin_visit_add(origin_id, ts)
        self.storage.snapshot_add(origin_id, visit['visit'], snapshot)

        returned_snapshot = snapshot_get_all_branches(self.storage,
                                                      snapshot['id'])
        self.assertEqual(snapshot, returned_snapshot)

    @given(origins().map(lambda x: x.to_dict()),
           datetimes(),
           branch_names(), branch_targets(only_objects=True))
    def test_snapshot_large(self, origin, ts, branch_name, branch_target):
        branch_target = branch_target.to_dict()
        origin_id = self.storage.origin_add_one(origin)
        visit = self.storage.origin_visit_add(origin_id, ts)

        snapshot = {
            'branches': {
                b'%s%05d' % (branch_name, i): branch_target
                for i in range(10000)
            }
        }
        snapshot['id'] = identifier_to_bytes(snapshot_identifier(snapshot))

        self.storage.snapshot_add(origin_id, visit['visit'], snapshot)

        returned_snapshot = snapshot_get_all_branches(self.storage,
                                                      snapshot['id'])
        self.assertEqual(snapshot, returned_snapshot)
