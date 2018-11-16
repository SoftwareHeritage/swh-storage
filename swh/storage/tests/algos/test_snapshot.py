# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

import pytest

from hypothesis import given
from hypothesis.strategies import (binary, composite, datetimes, dictionaries,
                                   from_regex, none, one_of, sampled_from)

from swh.model.identifiers import snapshot_identifier, identifier_to_bytes
from swh.storage.tests.storage_testing import StorageTestFixture

from swh.storage.algos.snapshot import snapshot_get_all_branches


def branch_names():
    return binary(min_size=5, max_size=10)


@composite
def branch_targets_object(draw):
    return {
        'target': draw(binary(min_size=20, max_size=20)),
        'target_type': draw(
            sampled_from([
                'content', 'directory', 'revision', 'release', 'snapshot',
            ])
        ),
    }


@composite
def branch_targets_alias(draw):
    return {
        'target': draw(branch_names()),
        'target_type': 'alias',
    }


def branch_targets(*, only_objects=False):
    if only_objects:
        return branch_targets_object()
    else:
        return one_of(none(), branch_targets_alias(), branch_targets_object())


@composite
def snapshots(draw, *, min_size=0, max_size=100, only_objects=False):
    branches = draw(dictionaries(
        keys=branch_names(),
        values=branch_targets(only_objects=only_objects),
        min_size=min_size,
        max_size=max_size,
    ))

    if not only_objects:
        # Make sure aliases point to actual branches
        unresolved_aliases = {
            target['target']
            for target in branches.values()
            if (target
                and target['target_type'] == 'alias'
                and target['target'] not in branches)
         }

        for alias in unresolved_aliases:
            branches[alias] = draw(branch_targets(only_objects=True))

    ret = {
        'branches': branches,
    }
    ret['id'] = identifier_to_bytes(snapshot_identifier(ret))
    return ret


@composite
def urls(draw):
    protocol = draw(sampled_from(['git', 'http', 'https', 'deb']))
    domain = draw(from_regex(r'\A([a-z]([a-z0-9-]*)\.){1,3}[a-z0-9]+\Z'))

    return '%s://%s' % (protocol, domain)


@composite
def origins(draw):
    return {
        'type': draw(sampled_from(['git', 'hg', 'svn', 'pypi', 'deb'])),
        'url': draw(urls()),
    }


@pytest.mark.db
@pytest.mark.property_based
class TestSnapshotAllBranches(StorageTestFixture, unittest.TestCase):
    @given(origins(), datetimes(), snapshots(min_size=0, max_size=10,
                                             only_objects=False))
    def test_snapshot_small(self, origin, ts, snapshot):
        origin_id = self.storage.origin_add_one(origin)
        visit = self.storage.origin_visit_add(origin_id, ts)
        self.storage.snapshot_add(origin_id, visit['visit'], snapshot)

        returned_snapshot = snapshot_get_all_branches(self.storage,
                                                      snapshot['id'])
        self.assertEqual(snapshot, returned_snapshot)

    @given(origins(), datetimes(),
           branch_names(), branch_targets(only_objects=True))
    def test_snapshot_large(self, origin, ts, branch_name, branch_target):
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
