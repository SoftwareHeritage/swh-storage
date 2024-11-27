# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests the 'table_options' configuration item from the config is applied
when creating tables"""

import time

import pytest

from swh.model.swhids import ExtendedSWHID
from swh.storage.interface import ObjectReference


@pytest.fixture
def swh_storage_backend_config(swh_storage_cassandra_backend_config):
    return {
        **swh_storage_cassandra_backend_config,
        "table_options": {
            "release": "default_time_to_live = 2",
            "object_references_*": "default_time_to_live = 2",
        },
    }


@pytest.mark.cassandra
class TestCassandraStorageTtl:
    def test_release_ttl(self, swh_storage, sample_data):
        release, release2 = sample_data.releases[:2]

        init_missing = swh_storage.release_missing([release.id, release2.id])
        assert list(init_missing) == [release.id, release2.id]

        swh_storage.release_add([release])

        time.sleep(3)

        swh_storage.release_add([release2])

        end_missing = swh_storage.release_missing([release.id, release2.id])
        assert list(end_missing) == []

        time.sleep(3)

        end_missing = swh_storage.release_missing([release.id])
        assert list(end_missing) == []

    def test_object_references_ttl(self, swh_storage):
        source = ExtendedSWHID.from_string(f"swh:1:dir:{0:040x}")
        target = ExtendedSWHID.from_string(f"swh:1:cnt:{1:040x}")

        assert swh_storage.object_references_add(
            [
                ObjectReference(source=source, target=target),
            ]
        ) == {"object_reference:add": 1}

        refs = swh_storage.object_find_recent_references(target_swhid=target, limit=10)
        assert refs == [source]

        time.sleep(5)

        refs = swh_storage.object_find_recent_references(target_swhid=target, limit=10)
        assert refs == []
