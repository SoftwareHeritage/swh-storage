# Copyright (C) 2019-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from hypothesis import HealthCheck, given, settings, strategies
import pytest

from swh.model import hypothesis_strategies

from .storage_tests import disabled_health_checks


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "noop",
        "objstorage": {
            "cls": "memory",
        },
    }


@settings(suppress_health_check=[HealthCheck.data_too_large] + disabled_health_checks)
@given(strategies.lists(hypothesis_strategies.objects(split_content=True), max_size=5))
def test_add_arbitrary(swh_storage, subtests, objects):
    objects = list({obj.id: obj for obj in objects if hasattr(obj, "id")}.values()) + [
        obj for obj in objects if not hasattr(obj, "id")
    ]
    random.shuffle(objects)

    for obj_type, obj in objects:
        method = getattr(swh_storage, f"{obj_type}_add")
        method([obj])

    for table in (
        "_contents",
        "_skipped_contents",
        "_directories",
        "_directory_entries",
        "_revisions",
        "_revision_parents",
        "_releases",
        "_snapshots",
        "_snapshot_branches",
        "_origins",
        "_origin_visits",
        "_origin_visit_statuses",
        "_metadata_authorities",
        "_metadata_fetchers",
        "_raw_extrinsic_metadata",
        "_raw_extrinsic_metadata_by_id",
        "_extid",
        "_object_references_tables",
    ):
        with subtests.test(msg=table):
            assert getattr(swh_storage._cql_runner, table).data == {}

    with subtests.test(msg="_content_indexes"):
        for index in swh_storage._cql_runner._content_indexes.values():
            assert index == {}

    with subtests.test(msg="_skipped_content_indexes"):
        for index in swh_storage._cql_runner._skipped_content_indexes.values():
            assert index == {}

    with subtests.test(msg="object_references"):
        for table in swh_storage._cql_runner._object_references.values():
            assert table.data == {}
