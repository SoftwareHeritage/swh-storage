# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from unittest.mock import patch

from swh.model.model import Origin, OriginVisit, OriginVisitStatus, Snapshot

from swh.storage.algos.origin import iter_origins, origin_get_latest_visit_status
from swh.storage.utils import now

from swh.storage.tests.test_storage import round_to_milliseconds
from swh.storage.tests.storage_data import data


def assert_list_eq(left, right, msg=None):
    assert list(left) == list(right), msg


@pytest.fixture
def swh_storage_backend_config():
    yield {"cls": "validate", "storage": {"cls": "memory",}}


def test_iter_origins(swh_storage):
    origins = [
        {"url": "bar"},
        {"url": "qux"},
        {"url": "quuz"},
    ]
    assert swh_storage.origin_add(origins) == {"origin:add": 3}
    assert_list_eq(iter_origins(swh_storage), origins)
    assert_list_eq(iter_origins(swh_storage, batch_size=1), origins)
    assert_list_eq(iter_origins(swh_storage, batch_size=2), origins)

    for i in range(1, 5):
        assert_list_eq(iter_origins(swh_storage, origin_from=i + 1), origins[i:], i)

        assert_list_eq(
            iter_origins(swh_storage, origin_from=i + 1, batch_size=1), origins[i:], i
        )

        assert_list_eq(
            iter_origins(swh_storage, origin_from=i + 1, batch_size=2), origins[i:], i
        )

        for j in range(i, 5):
            assert_list_eq(
                iter_origins(swh_storage, origin_from=i + 1, origin_to=j + 1),
                origins[i:j],
                (i, j),
            )

            assert_list_eq(
                iter_origins(
                    swh_storage, origin_from=i + 1, origin_to=j + 1, batch_size=1
                ),
                origins[i:j],
                (i, j),
            )

            assert_list_eq(
                iter_origins(
                    swh_storage, origin_from=i + 1, origin_to=j + 1, batch_size=2
                ),
                origins[i:j],
                (i, j),
            )


@patch("swh.storage.in_memory.InMemoryStorage.origin_get_range")
def test_iter_origins_batch_size(mock_origin_get_range, swh_storage):
    mock_origin_get_range.return_value = []

    list(iter_origins(swh_storage))
    mock_origin_get_range.assert_called_with(origin_from=1, origin_count=10000)

    list(iter_origins(swh_storage, batch_size=42))
    mock_origin_get_range.assert_called_with(origin_from=1, origin_count=42)


def test_origin_get_latest_visit_status_none(swh_storage):
    """Looking up unknown objects should return nothing

    """
    # unknown origin so no result
    assert origin_get_latest_visit_status(swh_storage, "unknown-origin") is None

    # unknown type so no result
    origin = Origin.from_dict(data.origin)
    swh_storage.origin_add_one(origin)
    swh_storage.origin_visit_add(
        [OriginVisit(origin=origin.url, date=data.date_visit1, type="git",),]
    )[0]
    actual_origin_visit = origin_get_latest_visit_status(
        swh_storage, origin.url, type="unknown"
    )
    assert actual_origin_visit is None

    actual_origin_visit = origin_get_latest_visit_status(
        swh_storage, origin.url, require_snapshot=True
    )
    assert actual_origin_visit is None

    actual_origin_visit = origin_get_latest_visit_status(
        swh_storage, origin.url, allowed_statuses=["unknown"]
    )
    assert actual_origin_visit is None


def init_storage_with_origin_visits(swh_storage):
    """Initialize storage with origin/origin-visit/origin-visit-status

    """
    origin1 = Origin.from_dict(data.origin)
    origin2 = Origin.from_dict(data.origin2)
    swh_storage.origin_add([origin1, origin2])

    ov1, ov2 = swh_storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin1.url, date=data.date_visit1, type=data.type_visit1,
            ),
            OriginVisit(
                origin=origin2.url, date=data.date_visit2, type=data.type_visit2,
            ),
        ]
    )

    snapshot = Snapshot.from_dict(data.complete_snapshot)
    swh_storage.snapshot_add([snapshot])

    date_now = now()
    date_now = round_to_milliseconds(date_now)
    assert data.date_visit1 < data.date_visit2
    assert data.date_visit2 < date_now

    # origin visit status 1 for origin visit 1
    ovs11 = OriginVisitStatus(
        origin=origin1.url,
        visit=ov1.visit,
        date=data.date_visit1,
        status="partial",
        snapshot=None,
    )
    # origin visit status 2 for origin visit 1
    ovs12 = OriginVisitStatus(
        origin=origin1.url,
        visit=ov1.visit,
        date=data.date_visit2,
        status="ongoing",
        snapshot=None,
    )
    # origin visit status 1 for origin visit 2
    ovs21 = OriginVisitStatus(
        origin=origin2.url,
        visit=ov2.visit,
        date=data.date_visit2,
        status="ongoing",
        snapshot=None,
    )
    # origin visit status 2 for origin visit 2
    ovs22 = OriginVisitStatus(
        origin=origin2.url,
        visit=ov2.visit,
        date=date_now,
        status="full",
        snapshot=snapshot.id,
        metadata={"something": "wicked"},
    )

    swh_storage.origin_visit_status_add([ovs11, ovs12, ovs21, ovs22])
    return {
        "origin": [origin1, origin2],
        "origin_visit": [ov1, ov2],
        "origin_visit_status": [ovs11, ovs12, ovs21, ovs22],
    }


def test_origin_get_latest_visit_status_filter_type(swh_storage):
    """Filtering origin visit per types should yield consistent results

    """
    objects = init_storage_with_origin_visits(swh_storage)
    origin1, origin2 = objects["origin"]
    ov1, ov2 = objects["origin_visit"]
    ovs11, ovs12, _, ovs22 = objects["origin_visit_status"]

    # no visit for origin1 url with type_visit2
    assert (
        origin_get_latest_visit_status(swh_storage, origin1.url, type=data.type_visit2)
        is None
    )

    # no visit for origin2 url with type_visit1
    assert (
        origin_get_latest_visit_status(swh_storage, origin2.url, type=data.type_visit1)
        is None
    )

    # Two visits, both with no snapshot, take the most recent
    actual_ov1, actual_ovs12 = origin_get_latest_visit_status(
        swh_storage, origin1.url, type=data.type_visit1
    )
    assert isinstance(actual_ov1, OriginVisit)
    assert isinstance(actual_ovs12, OriginVisitStatus)
    assert actual_ov1.origin == ov1.origin
    assert actual_ov1.visit == ov1.visit
    assert actual_ov1.type == data.type_visit1
    assert actual_ovs12 == ovs12

    # take the most recent visit with type_visit2
    actual_ov2, actual_ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, type=data.type_visit2
    )
    assert isinstance(actual_ov2, OriginVisit)
    assert isinstance(actual_ovs22, OriginVisitStatus)
    assert actual_ov2.origin == ov2.origin
    assert actual_ov2.visit == ov2.visit
    assert actual_ov2.type == data.type_visit2
    assert actual_ovs22 == ovs22


def test_origin_get_latest_visit_status_filter_status(swh_storage):
    objects = init_storage_with_origin_visits(swh_storage)
    origin1, origin2 = objects["origin"]
    ov1, ov2 = objects["origin_visit"]
    ovs11, ovs12, _, ovs22 = objects["origin_visit_status"]

    # no failed status for that visit
    assert (
        origin_get_latest_visit_status(
            swh_storage, origin2.url, allowed_statuses=["failed"]
        )
        is None
    )

    # only 1 partial for that visit
    actual_ov1, actual_ovs11 = origin_get_latest_visit_status(
        swh_storage, origin1.url, allowed_statuses=["partial"]
    )
    assert actual_ov1.origin == ov1.origin
    assert actual_ov1.visit == ov1.visit
    assert actual_ov1.type == data.type_visit1
    assert actual_ovs11 == ovs11

    # both status exist, take the latest one
    actual_ov1, actual_ovs12 = origin_get_latest_visit_status(
        swh_storage, origin1.url, allowed_statuses=["partial", "ongoing"]
    )
    assert actual_ov1.origin == ov1.origin
    assert actual_ov1.visit == ov1.visit
    assert actual_ov1.type == data.type_visit1
    assert actual_ovs12 == ovs12

    assert isinstance(actual_ov1, OriginVisit)
    assert isinstance(actual_ovs12, OriginVisitStatus)
    assert actual_ov1.origin == ov1.origin
    assert actual_ov1.visit == ov1.visit
    assert actual_ov1.type == data.type_visit1
    assert actual_ovs12 == ovs12

    # take the most recent visit with type_visit2
    actual_ov2, actual_ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, allowed_statuses=["full"]
    )
    assert actual_ov2.origin == ov2.origin
    assert actual_ov2.visit == ov2.visit
    assert actual_ov2.type == data.type_visit2
    assert actual_ovs22 == ovs22


def test_origin_get_latest_visit_status_filter_snapshot(swh_storage):
    objects = init_storage_with_origin_visits(swh_storage)
    origin1, origin2 = objects["origin"]
    _, ov2 = objects["origin_visit"]
    _, _, _, ovs22 = objects["origin_visit_status"]

    # there is no visit with snapshot yet for that visit
    assert (
        origin_get_latest_visit_status(swh_storage, origin1.url, require_snapshot=True)
        is None
    )

    # visit status with partial status visit elected
    actual_ov2, actual_ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, require_snapshot=True
    )
    assert actual_ov2.origin == ov2.origin
    assert actual_ov2.visit == ov2.visit
    assert actual_ov2.type == ov2.type
    assert actual_ovs22 == ovs22

    date_now = now()

    # Add another visit
    swh_storage.origin_visit_add(
        [OriginVisit(origin=origin2.url, date=date_now, type=data.type_visit2,),]
    )

    # Requiring the latest visit with a snapshot, we still find the previous visit
    ov2, ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, require_snapshot=True
    )
    assert actual_ov2.origin == ov2.origin
    assert actual_ov2.visit == ov2.visit
    assert actual_ov2.type == ov2.type
    assert actual_ovs22 == ovs22
