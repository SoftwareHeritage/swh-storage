# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model.model import Origin, OriginVisit, OriginVisitStatus
from swh.storage.algos.origin import (
    iter_origin_visit_statuses,
    iter_origin_visits,
    iter_origins,
    origin_get_latest_visit_status,
)
from swh.storage.interface import ListOrder
from swh.storage.tests.storage_tests import round_to_milliseconds
from swh.storage.utils import now


def test_iter_origins(swh_storage):
    origins = [
        Origin(url="bar"),
        Origin(url="qux"),
        Origin(url="quuz"),
    ]
    assert swh_storage.origin_add(origins) == {"origin:add": 3}

    # this returns all the origins, only the number of paged called is different
    assert list(iter_origins(swh_storage)) == origins
    assert list(iter_origins(swh_storage, limit=1)) == origins
    assert list(iter_origins(swh_storage, limit=2)) == origins


def test_origin_get_latest_visit_status_none(swh_storage, sample_data):
    """Looking up unknown objects should return nothing"""
    # unknown origin so no result
    assert origin_get_latest_visit_status(swh_storage, "unknown-origin") is None

    # unknown type so no result
    origin = sample_data.origin
    origin_visit = sample_data.origin_visit
    assert origin_visit.origin == origin.url

    swh_storage.origin_add([origin])
    swh_storage.origin_visit_add([origin_visit])[0]
    assert origin_visit.type != "unknown"
    actual_origin_visit = origin_get_latest_visit_status(
        swh_storage, origin.url, type="unknown"
    )
    assert actual_origin_visit is None

    actual_origin_visit = origin_get_latest_visit_status(
        swh_storage, origin.url, require_snapshot=True
    )
    assert actual_origin_visit is None


def init_storage_with_origin_visits(swh_storage, sample_data):
    """Initialize storage with origin/origin-visit/origin-visit-status"""
    snapshot = sample_data.snapshots[2]
    origin1, origin2 = sample_data.origins[:2]
    swh_storage.origin_add([origin1, origin2])

    ov1, ov2 = swh_storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin1.url,
                date=sample_data.date_visit1,
                type=sample_data.type_visit1,
            ),
            OriginVisit(
                origin=origin2.url,
                date=sample_data.date_visit2,
                type=sample_data.type_visit2,
            ),
        ]
    )

    swh_storage.snapshot_add([snapshot])

    date_now = now()
    date_now = round_to_milliseconds(date_now)
    assert sample_data.date_visit1 < sample_data.date_visit2
    assert sample_data.date_visit2 < date_now

    # origin visit status 1 for origin visit 1
    ovs11 = OriginVisitStatus(
        origin=ov1.origin,
        visit=ov1.visit,
        date=ov1.date + datetime.timedelta(seconds=10),  # so it's not ignored
        type=ov1.type,
        status="partial",
        snapshot=None,
    )
    # origin visit status 2 for origin visit 1
    ovs12 = OriginVisitStatus(
        origin=ov1.origin,
        visit=ov1.visit,
        date=sample_data.date_visit2,
        type=ov1.type,
        status="ongoing",
        snapshot=None,
    )
    # origin visit status 1 for origin visit 2
    ovs21 = OriginVisitStatus(
        origin=ov2.origin,
        visit=ov2.visit,
        date=ov2.date + datetime.timedelta(seconds=10),  # so it's not ignored
        type=ov2.type,
        status="ongoing",
        snapshot=None,
    )
    # origin visit status 2 for origin visit 2
    ovs22 = OriginVisitStatus(
        origin=ov2.origin,
        visit=ov2.visit,
        date=date_now,
        type=ov2.type,
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


def test_origin_get_latest_visit_status_filter_type(swh_storage, sample_data):
    """Filtering origin visit per types should yield consistent results"""
    objects = init_storage_with_origin_visits(swh_storage, sample_data)
    origin1, origin2 = objects["origin"]
    ov1, ov2 = objects["origin_visit"]
    ovs11, ovs12, _, ovs22 = objects["origin_visit_status"]

    # no visit for origin1 url with type_visit2
    assert (
        origin_get_latest_visit_status(
            swh_storage, origin1.url, type=sample_data.type_visit2
        )
        is None
    )

    # no visit for origin2 url with type_visit1
    assert (
        origin_get_latest_visit_status(
            swh_storage, origin2.url, type=sample_data.type_visit1
        )
        is None
    )

    # Two visits, both with no snapshot, take the most recent
    actual_ovs12 = origin_get_latest_visit_status(
        swh_storage, origin1.url, type=sample_data.type_visit1
    )
    assert isinstance(actual_ovs12, OriginVisitStatus)
    assert actual_ovs12 == ovs12
    assert actual_ovs12.origin == ov1.origin
    assert actual_ovs12.visit == ov1.visit
    assert actual_ovs12.type == sample_data.type_visit1

    # take the most recent visit with type_visit2
    actual_ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, type=sample_data.type_visit2
    )
    assert isinstance(actual_ovs22, OriginVisitStatus)
    assert actual_ovs22 == ovs22
    assert actual_ovs22.origin == ov2.origin
    assert actual_ovs22.visit == ov2.visit
    assert actual_ovs22.type == sample_data.type_visit2


def test_origin_get_latest_visit_status_filter_status(swh_storage, sample_data):
    objects = init_storage_with_origin_visits(swh_storage, sample_data)
    origin1, origin2 = objects["origin"]
    ov1, ov2 = objects["origin_visit"]
    ovs11, ovs12, _, ovs22 = objects["origin_visit_status"]

    # no partial status for that origin visit
    assert (
        origin_get_latest_visit_status(
            swh_storage, origin2.url, allowed_statuses=["partial"]
        )
        is None
    )

    # only 1 partial for that visit
    actual_ovs11 = origin_get_latest_visit_status(
        swh_storage, origin1.url, allowed_statuses=["partial"]
    )
    assert actual_ovs11 == ovs11
    assert actual_ovs11.origin == ov1.origin
    assert actual_ovs11.visit == ov1.visit
    assert actual_ovs11.type == sample_data.type_visit1

    # both status exist, take the latest one
    actual_ovs12 = origin_get_latest_visit_status(
        swh_storage, origin1.url, allowed_statuses=["partial", "ongoing"]
    )
    assert actual_ovs12 == ovs12
    assert actual_ovs12.origin == ov1.origin
    assert actual_ovs12.visit == ov1.visit
    assert actual_ovs12.type == sample_data.type_visit1

    assert isinstance(actual_ovs12, OriginVisitStatus)
    assert actual_ovs12 == ovs12
    assert actual_ovs12.origin == ov1.origin
    assert actual_ovs12.visit == ov1.visit
    assert actual_ovs12.type == sample_data.type_visit1

    # take the most recent visit with type_visit2
    actual_ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, allowed_statuses=["full"]
    )
    assert actual_ovs22 == ovs22
    assert actual_ovs22.origin == ov2.origin
    assert actual_ovs22.visit == ov2.visit
    assert actual_ovs22.type == sample_data.type_visit2


def test_origin_get_latest_visit_status_filter_snapshot(swh_storage, sample_data):
    objects = init_storage_with_origin_visits(swh_storage, sample_data)
    origin1, origin2 = objects["origin"]
    _, ov2 = objects["origin_visit"]
    _, _, _, ovs22 = objects["origin_visit_status"]

    # there is no visit with snapshot yet for that visit
    assert (
        origin_get_latest_visit_status(swh_storage, origin1.url, require_snapshot=True)
        is None
    )

    # visit status with partial status visit elected
    actual_ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, require_snapshot=True
    )
    assert actual_ovs22 == ovs22
    assert actual_ovs22.origin == ov2.origin
    assert actual_ovs22.visit == ov2.visit
    assert actual_ovs22.type == ov2.type

    date_now = now()

    # Add another visit
    swh_storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin2.url,
                date=date_now,
                type=sample_data.type_visit2,
            ),
        ]
    )

    # Requiring the latest visit with a snapshot, we still find the previous visit
    ovs22 = origin_get_latest_visit_status(
        swh_storage, origin2.url, require_snapshot=True
    )
    assert actual_ovs22 == ovs22
    assert actual_ovs22.origin == ov2.origin
    assert actual_ovs22.visit == ov2.visit
    assert actual_ovs22.type == ov2.type


def test_iter_origin_visits(swh_storage, sample_data):
    """Iter over origin visits for an origin returns all visits"""
    origin1, origin2 = sample_data.origins[:2]
    swh_storage.origin_add([origin1, origin2])

    date_past = now() - datetime.timedelta(weeks=20)

    new_visits = []
    for visit_id in range(20):
        new_visits.append(
            OriginVisit(
                origin=origin1.url,
                date=date_past + datetime.timedelta(days=visit_id),
                type="git",
            )
        )

    visits = swh_storage.origin_visit_add(new_visits)
    reversed_visits = list(reversed(visits))

    # no limit, order asc
    actual_visits = list(iter_origin_visits(swh_storage, origin1.url))
    assert actual_visits == visits

    # no limit, order desc
    actual_visits = list(
        iter_origin_visits(swh_storage, origin1.url, order=ListOrder.DESC)
    )
    assert actual_visits == reversed_visits

    # no result
    actual_visits = list(iter_origin_visits(swh_storage, origin2.url))
    assert actual_visits == []


def test_iter_origin_visit_status(swh_storage, sample_data):
    origin1, origin2 = sample_data.origins[:2]
    swh_storage.origin_add([origin1])

    ov1 = swh_storage.origin_visit_add([sample_data.origin_visit])[0]
    assert ov1.origin == origin1.url

    date_past = now() - datetime.timedelta(weeks=20)

    ovs1 = OriginVisitStatus(
        origin=ov1.origin,
        visit=ov1.visit,
        date=ov1.date,
        type=ov1.type,
        status="created",
        snapshot=None,
    )
    new_visit_statuses = [ovs1]
    for i in range(20):
        status_date = date_past + datetime.timedelta(days=i)

        new_visit_statuses.append(
            OriginVisitStatus(
                origin=ov1.origin,
                visit=ov1.visit,
                date=status_date,
                type=ov1.type,
                status="created",
                snapshot=None,
            )
        )

    swh_storage.origin_visit_status_add(new_visit_statuses)
    reversed_visit_statuses = list(reversed(new_visit_statuses))

    # order asc
    actual_visit_statuses = list(
        iter_origin_visit_statuses(swh_storage, ov1.origin, ov1.visit)
    )
    assert actual_visit_statuses == new_visit_statuses

    # order desc
    actual_visit_statuses = list(
        iter_origin_visit_statuses(
            swh_storage, ov1.origin, ov1.visit, order=ListOrder.DESC
        )
    )
    assert actual_visit_statuses == reversed_visit_statuses

    # no result
    actual_visit_statuses = list(
        iter_origin_visit_statuses(swh_storage, origin2.url, ov1.visit)
    )
    assert actual_visit_statuses == []
