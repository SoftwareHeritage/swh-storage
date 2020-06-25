# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hypothesis import given
import pytest

from swh.model.hypothesis_strategies import snapshots, branch_names, branch_targets
from swh.model.identifiers import snapshot_identifier, identifier_to_bytes
from swh.model.model import Origin, OriginVisit, OriginVisitStatus, Snapshot

from swh.storage.algos.snapshot import snapshot_get_all_branches, snapshot_get_latest
from swh.storage.utils import now

from swh.storage.tests.storage_data import data


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": None,
    }


@given(snapshot=snapshots(min_size=0, max_size=10, only_objects=False))
def test_snapshot_small(swh_storage, snapshot):  # noqa
    snapshot = snapshot.to_dict()
    swh_storage.snapshot_add([snapshot])

    returned_snapshot = snapshot_get_all_branches(swh_storage, snapshot["id"])
    assert snapshot == returned_snapshot


@given(branch_name=branch_names(), branch_target=branch_targets(only_objects=True))
def test_snapshot_large(swh_storage, branch_name, branch_target):  # noqa
    branch_target = branch_target.to_dict()

    snapshot = {
        "branches": {b"%s%05d" % (branch_name, i): branch_target for i in range(10000)}
    }
    snapshot["id"] = identifier_to_bytes(snapshot_identifier(snapshot))

    swh_storage.snapshot_add([snapshot])

    returned_snapshot = snapshot_get_all_branches(swh_storage, snapshot["id"])
    assert snapshot == returned_snapshot


def test_snapshot_get_latest_none(swh_storage):
    """Retrieve latest snapshot on unknown origin or origin without snapshot should
    yield no result

    """
    # unknown origin so None
    assert snapshot_get_latest(swh_storage, "unknown-origin") is None

    # no snapshot on origin visit so None
    origin = Origin.from_dict(data.origin)
    swh_storage.origin_add_one(origin)
    swh_storage.origin_visit_add(
        [OriginVisit(origin=origin.url, date=data.date_visit1, type=data.type_visit1,)]
    )
    assert snapshot_get_latest(swh_storage, origin.url) is None

    ov1 = swh_storage.origin_visit_get_latest(origin.url)
    assert ov1 is not None
    visit_id = ov1["visit"]

    # visit references a snapshot but the snapshot does not exist in backend for some
    # reason
    complete_snapshot = Snapshot.from_dict(data.complete_snapshot)
    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=visit_id,
                date=data.date_visit2,
                status="partial",
                snapshot=complete_snapshot.id,
            )
        ]
    )
    # so we do not find it
    assert snapshot_get_latest(swh_storage, origin.url) is None
    assert snapshot_get_latest(swh_storage, origin.url, branches_count=1) is None


def test_snapshot_get_latest(swh_storage):
    origin = Origin.from_dict(data.origin)
    swh_storage.origin_add_one(origin)

    visit1 = OriginVisit(
        origin=origin.url, date=data.date_visit1, type=data.type_visit1,
    )
    swh_storage.origin_visit_add([visit1])
    ov1 = swh_storage.origin_visit_get_latest(origin.url)
    visit_id = ov1["visit"]

    # Add snapshot to visit1, latest snapshot = visit 1 snapshot
    complete_snapshot = Snapshot.from_dict(data.complete_snapshot)
    swh_storage.snapshot_add([complete_snapshot])

    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=visit_id,
                date=data.date_visit2,
                status="partial",
                snapshot=None,
            )
        ]
    )
    assert data.date_visit1 < data.date_visit2

    # no snapshot associated to the visit, so None
    actual_snapshot = snapshot_get_latest(
        swh_storage, origin.url, allowed_statuses=["partial"]
    )
    assert actual_snapshot is None

    date_now = now()
    assert data.date_visit2 < date_now
    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=visit_id,
                date=date_now,
                status="full",
                snapshot=complete_snapshot.id,
            )
        ]
    )

    swh_storage.origin_visit_add(
        [OriginVisit(origin=origin.url, date=now(), type=data.type_visit1,)]
    )

    actual_snapshot = snapshot_get_latest(swh_storage, origin.url)
    assert actual_snapshot is not None
    assert actual_snapshot == complete_snapshot

    actual_snapshot = snapshot_get_latest(swh_storage, origin.url, branches_count=1)
    assert actual_snapshot is not None
    assert actual_snapshot.id == complete_snapshot.id
    assert len(actual_snapshot.branches.values()) == 1

    with pytest.raises(ValueError, match="branches_count must be a positive integer"):
        snapshot_get_latest(swh_storage, origin.url, branches_count="something-wrong")
