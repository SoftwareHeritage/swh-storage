# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hypothesis import given
import pytest

from swh.model.hypothesis_strategies import branch_names, branch_targets, snapshots
from swh.model.model import (
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.algos.snapshot import (
    snapshot_get_all_branches,
    snapshot_get_latest,
    snapshot_id_get_from_revision,
    snapshot_resolve_alias,
    visits_and_snapshots_get_from_revision,
)
from swh.storage.utils import now


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": None,
    }


@given(snapshot=snapshots(min_size=0, max_size=10, only_objects=False))
def test_snapshot_small(swh_storage, snapshot):  # noqa
    swh_storage.snapshot_add([snapshot])

    returned_snapshot = snapshot_get_all_branches(swh_storage, snapshot.id)
    assert snapshot == returned_snapshot


@given(branch_name=branch_names(), branch_target=branch_targets(only_objects=True))
def test_snapshot_large(swh_storage, branch_name, branch_target):  # noqa
    snapshot = Snapshot(
        branches={b"%s%05d" % (branch_name, i): branch_target for i in range(10000)},
    )

    swh_storage.snapshot_add([snapshot])

    returned_snapshot = snapshot_get_all_branches(swh_storage, snapshot.id)
    assert snapshot == returned_snapshot


def test_snapshot_get_latest_none(swh_storage, sample_data):
    """Retrieve latest snapshot on unknown origin or origin without snapshot should
    yield no result

    """
    # unknown origin so None
    assert snapshot_get_latest(swh_storage, "unknown-origin") is None

    # no snapshot on origin visit so None
    origin = sample_data.origin
    swh_storage.origin_add([origin])
    origin_visit, origin_visit2 = sample_data.origin_visits[:2]
    assert origin_visit.origin == origin.url

    swh_storage.origin_visit_add([origin_visit])
    assert snapshot_get_latest(swh_storage, origin.url) is None

    ov1 = swh_storage.origin_visit_get_latest(origin.url)
    assert ov1 is not None

    # visit references a snapshot but the snapshot does not exist in backend for some
    # reason
    complete_snapshot = sample_data.snapshots[2]
    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=ov1.visit,
                date=origin_visit2.date,
                status="partial",
                snapshot=complete_snapshot.id,
            )
        ]
    )
    # so we do not find it
    assert snapshot_get_latest(swh_storage, origin.url) is None
    assert snapshot_get_latest(swh_storage, origin.url, branches_count=1) is None


def test_snapshot_get_latest(swh_storage, sample_data):
    origin = sample_data.origin
    swh_storage.origin_add([origin])

    visit1, visit2 = sample_data.origin_visits[:2]
    assert visit1.origin == origin.url

    swh_storage.origin_visit_add([visit1])
    ov1 = swh_storage.origin_visit_get_latest(origin.url)

    # Add snapshot to visit1, latest snapshot = visit 1 snapshot
    complete_snapshot = sample_data.snapshots[2]
    swh_storage.snapshot_add([complete_snapshot])

    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=ov1.visit,
                date=visit2.date,
                status="partial",
                snapshot=None,
            )
        ]
    )
    assert visit1.date < visit2.date

    # no snapshot associated to the visit, so None
    actual_snapshot = snapshot_get_latest(
        swh_storage, origin.url, allowed_statuses=["partial"]
    )
    assert actual_snapshot is None

    date_now = now()
    assert visit2.date < date_now
    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=ov1.origin,
                visit=ov1.visit,
                date=date_now,
                type=ov1.type,
                status="full",
                snapshot=complete_snapshot.id,
            )
        ]
    )

    swh_storage.origin_visit_add(
        [OriginVisit(origin=origin.url, date=now(), type=visit1.type,)]
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


def test_snapshot_id_get_from_revision(swh_storage, sample_data):
    origin = sample_data.origin
    swh_storage.origin_add([origin])

    date_visit2 = now()
    visit1, visit2 = sample_data.origin_visits[:2]
    assert visit1.origin == origin.url

    ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])

    revision1, revision2, revision3 = sample_data.revisions[:3]
    swh_storage.revision_add([revision1, revision2])

    empty_snapshot, complete_snapshot = sample_data.snapshots[1:3]
    swh_storage.snapshot_add([complete_snapshot])

    # Add complete_snapshot to visit1 which targets revision1
    ovs1, ovs2 = [
        OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit2,
            type=ov1.type,
            status="partial",
            snapshot=complete_snapshot.id,
        ),
        OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=now(),
            type=ov2.type,
            status="full",
            snapshot=empty_snapshot.id,
        ),
    ]

    swh_storage.origin_visit_status_add([ovs1, ovs2])
    assert ov1.date < ov2.date
    assert ov2.date < ovs1.date
    assert ovs1.date < ovs2.date

    # revision3 does not exist so result is None
    actual_snapshot_id = snapshot_id_get_from_revision(
        swh_storage, origin.url, revision3.id
    )
    assert actual_snapshot_id is None

    # no snapshot targets revision2 for origin.url so result is None
    actual_snapshot_id = snapshot_id_get_from_revision(
        swh_storage, origin.url, revision2.id
    )
    assert actual_snapshot_id is None

    # complete_snapshot targets at least revision1
    actual_snapshot_id = snapshot_id_get_from_revision(
        swh_storage, origin.url, revision1.id
    )
    assert actual_snapshot_id == complete_snapshot.id


def test_visit_and_snapshot_get_from_revision(swh_storage, sample_data):
    origin = sample_data.origin
    swh_storage.origin_add([origin])

    date_visit2 = now()
    visit1, visit2 = sample_data.origin_visits[:2]
    assert visit1.origin == origin.url

    ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])

    revision1, revision2, revision3 = sample_data.revisions[:3]
    swh_storage.revision_add([revision1, revision2])

    empty_snapshot, complete_snapshot = sample_data.snapshots[1:3]
    swh_storage.snapshot_add([complete_snapshot])

    # Add complete_snapshot to visit1 which targets revision1
    ovs1, ovs2 = [
        OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit2,
            type=ov1.type,
            status="partial",
            snapshot=complete_snapshot.id,
        ),
        OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=now(),
            type=ov2.type,
            status="full",
            snapshot=empty_snapshot.id,
        ),
    ]

    swh_storage.origin_visit_status_add([ovs1, ovs2])
    assert ov1.date < ov2.date
    assert ov2.date < ovs1.date
    assert ovs1.date < ovs2.date

    # revision3 does not exist so result is None
    actual_snapshot_id = snapshot_id_get_from_revision(
        swh_storage, origin.url, revision3.id
    )
    assert actual_snapshot_id is None

    # no snapshot targets revision2 for origin.url so result is None
    res = list(
        visits_and_snapshots_get_from_revision(swh_storage, origin.url, revision2.id)
    )
    assert res == []

    # complete_snapshot targets at least revision1
    res = list(
        visits_and_snapshots_get_from_revision(swh_storage, origin.url, revision1.id)
    )
    assert res == [(ov1, ovs1, complete_snapshot)]


def test_snapshot_resolve_aliases_unknown_snapshot(swh_storage):
    assert snapshot_resolve_alias(swh_storage, b"foo", b"HEAD") is None


def test_snapshot_resolve_aliases_no_aliases(swh_storage):
    snapshot = Snapshot(branches={})
    swh_storage.snapshot_add([snapshot])

    assert snapshot_resolve_alias(swh_storage, snapshot.id, b"HEAD") is None


def test_snapshot_resolve_alias(swh_storage, sample_data):
    rev_branch_name = b"revision_branch"
    rel_branch_name = b"release_branch"
    rev_alias1_name = b"rev_alias1"
    rev_alias2_name = b"rev_alias2"
    rev_alias3_name = b"rev_alias3"
    rel_alias_name = b"rel_alias"
    rev_branch_info = SnapshotBranch(
        target=sample_data.revisions[0].id, target_type=TargetType.REVISION,
    )
    rel_branch_info = SnapshotBranch(
        target=sample_data.releases[0].id, target_type=TargetType.RELEASE,
    )
    rev_alias1_branch_info = SnapshotBranch(
        target=rev_branch_name, target_type=TargetType.ALIAS
    )
    rev_alias2_branch_info = SnapshotBranch(
        target=rev_alias1_name, target_type=TargetType.ALIAS
    )

    rev_alias3_branch_info = SnapshotBranch(
        target=rev_alias2_name, target_type=TargetType.ALIAS
    )
    rel_alias_branch_info = SnapshotBranch(
        target=rel_branch_name, target_type=TargetType.ALIAS
    )

    snapshot = Snapshot(
        branches={
            rev_branch_name: rev_branch_info,
            rel_branch_name: rel_branch_info,
            rev_alias1_name: rev_alias1_branch_info,
            rev_alias2_name: rev_alias2_branch_info,
            rev_alias3_name: rev_alias3_branch_info,
            rel_alias_name: rel_alias_branch_info,
        }
    )
    swh_storage.snapshot_add([snapshot])

    for alias_name, expected_branch in (
        (rev_alias1_name, rev_branch_info),
        (rev_alias2_name, rev_branch_info,),
        (rev_alias3_name, rev_branch_info,),
        (rel_alias_name, rel_branch_info),
    ):
        assert (
            snapshot_resolve_alias(swh_storage, snapshot.id, alias_name)
            == expected_branch
        )


def test_snapshot_resolve_alias_dangling_branch(swh_storage):
    dangling_branch_name = b"dangling_branch"
    alias_name = b"rev_alias"

    alias_branch = SnapshotBranch(
        target=dangling_branch_name, target_type=TargetType.ALIAS
    )

    snapshot = Snapshot(
        branches={dangling_branch_name: None, alias_name: alias_branch,}
    )
    swh_storage.snapshot_add([snapshot])

    assert snapshot_resolve_alias(swh_storage, snapshot.id, alias_name) is None


def test_snapshot_resolve_alias_missing_branch(swh_storage):
    missing_branch_name = b"missing_branch"
    alias_name = b"rev_alias"

    alias_branch = SnapshotBranch(
        target=missing_branch_name, target_type=TargetType.ALIAS
    )

    snapshot = Snapshot(id=b"42" * 10, branches={alias_name: alias_branch,})
    swh_storage.snapshot_add([snapshot])

    assert snapshot_resolve_alias(swh_storage, snapshot.id, alias_name) is None


def test_snapshot_resolve_alias_cycle_found(swh_storage):
    alias1_name = b"alias_1"
    alias2_name = b"alias_2"
    alias3_name = b"alias_3"
    alias4_name = b"alias_4"

    alias1_branch_info = SnapshotBranch(
        target=alias2_name, target_type=TargetType.ALIAS
    )
    alias2_branch_info = SnapshotBranch(
        target=alias3_name, target_type=TargetType.ALIAS
    )
    alias3_branch_info = SnapshotBranch(
        target=alias4_name, target_type=TargetType.ALIAS
    )
    alias4_branch_info = SnapshotBranch(
        target=alias2_name, target_type=TargetType.ALIAS
    )

    snapshot = Snapshot(
        branches={
            alias1_name: alias1_branch_info,
            alias2_name: alias2_branch_info,
            alias3_name: alias3_branch_info,
            alias4_name: alias4_branch_info,
        }
    )
    swh_storage.snapshot_add([snapshot])

    assert snapshot_resolve_alias(swh_storage, snapshot.id, alias1_name) is None
