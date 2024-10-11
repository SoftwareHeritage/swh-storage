# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Iterable, cast

import attr
import pytest

from swh.core.api.classes import stream_results
from swh.model.model import (
    Directory,
    DirectoryEntry,
    ExtID,
    OriginVisit,
    OriginVisitStatus,
    Release,
    ReleaseTargetType,
    Revision,
    RevisionType,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID, ObjectType
from swh.storage.exc import MaskedObjectException
from swh.storage.interface import HashDict
from swh.storage.proxies.masking import MaskingProxyStorage
from swh.storage.proxies.masking.db import MaskedState
from swh.storage.tests.storage_data import StorageData


def now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@pytest.fixture
def set_object_visibility(masking_admin):
    # Create a request
    request = masking_admin.create_request(slug="foo", reason="bar")

    def set_visibility(objects: Iterable[ExtendedSWHID], new_state: MaskedState):
        masking_admin.set_object_state(
            request_id=request.id,
            new_state=new_state,
            swhids=list(objects),
        )

    return set_visibility


def assert_masked_objects_raise(func, objects, set_object_visibility):
    """Check that the called function raises a MaskedObjectException for the
    list of objects, only if they're masked.

    To do so, we call the function three times:
      - once to record the expected return value for the function
      - once to check the exception is properly raised after masking the object
      - one last time check that the original behavior is restored when the
        object is unmasked.

    We expect that func's return value is deterministic, and non-falsey.
    """
    set_object_visibility(list(objects), MaskedState.VISIBLE)

    expected_result = func()

    assert expected_result

    set_object_visibility(list(objects), MaskedState.DECISION_PENDING)

    with pytest.raises(MaskedObjectException) as exc_info:
        func()
    assert exc_info.value.masked.keys() == set(objects)

    set_object_visibility(list(objects), MaskedState.VISIBLE)
    assert func() == expected_result

    return expected_result


def test_content_get_data(swh_storage, set_object_visibility):
    # Ensure that object masking doesn't prevent insertion
    set_object_visibility(
        [StorageData.content.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.content_add([StorageData.content, StorageData.content2])

    assert (
        swh_storage.content_get_data(StorageData.content2.hashes())
        == StorageData.content2.data
    )

    assert_masked_objects_raise(
        lambda: swh_storage.content_get_data(StorageData.content.hashes()),
        [StorageData.content.swhid().to_extended()],
        set_object_visibility,
    )

    with pytest.deprecated_call():
        assert_masked_objects_raise(
            lambda: swh_storage.content_get_data(StorageData.content.sha1),
            [StorageData.content.swhid().to_extended()],
            set_object_visibility,
        )


def test_content_get(swh_storage, set_object_visibility):
    # Ensure that object masking doesn't prevent insertion
    set_object_visibility(
        [StorageData.content.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.content_add([StorageData.content, StorageData.content2])

    assert swh_storage.content_get([StorageData.content2.sha256], algo="sha256") == [
        attr.evolve(StorageData.content2, data=None)
    ]

    assert_masked_objects_raise(
        lambda: swh_storage.content_get(
            [StorageData.content.sha256, StorageData.content2.sha256], algo="sha256"
        ),
        [StorageData.content.swhid().to_extended()],
        set_object_visibility,
    )


def test_content_find(swh_storage, set_object_visibility):
    # Ensure that object masking doesn't prevent insertion
    set_object_visibility(
        [StorageData.content.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.content_add([StorageData.content, StorageData.content3])

    assert swh_storage.content_find(StorageData.content3.hashes()) == [
        attr.evolve(StorageData.content3, data=None)
    ]

    assert_masked_objects_raise(
        lambda: swh_storage.content_find(StorageData.content.hashes()),
        [StorageData.content.swhid().to_extended()],
        set_object_visibility,
    )


def test_directory_ls(swh_storage, set_object_visibility):
    set_object_visibility(
        [StorageData.directory.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.directory_add([StorageData.directory, StorageData.directory2])

    dir2_ls = list(swh_storage.directory_ls(StorageData.directory2.id, recursive=False))
    assert dir2_ls

    assert_masked_objects_raise(
        lambda: list(swh_storage.directory_ls(StorageData.directory.id)),
        [StorageData.directory.swhid().to_extended()],
        set_object_visibility,
    )

    # Check that a masked entry doesn't affect the visibility of the directory itself

    assert StorageData.directory2.entries[0].type == "file"
    set_object_visibility(
        [
            ExtendedSWHID(
                object_id=StorageData.directory2.entries[0].target,
                object_type=ExtendedObjectType.CONTENT,
            )
        ],
        MaskedState.DECISION_PENDING,
    )

    assert (
        list(swh_storage.directory_ls(StorageData.directory2.id, recursive=False))
        == dir2_ls
    )


def test_directory_get_entries(swh_storage, set_object_visibility):
    set_object_visibility(
        [StorageData.directory.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.directory_add([StorageData.directory, StorageData.directory2])

    dir2_entries = list(
        stream_results(swh_storage.directory_get_entries, StorageData.directory2.id)
    )
    assert dir2_entries

    assert_masked_objects_raise(
        lambda: list(
            stream_results(swh_storage.directory_get_entries, StorageData.directory.id)
        ),
        [StorageData.directory.swhid().to_extended()],
        set_object_visibility,
    )

    # Check that a masked entry doesn't affect the visibility of the directory itself

    assert StorageData.directory2.entries[0].type == "file"
    set_object_visibility(
        [
            ExtendedSWHID(
                object_id=StorageData.directory2.entries[0].target,
                object_type=ExtendedObjectType.CONTENT,
            )
        ],
        MaskedState.DECISION_PENDING,
    )

    assert (
        list(
            stream_results(
                swh_storage.directory_get_entries,
                StorageData.directory2.id,
            )
        )
        == dir2_entries
    )


def test_directory_entry_get_by_path(swh_storage, set_object_visibility):
    set_object_visibility(
        [StorageData.directory.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.directory_add([StorageData.directory, StorageData.directory2])

    dir2_entries = swh_storage.directory_entry_get_by_path(
        StorageData.directory2.id,
        [StorageData.directory2.entries[0].name],
    )
    assert dir2_entries

    assert_masked_objects_raise(
        lambda: swh_storage.directory_entry_get_by_path(
            StorageData.directory.id,
            [StorageData.directory.entries[0].name],
        ),
        [StorageData.directory.swhid().to_extended()],
        set_object_visibility,
    )

    # Check that a masked entry doesn't affect the visibility of the directory itself

    assert StorageData.directory2.entries[0].type == "file"
    set_object_visibility(
        [
            ExtendedSWHID(
                object_id=StorageData.directory2.entries[0].target,
                object_type=ExtendedObjectType.CONTENT,
            )
        ],
        MaskedState.DECISION_PENDING,
    )

    assert (
        swh_storage.directory_entry_get_by_path(
            StorageData.directory2.id,
            [StorageData.directory2.entries[0].name],
        )
        == dir2_entries
    )


def test_directory_get_raw_manifest(swh_storage, set_object_visibility):
    directory_with_raw_manifest = StorageData.directory6
    assert directory_with_raw_manifest.raw_manifest

    set_object_visibility(
        [directory_with_raw_manifest.swhid().to_extended()],
        MaskedState.DECISION_PENDING,
    )

    swh_storage.directory_add([directory_with_raw_manifest, StorageData.directory2])

    assert swh_storage.directory_get_raw_manifest([StorageData.directory2.id]) == {
        StorageData.directory2.id: None
    }

    assert_masked_objects_raise(
        lambda: swh_storage.directory_get_raw_manifest(
            [directory_with_raw_manifest.id, StorageData.directory2.id],
        ),
        [directory_with_raw_manifest.swhid().to_extended()],
        set_object_visibility,
    )


def test_directory_get_id_partition(swh_storage, set_object_visibility):
    directories = [
        Directory(
            entries=(
                DirectoryEntry(
                    name=f"entry{i}".encode(),
                    type="file",
                    target=b"\x00" * 20,
                    perms=0,
                ),
            )
        )
        for i in range(100)
    ]
    swh_storage.directory_add(directories)

    nb_partitions = 4

    for partition in range(nb_partitions):
        if directories[0].id in stream_results(
            swh_storage.directory_get_id_partition, partition, nb_partitions
        ):
            break
    else:
        assert False, "No partition with directories[0] found?"

    assert_masked_objects_raise(
        lambda: list(
            stream_results(
                swh_storage.directory_get_id_partition, partition, nb_partitions
            )
        ),
        [directories[0].swhid().to_extended()],
        set_object_visibility,
    )


def test_revision_get(swh_storage, set_object_visibility):
    # revision2 has one parent, revision
    assert StorageData.revision2.parents == (StorageData.revision.id,)

    # Ensure that object masking doesn't prevent insertion
    set_object_visibility(
        [StorageData.revision.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.revision_add([StorageData.revision, StorageData.revision2])

    # This also checks that masking a parent doesn't mask the revision itself
    assert swh_storage.revision_get([StorageData.revision2.id]) == [
        StorageData.revision2
    ]

    # Check that masking the directory doesn't prevent getting the revision
    set_object_visibility(
        [
            ExtendedSWHID(
                object_id=StorageData.revision2.directory,
                object_type=ExtendedObjectType.DIRECTORY,
            )
        ],
        MaskedState.DECISION_PENDING,
    )
    assert swh_storage.revision_get([StorageData.revision2.id]) == [
        StorageData.revision2
    ]

    assert_masked_objects_raise(
        lambda: swh_storage.revision_get(
            [StorageData.revision.id, StorageData.revision2.id]
        ),
        [StorageData.revision.swhid().to_extended()],
        set_object_visibility,
    )


def test_revision_log(swh_storage, set_object_visibility):
    # revision2 has one parent, revision
    assert StorageData.revision2.parents == (StorageData.revision.id,)

    swh_storage.revision_add([StorageData.revision, StorageData.revision2])

    # Only mask the parent revision
    set_object_visibility(
        [StorageData.revision.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    # Getting one entry in the log still works
    assert list(swh_storage.revision_log([StorageData.revision2.id], limit=1))

    # But the parent is properly masked
    assert_masked_objects_raise(
        lambda: list(swh_storage.revision_log([StorageData.revision2.id], limit=2)),
        [StorageData.revision.swhid().to_extended()],
        set_object_visibility,
    )


def test_revision_shortlog(swh_storage, set_object_visibility):
    # revision2 has one parent, revision
    assert StorageData.revision2.parents == (StorageData.revision.id,)

    swh_storage.revision_add([StorageData.revision, StorageData.revision2])

    # Only mask the parent revision
    set_object_visibility(
        [StorageData.revision.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    # Getting one entry in the log still works
    assert list(swh_storage.revision_shortlog([StorageData.revision2.id], limit=1))

    # But the parent is properly masked
    assert_masked_objects_raise(
        lambda: list(
            swh_storage.revision_shortlog([StorageData.revision2.id], limit=2)
        ),
        [StorageData.revision.swhid().to_extended()],
        set_object_visibility,
    )


def test_revision_get_partition(swh_storage, set_object_visibility):
    revisions = [
        Revision(
            message=f"hello{i}".encode(),
            author=None,
            date=None,
            committer=None,
            committer_date=None,
            parents=(),
            type=RevisionType.GIT,
            directory=b"\x00" * 20,
            synthetic=True,
        )
        for i in range(100)
    ]
    swh_storage.revision_add(revisions)

    nb_partitions = 4

    for partition in range(nb_partitions):
        if revisions[0] in stream_results(
            swh_storage.revision_get_partition, partition, nb_partitions
        ):
            break
    else:
        assert False, "No partition with revisions[0] found?"

    assert_masked_objects_raise(
        lambda: list(
            stream_results(swh_storage.revision_get_partition, partition, nb_partitions)
        ),
        [revisions[0].swhid().to_extended()],
        set_object_visibility,
    )


def test_extid_get_from_target(swh_storage, set_object_visibility):
    gitids = [
        revision.id
        for revision in StorageData.revisions
        if revision.type.value == "git"
    ]
    extids = [
        ExtID(
            extid=gitid,
            extid_type="git",
            target=CoreSWHID(
                object_id=gitid,
                object_type=ObjectType.REVISION,
            ),
        )
        for gitid in gitids
    ]

    # Once more, check that visibility of both target and extid itself doesn't
    # affect insertion
    set_object_visibility(
        [extids[0].target.to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.extid_add(extids)

    # Check that extids get masked by target
    assert_masked_objects_raise(
        lambda: swh_storage.extid_get_from_target(
            extids[0].target.object_type,
            [extids[0].target.object_id, extids[1].target.object_id],
        ),
        [extids[0].target.to_extended()],
        set_object_visibility,
    )


def test_extid_get_from_extid():
    # Should this mask extids if the target is masked?
    pass


def test_release_get(swh_storage, set_object_visibility):
    # release.target = revision
    assert StorageData.release.target == StorageData.revision.id
    assert StorageData.release.target_type == ReleaseTargetType.REVISION

    # release2.target = revision2
    assert StorageData.release2.target == StorageData.revision2.id
    assert StorageData.release2.target_type == ReleaseTargetType.REVISION

    # Ensure that object masking doesn't prevent insertion
    set_object_visibility(
        [StorageData.release.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.release_add([StorageData.release, StorageData.release2])

    assert swh_storage.release_get([StorageData.release2.id]) == [StorageData.release2]

    # Check that masking the target doesn't prevent getting the release
    set_object_visibility(
        [StorageData.revision2.swhid().to_extended()],
        MaskedState.DECISION_PENDING,
    )
    assert swh_storage.release_get([StorageData.release2.id]) == [StorageData.release2]

    assert_masked_objects_raise(
        lambda: swh_storage.release_get(
            [StorageData.release.id, StorageData.release2.id]
        ),
        [StorageData.release.swhid().to_extended()],
        set_object_visibility,
    )


def test_release_get_partition(swh_storage, set_object_visibility):
    releases = [
        Release(
            name=f"release{i}".encode(),
            message=f"hello{i}".encode(),
            author=None,
            date=None,
            target=b"\x00" * 20,
            target_type=ReleaseTargetType.REVISION,
            synthetic=True,
        )
        for i in range(100)
    ]
    swh_storage.release_add(releases)

    nb_partitions = 4

    for partition in range(nb_partitions):
        if releases[0] in stream_results(
            swh_storage.release_get_partition, partition, nb_partitions
        ):
            break
    else:
        assert False, "No partition with releases[0] found?"

    assert_masked_objects_raise(
        lambda: list(
            stream_results(swh_storage.release_get_partition, partition, nb_partitions)
        ),
        [releases[0].swhid().to_extended()],
        set_object_visibility,
    )


def test_snapshot_get(swh_storage, set_object_visibility):
    # Ensure that object masking doesn't prevent insertion
    set_object_visibility(
        [StorageData.snapshot.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.snapshot_add([StorageData.snapshot, StorageData.complete_snapshot])

    complete_snapshot_dict = {
        **StorageData.complete_snapshot.to_dict(),
        "next_branch": None,
    }

    assert (
        swh_storage.snapshot_get(StorageData.complete_snapshot.id)
        == complete_snapshot_dict
    )
    # Check that masking branch targets doesn't affect snapshot visibility
    for branch in StorageData.complete_snapshot.branches.values():
        if not branch or branch.target_type == SnapshotTargetType.ALIAS:
            continue

        set_object_visibility(
            [branch.swhid().to_extended()], MaskedState.DECISION_PENDING
        )
        assert (
            swh_storage.snapshot_get(StorageData.complete_snapshot.id)
            == complete_snapshot_dict
        )

    assert_masked_objects_raise(
        lambda: swh_storage.snapshot_get(StorageData.snapshot.id),
        [StorageData.snapshot.swhid().to_extended()],
        set_object_visibility,
    )


def test_snapshot_count_branches(swh_storage, set_object_visibility):
    swh_storage.snapshot_add([StorageData.snapshot, StorageData.complete_snapshot])

    assert_masked_objects_raise(
        lambda: swh_storage.snapshot_count_branches(StorageData.complete_snapshot.id),
        [StorageData.complete_snapshot.swhid().to_extended()],
        set_object_visibility,
    )


def test_snapshot_get_branches(swh_storage, set_object_visibility):
    set_object_visibility(
        [StorageData.snapshot.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.snapshot_add([StorageData.snapshot, StorageData.complete_snapshot])

    get_branches_result = {
        "id": StorageData.complete_snapshot.id,
        "branches": StorageData.complete_snapshot.branches,
        "next_branch": None,
    }

    assert (
        swh_storage.snapshot_get_branches(StorageData.complete_snapshot.id)
        == get_branches_result
    )
    # Check that masking branch targets doesn't affect snapshot visibility
    for branch in StorageData.complete_snapshot.branches.values():
        if not branch or branch.target_type == SnapshotTargetType.ALIAS:
            continue

        set_object_visibility(
            [branch.swhid().to_extended()], MaskedState.DECISION_PENDING
        )
        assert (
            swh_storage.snapshot_get_branches(StorageData.complete_snapshot.id)
            == get_branches_result
        )

    assert_masked_objects_raise(
        lambda: swh_storage.snapshot_get_branches(StorageData.snapshot.id),
        [StorageData.snapshot.swhid().to_extended()],
        set_object_visibility,
    )


def test_snapshot_branch_get_by_name(swh_storage, set_object_visibility):
    set_object_visibility(
        [StorageData.snapshot.swhid().to_extended()], MaskedState.DECISION_PENDING
    )

    swh_storage.snapshot_add([StorageData.snapshot, StorageData.complete_snapshot])

    assert swh_storage.snapshot_branch_get_by_name(
        StorageData.complete_snapshot.id, branch_name=b"content"
    ).branch_found

    # Check that masking branch targets doesn't affect snapshot visibility
    for name, branch in StorageData.complete_snapshot.branches.items():
        if branch and branch.target_type != SnapshotTargetType.ALIAS:
            set_object_visibility(
                [branch.swhid().to_extended()], MaskedState.DECISION_PENDING
            )

        found = swh_storage.snapshot_branch_get_by_name(
            StorageData.complete_snapshot.id, branch_name=name, follow_alias_chain=False
        )

        assert found.branch_found
        assert found.target == branch

    assert_masked_objects_raise(
        lambda: swh_storage.snapshot_branch_get_by_name(
            StorageData.snapshot.id, b"HEAD"
        ),
        [StorageData.snapshot.swhid().to_extended()],
        set_object_visibility,
    )


def test_snapshot_get_id_partition(swh_storage, set_object_visibility):
    snapshots = [
        Snapshot(
            branches={
                f"branch{i}".encode(): SnapshotBranch(
                    target=b"\x00" * 20,
                    target_type=SnapshotTargetType.REVISION,
                ),
            },
        )
        for i in range(100)
    ]
    swh_storage.snapshot_add(snapshots)

    nb_partitions = 4

    for partition in range(nb_partitions):
        if snapshots[0].id in stream_results(
            swh_storage.snapshot_get_id_partition, partition, nb_partitions
        ):
            break
    else:
        assert False, "No partition with snapshots[0] found?"

    assert_masked_objects_raise(
        lambda: list(
            stream_results(
                swh_storage.snapshot_get_id_partition, partition, nb_partitions
            )
        ),
        [snapshots[0].swhid().to_extended()],
        set_object_visibility,
    )


def test_origin_get(swh_storage, set_object_visibility):
    set_object_visibility([StorageData.origin.swhid()], MaskedState.DECISION_PENDING)
    swh_storage.origin_add([StorageData.origin, StorageData.origin2])

    assert [StorageData.origin2] == swh_storage.origin_get([StorageData.origin2.url])

    assert_masked_objects_raise(
        lambda: swh_storage.origin_get([StorageData.origin.url]),
        [StorageData.origin.swhid()],
        set_object_visibility,
    )


def test_origin_get_by_sha1(swh_storage, set_object_visibility):
    set_object_visibility([StorageData.origin.swhid()], MaskedState.DECISION_PENDING)
    swh_storage.origin_add([StorageData.origin, StorageData.origin2])

    assert [{"url": StorageData.origin2.url}] == swh_storage.origin_get_by_sha1(
        [StorageData.origin2.id]
    )

    assert_masked_objects_raise(
        lambda: swh_storage.origin_get_by_sha1([StorageData.origin.id]),
        [StorageData.origin.swhid()],
        set_object_visibility,
    )


def test_origin_list(swh_storage, set_object_visibility):
    swh_storage.origin_add(StorageData.origins)

    res = list(stream_results(swh_storage.origin_list))
    assert len(res) == len(StorageData.origins)

    assert_masked_objects_raise(
        lambda: list(stream_results(swh_storage.origin_list)),
        [StorageData.origin.swhid()],
        set_object_visibility,
    )


def test_origin_search(swh_storage, set_object_visibility):
    swh_storage.origin_add(StorageData.origins)

    assert_masked_objects_raise(
        lambda: list(stream_results(swh_storage.origin_search, StorageData.origin.url)),
        [StorageData.origin.swhid()],
        set_object_visibility,
    )


def add_a_couple_visits(swh_storage):
    """Adds a couple of visits of one origin to swh_storage,
    returning the origin and the latest visit added."""
    origin = StorageData.origins[0]
    swh_storage.origin_add([origin])

    visit2_date = now()
    visit1_date = visit2_date - datetime.timedelta(seconds=3600)
    visit_type = "git"

    # set first visit to a null snapshot
    visit1 = swh_storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin.url,
                date=visit1_date,
                type=visit_type,
            )
        ]
    )[0]
    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=visit1.visit,
                date=visit1_date,
                status="created",
                snapshot=None,
            )
        ]
    )

    # set second visit to a non-null snapshot
    visit2 = swh_storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin.url,
                date=visit2_date,
                type=visit_type,
            )
        ]
    )[0]
    swh_storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin.url,
                visit=visit2.visit,
                date=visit2.date,
                status="created",
                snapshot=None,
            ),
            OriginVisitStatus(
                origin=origin.url,
                visit=visit2.visit,
                date=visit2.date + datetime.timedelta(seconds=60),
                status="full",
                snapshot=StorageData.snapshot.id,
            ),
        ]
    )

    return origin, visit2


def test_origin_snapshot_get_all(swh_storage, set_object_visibility):
    origin, _ = add_a_couple_visits(swh_storage)

    set_object_visibility([StorageData.snapshot.swhid()], MaskedState.DECISION_PENDING)
    assert swh_storage.origin_snapshot_get_all(origin.url) == [StorageData.snapshot.id]

    assert_masked_objects_raise(
        lambda: swh_storage.origin_snapshot_get_all(origin.url),
        [origin.swhid()],
        set_object_visibility,
    )


def test_origin_visit_behavior(swh_storage, set_object_visibility):
    origin, last_visit = add_a_couple_visits(swh_storage)

    for method in (
        "origin_visit_get",
        "origin_visit_get_latest",
        "origin_visit_get_with_statuses",
    ):
        assert_masked_objects_raise(
            lambda method=method: getattr(swh_storage, method)(origin.url),
            [origin.swhid()],
            set_object_visibility,
        )

    assert_masked_objects_raise(
        lambda: swh_storage.origin_visit_find_by_date(origin.url, last_visit.date),
        [origin.swhid()],
        set_object_visibility,
    )

    for method in (
        "origin_visit_get_by",
        "origin_visit_status_get",
        "origin_visit_status_get_latest",
    ):
        assert_masked_objects_raise(
            lambda method=method: getattr(swh_storage, method)(
                origin.url, last_visit.visit
            ),
            [origin.swhid()],
            set_object_visibility,
        )


def test_raw_extrinsic_metadata(swh_storage, set_object_visibility):
    origin = StorageData.origin
    origin_metadata = StorageData.origin_metadata

    authority1 = origin_metadata[0].authority
    authority2 = origin_metadata[-1].authority
    assert authority1 != authority2

    swh_storage.origin_add([origin])

    swh_storage.metadata_fetcher_add([m.fetcher for m in origin_metadata])
    swh_storage.metadata_authority_add([m.authority for m in origin_metadata])

    swh_storage.raw_extrinsic_metadata_add(origin_metadata)

    # The last metadata should not be masked
    assert swh_storage.raw_extrinsic_metadata_get(origin.swhid(), authority2).results

    # Masking both the origin and the metadata's swhid should mask the REMD object
    for masked_swhid in [origin.swhid(), origin_metadata[0].swhid()]:
        assert_masked_objects_raise(
            lambda: swh_storage.raw_extrinsic_metadata_get(origin.swhid(), authority1),
            [masked_swhid],
            set_object_visibility,
        )

        assert_masked_objects_raise(
            lambda: swh_storage.raw_extrinsic_metadata_get_by_ids(
                [origin_metadata[0].id]
            ),
            [masked_swhid],
            set_object_visibility,
        )

    # Authority information is only masked if the target is masked
    found_authorities = assert_masked_objects_raise(
        lambda: swh_storage.raw_extrinsic_metadata_get_authorities(origin.swhid()),
        [origin.swhid()],
        set_object_visibility,
    )

    set_object_visibility([origin_metadata[0].swhid()], MaskedState.DECISION_PENDING)

    assert (
        swh_storage.raw_extrinsic_metadata_get_authorities(origin.swhid())
        == found_authorities
    )


def test_proxy_overhead_metric(swh_storage: MaskingProxyStorage, mocker) -> None:
    timing = mocker.patch("swh.core.statsd.statsd.timing")
    monotonic = mocker.patch("swh.storage.metrics.monotonic")
    monotonic.side_effect = [
        15.0,  # outer start
        17.0,  # inner start
        17.7,  # inner finished (0.7s elapsed)
        18.2,  # outer finished (3.2s elapsed)
        Exception("Should not have been called again!"),
    ]

    swh_storage.content_add([StorageData.content, StorageData.content2])
    for call in timing.call_args_list:
        assert (
            call.args[0] != "swh_storage_masking_overhead_seconds"
        ), "Overhead metric sent unexpectedly"

    assert (
        swh_storage.content_get_data(cast(HashDict, StorageData.content.hashes()))
        is not None
    )

    timing.assert_any_call(
        "swh_storage_masking_overhead_seconds",
        # 3.2 s total - 0.7 s inner = 2500 ms overhead
        pytest.approx(2500.0),
        tags={"endpoint": "content_get_data"},
    )


def test_proxy_config_deprecation(masking_db_postgresql, swh_storage_backend):
    with pytest.warns(DeprecationWarning):
        assert MaskingProxyStorage(
            masking_db=masking_db_postgresql.info.dsn, storage=swh_storage_backend
        )
