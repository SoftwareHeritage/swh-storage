# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterator, List, Optional, Tuple

from swh.model.hashutil import hash_to_hex
from swh.model.model import (
    OriginVisit,
    OriginVisitStatus,
    Sha1Git,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.storage.algos.origin import (
    iter_origin_visit_statuses,
    iter_origin_visits,
    origin_get_latest_visit_status,
)
from swh.storage.interface import ListOrder, StorageInterface


def snapshot_get_all_branches(
    storage: StorageInterface, snapshot_id: Sha1Git
) -> Optional[Snapshot]:
    """Get all the branches for a given snapshot

    Args:
        storage (swh.storage.interface.StorageInterface): the storage instance
        snapshot_id (bytes): the snapshot's identifier
    Returns:
        A snapshot objects populated with all known branches if the snapshot is
        found or None.
    """
    ret = storage.snapshot_get_branches(snapshot_id)

    if not ret:
        return None

    next_branch = ret["next_branch"]
    while next_branch:
        data = storage.snapshot_get_branches(snapshot_id, branches_from=next_branch)
        assert data, f"Snapshot {hash_to_hex(snapshot_id)} ceased to exist"
        ret["branches"].update(data["branches"])
        next_branch = data["next_branch"]

    return Snapshot(id=ret["id"], branches=ret["branches"])


def snapshot_get_latest(
    storage: StorageInterface,
    origin: str,
    allowed_statuses: Optional[List[str]] = None,
    branches_count: Optional[int] = None,
    visit_type: Optional[str] = None,
) -> Optional[Snapshot]:
    """Get the latest snapshot for the given origin, optionally only from visits that have
    one of the given allowed_statuses.

    The branches of the snapshot are iterated in the lexicographical
    order of their names.

    Args:
        storage: Storage instance
        origin: the origin's URL
        allowed_statuses: list of visit statuses considered
            to find the latest snapshot for the visit. For instance,
            ``allowed_statuses=['full']`` will only consider visits that
            have successfully run to completion.
        branches_count: Optional parameter to retrieve snapshot with all branches
            (default behavior when None) or not. If set to positive number, the snapshot
            will be partial with only that number of branches.
        visit_type: Optional parameter to retrieve snapshot produced by a specific
            visit type

    Raises:
        ValueError if branches_count is not a positive value

    Returns:
        The snapshot object if one is found matching the criteria or None.

    """
    visit_status = origin_get_latest_visit_status(
        storage,
        origin,
        allowed_statuses=allowed_statuses,
        require_snapshot=True,
        type=visit_type,
    )
    if not visit_status:
        return None

    snapshot_id = visit_status.snapshot
    if not snapshot_id:
        return None

    if branches_count:  # partial snapshot
        if not isinstance(branches_count, int) or branches_count <= 0:
            raise ValueError(
                "Parameter branches_count must be a positive integer. "
                f"Current value is {branches_count}"
            )
        snapshot = storage.snapshot_get_branches(
            snapshot_id, branches_count=branches_count
        )
        if snapshot is None:
            return None
        return Snapshot(id=snapshot["id"], branches=snapshot["branches"])
    else:
        return snapshot_get_all_branches(storage, snapshot_id)


def snapshot_id_get_from_revision(
    storage: StorageInterface, origin: str, revision_id: bytes
) -> Optional[bytes]:
    """Retrieve the most recent snapshot id targeting the revision_id for the given origin.

    *Warning* This is a potentially highly costly operation

    Returns
        The snapshot id if found. None otherwise.

    """
    res = visits_and_snapshots_get_from_revision(storage, origin, revision_id)

    # they are sorted by descending date, so we just need to return the first one,
    # if any.
    for visit, status, snapshot in res:
        return snapshot.id

    return None


def visits_and_snapshots_get_from_revision(
    storage: StorageInterface, origin: str, revision_id: bytes
) -> Iterator[Tuple[OriginVisit, OriginVisitStatus, Snapshot]]:
    """Retrieve all visits, visit statuses, and matching snapshot of the given origin,
    such that the snapshot targets the revision_id.

    *Warning* This is a potentially highly costly operation

    Yields:
        Tuples of (visit, status, snapshot)

    """
    revision = storage.revision_get([revision_id])
    if not revision:
        return

    for visit in iter_origin_visits(storage, origin, order=ListOrder.DESC):
        assert visit.visit is not None
        for visit_status in iter_origin_visit_statuses(
            storage, origin, visit.visit, order=ListOrder.DESC
        ):
            snapshot_id = visit_status.snapshot
            if snapshot_id is None:
                continue

            snapshot = snapshot_get_all_branches(storage, snapshot_id)
            if not snapshot:
                continue
            for branch_name, branch in snapshot.branches.items():
                if (
                    branch is not None
                    and branch.target_type == SnapshotTargetType.REVISION
                    and branch.target == revision_id
                ):  # snapshot found
                    yield (visit, visit_status, snapshot)


def snapshot_resolve_alias(
    storage: StorageInterface, snapshot_id: Sha1Git, alias_name: bytes
) -> Optional[SnapshotBranch]:
    """
    Transitively resolve snapshot branch alias to its real target, and return it;
    ie. follows every branch that is an alias, until a branch that isn't an alias
    is found.

    *Warning* This function is deprecated; use storage.snapshot_branch_get_by_name instead

    Args:
        storage: Storage instance
        snapshot_id: snapshot identifier
        alias_name: name of the branch alias to resolve

    Returns:
        The first branch that isn't an alias, in the alias chain; or None if
        there is no such branch (ie. either because of a cycle alias, or a dangling
        branch).
    """

    final_branch = storage.snapshot_branch_get_by_name(
        snapshot_id=snapshot_id, branch_name=alias_name
    )
    if final_branch is not None:
        return final_branch.target
    return None
