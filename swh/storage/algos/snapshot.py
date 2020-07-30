# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import List, Optional

from swh.model.model import Snapshot, TargetType

from swh.storage.algos.origin import (
    origin_get_latest_visit_status,
    iter_origin_visits,
    iter_origin_visit_statuses,
)
from swh.storage.interface import ListOrder, StorageInterface


def snapshot_get_all_branches(storage, snapshot_id):
    """Get all the branches for a given snapshot

    Args:
        storage (swh.storage.storage.Storage): the storage instance
        snapshot_id (bytes): the snapshot's identifier
    Returns:
        dict: a dict with two keys:
            * **id**: identifier of the snapshot
            * **branches**: a dict of branches contained in the snapshot
              whose keys are the branches' names.
    """
    ret = storage.snapshot_get(snapshot_id)

    if not ret:
        return

    next_branch = ret.pop("next_branch", None)
    while next_branch:
        data = storage.snapshot_get_branches(snapshot_id, branches_from=next_branch)
        ret["branches"].update(data["branches"])
        next_branch = data.get("next_branch")

    return ret


def snapshot_get_latest(
    storage,
    origin: str,
    allowed_statuses: Optional[List[str]] = None,
    branches_count: Optional[int] = None,
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

    Raises:
        ValueError if branches_count is not a positive value

    Returns:
        The snapshot object if one is found matching the criteria or None.

    """
    visit_and_status = origin_get_latest_visit_status(
        storage, origin, allowed_statuses=allowed_statuses, require_snapshot=True,
    )

    if not visit_and_status:
        return None

    _, visit_status = visit_and_status
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
        snapshot.pop("next_branch")
    else:
        snapshot = snapshot_get_all_branches(storage, snapshot_id)
    return Snapshot.from_dict(snapshot) if snapshot else None


def snapshot_id_get_from_revision(
    storage: StorageInterface, origin: str, revision_id: bytes
) -> Optional[bytes]:
    """Retrieve the most recent snapshot id targeting the revision_id for the given origin.

    *Warning* This is a potentially highly costly operation

    Returns
        The snapshot id if found. None otherwise.

    """
    revision = storage.revision_get([revision_id])
    if not revision:
        return None

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
            for branch_name, branch in snapshot["branches"].items():
                if (
                    branch is not None
                    and branch["target_type"] == TargetType.REVISION.value
                    and branch["target"] == revision_id
                ):  # snapshot found
                    return snapshot_id

    return None
