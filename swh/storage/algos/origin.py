# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterator, List, Optional, Tuple

from swh.model.model import Origin, OriginVisit, OriginVisitStatus
from swh.storage.interface import StorageInterface


def iter_origins(
    storage: StorageInterface,
    origin_from: int = 1,
    origin_to: Optional[int] = None,
    batch_size: int = 10000,
) -> Iterator[Origin]:
    """Iterates over all origins in the storage.

    Args:
        storage: the storage object used for queries.
        origin_from: lower interval boundary
        origin_to: upper interval boundary
        batch_size: number of origins per query

    Yields:
        origin within the boundary [origin_to, origin_from] in batch_size

    """
    start = origin_from
    while True:
        if origin_to:
            origin_count = min(origin_to - start, batch_size)
        else:
            origin_count = batch_size
        origins = list(
            storage.origin_get_range(origin_from=start, origin_count=origin_count)
        )
        if not origins:
            break
        start = origins[-1]["id"] + 1
        for origin in origins:
            del origin["id"]
            yield Origin.from_dict(origin)
        if origin_to and start > origin_to:
            break


def origin_get_latest_visit_status(
    storage: StorageInterface,
    origin_url: str,
    type: Optional[str] = None,
    allowed_statuses: Optional[List[str]] = None,
    require_snapshot: bool = False,
) -> Optional[Tuple[OriginVisit, OriginVisitStatus]]:
    """Get the latest origin visit (and status) of an origin. Optionally, a combination of
    criteria can be provided, origin type, allowed statuses or if a visit has a
    snapshot.

    If no visit matching the criteria is found, returns None. Otherwise, returns a tuple
    of origin visit, origin visit status.

    Args:
        storage: A storage backend
        origin: origin URL
        type: Optional visit type to filter on (e.g git, tar, dsc, svn,
            hg, npm, pypi, ...)
        allowed_statuses: list of visit statuses considered
            to find the latest visit. For instance,
            ``allowed_statuses=['full']`` will only consider visits that
            have successfully run to completion.
        require_snapshot: If True, only a visit with a snapshot
            will be returned.

    Returns:
        a tuple of (visit, visit_status) model object if the visit *and* the visit
        status exist (and match the search criteria), None otherwise.

    """
    visit = storage.origin_visit_get_latest(
        origin_url,
        type=type,
        allowed_statuses=allowed_statuses,
        require_snapshot=require_snapshot,
    )
    result: Optional[Tuple[OriginVisit, OriginVisitStatus]] = None
    if visit:
        visit_status = storage.origin_visit_status_get_latest(
            origin_url,
            visit.visit,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
        )
        if visit_status:
            result = visit, visit_status
    return result
