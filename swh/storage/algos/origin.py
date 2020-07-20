# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Optional, Iterable, Iterator, Tuple

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
    storage,
    origin_url: str,
    type: Optional[str] = None,
    allowed_statuses: Optional[Iterable[str]] = None,
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
    last_visit = None
    while True:
        visits = list(
            storage.origin_visit_get(
                origin_url, last_visit=last_visit, order="desc", limit=10,
            )
        )
        if not visits:
            return None
        last_visit = visits[-1]["visit"]

        visit_status: Optional[OriginVisitStatus] = None
        visit: Dict[str, Any]
        for visit in visits:
            if type is not None and visit["type"] != type:
                continue
            visit_status = storage.origin_visit_status_get_latest(
                origin_url,
                visit["visit"],
                allowed_statuses=allowed_statuses,
                require_snapshot=require_snapshot,
            )
            if visit_status is not None:
                # storage api gives us too many data which no longer map to an
                # origin-visit, so we drop those
                for key in ["metadata", "status", "snapshot"]:
                    visit.pop(key, None)
                return (OriginVisit.from_dict(visit), visit_status)
