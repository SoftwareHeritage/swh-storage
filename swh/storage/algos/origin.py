# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterator, List, Optional, Tuple

from swh.core.api.classes import stream_results
from swh.model.model import Origin, OriginVisit, OriginVisitStatus
from swh.storage.interface import ListOrder, StorageInterface


def iter_origins(storage: StorageInterface, limit: int = 10000,) -> Iterator[Origin]:
    """Iterates over origins in the storage.

    Args:
        storage: the storage object used for queries.
        limit: maximum number of origins per page

    Yields:
        origin model objects from the storage in page of `limit` origins

    """
    yield from stream_results(storage.origin_list, limit=limit)


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
        assert visit.visit is not None
        visit_status = storage.origin_visit_status_get_latest(
            origin_url,
            visit.visit,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
        )
        if visit_status:
            result = visit, visit_status
    return result


def iter_origin_visits(
    storage: StorageInterface, origin: str, order: ListOrder = ListOrder.ASC
) -> Iterator[OriginVisit]:
    """Iter over origin visits from an origin

    """
    yield from stream_results(storage.origin_visit_get, origin, order=order)


def iter_origin_visit_statuses(
    storage: StorageInterface, origin: str, visit: int, order: ListOrder = ListOrder.ASC
) -> Iterator[OriginVisitStatus]:
    """Iter over origin visit status from an origin visit

    """
    yield from stream_results(
        storage.origin_visit_status_get, origin, visit, order=order
    )
