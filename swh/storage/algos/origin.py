# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional, Iterable, Tuple
from swh.model.model import OriginVisit, OriginVisitStatus


def iter_origins(storage, origin_from=1, origin_to=None, batch_size=10000):
    """Iterates over all origins in the storage.

    Args:
        storage: the storage object used for queries.
        batch_size: number of origins per query
    Yields:
        dict: the origin dictionary with the keys:

        - type: origin's type
        - url: origin's url
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
            yield origin
        if origin_to and start > origin_to:
            break


def origin_get_latest_visit_status(
    storage,
    origin_url: str,
    type: Optional[str] = None,
    allowed_statuses: Optional[Iterable[str]] = None,
    require_snapshot: bool = False,
) -> Optional[Tuple[OriginVisit, OriginVisitStatus]]:
    """Get the latest origin visit and visit status information for a given origin,
       optionally looking only for those with one of the given allowed_statuses or for
       those with a snapshot.

       If nothing matches the criteria, this returns None.

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
        status exist, None otherwise.

    """
    visit_d = storage.origin_visit_get_latest(origin_url, type=type)
    if not visit_d:
        return None
    visit = OriginVisit.from_dict(visit_d)
    visit_status = storage.origin_visit_status_get_latest(
        origin_url,
        visit.visit,
        allowed_statuses=allowed_statuses,
        require_snapshot=require_snapshot,
    )
    if not visit_status:
        return None
    return (visit, visit_status)
