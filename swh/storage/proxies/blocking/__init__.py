# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
from typing import Dict, Iterable, Iterator, List, Optional, Union
import warnings

import psycopg2.pool

from swh.model.model import Origin, OriginVisit, OriginVisitStatus
from swh.storage import get_storage
from swh.storage.exc import BlockedOriginException
from swh.storage.interface import StorageInterface
from swh.storage.metrics import DifferentialTimer
from swh.storage.proxies.blocking.db import BlockingState

from .db import BlockingQuery

BLOCKING_OVERHEAD_METRIC = "swh_storage_blocking_overhead_seconds"


def get_datastore(cls, db=None, blocking_db=None, **kwargs):
    assert cls in ("postgresql", "blocking")
    from .db import BlockingAdmin

    if db is None:
        db = blocking_db
    return BlockingAdmin.connect(db)


def blocking_overhead_timer(method_name: str) -> DifferentialTimer:
    """Return a properly setup DifferentialTimer for ``method_name`` of the storage"""
    return DifferentialTimer(BLOCKING_OVERHEAD_METRIC, tags={"endpoint": method_name})


class BlockingProxyStorage:
    """Blocking storage proxy

    This proxy prevents visits from a known list of origins to be performed at all.

    It uses a specific PostgreSQL database (which for now is colocated with the
    swh.storage PostgreSQL database), the access to which is implemented in the
    :mod:`.db` submodule.

    Sample configuration

    .. code-block: yaml

        storage:
          cls: blocking
          db: 'dbname=swh-blocking-proxy'
          max_pool_conns: 10
          storage:
          - cls: remote
            url: http://storage.internal.staging.swh.network:5002/

    """

    def __init__(
        self,
        storage: Union[Dict, StorageInterface],
        db: Optional[str] = None,
        blocking_db: Optional[str] = None,
        min_pool_conns: int = 1,
        max_pool_conns: int = 5,
    ):
        if db is None:
            assert blocking_db is not None
            warnings.warn(
                "'blocking_db' field in the blocking storage configuration "
                "was renamed 'db' field",
                DeprecationWarning,
            )
            db = blocking_db
        self.storage: StorageInterface = (
            get_storage(**storage) if isinstance(storage, dict) else storage
        )

        self._blocking_pool = psycopg2.pool.ThreadedConnectionPool(
            min_pool_conns, max_pool_conns, db
        )

    def origin_visit_status_add(
        self, visit_statuses: List[OriginVisitStatus]
    ) -> Dict[str, int]:
        with self._blocking_query() as q:
            statuses = q.origins_are_blocked([v.origin for v in visit_statuses])
            if statuses and any(
                status.state != BlockingState.NON_BLOCKED
                for status in statuses.values()
            ):
                raise BlockedOriginException(statuses)
        return self.storage.origin_visit_status_add(visit_statuses)

    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        with self._blocking_query() as q:
            statuses = q.origins_are_blocked([v.origin for v in visits])
            if statuses and any(
                status.state != BlockingState.NON_BLOCKED
                for status in statuses.values()
            ):
                raise BlockedOriginException(statuses)
        return self.storage.origin_visit_add(visits)

    def origin_add(self, origins: List[Origin]) -> Dict[str, int]:
        with self._blocking_query() as q:
            statuses = {}
            for origin in origins:
                status = q.origin_is_blocked(origin.url)
                if status and status.state != BlockingState.NON_BLOCKED:
                    statuses[origin.url] = status
            if statuses:
                raise BlockedOriginException(statuses)
        return self.storage.origin_add(origins)

    @contextmanager
    def _blocking_query(self) -> Iterator[BlockingQuery]:
        ret = None
        try:
            ret = BlockingQuery.from_pool(self._blocking_pool)
            yield ret
        finally:
            if ret:
                ret.put_conn()

    def __getattr__(self, key):
        method = getattr(self.storage, key)
        if method:
            # Avoid going through __getattr__ again next time
            setattr(self, key, method)
            return method

        # Raise a NotImplementedError to make sure we don't forget to add
        # masking to any new storage functions
        raise NotImplementedError(key)
