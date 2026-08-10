# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import TYPE_CHECKING, Dict, List

from swh.model.hashutil import hash_to_hex
from swh.storage.utils import content_bytes_hashes, content_hex_hashes

if TYPE_CHECKING:
    from swh.model.swhids import ExtendedSWHID
    from swh.storage.proxies.blocking.db import BlockingStatus
    from swh.storage.proxies.masking.db import MaskedStatus


class StorageDBError(Exception):
    """Specific storage db error (connection, erroneous queries, etc...)"""

    def __str__(self):
        return "An unexpected error occurred in the backend: %s" % (self.args,)


class StorageAPIError(Exception):
    """Specific internal storage api (mainly connection)"""

    def __str__(self):
        args = self.args
        return "An unexpected error occurred in the api backend: %s" % (args,)


class NonRetryableException(Exception):
    """Persistent storage exceptions for which clients should not issue automatic
    retries"""

    pass


class StorageArgumentException(NonRetryableException):
    """Argument passed to a Storage endpoint is invalid."""

    pass


class QueryTimeout(StorageAPIError):
    """Raised when a query to the backend (Cassandra or PostgreSQL) took too long."""

    pass


class UnknownMetadataAuthority(StorageArgumentException):
    """Raised when ``raw_extrinsic_metadata_add`` is called with a non-existing
    metadata authority as argument."""

    pass


class UnknownMetadataFetcher(StorageArgumentException):
    """Raised when ``raw_extrinsic_metadata_add`` is called with a non-existing
    metadata fetcher as argument."""

    pass


class BlockedOriginException(NonRetryableException):
    """Raised when the blocking proxy prevent from inserting a blocked origin"""

    def __init__(self, blocked: "Dict[str, BlockingStatus]"):
        blocked = {
            url: status
            for url, status in blocked.items()
            if status.state.name != "NON_BLOCKED"
        }
        if not blocked:
            raise ValueError(
                "Can't raise a BlockedOriginException if no origin is actually blocked"
            )

        self.blocked = blocked
        super().__init__(blocked)

    def __str__(self):
        return "Some origins are blocked: %s" % ", ".join(self.blocked)


class MaskedObjectException(NonRetryableException):
    """Raised when the masking proxy attempts to return a masked object"""

    def __init__(self, masked: "Dict[ExtendedSWHID, List[MaskedStatus]]"):
        if not masked:
            raise ValueError(
                "Can't raise a MaskedObjectException if no objects are masked"
            )
        for swhid, statuses in masked.items():
            if not statuses:
                raise ValueError(f"MaskedObjectException has no statuses for {swhid}")

        self.masked = masked

        super().__init__(masked)

    @staticmethod
    def _str_one_masked(obj, statuses):
        status = statuses[0]
        return f"{obj} by request {status.request} ({status.state.name})" + (
            " and others" if len(statuses) > 1 else ""
        )

    def __str__(self):
        return "Some objects are masked: %s" % ", ".join(
            self._str_one_masked(obj, statuses)
            for obj, statuses in self.masked.items()
            if statuses
        )


class HashCollision(Exception):
    """Exception raised when a content collides in a storage backend"""

    def __init__(self, algo, hash_id, colliding_contents):
        self.algo = algo
        self.hash_id = hash_to_hex(hash_id)
        self.colliding_contents = [content_hex_hashes(c) for c in colliding_contents]
        super().__init__(self.algo, self.hash_id, self.colliding_contents)

    def colliding_content_hashes(self) -> List[Dict[str, bytes]]:
        return [content_bytes_hashes(c) for c in self.colliding_contents]
