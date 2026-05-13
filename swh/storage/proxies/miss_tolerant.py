# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Defense-in-depth proxy for the concurrent ``content_add`` consistency window.

When ``CassandraStorage`` is configured with
``content_add_algo: "concurrent"`` (see architectural issue
``swh/devel/swh-storage#4727``), inserts to the 4 index tables and the
main ``content`` row complete in unordered fashion.  A reader doing a
hash lookup via an index table may briefly observe a missing row even
though the write batch has begun: this is the consistency window the
journal-driven content reconciler
(:class:`swh.storage.reconciler.ContentReconciler`) closes by replaying
``swh.journal.objects.content`` at consumer-lag.

The reconciler is the load-bearing safety mechanism.  This proxy is
**defense-in-depth on the read path**: it wraps a small set of
``StorageInterface`` read methods so that an empty / ``None`` result is
retried once after a short delay before being returned to the caller.
Under healthy conditions the retry is never needed; under the rare
consistency-window race the retry succeeds because the in-flight write
has completed by the time the retry runs.

**Default disabled.**  Wire this proxy into the read pipeline only if
production metrics (``swh-web`` 404 rate, the reconciler's
``repairs_total`` rate, or user reports) show user-visible misses from
the consistency window.  Otherwise it adds latency to every "real" miss
without buying anything.

Sample configuration:

.. code-block:: yaml

    storage:
      cls: miss_tolerant
      retry_delay: 0.05
      storage:
        cls: cassandra
        hosts: [cassandra-1]
        keyspace: swh
        content_add_algo: concurrent
"""

from __future__ import annotations

import logging
import time
from typing import Iterable, List, Optional

from swh.model.model import Content
from swh.storage import StorageSpec, get_storage
from swh.storage.interface import HashDict, Sha1Git, StorageInterface

logger = logging.getLogger(__name__)


#: Default delay (in seconds) between the first read and the retry on
#: an empty result.  Chosen to comfortably exceed the cassandra-driver's
#: per-request latency at the configured consistency level while staying
#: imperceptible to end-users on the rare retry path.
DEFAULT_RETRY_DELAY = 0.05


class MissTolerantProxyStorage:
    """Retry read paths once on empty result.

    Wraps :meth:`content_find`, :meth:`content_missing_per_sha1`,
    :meth:`content_missing_per_sha1_git`, and :meth:`content_get` so
    that a result that *looks like* a miss (empty iterable, all-``None``
    list, or ``content_missing`` returning every input as missing) is
    retried once after :attr:`retry_delay` seconds.  All other methods
    pass through to the wrapped storage via ``__getattr__``.

    Args:
        storage: Configuration of the storage to wrap (typically
            ``cls: cassandra``).
        retry_delay: Seconds to sleep before retrying an empty result.
            Defaults to :data:`DEFAULT_RETRY_DELAY` (50ms).
    """

    def __init__(
        self,
        storage: StorageSpec,
        retry_delay: float = DEFAULT_RETRY_DELAY,
    ) -> None:
        if retry_delay < 0:
            raise ValueError(f"retry_delay must be >= 0 (got {retry_delay!r})")
        self.storage: StorageInterface = get_storage(**storage)
        self.retry_delay = retry_delay

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    # ----- wrapped read methods --------------------------------------

    def content_find(self, content: HashDict) -> List[Content]:
        result = self.storage.content_find(content)
        if result:
            return result
        time.sleep(self.retry_delay)
        retried = self.storage.content_find(content)
        if retried:
            logger.debug(
                "MissTolerantProxy: content_find recovered on retry for %r",
                content,
            )
        return retried

    def content_get(
        self, contents: List[bytes], algo: str = "sha1"
    ) -> List[Optional[Content]]:
        result = self.storage.content_get(contents, algo=algo)
        # Only retry if every position is None — a partial result means
        # the database is responsive and the misses are likely real.
        if any(item is not None for item in result):
            return result
        time.sleep(self.retry_delay)
        retried = self.storage.content_get(contents, algo=algo)
        if any(item is not None for item in retried):
            logger.debug(
                "MissTolerantProxy: content_get recovered on retry "
                "(input %d hashes)",
                len(contents),
            )
        return retried

    def content_missing_per_sha1(self, contents: List[bytes]) -> Iterable[bytes]:
        result = list(self.storage.content_missing_per_sha1(contents))
        # Retry only when the proxy reports *all* inputs as missing —
        # that's the worst-case race-window shape.  Partial misses imply
        # the storage is answering and the misses are real.
        if not contents or len(result) < len(contents):
            return iter(result)
        time.sleep(self.retry_delay)
        retried = list(self.storage.content_missing_per_sha1(contents))
        if len(retried) < len(result):
            logger.debug(
                "MissTolerantProxy: content_missing_per_sha1 recovered on retry "
                "(%d → %d missing)",
                len(result),
                len(retried),
            )
        return iter(retried)

    def content_missing_per_sha1_git(
        self, contents: List[Sha1Git]
    ) -> Iterable[Sha1Git]:
        result = list(self.storage.content_missing_per_sha1_git(contents))
        if not contents or len(result) < len(contents):
            return iter(result)
        time.sleep(self.retry_delay)
        retried = list(self.storage.content_missing_per_sha1_git(contents))
        if len(retried) < len(result):
            logger.debug(
                "MissTolerantProxy: content_missing_per_sha1_git recovered on retry "
                "(%d → %d missing)",
                len(result),
                len(retried),
            )
        return iter(retried)
