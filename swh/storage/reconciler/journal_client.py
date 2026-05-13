# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""JournalClient consumer + worker callback for the content reconciler.

Commit 1 of the reconciler MR series ships this skeleton: the
:class:`ContentReconciler` worker counts Content events seen, in
``observe-only`` mode, and emits statsd metrics. The verification +
repair logic is added in the next commit; verifier hooks here are
explicit ``_verify_one`` calls that currently do nothing.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from swh.core.statsd import Statsd
from swh.model.model import Content

logger = logging.getLogger(__name__)


# All metrics emitted by the reconciler share this prefix.
_STATSD_NAMESPACE = "swh_storage_reconciler"


class ContentReconciler:
    """Reconcile ``Content`` events from the journal against Cassandra state.

    The consumer is built around the
    :class:`~swh.journal.client.JournalClient` ``process(worker_fn)``
    contract: the journal client deserializes one Kafka batch into
    ``{"content": [Content, ...]}`` and invokes :meth:`process_batch`.

    Two modes:

      - ``observe-only`` (default): the verifier runs but no writes are
        made. The ``swh_storage_reconciler_repairs_total`` counter still
        increments per missing row so an operator can see how often
        repair *would* fire before flipping the switch.
      - ``repair_enabled=True``: missing rows are idempotently re-inserted
        via the writer's normal helpers.

    Production-safety contract: the verifier reads Cassandra at the
    *same* consistency level as the writer that produced the event.
    Reading at a stricter level would generate false-misses; reading at
    a weaker level would miss real misses.  The storage instance passed
    in carries the consistency level (its ``_consistency_level``
    attribute) so the verifier inherits it automatically.
    """

    def __init__(
        self,
        storage,
        *,
        repair_enabled: bool = False,
        statsd_constant_tags: Optional[Dict[str, str]] = None,
    ):
        self.storage = storage
        self.repair_enabled = repair_enabled
        self.statsd = Statsd(
            namespace=_STATSD_NAMESPACE,
            constant_tags=statsd_constant_tags or {},
        )

    def process_batch(self, objects: Dict[str, List[Any]]) -> None:
        """Worker callback for :meth:`JournalClient.process`.

        The journal client batches messages by object_type; for the
        reconciler we only ever subscribe to ``"content"``.
        """
        contents: List[Content] = objects.get("content", [])
        if not contents:
            return

        self.statsd.increment("throughput_total", len(contents))

        for content in contents:
            self._verify_one(content)

    def _verify_one(self, content: Content) -> None:
        """Verify a single ``Content`` event against Cassandra state.

        Skeleton in commit 1.  Verification + repair logic lands in the
        next commit; this hook keeps the worker callback signature
        stable.
        """
        # Skeleton; verifier wired up in the next commit.
        return None
