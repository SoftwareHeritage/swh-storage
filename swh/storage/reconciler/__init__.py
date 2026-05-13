# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Journal-driven content reconciler.

Consumes the ``swh.journal.objects.content`` Kafka topic, verifies that
each :class:`~swh.model.model.Content` event has all 5 corresponding
Cassandra rows (the main ``content`` row + 4 ``content_by_<algo>`` index
rows), and idempotently re-emits any missing rows.

The reconciler is the production-safety prerequisite for enabling
``content_add_algo: "concurrent"`` on ``CassandraStorage``: the
concurrent path relaxes the "indexes written before main row" ordering
guarantee, so a concurrent reader doing a hash lookup may briefly see
the main row with an index row not yet present.  The reconciler closes
this consistency window by replaying the journal at consumer-lag and
repairing any missing rows.

See the architectural issue ``swh/devel/swh-storage#4727`` for the full
safety story (Kafka as durable intent log, Cassandra as derived index,
journal-driven reconciler as repair path).
"""

from .journal_client import ContentReconciler

__all__ = ["ContentReconciler"]
