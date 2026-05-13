# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Per-content Cassandra-state verifier with idempotent repair.

Given a :class:`~swh.model.model.Content` event from the journal, the
verifier checks that all 5 corresponding rows are present in Cassandra:

  - 1 main row in the ``content`` table.
  - 4 index rows: ``content_by_sha1``, ``content_by_sha1_git``,
    ``content_by_sha256``, ``content_by_blake2s256`` — each maps the
    per-algo hash to the main row's ``target_token``.

If any row is missing, :meth:`ContentVerifier.verify_and_repair` reports
the miss via the result object.  If ``repair_enabled`` is True, the
verifier additionally re-inserts the missing row(s) using the same
``CqlRunner`` helpers the writer uses — so the repair is byte-identical
to a normal write.

Consistency contract: the verifier reads through the storage instance's
``_cql_runner``, which uses the consistency level the storage was
configured with (``_consistency_level``).  Reads therefore match the
writer's consistency, which is the only correct choice — reading at a
stricter level would generate false-misses; reading at a weaker level
would miss real misses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, cast

from swh.model.model import Content
from swh.storage.cassandra.model import ContentRow
from swh.storage.cassandra.schema import HASH_ALGORITHMS
from swh.storage.utils import remove_keys


@dataclass
class VerifyResult:
    """Outcome of a single :meth:`ContentVerifier.verify_and_repair` call."""

    #: Hash algorithms whose index row was missing.
    missing_indexes: List[str] = field(default_factory=list)

    #: True if the main ``content`` row was missing.
    main_missing: bool = False

    #: True if any of the missing rows were re-inserted by the verifier.
    repaired: bool = False

    @property
    def all_present(self) -> bool:
        """True if all 5 rows were present and no repair was needed."""
        return not self.missing_indexes and not self.main_missing


class ContentVerifier:
    """Verify (and optionally repair) all 5 Cassandra rows for a Content.

    The verifier is stateless: each :meth:`verify_and_repair` call is
    independent. The ``storage`` argument is the
    :class:`~swh.storage.cassandra.CassandraStorage` whose ``_cql_runner``
    is used both to read the current state and to re-insert missing rows.

    Args:
        storage: A ``CassandraStorage`` instance (or any subclass with a
            compatible ``_cql_runner`` attribute, e.g. ``InMemoryStorage``
            during integration testing).
        repair_enabled: If False (default), misses are detected and
            reported but no writes occur. If True, missing rows are
            re-inserted idempotently.
    """

    def __init__(self, storage, *, repair_enabled: bool = False):
        self.storage = storage
        self.repair_enabled = repair_enabled

    def verify_and_repair(self, content: Content) -> VerifyResult:
        cql = self.storage._cql_runner

        # Derive the token deterministically from the Content's primary
        # key.  ``content_add_prepare`` returns ``(token, finalizer)``
        # without doing any I/O for the token computation — the
        # ``finalizer`` would do the actual main-row INSERT if called.
        content_row = ContentRow(**remove_keys(content.to_dict(), ("data",)))
        (expected_token, main_finalizer) = cql.content_add_prepare(content_row)

        # 1) Check each of the 4 index tables for a row pointing at
        #    ``expected_token``.  ``content_get_tokens_from_single_algo``
        #    returns every token currently mapped to that algo-hash; if
        #    ``expected_token`` is not in the result, the index row for
        #    this Content is missing.
        missing_indexes: List[str] = []
        for algo in HASH_ALGORITHMS:
            hash_value = content.get_hash(algo)
            tokens = list(cql.content_get_tokens_from_single_algo(algo, [hash_value]))
            if expected_token not in tokens:
                missing_indexes.append(algo)

        # 2) Check the main 'content' row.
        hashes_dict = {algo: content.get_hash(algo) for algo in HASH_ALGORITHMS}
        main_row: Optional[ContentRow] = cql.content_get_from_pk(
            cast(dict, hashes_dict)
        )
        main_missing = main_row is None

        result = VerifyResult(
            missing_indexes=missing_indexes,
            main_missing=main_missing,
        )

        if result.all_present:
            return result

        # 3) Idempotent repair.  ``content_index_add_one`` and the
        #    ``main_finalizer`` issue plain INSERT statements; Cassandra
        #    treats them as upserts so concurrent reconciler instances
        #    racing on the same row are safe.
        if self.repair_enabled:
            for algo in missing_indexes:
                cql.content_index_add_one(algo, content, expected_token)
            if main_missing:
                main_finalizer()
            result.repaired = True

        return result
