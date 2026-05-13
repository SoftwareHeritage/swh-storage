# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Unit tests for :mod:`swh.storage.reconciler`.

These tests use ``InMemoryStorage`` as a stand-in for ``CassandraStorage``
— the verifier only touches ``_cql_runner`` methods, which the
``InMemoryCqlRunner`` implements with the same signatures.  The
integration test against a real Cassandra cluster is the same pattern,
just with the storage configured via ``cls: cassandra``.
"""

from __future__ import annotations

import hashlib
from typing import List

from swh.model.model import Content
from swh.storage.cassandra.schema import HASH_ALGORITHMS
from swh.storage.in_memory import InMemoryStorage
from swh.storage.reconciler import ContentReconciler
from swh.storage.reconciler.verifier import ContentVerifier, VerifyResult


def _make_content(seed: int = 0) -> Content:
    """Build a deterministic ``Content`` for testing."""
    data = hashlib.blake2b(seed.to_bytes(8, "little"), digest_size=64).digest()
    return Content.from_data(data)


def _populate(storage: InMemoryStorage, contents: List[Content]) -> None:
    """Insert contents through the normal storage API to set up state."""
    storage.content_add(contents)


def _delete_index_row(storage: InMemoryStorage, content: Content, algo: str) -> None:
    """Simulate a partial-write crash by deleting one index-table row.

    The ``InMemoryCqlRunner`` stores indexes as a nested mapping
    ``_content_indexes[algo][hash_value] -> set[token]``.  Clearing the
    set for our content's algo-hash drops the index row, leaving the
    main ``content`` row and the other 3 indexes intact.
    """
    cql = storage._cql_runner
    hash_value = content.get_hash(algo)
    algo_index = cql._content_indexes.get(algo, {})
    algo_index.pop(hash_value, None)


# ---------------------------------------------------------------------------
# ContentVerifier — direct unit tests
# ---------------------------------------------------------------------------


def test_verifier_all_rows_present_reports_clean():
    storage = InMemoryStorage()
    content = _make_content(seed=1)
    _populate(storage, [content])

    verifier = ContentVerifier(storage, repair_enabled=False)
    result = verifier.verify_and_repair(content)

    assert result.all_present
    assert result.missing_indexes == []
    assert result.main_missing is False
    assert result.repaired is False


def test_verifier_reports_missing_index_in_observe_only_mode():
    storage = InMemoryStorage()
    content = _make_content(seed=2)
    _populate(storage, [content])
    _delete_index_row(storage, content, algo="sha1")

    verifier = ContentVerifier(storage, repair_enabled=False)
    result = verifier.verify_and_repair(content)

    assert result.missing_indexes == ["sha1"]
    assert result.main_missing is False
    assert result.repaired is False
    # And re-running confirms the miss is still there — observe-only.
    again = verifier.verify_and_repair(content)
    assert again.missing_indexes == ["sha1"]


def test_verifier_repairs_missing_index_when_enabled():
    storage = InMemoryStorage()
    content = _make_content(seed=3)
    _populate(storage, [content])
    _delete_index_row(storage, content, algo="sha256")

    verifier = ContentVerifier(storage, repair_enabled=True)
    result = verifier.verify_and_repair(content)

    assert result.missing_indexes == ["sha256"]
    assert result.repaired is True
    # After repair, the next run sees a clean slate.
    again = verifier.verify_and_repair(content)
    assert again.all_present


def test_verifier_repairs_multiple_missing_indexes():
    storage = InMemoryStorage()
    content = _make_content(seed=4)
    _populate(storage, [content])
    for algo in ("sha1", "sha1_git", "blake2s256"):
        _delete_index_row(storage, content, algo=algo)

    verifier = ContentVerifier(storage, repair_enabled=True)
    result = verifier.verify_and_repair(content)

    assert set(result.missing_indexes) == {"sha1", "sha1_git", "blake2s256"}
    assert result.repaired is True
    assert verifier.verify_and_repair(content).all_present


def test_verifier_idempotent_repair_on_clean_state():
    """A repair call against an already-clean state is a no-op."""
    storage = InMemoryStorage()
    content = _make_content(seed=5)
    _populate(storage, [content])

    verifier = ContentVerifier(storage, repair_enabled=True)
    r1 = verifier.verify_and_repair(content)
    r2 = verifier.verify_and_repair(content)

    assert r1.all_present and not r1.repaired
    assert r2.all_present and not r2.repaired


def test_verifier_result_dataclass_defaults():
    """Sanity check the default-construction of VerifyResult."""
    r = VerifyResult()
    assert r.missing_indexes == []
    assert r.main_missing is False
    assert r.repaired is False
    assert r.all_present is True


# ---------------------------------------------------------------------------
# ContentReconciler — worker callback shape
# ---------------------------------------------------------------------------


def test_reconciler_process_batch_empty_input_is_noop():
    storage = InMemoryStorage()
    reconciler = ContentReconciler(storage, repair_enabled=False)
    # JournalClient.process invokes worker_fn({}) when no messages arrive.
    reconciler.process_batch({})
    reconciler.process_batch({"content": []})
    # No assertion beyond "does not raise".


def test_reconciler_process_batch_routes_contents_through_verifier():
    storage = InMemoryStorage()
    contents = [_make_content(seed=i) for i in (10, 11, 12)]
    _populate(storage, contents)
    # Break one row so the verifier has something to report.
    _delete_index_row(storage, contents[1], algo="sha1")

    reconciler = ContentReconciler(storage, repair_enabled=True)
    reconciler.process_batch({"content": contents})

    # After processing with repair enabled, every content's state is clean.
    for content in contents:
        result = reconciler.verifier.verify_and_repair(content)
        assert result.all_present, content


def test_reconciler_observe_only_does_not_write():
    storage = InMemoryStorage()
    content = _make_content(seed=20)
    _populate(storage, [content])
    _delete_index_row(storage, content, algo="blake2s256")

    reconciler = ContentReconciler(storage, repair_enabled=False)
    reconciler.process_batch({"content": [content]})

    # The index row should still be missing — observe-only means observe.
    result = ContentVerifier(storage, repair_enabled=False).verify_and_repair(content)
    assert result.missing_indexes == ["blake2s256"]


def test_reconciler_ignores_non_content_keys():
    storage = InMemoryStorage()
    content = _make_content(seed=30)
    _populate(storage, [content])

    reconciler = ContentReconciler(storage, repair_enabled=True)
    # Pretend the journal sent us a directory event by accident; the
    # worker should look at the "content" key only.
    reconciler.process_batch({"directory": ["not a content object"], "content": []})
    # No exception, no state change.
    assert ContentVerifier(storage).verify_and_repair(content).all_present


def test_verifier_against_all_four_index_algos():
    """Run the per-algo regression once per HASH_ALGORITHM."""
    for algo in HASH_ALGORITHMS:
        storage = InMemoryStorage()
        content = _make_content(seed=40 + hash(algo) % 100)
        _populate(storage, [content])
        _delete_index_row(storage, content, algo=algo)
        verifier = ContentVerifier(storage, repair_enabled=True)
        result = verifier.verify_and_repair(content)
        assert result.missing_indexes == [algo], algo
        assert result.repaired is True
        assert verifier.verify_and_repair(content).all_present, algo
