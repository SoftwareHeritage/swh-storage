# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Unit tests for :class:`swh.storage.proxies.miss_tolerant.MissTolerantProxyStorage`.

The proxy's value-add is purely on the read path: it retries once on
an empty result.  These tests use ``unittest.mock`` to stand in for the
wrapped storage so the proxy's retry behaviour is observable
independently of any backend.
"""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock

import pytest

from swh.model.model import Content
from swh.storage.proxies.miss_tolerant import (
    DEFAULT_RETRY_DELAY,
    MissTolerantProxyStorage,
)


def _make_content(seed: int = 0) -> Content:
    data = hashlib.blake2b(seed.to_bytes(8, "little"), digest_size=64).digest()
    return Content.from_data(data)


@pytest.fixture
def mock_storage():
    """A bare-bones ``StorageInterface`` mock — every method is a MagicMock."""
    return MagicMock(name="wrapped_storage")


@pytest.fixture
def proxy(mock_storage, monkeypatch):
    """A proxy whose ``get_storage`` returns our mock without any real I/O."""
    monkeypatch.setattr(
        "swh.storage.proxies.miss_tolerant.get_storage",
        lambda **_: mock_storage,
    )
    # retry_delay=0 keeps the test fast; the production default is 50ms.
    return MissTolerantProxyStorage(storage={"cls": "memory"}, retry_delay=0)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_default_retry_delay():
    assert DEFAULT_RETRY_DELAY == 0.05


def test_init_rejects_negative_delay(monkeypatch):
    monkeypatch.setattr(
        "swh.storage.proxies.miss_tolerant.get_storage",
        lambda **_: MagicMock(),
    )
    with pytest.raises(ValueError):
        MissTolerantProxyStorage(storage={"cls": "memory"}, retry_delay=-1)


# ---------------------------------------------------------------------------
# content_find
# ---------------------------------------------------------------------------


def test_content_find_returns_result_without_retry(proxy, mock_storage):
    content = _make_content(seed=1)
    mock_storage.content_find.return_value = [content]
    result = proxy.content_find({"sha1": content.sha1})
    assert result == [content]
    assert mock_storage.content_find.call_count == 1  # no retry


def test_content_find_retries_on_empty(proxy, mock_storage):
    content = _make_content(seed=2)
    # First call → empty (race window).  Second call → hit (writer caught up).
    mock_storage.content_find.side_effect = [[], [content]]
    result = proxy.content_find({"sha1": content.sha1})
    assert result == [content]
    assert mock_storage.content_find.call_count == 2


def test_content_find_returns_empty_after_retry_still_empty(proxy, mock_storage):
    mock_storage.content_find.side_effect = [[], []]
    result = proxy.content_find({"sha1": b"\x00" * 20})
    assert result == []
    assert mock_storage.content_find.call_count == 2


# ---------------------------------------------------------------------------
# content_get
# ---------------------------------------------------------------------------


def test_content_get_no_retry_on_partial_hits(proxy, mock_storage):
    contents = [_make_content(seed=i) for i in (10, 11)]
    # First content present, second a real miss → no retry: a partial
    # result implies the storage is answering.
    mock_storage.content_get.return_value = [contents[0], None]
    result = proxy.content_get([c.sha1 for c in contents])
    assert result == [contents[0], None]
    assert mock_storage.content_get.call_count == 1


def test_content_get_retries_when_all_none(proxy, mock_storage):
    contents = [_make_content(seed=i) for i in (20, 21)]
    mock_storage.content_get.side_effect = [[None, None], contents]
    result = proxy.content_get([c.sha1 for c in contents])
    assert result == contents
    assert mock_storage.content_get.call_count == 2


def test_content_get_returns_all_none_after_retry(proxy, mock_storage):
    mock_storage.content_get.side_effect = [[None, None], [None, None]]
    result = proxy.content_get([b"\x00" * 20, b"\xff" * 20])
    assert result == [None, None]
    assert mock_storage.content_get.call_count == 2


# ---------------------------------------------------------------------------
# content_missing_per_sha1{,_git}
# ---------------------------------------------------------------------------


def test_content_missing_per_sha1_no_retry_on_partial(proxy, mock_storage):
    # 3 inputs, 2 missing — partial, no retry.
    inputs = [b"a" * 20, b"b" * 20, b"c" * 20]
    mock_storage.content_missing_per_sha1.return_value = iter(inputs[:2])
    result = list(proxy.content_missing_per_sha1(inputs))
    assert result == inputs[:2]
    assert mock_storage.content_missing_per_sha1.call_count == 1


def test_content_missing_per_sha1_retries_when_all_missing(proxy, mock_storage):
    # All 3 inputs missing on first call → retry.  Retry sees 1 missing.
    inputs = [b"a" * 20, b"b" * 20, b"c" * 20]
    mock_storage.content_missing_per_sha1.side_effect = [
        iter(inputs),
        iter(inputs[:1]),
    ]
    result = list(proxy.content_missing_per_sha1(inputs))
    assert result == inputs[:1]
    assert mock_storage.content_missing_per_sha1.call_count == 2


def test_content_missing_per_sha1_git_retries_when_all_missing(proxy, mock_storage):
    inputs = [b"x" * 20, b"y" * 20]
    mock_storage.content_missing_per_sha1_git.side_effect = [
        iter(inputs),
        iter([]),
    ]
    result = list(proxy.content_missing_per_sha1_git(inputs))
    assert result == []
    assert mock_storage.content_missing_per_sha1_git.call_count == 2


def test_content_missing_per_sha1_empty_input(proxy, mock_storage):
    """Empty input — short-circuit, no retry."""
    mock_storage.content_missing_per_sha1.return_value = iter([])
    result = list(proxy.content_missing_per_sha1([]))
    assert result == []
    assert mock_storage.content_missing_per_sha1.call_count == 1


# ---------------------------------------------------------------------------
# Pass-through (everything else)
# ---------------------------------------------------------------------------


def test_other_methods_pass_through(proxy, mock_storage):
    mock_storage.directory_get_entries.return_value = "passthrough_result"
    assert proxy.directory_get_entries() == "passthrough_result"


def test_check_config_pass_through(proxy, mock_storage):
    mock_storage.check_config.return_value = True
    assert proxy.check_config(check_write=True) is True
    mock_storage.check_config.assert_called_with(check_write=True)
