# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from unittest.mock import patch

from swh.storage import get_storage

from swh.storage.api.client import RemoteStorage
from swh.storage.storage import Storage as DbStorage
from swh.storage.in_memory import InMemoryStorage
from swh.storage.buffer import BufferingProxyStorage
from swh.storage.filter import FilteringProxyStorage
from swh.storage.retry import RetryingProxyStorage


@patch("swh.storage.storage.psycopg2.pool")
def test_get_storage(mock_pool):
    """Instantiating an existing storage should be ok

    """
    mock_pool.ThreadedConnectionPool.return_value = None
    for cls, real_class, dummy_args in [
        ("remote", RemoteStorage, {"url": "url"}),
        ("memory", InMemoryStorage, {}),
        (
            "local",
            DbStorage,
            {"db": "postgresql://db", "objstorage": {"cls": "memory", "args": {},},},
        ),
        ("filter", FilteringProxyStorage, {"storage": {"cls": "memory"}}),
        ("buffer", BufferingProxyStorage, {"storage": {"cls": "memory"}}),
        ("retry", RetryingProxyStorage, {"storage": {"cls": "memory"}}),
    ]:
        actual_storage = get_storage(cls, **dummy_args)
        assert actual_storage is not None
        assert isinstance(actual_storage, real_class)


@patch("swh.storage.storage.psycopg2.pool")
def test_get_storage_legacy_args(mock_pool):
    """Instantiating an existing storage should be ok even with the legacy
    explicit 'args' keys

    """
    mock_pool.ThreadedConnectionPool.return_value = None
    for cls, real_class, dummy_args in [
        ("remote", RemoteStorage, {"url": "url"}),
        ("memory", InMemoryStorage, {}),
        (
            "local",
            DbStorage,
            {"db": "postgresql://db", "objstorage": {"cls": "memory", "args": {},},},
        ),
        ("filter", FilteringProxyStorage, {"storage": {"cls": "memory", "args": {}}}),
        ("buffer", BufferingProxyStorage, {"storage": {"cls": "memory", "args": {}}}),
    ]:
        with pytest.warns(DeprecationWarning):
            actual_storage = get_storage(cls, args=dummy_args)
        assert actual_storage is not None
        assert isinstance(actual_storage, real_class)


def test_get_storage_failure():
    """Instantiating an unknown storage should raise

    """
    with pytest.raises(ValueError, match="Unknown storage class `unknown`"):
        get_storage("unknown", args=[])


def test_get_storage_pipeline():
    config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "filter",},
            {"cls": "buffer", "min_batch_size": {"content": 10,},},
            {"cls": "memory",},
        ],
    }

    storage = get_storage(**config)

    assert isinstance(storage, FilteringProxyStorage)
    assert isinstance(storage.storage, BufferingProxyStorage)
    assert isinstance(storage.storage.storage, InMemoryStorage)


def test_get_storage_pipeline_legacy_args():
    config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "filter",},
            {"cls": "buffer", "args": {"min_batch_size": {"content": 10,},}},
            {"cls": "memory",},
        ],
    }

    with pytest.warns(DeprecationWarning):
        storage = get_storage(**config)

    assert isinstance(storage, FilteringProxyStorage)
    assert isinstance(storage.storage, BufferingProxyStorage)
    assert isinstance(storage.storage.storage, InMemoryStorage)
