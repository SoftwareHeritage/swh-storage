# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

import pytest

from swh.core.pytest_plugin import RPCTestAdapter
from swh.storage import get_storage
from swh.storage.api import client, server
from swh.storage.buffer import BufferingProxyStorage
from swh.storage.filter import FilteringProxyStorage
from swh.storage.in_memory import InMemoryStorage
from swh.storage.postgresql.storage import Storage as DbStorage
from swh.storage.retry import RetryingProxyStorage

STORAGES = [
    pytest.param(cls, real_class, kwargs, id=cls)
    for (cls, real_class, kwargs) in [
        ("remote", client.RemoteStorage, {"url": "url"}),
        ("memory", InMemoryStorage, {}),
        (
            "local",
            DbStorage,
            {"db": "postgresql://db", "objstorage": {"cls": "memory"}},
        ),
        ("filter", FilteringProxyStorage, {"storage": {"cls": "memory"}}),
        ("buffer", BufferingProxyStorage, {"storage": {"cls": "memory"}}),
        ("retry", RetryingProxyStorage, {"storage": {"cls": "memory"}}),
    ]
]


@pytest.mark.parametrize("cls,real_class,args", STORAGES)
@patch("swh.storage.postgresql.storage.psycopg2.pool")
def test_get_storage(mock_pool, cls, real_class, args):
    """Instantiating an existing storage should be ok

    """
    mock_pool.ThreadedConnectionPool.return_value = None
    actual_storage = get_storage(cls, **args)
    assert actual_storage is not None
    assert isinstance(actual_storage, real_class)


@pytest.mark.parametrize("cls,real_class,args", STORAGES)
@patch("swh.storage.postgresql.storage.psycopg2.pool")
def test_get_storage_legacy_args(mock_pool, cls, real_class, args):
    """Instantiating an existing storage should be ok even with the legacy
    explicit 'args' keys

    """
    mock_pool.ThreadedConnectionPool.return_value = None
    with pytest.warns(DeprecationWarning):
        actual_storage = get_storage(cls, args=args)
        assert actual_storage is not None
        assert isinstance(actual_storage, real_class)


def test_get_storage_failure():
    """Instantiating an unknown storage should raise

    """
    with pytest.raises(ValueError, match="Unknown storage class `unknown`"):
        get_storage("unknown")


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


# get_storage's check_config argument tests

# the "remote" and "pipeline" cases are tested in dedicated test functions below
@pytest.mark.parametrize(
    "cls,real_class,kwargs", [x for x in STORAGES if x.id not in ("remote", "local")]
)
def test_get_storage_check_config(cls, real_class, kwargs, monkeypatch):
    """Instantiating an existing storage with check_config should be ok

    """
    check_backend_check_config(monkeypatch, dict(cls=cls, **kwargs))


@patch("swh.storage.postgresql.storage.psycopg2.pool")
def test_get_storage_local_check_config(mock_pool, monkeypatch):
    """Instantiating a local storage with check_config should be ok

    """
    mock_pool.ThreadedConnectionPool.return_value = None
    check_backend_check_config(
        monkeypatch,
        {"cls": "local", "db": "postgresql://db", "objstorage": {"cls": "memory"}},
        backend_storage_cls=DbStorage,
    )


def test_get_storage_pipeline_check_config(monkeypatch):
    """Test that the check_config option works as intended for a pipelined storage"""
    config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "filter",},
            {"cls": "buffer", "min_batch_size": {"content": 10,},},
            {"cls": "memory",},
        ],
    }
    check_backend_check_config(
        monkeypatch, config,
    )


def test_get_storage_remote_check_config(monkeypatch):
    """Test that the check_config option works as intended for a remote storage"""

    monkeypatch.setattr(
        server, "storage", get_storage(cls="memory", journal_writer={"cls": "memory"})
    )
    test_client = server.app.test_client()

    class MockedRemoteStorage(client.RemoteStorage):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.session.adapters.clear()
            self.session.mount("mock://", RPCTestAdapter(test_client))

    monkeypatch.setattr(client, "RemoteStorage", MockedRemoteStorage)

    config = {
        "cls": "remote",
        "url": "mock://example.com",
    }
    check_backend_check_config(
        monkeypatch, config,
    )


def check_backend_check_config(
    monkeypatch, config, backend_storage_cls=InMemoryStorage
):
    """Check the staged/indirect storage (pipeline or remote) works
    as desired with regard to the check_config option of the get_storage()
    factory function.

    If set, the check_config argument is used to call the Storage.check_config() at
    instantiation time in the get_storage() factory function. This is supposed to be
    passed through each step of the Storage pipeline until it reached the actual
    backend's (typically in memory or local) check_config() method which will perform
    the verification for read/write access to the backend storage.

    monkeypatch is supposed to be the monkeypatch pytest fixture to be used from the
    calling test_ function.

    config is the config dict passed to get_storage()

    backend_storage_cls is the class of the backend storage to be mocked to
    simulate the check_config behavior; it should then be the class of the
    actual backend storage defined in the `config`.
    """
    access = None

    def mockcheck(self, check_write=False):
        if access == "none":
            return False
        if access == "read":
            return check_write is False
        if access == "write":
            return True

    monkeypatch.setattr(backend_storage_cls, "check_config", mockcheck)

    # simulate no read nor write access to the underlying (memory) storage
    access = "none"
    # by default, no check, so no complain
    assert get_storage(**config)
    # if asked to check, complain
    with pytest.raises(EnvironmentError):
        get_storage(check_config={"check_write": False}, **config)
    with pytest.raises(EnvironmentError):
        get_storage(check_config={"check_write": True}, **config)

    # simulate no write access to the underlying (memory) storage
    access = "read"
    # by default, no check so no complain
    assert get_storage(**config)
    # if asked to check for read access, no complain
    get_storage(check_config={"check_write": False}, **config)
    # if asked to check for write access, complain
    with pytest.raises(EnvironmentError):
        get_storage(check_config={"check_write": True}, **config)

    # simulate read & write access to the underlying (memory) storage
    access = "write"
    # by default, no check so no complain
    assert get_storage(**config)
    # if asked to check for read access, no complain
    get_storage(check_config={"check_write": False}, **config)
    # if asked to check for write access, no complain
    get_storage(check_config={"check_write": True}, **config)
