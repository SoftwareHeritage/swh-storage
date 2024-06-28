# Copyright (C) 2020-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import call

import attr
import psycopg2
import pytest

from swh.core.api import TransientRemoteException
from swh.storage.exc import HashCollision, StorageArgumentException
from swh.storage.utils import now


@pytest.fixture
def storage_retry_sleep_mock(mocker):
    """In test context, we don't want to wait, make test faster by mocking
    the sleep function used by retryable storage methods"""
    return mocker.patch("time.sleep")


@pytest.fixture
def fake_hash_collision(sample_data):
    return HashCollision("sha1", "38762cf7f55934b34d179ae6a4c80cadccbb7f0a", [])


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "pipeline",
        "steps": [
            {"cls": "retry"},
            {"cls": "memory"},
        ],
    }


def test_retrying_proxy_storage_content_add(swh_storage, sample_data):
    """Standard content_add works as before"""
    sample_content = sample_data.content
    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": sample_content.length,
    }

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content == sample_content.data


def test_retrying_proxy_storage_content_add_with_retry(
    storage_retry_sleep_mock,
    swh_storage,
    sample_data,
    mocker,
    fake_hash_collision,
):
    """Multiple retries for hash collision and psycopg2 error but finally ok"""
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("content already inserted"),
        # ok then!
        {"content:add": 1},
    ]

    sample_content = sample_data.content

    sleep = storage_retry_sleep_mock
    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    s = swh_storage.content_add([sample_content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [
            call([sample_content]),
            call([sample_content]),
            call([sample_content]),
        ]
    )

    assert len(sleep.mock_calls) == 2
    (_, args1, _) = sleep.mock_calls[0]
    (_, args2, _) = sleep.mock_calls[1]
    assert 0 < args1[0] < 1
    assert 0 < args2[0] < 2


def test_retrying_proxy_storage_content_add_with_retry_of_transient(
    storage_retry_sleep_mock,
    swh_storage,
    sample_data,
    mocker,
):
    """Multiple retries for hash collision and psycopg2 error but finally ok
    after many attempts"""
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = [
        TransientRemoteException("temporary failure"),
        TransientRemoteException("temporary failure"),
        # ok then!
        {"content:add": 1},
    ]

    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    sleep = storage_retry_sleep_mock
    s = swh_storage.content_add([sample_content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [
            call([sample_content]),
            call([sample_content]),
            call([sample_content]),
        ]
    )

    assert len(sleep.mock_calls) == 2
    (_, args1, _) = sleep.mock_calls[0]
    (_, args2, _) = sleep.mock_calls[1]
    assert 10 < args1[0] < 11
    assert 10 < args2[0] < 12


def test_retrying_proxy_swh_storage_content_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry"""
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add content always!")

    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.content_add([sample_content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_content_add_metadata(swh_storage, sample_data):
    """Standard content_add_metadata works as before"""
    sample_content = sample_data.content
    content = attr.evolve(sample_content, data=None)

    pk = content.sha1
    content_metadata = swh_storage.content_get([pk])
    assert content_metadata == [None]

    s = swh_storage.content_add_metadata([attr.evolve(content, ctime=now())])
    assert s == {
        "content:add": 1,
    }

    content_metadata = swh_storage.content_get([pk])
    assert len(content_metadata) == 1
    assert content_metadata[0].sha1 == pk


def test_retrying_proxy_storage_content_add_metadata_with_retry(
    storage_retry_sleep_mock, swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok"""
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.content_add_metadata"
    )
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("content_metadata already inserted"),
        # ok then!
        {"content:add": 1},
    ]

    sample_content = sample_data.content
    content = attr.evolve(sample_content, data=None)

    s = swh_storage.content_add_metadata([content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [
            call([content]),
            call([content]),
            call([content]),
        ]
    )


def test_retrying_proxy_swh_storage_content_add_metadata_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry"""
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.content_add_metadata"
    )
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add content_metadata!"
    )

    sample_content = sample_data.content
    content = attr.evolve(sample_content, data=None)

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.content_add_metadata([content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_swh_storage_keyboardinterrupt(swh_storage, sample_data, mocker):
    """Unfiltered errors are raising without retry"""
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = KeyboardInterrupt()

    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    with pytest.raises(KeyboardInterrupt):
        swh_storage.content_add([sample_content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_swh_storage_content_add_metadata_retry_failed(
    swh_storage, sample_data, mocker
):
    """When retrying fails every time, the last exception gets re-raised instead
    of a RetryError"""

    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.content_add_metadata"
    )
    mock_memory.side_effect = [
        ValueError(f"This will get raised eventually (attempt {i})")
        for i in range(1, 10)
    ]

    sample_content = sample_data.content
    content = attr.evolve(sample_content, data=None)

    with pytest.raises(ValueError, match="(attempt 3)"):
        swh_storage.content_add_metadata([content])

    assert mock_memory.call_count == 3
