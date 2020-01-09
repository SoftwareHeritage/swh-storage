# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import pytest

from swh.storage import HashCollision
from swh.storage.retry import (
    RetryingProxyStorage, should_retry_adding, RETRY_EXCEPTIONS
)


def test_should_retry_adding():
    """Specific exceptions should be elected for retrial

    """
    for exc in RETRY_EXCEPTIONS:
        assert should_retry_adding(exc('error')) is True


def test_should_retry_adding_no_retry():
    """Unspecific exceptions should raise as usual

    """
    for exc in [ValueError, Exception]:
        assert should_retry_adding(exc('fail!')) is False


def test_retrying_proxy_storage_content(sample_data):
    """Standard content_add works as before

    """
    sample_content = sample_data['content'][0]
    storage = RetryingProxyStorage(storage={'cls': 'memory'})

    content = next(storage.content_get([sample_content['sha1']]))
    assert not content

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
        'content:add:bytes': sample_content['length'],
        'skipped_content:add': 0
    }


def test_retrying_proxy_storage_with_retry(sample_data, mocker):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.content_add')
    mock_memory.side_effect = [
        # first try goes ko
        HashCollision('content hash collision'),
        # second try goes ko
        psycopg2.IntegrityError('content already inserted'),
        # ok then!
        {'content:add': 1}
    ]

    sample_content = sample_data['content'][0]
    storage = RetryingProxyStorage(storage={'cls': 'memory'})

    content = next(storage.content_get([sample_content['sha1']]))
    assert not content

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
    }


def test_retrying_proxy_storage_failure_to_add(sample_data, mocker):
    """Other errors are raising as usual

    """
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.content_add')
    mock_memory.side_effect = ValueError('Refuse to add content always!')

    sample_content = sample_data['content'][0]
    storage = RetryingProxyStorage(storage={'cls': 'memory'})

    content = next(storage.content_get([sample_content['sha1']]))
    assert not content

    with pytest.raises(ValueError, match='Refuse to add'):
        storage.content_add([sample_content])
