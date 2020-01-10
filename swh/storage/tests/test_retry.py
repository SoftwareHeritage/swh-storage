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


@pytest.fixture
def swh_storage():
    return RetryingProxyStorage(storage={'cls': 'memory'})


def test_retrying_proxy_storage_content_add(swh_storage, sample_data):
    """Standard content_add works as before

    """
    sample_content = sample_data['content'][0]

    content = next(swh_storage.content_get([sample_content['sha1']]))
    assert not content

    s = swh_storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
        'content:add:bytes': sample_content['length'],
        'skipped_content:add': 0
    }


def test_retrying_proxy_storage_content_add_with_retry(
        swh_storage, sample_data, mocker):
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

    content = next(swh_storage.content_get([sample_content['sha1']]))
    assert not content

    s = swh_storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
    }


def test_retrying_proxy_swh_storage_content_add_failure(
        swh_storage, sample_data, mocker):
    """Other errors are raising as usual

    """
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.content_add')
    mock_memory.side_effect = ValueError('Refuse to add content always!')

    sample_content = sample_data['content'][0]

    content = next(swh_storage.content_get([sample_content['sha1']]))
    assert not content

    with pytest.raises(ValueError, match='Refuse to add'):
        swh_storage.content_add([sample_content])

    content = next(swh_storage.content_get([sample_content['sha1']]))
    assert not content


def test_retrying_proxy_swh_storage_origin_add_one(swh_storage, sample_data):
    """Standard content_add works as before

    """
    sample_origin = sample_data['origin'][0]

    origin = swh_storage.origin_get(sample_origin)
    assert not origin

    r = swh_storage.origin_add_one(sample_origin)
    assert r == sample_origin['url']

    origin = swh_storage.origin_get(sample_origin)
    assert origin['url'] == sample_origin['url']


def test_retrying_proxy_swh_storage_origin_add_one_retry(
        swh_storage, sample_data, mocker):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_origin = sample_data['origin'][1]
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.origin_add_one')
    mock_memory.side_effect = [
        # first try goes ko
        HashCollision('origin hash collision'),
        # second try goes ko
        psycopg2.IntegrityError('origin already inserted'),
        # ok then!
        sample_origin['url']
    ]

    origin = swh_storage.origin_get(sample_origin)
    assert not origin

    r = swh_storage.origin_add_one(sample_origin)
    assert r == sample_origin['url']


def test_retrying_proxy_swh_storage_origin_add_one_failure(
        swh_storage, sample_data, mocker):
    """Other errors are raising as usual

    """
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.origin_add_one')
    mock_memory.side_effect = ValueError('Refuse to add origin always!')

    sample_origin = sample_data['origin'][0]

    origin = swh_storage.origin_get(sample_origin)
    assert not origin

    with pytest.raises(ValueError, match='Refuse to add'):
        swh_storage.origin_add_one([sample_origin])

    origin = swh_storage.origin_get(sample_origin)
    assert not origin


def test_retrying_proxy_swh_storage_origin_visit_add(swh_storage, sample_data):
    """Standard content_add works as before

    """
    sample_origin = sample_data['origin'][0]
    swh_storage.origin_add_one(sample_origin)
    origin_url = sample_origin['url']

    origin = list(swh_storage.origin_visit_get(origin_url))
    assert not origin

    origin_visit = swh_storage.origin_visit_add(
        origin_url, '2020-01-01', 'hg')
    assert origin_visit['origin'] == origin_url
    assert isinstance(origin_visit['visit'], int)

    origin_visit = next(swh_storage.origin_visit_get(origin_url))
    assert origin_visit['origin'] == origin_url
    assert isinstance(origin_visit['visit'], int)


def test_retrying_proxy_swh_storage_origin_visit_add_retry(
        swh_storage, sample_data, mocker):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_origin = sample_data['origin'][1]
    swh_storage.origin_add_one(sample_origin)
    origin_url = sample_origin['url']

    mock_memory = mocker.patch(
        'swh.storage.in_memory.Storage.origin_visit_add')
    mock_memory.side_effect = [
        # first try goes ko
        HashCollision('origin hash collision'),
        # second try goes ko
        psycopg2.IntegrityError('origin already inserted'),
        # ok then!
        {'origin': origin_url, 'visit': 1}
    ]

    origin = list(swh_storage.origin_visit_get(origin_url))
    assert not origin

    r = swh_storage.origin_visit_add(sample_origin, '2020-01-01', 'git')
    assert r == {'origin': origin_url, 'visit': 1}


def test_retrying_proxy_swh_storage_origin_visit_add_failure(
        swh_storage, sample_data, mocker):
    """Other errors are raising as usual

    """
    mock_memory = mocker.patch(
        'swh.storage.in_memory.Storage.origin_visit_add')
    mock_memory.side_effect = ValueError('Refuse to add origin always!')

    origin_url = sample_data['origin'][0]['url']

    origin = list(swh_storage.origin_visit_get(origin_url))
    assert not origin

    with pytest.raises(ValueError, match='Refuse to add'):
        swh_storage.origin_visit_add(origin_url, '2020-01-01', 'svn')


def test_retrying_proxy_storage_tool_add(swh_storage, sample_data):
    """Standard tool_add works as before

    """
    sample_tool = sample_data['tool'][0]

    tool = swh_storage.tool_get(sample_tool)
    assert not tool

    tools = swh_storage.tool_add([sample_tool])
    assert tools
    tool = tools[0]
    tool.pop('id')
    assert tool == sample_tool

    tool = swh_storage.tool_get(sample_tool)
    tool.pop('id')
    assert tool == sample_tool


def test_retrying_proxy_storage_tool_add_with_retry(
        swh_storage, sample_data, mocker):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_tool = sample_data['tool'][0]
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.tool_add')
    mock_memory.side_effect = [
        # first try goes ko
        HashCollision('tool hash collision'),
        # second try goes ko
        psycopg2.IntegrityError('tool already inserted'),
        # ok then!
        [sample_tool]
    ]

    tool = swh_storage.tool_get(sample_tool)
    assert not tool

    tools = swh_storage.tool_add([sample_tool])
    assert tools == [sample_tool]


def test_retrying_proxy_swh_storage_tool_add_failure(
        swh_storage, sample_data, mocker):
    """Other errors are raising as usual

    """
    mock_memory = mocker.patch('swh.storage.in_memory.Storage.tool_add')
    mock_memory.side_effect = ValueError('Refuse to add tool always!')

    sample_tool = sample_data['tool'][0]

    tool = swh_storage.tool_get(sample_tool)
    assert not tool

    with pytest.raises(ValueError, match='Refuse to add'):
        swh_storage.tool_add([sample_tool])

    tool = swh_storage.tool_get(sample_tool)
    assert not tool
