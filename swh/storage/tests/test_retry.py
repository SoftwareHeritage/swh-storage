# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict
from unittest.mock import call

import psycopg2
import pytest

from swh.model.model import (
    Content,
    Directory,
    Release,
    Revision,
    Snapshot,
    SkippedContent,
    Origin,
)

from swh.storage import get_storage
from swh.storage.exc import HashCollision, StorageArgumentException

from .storage_data import date_visit1


@pytest.fixture
def fake_hash_collision(sample_data):
    return HashCollision("sha1", "38762cf7f55934b34d179ae6a4c80cadccbb7f0a", [])


@pytest.fixture
def swh_storage():
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "validate"}, {"cls": "retry"}, {"cls": "memory"},],
    }
    return get_storage(**storage_config)


def test_retrying_proxy_storage_content_add(swh_storage, sample_data):
    """Standard content_add works as before

    """
    sample_content = sample_data["content"][0]

    content = next(swh_storage.content_get([sample_content["sha1"]]))
    assert not content

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": sample_content["length"],
    }

    content = next(swh_storage.content_get([sample_content["sha1"]]))
    assert content["sha1"] == sample_content["sha1"]


def test_retrying_proxy_storage_content_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("content already inserted"),
        # ok then!
        {"content:add": 1},
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".content_add.retry.sleep"
    )

    sample_content = sample_data["content"][0]

    content = next(swh_storage.content_get([sample_content["sha1"]]))
    assert not content

    s = swh_storage.content_add([sample_content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [
            call([Content.from_dict(sample_content)]),
            call([Content.from_dict(sample_content)]),
            call([Content.from_dict(sample_content)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_content_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add content always!")

    sample_content = sample_data["content"][0]

    content = next(swh_storage.content_get([sample_content["sha1"]]))
    assert not content

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.content_add([sample_content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_content_add_metadata(swh_storage, sample_data):
    """Standard content_add_metadata works as before

    """
    sample_content = sample_data["content_metadata"][0]

    pk = sample_content["sha1"]
    content_metadata = swh_storage.content_get_metadata([pk])
    assert not content_metadata[pk]

    s = swh_storage.content_add_metadata([sample_content])
    assert s == {
        "content:add": 1,
    }

    content_metadata = swh_storage.content_get_metadata([pk])
    assert len(content_metadata[pk]) == 1
    assert content_metadata[pk][0]["sha1"] == pk


def test_retrying_proxy_storage_content_add_metadata_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
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
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".content_add_metadata.retry.sleep"
    )

    sample_content = sample_data["content_metadata"][0]

    s = swh_storage.content_add_metadata([sample_content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [
            call([Content.from_dict(sample_content)]),
            call([Content.from_dict(sample_content)]),
            call([Content.from_dict(sample_content)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_content_add_metadata_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.content_add_metadata"
    )
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add content_metadata!"
    )

    sample_content = sample_data["content_metadata"][0]
    pk = sample_content["sha1"]

    content_metadata = swh_storage.content_get_metadata([pk])
    assert not content_metadata[pk]

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.content_add_metadata([sample_content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_skipped_content_add(swh_storage, sample_data):
    """Standard skipped_content_add works as before

    """
    sample_content = sample_data["skipped_content"][0]

    skipped_contents = list(swh_storage.skipped_content_missing([sample_content]))
    assert len(skipped_contents) == 1

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    skipped_content = list(swh_storage.skipped_content_missing([sample_content]))
    assert len(skipped_content) == 0


def test_retrying_proxy_storage_skipped_content_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.skipped_content_add"
    )
    mock_memory.side_effect = [
        # 1st & 2nd try goes ko
        fake_hash_collision,
        psycopg2.IntegrityError("skipped_content already inserted"),
        # ok then!
        {"skipped_content:add": 1},
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".skipped_content_add.retry.sleep"
    )

    sample_content = sample_data["skipped_content"][0]

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {"skipped_content:add": 1}

    mock_memory.assert_has_calls(
        [
            call([SkippedContent.from_dict(sample_content)]),
            call([SkippedContent.from_dict(sample_content)]),
            call([SkippedContent.from_dict(sample_content)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_skipped_content_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.skipped_content_add"
    )
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add content_metadata!"
    )

    sample_content = sample_data["skipped_content"][0]

    skipped_contents = list(swh_storage.skipped_content_missing([sample_content]))
    assert len(skipped_contents) == 1

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.skipped_content_add([sample_content])

    skipped_contents = list(swh_storage.skipped_content_missing([sample_content]))
    assert len(skipped_contents) == 1

    assert mock_memory.call_count == 1


def test_retrying_proxy_swh_storage_origin_add_one(swh_storage, sample_data):
    """Standard origin_add_one works as before

    """
    sample_origin = sample_data["origin"][0]

    origin = swh_storage.origin_get(sample_origin)
    assert not origin

    r = swh_storage.origin_add_one(sample_origin)
    assert r == sample_origin["url"]

    origin = swh_storage.origin_get(sample_origin)
    assert origin["url"] == sample_origin["url"]


def test_retrying_proxy_swh_storage_origin_add_one_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_origin = sample_data["origin"][1]
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.origin_add_one")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("origin already inserted"),
        # ok then!
        sample_origin["url"],
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".origin_add_one.retry.sleep"
    )

    origin = swh_storage.origin_get(sample_origin)
    assert not origin

    r = swh_storage.origin_add_one(sample_origin)
    assert r == sample_origin["url"]

    mock_memory.assert_has_calls(
        [
            call(Origin.from_dict(sample_origin)),
            call(Origin.from_dict(sample_origin)),
            call(Origin.from_dict(sample_origin)),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_origin_add_one_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.origin_add_one")
    mock_memory.side_effect = StorageArgumentException("Refuse to add origin always!")

    sample_origin = sample_data["origin"][0]

    origin = swh_storage.origin_get(sample_origin)
    assert not origin

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.origin_add_one(sample_origin)

    assert mock_memory.call_count == 1


def test_retrying_proxy_swh_storage_origin_visit_add(swh_storage, sample_data):
    """Standard origin_visit_add works as before

    """
    sample_origin = sample_data["origin"][0]
    origin_url = swh_storage.origin_add_one(sample_origin)

    origin = list(swh_storage.origin_visit_get(origin_url))
    assert not origin

    origin_visit = swh_storage.origin_visit_add(origin_url, date_visit1, "hg")
    assert origin_visit.origin == origin_url
    assert isinstance(origin_visit.visit, int)

    origin_visit = next(swh_storage.origin_visit_get(origin_url))
    assert origin_visit["origin"] == origin_url
    assert isinstance(origin_visit["visit"], int)


def test_retrying_proxy_swh_storage_origin_visit_add_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_origin = sample_data["origin"][1]
    origin_url = swh_storage.origin_add_one(sample_origin)

    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.origin_visit_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("origin already inserted"),
        # ok then!
        {"origin": origin_url, "visit": 1},
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".origin_visit_add.retry.sleep"
    )

    origin = list(swh_storage.origin_visit_get(origin_url))
    assert not origin

    r = swh_storage.origin_visit_add(origin_url, date_visit1, "git")
    assert r == {"origin": origin_url, "visit": 1}

    mock_memory.assert_has_calls(
        [
            call(origin_url, date_visit1, "git"),
            call(origin_url, date_visit1, "git"),
            call(origin_url, date_visit1, "git"),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_origin_visit_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.origin_visit_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add origin always!")

    origin_url = sample_data["origin"][0]["url"]

    origin = list(swh_storage.origin_visit_get(origin_url))
    assert not origin

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.origin_visit_add(origin_url, date_visit1, "svn")

    mock_memory.assert_has_calls(
        [call(origin_url, date_visit1, "svn"),]
    )


def test_retrying_proxy_storage_tool_add(swh_storage, sample_data):
    """Standard tool_add works as before

    """
    sample_tool = sample_data["tool"][0]

    tool = swh_storage.tool_get(sample_tool)
    assert not tool

    tools = swh_storage.tool_add([sample_tool])
    assert tools
    tool = tools[0]
    tool.pop("id")
    assert tool == sample_tool

    tool = swh_storage.tool_get(sample_tool)
    tool.pop("id")
    assert tool == sample_tool


def test_retrying_proxy_storage_tool_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_tool = sample_data["tool"][0]
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.tool_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("tool already inserted"),
        # ok then!
        [sample_tool],
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".tool_add.retry.sleep"
    )

    tool = swh_storage.tool_get(sample_tool)
    assert not tool

    tools = swh_storage.tool_add([sample_tool])
    assert tools == [sample_tool]

    mock_memory.assert_has_calls(
        [call([sample_tool]), call([sample_tool]), call([sample_tool]),]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_tool_add_failure(swh_storage, sample_data, mocker):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.tool_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add tool always!")

    sample_tool = sample_data["tool"][0]

    tool = swh_storage.tool_get(sample_tool)
    assert not tool

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.tool_add([sample_tool])

    assert mock_memory.call_count == 1


def to_provider(provider: Dict) -> Dict:
    return {
        "provider_name": provider["name"],
        "provider_url": provider["url"],
        "provider_type": provider["type"],
        "metadata": provider["metadata"],
    }


def test_retrying_proxy_storage_metadata_provider_add(swh_storage, sample_data):
    """Standard metadata_provider_add works as before

    """
    provider = sample_data["provider"][0]
    provider_get = to_provider(provider)

    provider = swh_storage.metadata_provider_get_by(provider_get)
    assert not provider

    provider_id = swh_storage.metadata_provider_add(**provider_get)
    assert provider_id

    actual_provider = swh_storage.metadata_provider_get(provider_id)
    assert actual_provider
    actual_provider_id = actual_provider.pop("id")
    assert actual_provider_id == provider_id
    assert actual_provider == provider_get


def test_retrying_proxy_storage_metadata_provider_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    provider = sample_data["provider"][0]
    provider_get = to_provider(provider)

    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.metadata_provider_add"
    )
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("provider_id already inserted"),
        # ok then!
        "provider_id",
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".metadata_provider_add.retry.sleep"
    )

    provider = swh_storage.metadata_provider_get_by(provider_get)
    assert not provider

    provider_id = swh_storage.metadata_provider_add(**provider_get)
    assert provider_id == "provider_id"

    provider_arg_names = ("provider_name", "provider_type", "provider_url", "metadata")
    provider_args = [provider_get[key] for key in provider_arg_names]
    mock_memory.assert_has_calls(
        [call(*provider_args), call(*provider_args), call(*provider_args),]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_metadata_provider_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.metadata_provider_add"
    )
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add provider_id always!"
    )

    provider = sample_data["provider"][0]
    provider_get = to_provider(provider)

    provider_id = swh_storage.metadata_provider_get_by(provider_get)
    assert not provider_id

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.metadata_provider_add(**provider_get)

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_origin_metadata_add(swh_storage, sample_data):
    """Standard origin_metadata_add works as before

    """
    ori_meta = sample_data["origin_metadata"][0]
    origin = ori_meta["origin"]
    swh_storage.origin_add_one(origin)
    provider_get = to_provider(ori_meta["provider"])
    provider_id = swh_storage.metadata_provider_add(**provider_get)

    origin_metadata = swh_storage.origin_metadata_get_by(origin["url"])
    assert not origin_metadata

    swh_storage.origin_metadata_add(
        origin["url"],
        ori_meta["discovery_date"],
        provider_id,
        ori_meta["tool"],
        ori_meta["metadata"],
    )

    origin_metadata = swh_storage.origin_metadata_get_by(origin["url"])
    assert origin_metadata


def test_retrying_proxy_storage_origin_metadata_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    ori_meta = sample_data["origin_metadata"][0]
    origin = ori_meta["origin"]
    swh_storage.origin_add_one(origin)
    provider_get = to_provider(ori_meta["provider"])
    provider_id = swh_storage.metadata_provider_add(**provider_get)
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.origin_metadata_add"
    )

    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("provider_id already inserted"),
        # ok then!
        None,
    ]

    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".origin_metadata_add.retry.sleep"
    )

    url = origin["url"]
    ts = ori_meta["discovery_date"]
    tool_id = ori_meta["tool"]
    metadata = ori_meta["metadata"]

    # No exception raised as insertion finally came through
    swh_storage.origin_metadata_add(url, ts, provider_id, tool_id, metadata)

    mock_memory.assert_has_calls(
        [  # 3 calls, as long as error raised
            call(url, ts, provider_id, tool_id, metadata),
            call(url, ts, provider_id, tool_id, metadata),
            call(url, ts, provider_id, tool_id, metadata),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_origin_metadata_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.origin_metadata_add"
    )
    mock_memory.side_effect = StorageArgumentException("Refuse to add always!")

    ori_meta = sample_data["origin_metadata"][0]
    origin = ori_meta["origin"]
    swh_storage.origin_add_one(origin)

    url = origin["url"]
    ts = ori_meta["discovery_date"]
    provider_id = "provider_id"
    tool_id = ori_meta["tool"]
    metadata = ori_meta["metadata"]

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.origin_metadata_add(url, ts, provider_id, tool_id, metadata)

    assert mock_memory.call_count == 1


def test_retrying_proxy_swh_storage_origin_visit_update(swh_storage, sample_data):
    """Standard origin_visit_update works as before

    """
    sample_origin = sample_data["origin"][0]
    origin_url = swh_storage.origin_add_one(sample_origin)
    origin_visit = swh_storage.origin_visit_add(origin_url, date_visit1, "hg")

    ov = next(swh_storage.origin_visit_get(origin_url))
    assert ov["origin"] == origin_url
    assert ov["visit"] == origin_visit.visit
    assert ov["status"] == "ongoing"
    assert ov["snapshot"] is None
    assert ov["metadata"] is None

    swh_storage.origin_visit_update(origin_url, origin_visit.visit, status="full")

    ov = next(swh_storage.origin_visit_get(origin_url))
    assert ov["origin"] == origin_url
    assert ov["visit"] == origin_visit.visit
    assert ov["status"] == "full"
    assert ov["snapshot"] is None
    assert ov["metadata"] is None


def test_retrying_proxy_swh_storage_origin_visit_update_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    sample_origin = sample_data["origin"][1]
    origin_url = sample_origin["url"]

    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.origin_visit_update"
    )
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("origin already inserted"),
        # ok then!
        {"origin": origin_url, "visit": 1},
    ]

    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".origin_visit_update.retry.sleep"
    )

    visit_id = 1
    swh_storage.origin_visit_update(origin_url, visit_id, status="full")

    mock_memory.assert_has_calls(
        [
            call(origin_url, visit_id, metadata=None, snapshot=None, status="full"),
            call(origin_url, visit_id, metadata=None, snapshot=None, status="full"),
            call(origin_url, visit_id, metadata=None, snapshot=None, status="full"),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_origin_visit_update_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.origin_visit_update"
    )
    mock_memory.side_effect = StorageArgumentException("Refuse to add origin always!")
    origin_url = sample_data["origin"][0]["url"]
    visit_id = 9

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.origin_visit_update(origin_url, visit_id, "partial")

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_directory_add(swh_storage, sample_data):
    """Standard directory_add works as before

    """
    sample_dir = sample_data["directory"][0]

    directory = swh_storage.directory_get_random()  # no directory
    assert not directory

    s = swh_storage.directory_add([sample_dir])
    assert s == {
        "directory:add": 1,
    }

    directory_id = swh_storage.directory_get_random()  # only 1
    assert directory_id == sample_dir["id"]


def test_retrying_proxy_storage_directory_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.directory_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("directory already inserted"),
        # ok then!
        {"directory:add": 1},
    ]
    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".directory_add.retry.sleep"
    )

    sample_dir = sample_data["directory"][1]

    directory_id = swh_storage.directory_get_random()  # no directory
    assert not directory_id

    s = swh_storage.directory_add([sample_dir])
    assert s == {
        "directory:add": 1,
    }

    mock_memory.assert_has_calls(
        [
            call([Directory.from_dict(sample_dir)]),
            call([Directory.from_dict(sample_dir)]),
            call([Directory.from_dict(sample_dir)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_directory_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.directory_add")
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add directory always!"
    )

    sample_dir = sample_data["directory"][0]

    directory_id = swh_storage.directory_get_random()  # no directory
    assert not directory_id

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.directory_add([sample_dir])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_revision_add(swh_storage, sample_data):
    """Standard revision_add works as before

    """
    sample_rev = sample_data["revision"][0]

    revision = next(swh_storage.revision_get([sample_rev["id"]]))
    assert not revision

    s = swh_storage.revision_add([sample_rev])
    assert s == {
        "revision:add": 1,
    }

    revision = next(swh_storage.revision_get([sample_rev["id"]]))
    assert revision["id"] == sample_rev["id"]


def test_retrying_proxy_storage_revision_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.revision_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("revision already inserted"),
        # ok then!
        {"revision:add": 1},
    ]

    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".revision_add.retry.sleep"
    )

    sample_rev = sample_data["revision"][0]

    revision = next(swh_storage.revision_get([sample_rev["id"]]))
    assert not revision

    s = swh_storage.revision_add([sample_rev])
    assert s == {
        "revision:add": 1,
    }

    mock_memory.assert_has_calls(
        [
            call([Revision.from_dict(sample_rev)]),
            call([Revision.from_dict(sample_rev)]),
            call([Revision.from_dict(sample_rev)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_revision_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.revision_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add revision always!")

    sample_rev = sample_data["revision"][0]

    revision = next(swh_storage.revision_get([sample_rev["id"]]))
    assert not revision

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.revision_add([sample_rev])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_release_add(swh_storage, sample_data):
    """Standard release_add works as before

    """
    sample_rel = sample_data["release"][0]

    release = next(swh_storage.release_get([sample_rel["id"]]))
    assert not release

    s = swh_storage.release_add([sample_rel])
    assert s == {
        "release:add": 1,
    }

    release = next(swh_storage.release_get([sample_rel["id"]]))
    assert release["id"] == sample_rel["id"]


def test_retrying_proxy_storage_release_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.release_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("release already inserted"),
        # ok then!
        {"release:add": 1},
    ]

    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".release_add.retry.sleep"
    )

    sample_rel = sample_data["release"][0]

    release = next(swh_storage.release_get([sample_rel["id"]]))
    assert not release

    s = swh_storage.release_add([sample_rel])
    assert s == {
        "release:add": 1,
    }

    mock_memory.assert_has_calls(
        [
            call([Release.from_dict(sample_rel)]),
            call([Release.from_dict(sample_rel)]),
            call([Release.from_dict(sample_rel)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_release_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.release_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add release always!")

    sample_rel = sample_data["release"][0]

    release = next(swh_storage.release_get([sample_rel["id"]]))
    assert not release

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.release_add([sample_rel])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_snapshot_add(swh_storage, sample_data):
    """Standard snapshot_add works as before

    """
    sample_snap = sample_data["snapshot"][0]

    snapshot = swh_storage.snapshot_get(sample_snap["id"])
    assert not snapshot

    s = swh_storage.snapshot_add([sample_snap])
    assert s == {
        "snapshot:add": 1,
    }

    snapshot = swh_storage.snapshot_get(sample_snap["id"])
    assert snapshot["id"] == sample_snap["id"]


def test_retrying_proxy_storage_snapshot_add_with_retry(
    swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.snapshot_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("snapshot already inserted"),
        # ok then!
        {"snapshot:add": 1},
    ]

    mock_sleep = mocker.patch(
        "swh.storage.retry.RetryingProxyStorage" ".snapshot_add.retry.sleep"
    )

    sample_snap = sample_data["snapshot"][0]

    snapshot = swh_storage.snapshot_get(sample_snap["id"])
    assert not snapshot

    s = swh_storage.snapshot_add([sample_snap])
    assert s == {
        "snapshot:add": 1,
    }

    mock_memory.assert_has_calls(
        [
            call([Snapshot.from_dict(sample_snap)]),
            call([Snapshot.from_dict(sample_snap)]),
            call([Snapshot.from_dict(sample_snap)]),
        ]
    )
    assert mock_sleep.call_count == 2


def test_retrying_proxy_swh_storage_snapshot_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.snapshot_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add snapshot always!")

    sample_snap = sample_data["snapshot"][0]

    snapshot = swh_storage.snapshot_get(sample_snap["id"])
    assert not snapshot

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.snapshot_add([sample_snap])

    assert mock_memory.call_count == 1
