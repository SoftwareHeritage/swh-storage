# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import attr

from unittest.mock import call

import psycopg2
import pytest

from swh.model.model import MetadataTargetType

from swh.storage.exc import HashCollision, StorageArgumentException
from swh.storage.utils import now


@pytest.fixture
def monkeypatch_sleep(monkeypatch, swh_storage):
    """In test context, we don't want to wait, make test faster

    """
    from swh.storage.retry import RetryingProxyStorage

    for method_name, method in RetryingProxyStorage.__dict__.items():
        if "_add" in method_name or "_update" in method_name:
            monkeypatch.setattr(method.retry, "sleep", lambda x: None)

    return monkeypatch


@pytest.fixture
def fake_hash_collision(sample_data):
    return HashCollision("sha1", "38762cf7f55934b34d179ae6a4c80cadccbb7f0a", [])


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "pipeline",
        "steps": [{"cls": "retry"}, {"cls": "memory"},],
    }


def test_retrying_proxy_storage_content_add(swh_storage, sample_data):
    """Standard content_add works as before

    """
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
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision,
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

    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    s = swh_storage.content_add([sample_content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [call([sample_content]), call([sample_content]), call([sample_content]),]
    )


def test_retrying_proxy_swh_storage_content_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.content_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add content always!")

    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.content_add([sample_content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_content_add_metadata(swh_storage, sample_data):
    """Standard content_add_metadata works as before

    """
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
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
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

    sample_content = sample_data.content
    content = attr.evolve(sample_content, data=None)

    s = swh_storage.content_add_metadata([content])
    assert s == {"content:add": 1}

    mock_memory.assert_has_calls(
        [call([content]), call([content]), call([content]),]
    )


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

    sample_content = sample_data.content
    content = attr.evolve(sample_content, data=None)

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.content_add_metadata([content])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_skipped_content_add(swh_storage, sample_data):
    """Standard skipped_content_add works as before

    """
    sample_content = sample_data.skipped_content
    sample_content_dict = sample_content.to_dict()

    skipped_contents = list(swh_storage.skipped_content_missing([sample_content_dict]))
    assert len(skipped_contents) == 1

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    skipped_content = list(swh_storage.skipped_content_missing([sample_content_dict]))
    assert len(skipped_content) == 0


def test_retrying_proxy_storage_skipped_content_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
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

    sample_content = sample_data.skipped_content

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {"skipped_content:add": 1}

    mock_memory.assert_has_calls(
        [call([sample_content]), call([sample_content]), call([sample_content]),]
    )


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

    sample_content = sample_data.skipped_content
    sample_content_dict = sample_content.to_dict()

    skipped_contents = list(swh_storage.skipped_content_missing([sample_content_dict]))
    assert len(skipped_contents) == 1

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.skipped_content_add([sample_content])

    skipped_contents = list(swh_storage.skipped_content_missing([sample_content_dict]))
    assert len(skipped_contents) == 1

    assert mock_memory.call_count == 1


def test_retrying_proxy_swh_storage_origin_visit_add(swh_storage, sample_data):
    """Standard origin_visit_add works as before

    """
    origin = sample_data.origin
    visit = sample_data.origin_visit
    assert visit.origin == origin.url

    swh_storage.origin_add([origin])

    origins = swh_storage.origin_visit_get(origin.url).results
    assert not origins

    origin_visit = swh_storage.origin_visit_add([visit])[0]
    assert origin_visit.origin == origin.url
    assert isinstance(origin_visit.visit, int)

    actual_visit = swh_storage.origin_visit_get(origin.url).results[0]
    assert actual_visit == visit


def test_retrying_proxy_swh_storage_origin_visit_add_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    origin = sample_data.origin
    visit = sample_data.origin_visit
    assert visit.origin == origin.url

    swh_storage.origin_add([origin])

    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.origin_visit_add")
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("origin already inserted"),
        # ok then!
        [visit],
    ]

    origins = swh_storage.origin_visit_get(origin.url).results
    assert not origins

    r = swh_storage.origin_visit_add([visit])
    assert r == [visit]

    mock_memory.assert_has_calls(
        [call([visit]), call([visit]), call([visit]),]
    )


def test_retrying_proxy_swh_storage_origin_visit_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.origin_visit_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add origin always!")

    origin = sample_data.origin
    visit = sample_data.origin_visit
    assert visit.origin == origin.url

    origins = swh_storage.origin_visit_get(origin.url).results
    assert not origins

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.origin_visit_add([visit])

    mock_memory.assert_has_calls(
        [call([visit]),]
    )


def test_retrying_proxy_storage_metadata_fetcher_add(swh_storage, sample_data):
    """Standard metadata_fetcher_add works as before

    """
    fetcher = sample_data.metadata_fetcher

    metadata_fetcher = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
    assert not metadata_fetcher

    swh_storage.metadata_fetcher_add([fetcher])

    actual_fetcher = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
    assert actual_fetcher == fetcher


def test_retrying_proxy_storage_metadata_fetcher_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision,
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    fetcher = sample_data.metadata_fetcher
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.metadata_fetcher_add"
    )
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("metadata_fetcher already inserted"),
        # ok then!
        [fetcher],
    ]

    actual_fetcher = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
    assert not actual_fetcher

    swh_storage.metadata_fetcher_add([fetcher])

    mock_memory.assert_has_calls(
        [call([fetcher]), call([fetcher]), call([fetcher]),]
    )


def test_retrying_proxy_swh_storage_metadata_fetcher_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.metadata_fetcher_add"
    )
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add metadata_fetcher always!"
    )

    fetcher = sample_data.metadata_fetcher

    actual_fetcher = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
    assert not actual_fetcher

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.metadata_fetcher_add([fetcher])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_metadata_authority_add(swh_storage, sample_data):
    """Standard metadata_authority_add works as before

    """
    authority = sample_data.metadata_authority

    assert not swh_storage.metadata_authority_get(authority.type, authority.url)

    swh_storage.metadata_authority_add([authority])

    actual_authority = swh_storage.metadata_authority_get(authority.type, authority.url)
    assert actual_authority == authority


def test_retrying_proxy_storage_metadata_authority_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision,
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    authority = sample_data.metadata_authority

    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.metadata_authority_add"
    )
    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("foo bar"),
        # ok then!
        None,
    ]

    assert not swh_storage.metadata_authority_get(authority.type, authority.url)

    swh_storage.metadata_authority_add([authority])

    mock_memory.assert_has_calls(
        [call([authority]), call([authority]), call([authority])]
    )


def test_retrying_proxy_swh_storage_metadata_authority_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.metadata_authority_add"
    )
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add authority_id always!"
    )

    authority = sample_data.metadata_authority

    swh_storage.metadata_authority_get(authority.type, authority.url)

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.metadata_authority_add([authority])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_raw_extrinsic_metadata_add(swh_storage, sample_data):
    """Standard raw_extrinsic_metadata_add works as before

    """
    origin = sample_data.origin
    ori_meta = sample_data.origin_metadata1
    assert origin.url == ori_meta.id
    swh_storage.origin_add([origin])
    swh_storage.metadata_authority_add([sample_data.metadata_authority])
    swh_storage.metadata_fetcher_add([sample_data.metadata_fetcher])

    origin_metadata = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.ORIGIN, ori_meta.id, ori_meta.authority
    )
    assert origin_metadata.next_page_token is None
    assert not origin_metadata.results

    swh_storage.raw_extrinsic_metadata_add([ori_meta])

    origin_metadata = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.ORIGIN, ori_meta.id, ori_meta.authority
    )
    assert origin_metadata


def test_retrying_proxy_storage_raw_extrinsic_metadata_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision,
):
    """Multiple retries for hash collision and psycopg2 error but finally ok

    """
    origin = sample_data.origin
    ori_meta = sample_data.origin_metadata1
    assert origin.url == ori_meta.id
    swh_storage.origin_add([origin])
    swh_storage.metadata_authority_add([sample_data.metadata_authority])
    swh_storage.metadata_fetcher_add([sample_data.metadata_fetcher])
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.raw_extrinsic_metadata_add"
    )

    mock_memory.side_effect = [
        # first try goes ko
        fake_hash_collision,
        # second try goes ko
        psycopg2.IntegrityError("foo bar"),
        # ok then!
        None,
    ]

    # No exception raised as insertion finally came through
    swh_storage.raw_extrinsic_metadata_add([ori_meta])

    mock_memory.assert_has_calls(
        [  # 3 calls, as long as error raised
            call([ori_meta]),
            call([ori_meta]),
            call([ori_meta]),
        ]
    )


def test_retrying_proxy_swh_storage_raw_extrinsic_metadata_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch(
        "swh.storage.in_memory.InMemoryStorage.raw_extrinsic_metadata_add"
    )
    mock_memory.side_effect = StorageArgumentException("Refuse to add always!")

    origin = sample_data.origin
    ori_meta = sample_data.origin_metadata1
    assert origin.url == ori_meta.id
    swh_storage.origin_add([origin])

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.raw_extrinsic_metadata_add([ori_meta])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_directory_add(swh_storage, sample_data):
    """Standard directory_add works as before

    """
    sample_dir = sample_data.directory

    s = swh_storage.directory_add([sample_dir])
    assert s == {
        "directory:add": 1,
    }

    directory_id = swh_storage.directory_get_random()  # only 1
    assert directory_id == sample_dir.id


def test_retrying_proxy_storage_directory_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
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

    sample_dir = sample_data.directories[1]

    s = swh_storage.directory_add([sample_dir])
    assert s == {
        "directory:add": 1,
    }

    mock_memory.assert_has_calls(
        [call([sample_dir]), call([sample_dir]), call([sample_dir]),]
    )


def test_retrying_proxy_swh_storage_directory_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.directory_add")
    mock_memory.side_effect = StorageArgumentException(
        "Refuse to add directory always!"
    )

    sample_dir = sample_data.directory

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.directory_add([sample_dir])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_revision_add(swh_storage, sample_data):
    """Standard revision_add works as before

    """
    sample_rev = sample_data.revision

    revision = next(swh_storage.revision_get([sample_rev.id]))
    assert not revision

    s = swh_storage.revision_add([sample_rev])
    assert s == {
        "revision:add": 1,
    }

    revision = next(swh_storage.revision_get([sample_rev.id]))
    assert revision["id"] == sample_rev.id


def test_retrying_proxy_storage_revision_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
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

    sample_rev = sample_data.revision

    revision = next(swh_storage.revision_get([sample_rev.id]))
    assert not revision

    s = swh_storage.revision_add([sample_rev])
    assert s == {
        "revision:add": 1,
    }

    mock_memory.assert_has_calls(
        [call([sample_rev]), call([sample_rev]), call([sample_rev]),]
    )


def test_retrying_proxy_swh_storage_revision_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.revision_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add revision always!")

    sample_rev = sample_data.revision

    revision = next(swh_storage.revision_get([sample_rev.id]))
    assert not revision

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.revision_add([sample_rev])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_release_add(swh_storage, sample_data):
    """Standard release_add works as before

    """
    sample_rel = sample_data.release

    release = next(swh_storage.release_get([sample_rel.id]))
    assert not release

    s = swh_storage.release_add([sample_rel])
    assert s == {
        "release:add": 1,
    }

    release = next(swh_storage.release_get([sample_rel.id]))
    assert release["id"] == sample_rel.id


def test_retrying_proxy_storage_release_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
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

    sample_rel = sample_data.release

    release = next(swh_storage.release_get([sample_rel.id]))
    assert not release

    s = swh_storage.release_add([sample_rel])
    assert s == {
        "release:add": 1,
    }

    mock_memory.assert_has_calls(
        [call([sample_rel]), call([sample_rel]), call([sample_rel]),]
    )


def test_retrying_proxy_swh_storage_release_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.release_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add release always!")

    sample_rel = sample_data.release

    release = next(swh_storage.release_get([sample_rel.id]))
    assert not release

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.release_add([sample_rel])

    assert mock_memory.call_count == 1


def test_retrying_proxy_storage_snapshot_add(swh_storage, sample_data):
    """Standard snapshot_add works as before

    """
    sample_snap = sample_data.snapshot

    snapshot = swh_storage.snapshot_get(sample_snap.id)
    assert not snapshot

    s = swh_storage.snapshot_add([sample_snap])
    assert s == {
        "snapshot:add": 1,
    }

    snapshot = swh_storage.snapshot_get(sample_snap.id)
    assert snapshot["id"] == sample_snap.id


def test_retrying_proxy_storage_snapshot_add_with_retry(
    monkeypatch_sleep, swh_storage, sample_data, mocker, fake_hash_collision
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

    sample_snap = sample_data.snapshot

    snapshot = swh_storage.snapshot_get(sample_snap.id)
    assert not snapshot

    s = swh_storage.snapshot_add([sample_snap])
    assert s == {
        "snapshot:add": 1,
    }

    mock_memory.assert_has_calls(
        [call([sample_snap]), call([sample_snap]), call([sample_snap]),]
    )


def test_retrying_proxy_swh_storage_snapshot_add_failure(
    swh_storage, sample_data, mocker
):
    """Unfiltered errors are raising without retry

    """
    mock_memory = mocker.patch("swh.storage.in_memory.InMemoryStorage.snapshot_add")
    mock_memory.side_effect = StorageArgumentException("Refuse to add snapshot always!")

    sample_snap = sample_data.snapshot

    snapshot = swh_storage.snapshot_get(sample_snap.id)
    assert not snapshot

    with pytest.raises(StorageArgumentException, match="Refuse to add"):
        swh_storage.snapshot_add([sample_snap])

    assert mock_memory.call_count == 1
