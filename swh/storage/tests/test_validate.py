# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import attr
import pytest

from swh.model.hashutil import hash_to_hex
from swh.storage import get_storage
from swh.storage.exc import StorageArgumentException


@pytest.fixture
def swh_storage():
    storage_config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "validate"},
            {"cls": "memory"},
        ],
    }

    return get_storage(**storage_config)


def test_validating_proxy_storage_content(swh_storage, sample_data):
    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    with pytest.raises(StorageArgumentException, match="hashes"):
        s = swh_storage.content_add([attr.evolve(sample_content, sha1=b"a" * 20)])

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": sample_content.length,
    }

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is not None


def test_validating_proxy_storage_skipped_content(swh_storage, sample_data):
    sample_content = sample_data.skipped_content
    sample_content = attr.evolve(sample_content, sha1=b"a" * 20)
    sample_content_dict = sample_content.to_dict()

    s = swh_storage.skipped_content_add([sample_content])

    content = list(swh_storage.skipped_content_missing([sample_content_dict]))
    assert content == []

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 0,
    }


def test_validating_proxy_storage_directory(swh_storage, sample_data):
    sample_directory = sample_data.directory
    id_ = hash_to_hex(sample_directory.id)

    assert swh_storage.directory_missing([sample_directory.id]) == [sample_directory.id]

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.directory_add([attr.evolve(sample_directory, id=b"a" * 20)])

    assert swh_storage.directory_missing([sample_directory.id]) == [sample_directory.id]

    s = swh_storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 1,
    }

    assert swh_storage.directory_missing([sample_directory.id]) == []


def test_validating_proxy_storage_revision(swh_storage, sample_data):
    sample_revision = sample_data.revision
    id_ = hash_to_hex(sample_revision.id)

    assert swh_storage.revision_missing([sample_revision.id]) == [sample_revision.id]

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.revision_add([attr.evolve(sample_revision, id=b"a" * 20)])

    assert swh_storage.revision_missing([sample_revision.id]) == [sample_revision.id]

    s = swh_storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 1,
    }

    assert swh_storage.revision_missing([sample_revision.id]) == []


def test_validating_proxy_storage_release(swh_storage, sample_data):
    sample_release = sample_data.release
    id_ = hash_to_hex(sample_release.id)

    assert swh_storage.release_missing([sample_release.id]) == [sample_release.id]

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.release_add([attr.evolve(sample_release, id=b"a" * 20)])

    assert swh_storage.release_missing([sample_release.id]) == [sample_release.id]

    s = swh_storage.release_add([sample_release])
    assert s == {
        "release:add": 1,
    }

    assert swh_storage.release_missing([sample_release.id]) == []


def test_validating_proxy_storage_snapshot(swh_storage, sample_data):
    sample_snapshot = sample_data.snapshot
    id_ = hash_to_hex(sample_snapshot.id)

    assert swh_storage.snapshot_missing([sample_snapshot.id]) == [sample_snapshot.id]

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.snapshot_add([attr.evolve(sample_snapshot, id=b"a" * 20)])

    assert swh_storage.snapshot_missing([sample_snapshot.id]) == [sample_snapshot.id]

    s = swh_storage.snapshot_add([sample_snapshot])
    assert s == {
        "snapshot:add": 1,
    }

    assert swh_storage.snapshot_missing([sample_snapshot.id]) == []


def test_validating_proxy_storage_origin(swh_storage, sample_data):
    sample = sample_data.origin
    sample.compute_hash()
    id_ = hash_to_hex(sample.id)

    assert list(swh_storage.origin_get_by_sha1([sample.id])) == [None]

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.origin_add([attr.evolve(sample, id=b"a" * 20)])

    assert list(swh_storage.origin_get_by_sha1([sample.id])) == [None]

    s = swh_storage.origin_add([sample])
    assert s == {
        "origin:add": 1,
    }

    assert list(swh_storage.origin_get_by_sha1([sample.id])) == [{"url": sample.url}]


def test_validating_proxy_storage_raw_extrinsic_metadata(swh_storage, sample_data):
    sample = sample_data.origin_metadata1
    swh_storage.metadata_fetcher_add([sample.fetcher])
    swh_storage.metadata_authority_add([sample.authority])
    sample.compute_hash()
    id_ = hash_to_hex(sample.id)

    # XXX: yes this API is inconsistent with most other xxx_get() methods
    assert list(swh_storage.raw_extrinsic_metadata_get_by_ids([sample.id])) == []

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.raw_extrinsic_metadata_add([attr.evolve(sample, id=b"a" * 20)])

    assert list(swh_storage.raw_extrinsic_metadata_get_by_ids([sample.id])) == []

    s = swh_storage.raw_extrinsic_metadata_add([sample])
    assert s == {
        "ori_metadata:add": 1,
    }

    assert list(swh_storage.raw_extrinsic_metadata_get_by_ids([sample.id])) == [sample]


def test_validating_proxy_storage_extid(swh_storage, sample_data):
    sample = sample_data.extid1
    sample.compute_hash()
    id_ = hash_to_hex(sample.id)

    assert (
        list(swh_storage.extid_get_from_extid(sample.extid_type, [sample.extid])) == []
    )

    with pytest.raises(StorageArgumentException, match=f"should be {id_}"):
        s = swh_storage.extid_add([attr.evolve(sample, id=b"a" * 20)])

    assert (
        list(swh_storage.extid_get_from_extid(sample.extid_type, [sample.extid])) == []
    )

    s = swh_storage.extid_add([sample])
    assert s == {
        "extid:add": 1,
    }

    assert list(
        swh_storage.extid_get_from_extid(sample.extid_type, [sample.extid])
    ) == [sample]
