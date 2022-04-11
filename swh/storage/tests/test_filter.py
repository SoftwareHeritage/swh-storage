# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import Mock

import attr
import pytest

from swh.storage import get_storage


@pytest.fixture
def swh_storage():
    storage_config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "filter"},
            {"cls": "memory"},
        ],
    }

    return get_storage(**storage_config)


def test_filtering_proxy_storage_content(swh_storage, sample_data):
    sample_content = sample_data.content

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is None

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": sample_content.length,
    }

    content = swh_storage.content_get_data(sample_content.sha1)
    assert content is not None

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 0,
        "content:add:bytes": 0,
    }


def test_filtering_proxy_storage_skipped_content(swh_storage, sample_data):
    sample_content = sample_data.skipped_content
    sample_content_dict = sample_content.to_dict()

    content = next(swh_storage.skipped_content_missing([sample_content_dict]))
    assert content["sha1"] == sample_content.sha1

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(swh_storage.skipped_content_missing([sample_content_dict]))
    assert content == []

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 0,
    }


def test_filtering_proxy_storage_skipped_content_missing_sha1_git(
    swh_storage, sample_data
):
    sample_contents = [
        attr.evolve(c, sha1_git=None) for c in sample_data.skipped_contents
    ]
    sample_content, sample_content2 = [c.to_dict() for c in sample_contents[:2]]

    content = next(swh_storage.skipped_content_missing([sample_content]))
    assert content["sha1"] == sample_content["sha1"]

    s = swh_storage.skipped_content_add([sample_contents[0]])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(swh_storage.skipped_content_missing([sample_content]))
    assert content == []

    s = swh_storage.skipped_content_add([sample_contents[1]])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(swh_storage.skipped_content_missing([sample_content2]))
    assert content == []


def test_filtering_proxy_storage_revision(swh_storage, sample_data):
    sample_revision = sample_data.revision

    revision = swh_storage.revision_get([sample_revision.id])[0]
    assert revision is None

    s = swh_storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 1,
    }

    revision = swh_storage.revision_get([sample_revision.id])[0]
    assert revision is not None

    s = swh_storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 0,
    }


def test_filtering_proxy_storage_release(swh_storage, sample_data):
    sample_release = sample_data.release

    release = swh_storage.release_get([sample_release.id])[0]
    assert release is None

    s = swh_storage.release_add([sample_release])
    assert s == {
        "release:add": 1,
    }

    release = swh_storage.release_get([sample_release.id])[0]
    assert release is not None

    s = swh_storage.release_add([sample_release])
    assert s == {
        "release:add": 0,
    }


def test_filtering_proxy_storage_directory(swh_storage, sample_data):
    sample_directory = sample_data.directory

    directory = list(swh_storage.directory_missing([sample_directory.id]))[0]
    assert directory

    s = swh_storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 1,
    }

    directory = list(swh_storage.directory_missing([sample_directory.id]))
    assert not directory

    s = swh_storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 0,
    }


def test_filtering_proxy_storage_empty_list(swh_storage, sample_data):
    swh_storage.storage = mock_storage = Mock(wraps=swh_storage.storage)

    calls = 0
    for object_type in swh_storage.object_types:
        calls += 1

        method_name = f"{object_type}_add"
        method = getattr(swh_storage, method_name)
        one_object = getattr(sample_data, object_type)

        # Call with empty list: ensure underlying storage not called
        method([])
        assert method_name not in {c[0] for c in mock_storage.method_calls}
        mock_storage.reset_mock()

        # Call with an object: ensure underlying storage is called
        method([one_object])
        assert method_name in {c[0] for c in mock_storage.method_calls}
        mock_storage.reset_mock()

        # Call with the same object: ensure underlying storage is not called again
        method([one_object])
        assert method_name not in {c[0] for c in mock_storage.method_calls}
        mock_storage.reset_mock()

    assert calls > 0, "Empty list never tested"
