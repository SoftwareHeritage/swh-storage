# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.storage import get_storage


@pytest.fixture
def swh_storage():
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "validate"}, {"cls": "filter"}, {"cls": "memory"},],
    }

    return get_storage(**storage_config)


def test_filtering_proxy_storage_content(swh_storage, sample_data):
    sample_content = sample_data["content"][0]

    content = next(swh_storage.content_get([sample_content["sha1"]]))
    assert not content

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": sample_content["length"],
    }

    content = next(swh_storage.content_get([sample_content["sha1"]]))
    assert content is not None

    s = swh_storage.content_add([sample_content])
    assert s == {
        "content:add": 0,
        "content:add:bytes": 0,
    }


def test_filtering_proxy_storage_skipped_content(swh_storage, sample_data):
    sample_content = sample_data["skipped_content"][0]

    content = next(swh_storage.skipped_content_missing([sample_content]))
    assert content["sha1"] == sample_content["sha1"]

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(swh_storage.skipped_content_missing([sample_content]))
    assert content == []

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 0,
    }


def test_filtering_proxy_storage_skipped_content_missing_sha1_git(
    swh_storage, sample_data
):
    sample_content = sample_data["skipped_content"][0]
    sample_content2 = sample_data["skipped_content"][1]

    sample_content["sha1_git"] = sample_content2["sha1_git"] = None
    content = next(swh_storage.skipped_content_missing([sample_content]))
    assert content["sha1"] == sample_content["sha1"]

    s = swh_storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(swh_storage.skipped_content_missing([sample_content]))
    assert content == []

    s = swh_storage.skipped_content_add([sample_content2])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(swh_storage.skipped_content_missing([sample_content2]))
    assert content == []


def test_filtering_proxy_storage_revision(swh_storage, sample_data):
    sample_revision = sample_data["revision"][0]

    revision = next(swh_storage.revision_get([sample_revision["id"]]))
    assert not revision

    s = swh_storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 1,
    }

    revision = next(swh_storage.revision_get([sample_revision["id"]]))
    assert revision is not None

    s = swh_storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 0,
    }


def test_filtering_proxy_storage_directory(swh_storage, sample_data):
    sample_directory = sample_data["directory"][0]

    directory = next(swh_storage.directory_missing([sample_directory["id"]]))
    assert directory

    s = swh_storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 1,
    }

    directory = list(swh_storage.directory_missing([sample_directory["id"]]))
    assert not directory

    s = swh_storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 0,
    }
