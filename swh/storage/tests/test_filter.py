# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.storage import get_storage


storage_config = {
    "cls": "pipeline",
    "steps": [{"cls": "validate"}, {"cls": "filter"}, {"cls": "memory"},],
}


def test_filtering_proxy_storage_content(sample_data):
    sample_content = sample_data["content"][0]
    storage = get_storage(**storage_config)

    content = next(storage.content_get([sample_content["sha1"]]))
    assert not content

    s = storage.content_add([sample_content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": sample_content["length"],
    }

    content = next(storage.content_get([sample_content["sha1"]]))
    assert content is not None

    s = storage.content_add([sample_content])
    assert s == {
        "content:add": 0,
        "content:add:bytes": 0,
    }


def test_filtering_proxy_storage_skipped_content(sample_data):
    sample_content = sample_data["skipped_content"][0]
    storage = get_storage(**storage_config)

    content = next(storage.skipped_content_missing([sample_content]))
    assert content["sha1"] == sample_content["sha1"]

    s = storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(storage.skipped_content_missing([sample_content]))
    assert content == []

    s = storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 0,
    }


def test_filtering_proxy_storage_skipped_content_missing_sha1_git(sample_data):
    sample_content = sample_data["skipped_content"][0]
    sample_content2 = sample_data["skipped_content"][1]
    storage = get_storage(**storage_config)

    sample_content["sha1_git"] = sample_content2["sha1_git"] = None
    content = next(storage.skipped_content_missing([sample_content]))
    assert content["sha1"] == sample_content["sha1"]

    s = storage.skipped_content_add([sample_content])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(storage.skipped_content_missing([sample_content]))
    assert content == []

    s = storage.skipped_content_add([sample_content2])
    assert s == {
        "skipped_content:add": 1,
    }

    content = list(storage.skipped_content_missing([sample_content2]))
    assert content == []


def test_filtering_proxy_storage_revision(sample_data):
    sample_revision = sample_data["revision"][0]
    storage = get_storage(**storage_config)

    revision = next(storage.revision_get([sample_revision["id"]]))
    assert not revision

    s = storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 1,
    }

    revision = next(storage.revision_get([sample_revision["id"]]))
    assert revision is not None

    s = storage.revision_add([sample_revision])
    assert s == {
        "revision:add": 0,
    }


def test_filtering_proxy_storage_directory(sample_data):
    sample_directory = sample_data["directory"][0]
    storage = get_storage(**storage_config)

    directory = next(storage.directory_missing([sample_directory["id"]]))
    assert directory

    s = storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 1,
    }

    directory = list(storage.directory_missing([sample_directory["id"]]))
    assert not directory

    s = storage.directory_add([sample_directory])
    assert s == {
        "directory:add": 0,
    }


def test_filtering_proxy_storage_clear(sample_data):
    """Clear operation on filter proxy

    """
    threshold = 10
    contents = sample_data['content']
    assert 0 < len(contents) < threshold
    skipped_contents = sample_data['skipped_content']
    assert 0 < len(skipped_contents) < threshold
    directories = sample_data['directory']
    assert 0 < len(directories) < threshold
    revisions = sample_data['revision']
    assert 0 < len(revisions) < threshold
    releases = sample_data['release']
    assert 0 < len(releases) < threshold

    storage = get_storage(**storage_config)

    s = storage.content_add(contents)
    assert s['content:add'] == len(contents)
    s = storage.skipped_content_add(skipped_contents)
    assert s == {
        'skipped_content:add': len(directories),
    }
    s = storage.directory_add(directories)
    assert s == {
        'directory:add': len(directories),
    }
    s = storage.revision_add(revisions)
    assert s == {
        'revision:add': len(revisions),
    }

    assert len(storage.objects_seen['content']) == len(contents)
    assert len(storage.objects_seen['skipped_content']) == len(
        skipped_contents)
    assert len(storage.objects_seen['directory']) == len(directories)
    assert len(storage.objects_seen['revision']) == len(revisions)

    # clear only content from the buffer
    s = storage.clear_buffers(['content'])
    assert s is None

    # specific clear operation on specific object type content only touched
    # them
    assert len(storage.objects_seen['content']) == 0
    assert len(storage.objects_seen['skipped_content']) == len(
        skipped_contents)
    assert len(storage.objects_seen['directory']) == len(directories)
    assert len(storage.objects_seen['revision']) == len(revisions)

    # clear current buffer from all object types
    s = storage.clear_buffers()
    assert s is None

    assert len(storage.objects_seen['content']) == 0
    assert len(storage.objects_seen['skipped_content']) == 0
    assert len(storage.objects_seen['directory']) == 0
    assert len(storage.objects_seen['revision']) == 0
