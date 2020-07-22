# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import get_storage


def get_storage_with_buffer_config(**buffer_config):
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "buffer", **buffer_config}, {"cls": "memory"},],
    }

    return get_storage(**storage_config)


def test_buffering_proxy_storage_content_threshold_not_hit(sample_data):
    contents = sample_data.contents[:2]
    contents_dict = [c.to_dict() for c in contents]

    storage = get_storage_with_buffer_config(min_batch_size={"content": 10,})
    s = storage.content_add(contents)
    assert s == {}

    # contents have not been written to storage
    missing_contents = storage.content_missing(contents_dict)
    assert set(missing_contents) == set([contents[0].sha1, contents[1].sha1])

    s = storage.flush()
    assert s == {
        "content:add": 1 + 1,
        "content:add:bytes": contents[0].length + contents[1].length,
    }

    missing_contents = storage.content_missing(contents_dict)
    assert list(missing_contents) == []


def test_buffering_proxy_storage_content_threshold_nb_hit(sample_data):
    content = sample_data.content
    content_dict = content.to_dict()

    storage = get_storage_with_buffer_config(min_batch_size={"content": 1,})

    s = storage.content_add([content])
    assert s == {
        "content:add": 1,
        "content:add:bytes": content.length,
    }

    missing_contents = storage.content_missing([content_dict])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_content_deduplicate(sample_data):
    contents = sample_data.contents[:2]
    storage = get_storage_with_buffer_config(min_batch_size={"content": 2,})

    s = storage.content_add([contents[0], contents[0]])
    assert s == {}

    s = storage.content_add([contents[0]])
    assert s == {}

    s = storage.content_add([contents[1]])
    assert s == {
        "content:add": 1 + 1,
        "content:add:bytes": contents[0].length + contents[1].length,
    }

    missing_contents = storage.content_missing([c.to_dict() for c in contents])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_content_threshold_bytes_hit(sample_data):
    contents = sample_data.contents[:2]
    content_bytes_min_batch_size = 2
    storage = get_storage_with_buffer_config(
        min_batch_size={"content": 10, "content_bytes": content_bytes_min_batch_size,}
    )

    assert contents[0].length > content_bytes_min_batch_size

    s = storage.content_add([contents[0]])
    assert s == {
        "content:add": 1,
        "content:add:bytes": contents[0].length,
    }

    missing_contents = storage.content_missing([contents[0].to_dict()])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_skipped_content_threshold_not_hit(sample_data):
    contents = sample_data.skipped_contents
    contents_dict = [c.to_dict() for c in contents]
    storage = get_storage_with_buffer_config(min_batch_size={"skipped_content": 10,})
    s = storage.skipped_content_add([contents[0], contents[1]])
    assert s == {}

    # contents have not been written to storage
    missing_contents = storage.skipped_content_missing(contents_dict)
    assert {c["sha1"] for c in missing_contents} == {c.sha1 for c in contents}

    s = storage.flush()
    assert s == {"skipped_content:add": 1 + 1}

    missing_contents = storage.skipped_content_missing(contents_dict)
    assert list(missing_contents) == []


def test_buffering_proxy_storage_skipped_content_threshold_nb_hit(sample_data):
    contents = sample_data.skipped_contents
    storage = get_storage_with_buffer_config(min_batch_size={"skipped_content": 1,})

    s = storage.skipped_content_add([contents[0]])
    assert s == {"skipped_content:add": 1}

    missing_contents = storage.skipped_content_missing([contents[0].to_dict()])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_skipped_content_deduplicate(sample_data):
    contents = sample_data.skipped_contents[:2]
    storage = get_storage_with_buffer_config(min_batch_size={"skipped_content": 2,})

    s = storage.skipped_content_add([contents[0], contents[0]])
    assert s == {}

    s = storage.skipped_content_add([contents[0]])
    assert s == {}

    s = storage.skipped_content_add([contents[1]])
    assert s == {
        "skipped_content:add": 1 + 1,
    }

    missing_contents = storage.skipped_content_missing([c.to_dict() for c in contents])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_directory_threshold_not_hit(sample_data):
    directory = sample_data.directory
    storage = get_storage_with_buffer_config(min_batch_size={"directory": 10,})
    s = storage.directory_add([directory])
    assert s == {}

    missing_directories = storage.directory_missing([directory.id])
    assert list(missing_directories) == [directory.id]

    s = storage.flush()
    assert s == {
        "directory:add": 1,
    }

    missing_directories = storage.directory_missing([directory.id])
    assert list(missing_directories) == []


def test_buffering_proxy_storage_directory_threshold_hit(sample_data):
    directory = sample_data.directory
    storage = get_storage_with_buffer_config(min_batch_size={"directory": 1,})
    s = storage.directory_add([directory])
    assert s == {
        "directory:add": 1,
    }

    missing_directories = storage.directory_missing([directory.id])
    assert list(missing_directories) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_directory_deduplicate(sample_data):
    directories = sample_data.directories[:2]
    storage = get_storage_with_buffer_config(min_batch_size={"directory": 2,})

    s = storage.directory_add([directories[0], directories[0]])
    assert s == {}

    s = storage.directory_add([directories[0]])
    assert s == {}

    s = storage.directory_add([directories[1]])
    assert s == {
        "directory:add": 1 + 1,
    }

    missing_directories = storage.directory_missing([d.id for d in directories])
    assert list(missing_directories) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_revision_threshold_not_hit(sample_data):
    revision = sample_data.revision
    storage = get_storage_with_buffer_config(min_batch_size={"revision": 10,})
    s = storage.revision_add([revision])
    assert s == {}

    missing_revisions = storage.revision_missing([revision.id])
    assert list(missing_revisions) == [revision.id]

    s = storage.flush()
    assert s == {
        "revision:add": 1,
    }

    missing_revisions = storage.revision_missing([revision.id])
    assert list(missing_revisions) == []


def test_buffering_proxy_storage_revision_threshold_hit(sample_data):
    revision = sample_data.revision
    storage = get_storage_with_buffer_config(min_batch_size={"revision": 1,})
    s = storage.revision_add([revision])
    assert s == {
        "revision:add": 1,
    }

    missing_revisions = storage.revision_missing([revision.id])
    assert list(missing_revisions) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_revision_deduplicate(sample_data):
    revisions = sample_data.revisions[:2]
    storage = get_storage_with_buffer_config(min_batch_size={"revision": 2,})

    s = storage.revision_add([revisions[0], revisions[0]])
    assert s == {}

    s = storage.revision_add([revisions[0]])
    assert s == {}

    s = storage.revision_add([revisions[1]])
    assert s == {
        "revision:add": 1 + 1,
    }

    missing_revisions = storage.revision_missing([r.id for r in revisions])
    assert list(missing_revisions) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_release_threshold_not_hit(sample_data):
    releases = sample_data.releases
    threshold = 10

    assert len(releases) < threshold
    storage = get_storage_with_buffer_config(
        min_batch_size={"release": threshold,}  # configuration set
    )
    s = storage.release_add(releases)
    assert s == {}

    release_ids = [r.id for r in releases]
    missing_releases = storage.release_missing(release_ids)
    assert list(missing_releases) == release_ids

    s = storage.flush()
    assert s == {
        "release:add": len(releases),
    }

    missing_releases = storage.release_missing(release_ids)
    assert list(missing_releases) == []


def test_buffering_proxy_storage_release_threshold_hit(sample_data):
    releases = sample_data.releases
    threshold = 2
    assert len(releases) > threshold

    storage = get_storage_with_buffer_config(
        min_batch_size={"release": threshold,}  # configuration set
    )

    s = storage.release_add(releases)
    assert s == {
        "release:add": len(releases),
    }

    release_ids = [r.id for r in releases]
    missing_releases = storage.release_missing(release_ids)
    assert list(missing_releases) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_release_deduplicate(sample_data):
    releases = sample_data.releases[:2]
    storage = get_storage_with_buffer_config(min_batch_size={"release": 2,})

    s = storage.release_add([releases[0], releases[0]])
    assert s == {}

    s = storage.release_add([releases[0]])
    assert s == {}

    s = storage.release_add([releases[1]])
    assert s == {
        "release:add": 1 + 1,
    }

    missing_releases = storage.release_missing([r.id for r in releases])
    assert list(missing_releases) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_clear(sample_data):
    """Clear operation on buffer

    """
    threshold = 10
    contents = sample_data.contents
    assert 0 < len(contents) < threshold
    skipped_contents = sample_data.skipped_contents
    assert 0 < len(skipped_contents) < threshold
    directories = sample_data.directories
    assert 0 < len(directories) < threshold
    revisions = sample_data.revisions
    assert 0 < len(revisions) < threshold
    releases = sample_data.releases
    assert 0 < len(releases) < threshold

    storage = get_storage_with_buffer_config(
        min_batch_size={
            "content": threshold,
            "skipped_content": threshold,
            "directory": threshold,
            "revision": threshold,
            "release": threshold,
        }
    )

    s = storage.content_add(contents)
    assert s == {}
    s = storage.skipped_content_add(skipped_contents)
    assert s == {}
    s = storage.directory_add(directories)
    assert s == {}
    s = storage.revision_add(revisions)
    assert s == {}
    s = storage.release_add(releases)
    assert s == {}

    assert len(storage._objects["content"]) == len(contents)
    assert len(storage._objects["skipped_content"]) == len(skipped_contents)
    assert len(storage._objects["directory"]) == len(directories)
    assert len(storage._objects["revision"]) == len(revisions)
    assert len(storage._objects["release"]) == len(releases)

    # clear only content from the buffer
    s = storage.clear_buffers(["content"])
    assert s is None

    # specific clear operation on specific object type content only touched
    # them
    assert len(storage._objects["content"]) == 0
    assert len(storage._objects["skipped_content"]) == len(skipped_contents)
    assert len(storage._objects["directory"]) == len(directories)
    assert len(storage._objects["revision"]) == len(revisions)
    assert len(storage._objects["release"]) == len(releases)

    # clear current buffer from all object types
    s = storage.clear_buffers()
    assert s is None

    assert len(storage._objects["content"]) == 0
    assert len(storage._objects["skipped_content"]) == 0
    assert len(storage._objects["directory"]) == 0
    assert len(storage._objects["revision"]) == 0
    assert len(storage._objects["release"]) == 0
