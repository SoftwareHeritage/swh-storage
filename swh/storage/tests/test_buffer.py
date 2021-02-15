# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional
from unittest.mock import Mock

from swh.storage import get_storage
from swh.storage.buffer import BufferingProxyStorage


def get_storage_with_buffer_config(**buffer_config) -> BufferingProxyStorage:
    steps = [
        {"cls": "buffer", **buffer_config},
        {"cls": "memory"},
    ]

    ret = get_storage("pipeline", steps=steps)
    assert isinstance(ret, BufferingProxyStorage)
    return ret


def test_buffering_proxy_storage_content_threshold_not_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_content_threshold_nb_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_content_deduplicate(sample_data) -> None:
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


def test_buffering_proxy_storage_content_threshold_bytes_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_skipped_content_threshold_not_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_skipped_content_threshold_nb_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_directory_threshold_not_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_directory_threshold_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_directory_deduplicate(sample_data) -> None:
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


def test_buffering_proxy_storage_revision_threshold_not_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_revision_threshold_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_revision_deduplicate(sample_data) -> None:
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


def test_buffering_proxy_storage_release_threshold_not_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_release_threshold_hit(sample_data) -> None:
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


def test_buffering_proxy_storage_release_deduplicate(sample_data) -> None:
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


def test_buffering_proxy_storage_snapshot_threshold_not_hit(sample_data) -> None:
    snapshots = sample_data.snapshots
    threshold = 10

    assert len(snapshots) < threshold
    storage = get_storage_with_buffer_config(
        min_batch_size={"snapshot": threshold,}  # configuration set
    )
    s = storage.snapshot_add(snapshots)
    assert s == {}

    snapshot_ids = [r.id for r in snapshots]
    missing_snapshots = storage.snapshot_missing(snapshot_ids)
    assert list(missing_snapshots) == snapshot_ids

    s = storage.flush()
    assert s == {
        "snapshot:add": len(snapshots),
    }

    missing_snapshots = storage.snapshot_missing(snapshot_ids)
    assert list(missing_snapshots) == []


def test_buffering_proxy_storage_snapshot_threshold_hit(sample_data) -> None:
    snapshots = sample_data.snapshots
    threshold = 2
    assert len(snapshots) > threshold

    storage = get_storage_with_buffer_config(
        min_batch_size={"snapshot": threshold,}  # configuration set
    )

    s = storage.snapshot_add(snapshots)
    assert s == {
        "snapshot:add": len(snapshots),
    }

    snapshot_ids = [r.id for r in snapshots]
    missing_snapshots = storage.snapshot_missing(snapshot_ids)
    assert list(missing_snapshots) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_snapshot_deduplicate(sample_data) -> None:
    snapshots = sample_data.snapshots[:2]
    storage = get_storage_with_buffer_config(min_batch_size={"snapshot": 2,})

    s = storage.snapshot_add([snapshots[0], snapshots[0]])
    assert s == {}

    s = storage.snapshot_add([snapshots[0]])
    assert s == {}

    s = storage.snapshot_add([snapshots[1]])
    assert s == {
        "snapshot:add": 1 + 1,
    }

    missing_snapshots = storage.snapshot_missing([r.id for r in snapshots])
    assert list(missing_snapshots) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_clear(sample_data) -> None:
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
    snapshots = sample_data.snapshots
    assert 0 < len(snapshots) < threshold

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
    s = storage.snapshot_add(snapshots)
    assert s == {}

    assert len(storage._objects["content"]) == len(contents)
    assert len(storage._objects["skipped_content"]) == len(skipped_contents)
    assert len(storage._objects["directory"]) == len(directories)
    assert len(storage._objects["revision"]) == len(revisions)
    assert len(storage._objects["release"]) == len(releases)
    assert len(storage._objects["snapshot"]) == len(snapshots)

    # clear only content from the buffer
    s = storage.clear_buffers(["content"])  # type: ignore
    assert s is None

    # specific clear operation on specific object type content only touched
    # them
    assert len(storage._objects["content"]) == 0
    assert len(storage._objects["skipped_content"]) == len(skipped_contents)
    assert len(storage._objects["directory"]) == len(directories)
    assert len(storage._objects["revision"]) == len(revisions)
    assert len(storage._objects["release"]) == len(releases)
    assert len(storage._objects["snapshot"]) == len(snapshots)

    # clear current buffer from all object types
    s = storage.clear_buffers()  # type: ignore
    assert s is None

    assert len(storage._objects["content"]) == 0
    assert len(storage._objects["skipped_content"]) == 0
    assert len(storage._objects["directory"]) == 0
    assert len(storage._objects["revision"]) == 0
    assert len(storage._objects["release"]) == 0
    assert len(storage._objects["snapshot"]) == 0


def test_buffer_proxy_with_default_args() -> None:
    storage = get_storage_with_buffer_config()

    assert storage is not None


def test_buffer_flush_stats(sample_data) -> None:
    storage = get_storage_with_buffer_config()

    s = storage.content_add(sample_data.contents)
    assert s == {}
    s = storage.skipped_content_add(sample_data.skipped_contents)
    assert s == {}
    s = storage.directory_add(sample_data.directories)
    assert s == {}
    s = storage.revision_add(sample_data.revisions)
    assert s == {}
    s = storage.release_add(sample_data.releases)
    assert s == {}
    s = storage.snapshot_add(sample_data.snapshots)
    assert s == {}

    # Flush all the things
    s = storage.flush()
    assert s["content:add"] > 0
    assert s["content:add:bytes"] > 0
    assert s["skipped_content:add"] > 0
    assert s["directory:add"] > 0
    assert s["revision:add"] > 0
    assert s["release:add"] > 0
    assert s["snapshot:add"] > 0


def test_buffer_operation_order(sample_data) -> None:
    storage = get_storage_with_buffer_config()

    # Wrap the inner storage in a mock to track all method calls.
    storage.storage = mocked_storage = Mock(wraps=storage.storage)

    # Simulate a loader: add contents, directories, revisions, releases, then
    # snapshots.
    storage.content_add(sample_data.contents)
    storage.skipped_content_add(sample_data.skipped_contents)
    storage.directory_add(sample_data.directories)
    storage.revision_add(sample_data.revisions)
    storage.release_add(sample_data.releases)
    storage.snapshot_add(sample_data.snapshots)

    # Check that nothing has been flushed yet
    assert mocked_storage.method_calls == []

    # Flush all the things
    storage.flush()

    methods_called = [c[0] for c in mocked_storage.method_calls]
    prev = -1
    for method in [
        "content_add",
        "skipped_content_add",
        "directory_add",
        "revision_add",
        "release_add",
        "snapshot_add",
        "flush",
    ]:
        try:
            cur: Optional[int] = methods_called.index(method)
        except ValueError:
            cur = None

        assert cur is not None, "Method %s not called" % method
        assert cur > prev, "Method %s called out of order; all calls were: %s" % (
            method,
            methods_called,
        )
        prev = cur
