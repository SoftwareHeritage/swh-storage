# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from typing import Optional
from unittest.mock import Mock

import pytest

from swh.storage import get_storage
from swh.storage.proxies.buffer import (
    BufferingProxyStorage,
    estimate_release_size,
    estimate_revision_size,
)


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

    storage = get_storage_with_buffer_config(
        min_batch_size={
            "content": 10,
        }
    )
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

    storage = get_storage_with_buffer_config(
        min_batch_size={
            "content": 1,
        }
    )

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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "content": 2,
        }
    )

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
        min_batch_size={
            "content": 10,
            "content_bytes": content_bytes_min_batch_size,
        }
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "skipped_content": 10,
        }
    )
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "skipped_content": 1,
        }
    )

    s = storage.skipped_content_add([contents[0]])
    assert s == {"skipped_content:add": 1}

    missing_contents = storage.skipped_content_missing([contents[0].to_dict()])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_skipped_content_deduplicate(sample_data):
    contents = sample_data.skipped_contents[:2]
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "skipped_content": 2,
        }
    )

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


def test_buffering_proxy_storage_extid_threshold_not_hit(sample_data) -> None:
    extid = sample_data.extid1
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "extid": 10,
        }
    )
    s = storage.extid_add([extid])
    assert s == {}

    present_extids = storage.extid_get_from_target(
        extid.target.object_type, [extid.target.object_id]
    )
    assert list(present_extids) == []

    s = storage.flush()
    assert s == {
        "extid:add": 1,
    }

    present_extids = storage.extid_get_from_target(
        extid.target.object_type, [extid.target.object_id]
    )
    assert list(present_extids) == [extid]


def test_buffering_proxy_storage_extid_threshold_hit(sample_data) -> None:
    extid = sample_data.extid1
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "extid": 1,
        }
    )
    s = storage.extid_add([extid])
    assert s == {
        "extid:add": 1,
    }

    present_extids = storage.extid_get_from_target(
        extid.target.object_type, [extid.target.object_id]
    )
    assert list(present_extids) == [extid]

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_extid_deduplicate(sample_data) -> None:
    extids = sample_data.extids[:2]
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "extid": 2,
        }
    )

    s = storage.extid_add([extids[0], extids[0]])
    assert s == {}

    s = storage.extid_add([extids[0]])
    assert s == {}

    s = storage.extid_add([extids[1]])
    assert s == {
        "extid:add": 1 + 1,
    }

    for extid in extids:
        present_extids = storage.extid_get_from_target(
            extid.target.object_type, [extid.target.object_id]
        )
        assert list(present_extids) == [extid]

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_directory_threshold_not_hit(sample_data) -> None:
    directory = sample_data.directory
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "directory": 10,
        }
    )
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "directory": 1,
        }
    )
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "directory": 2,
        }
    )

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


def test_buffering_proxy_storage_directory_entries_threshold(sample_data) -> None:
    directories = sample_data.directories
    n_entries = sum(len(d.entries) for d in directories)
    threshold = sum(len(d.entries) for d in directories[:-2])

    # ensure the threshold is in the middle
    assert 0 < threshold < n_entries

    storage = get_storage_with_buffer_config(
        min_batch_size={"directory_entries": threshold}
    )
    storage.storage = Mock(wraps=storage.storage)

    for directory in directories:
        storage.directory_add([directory])
    storage.flush()

    # We should have called the underlying directory_add at least twice, as
    # we have hit the threshold for number of entries on directory n-2
    method_calls = Counter(c[0] for c in storage.storage.method_calls)
    assert method_calls["directory_add"] >= 2


def test_buffering_proxy_storage_revision_threshold_not_hit(sample_data) -> None:
    revision = sample_data.revision
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "revision": 10,
        }
    )
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "revision": 1,
        }
    )
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "revision": 2,
        }
    )

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


def test_buffering_proxy_storage_revision_parents_threshold(sample_data) -> None:
    revisions = sample_data.revisions
    n_parents = sum(len(r.parents) for r in revisions)
    threshold = sum(len(r.parents) for r in revisions[:-2])

    # ensure the threshold is in the middle
    assert 0 < threshold < n_parents

    storage = get_storage_with_buffer_config(
        min_batch_size={"revision_parents": threshold}
    )
    storage.storage = Mock(wraps=storage.storage)

    for revision in revisions:
        storage.revision_add([revision])
    storage.flush()

    # We should have called the underlying revision_add at least twice, as
    # we have hit the threshold for number of parents on revision n-2
    method_calls = Counter(c[0] for c in storage.storage.method_calls)
    assert method_calls["revision_add"] >= 2


def test_buffering_proxy_storage_revision_size_threshold(sample_data) -> None:
    revisions = sample_data.revisions
    total_size = sum(estimate_revision_size(r) for r in revisions)
    threshold = sum(estimate_revision_size(r) for r in revisions[:-2])

    # ensure the threshold is in the middle
    assert 0 < threshold < total_size

    storage = get_storage_with_buffer_config(
        min_batch_size={"revision_bytes": threshold}
    )
    storage.storage = Mock(wraps=storage.storage)

    for revision in revisions:
        storage.revision_add([revision])
    storage.flush()

    # We should have called the underlying revision_add at least twice, as
    # we have hit the threshold for number of parents on revision n-2
    method_calls = Counter(c[0] for c in storage.storage.method_calls)
    assert method_calls["revision_add"] >= 2


def test_buffering_proxy_storage_release_threshold_not_hit(sample_data) -> None:
    releases = sample_data.releases
    threshold = 10

    assert len(releases) < threshold
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "release": threshold,
        }  # configuration set
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
        min_batch_size={
            "release": threshold,
        }  # configuration set
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "release": 2,
        }
    )

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


def test_buffering_proxy_storage_release_size_threshold(sample_data) -> None:
    releases = sample_data.releases
    total_size = sum(estimate_release_size(r) for r in releases)
    threshold = sum(estimate_release_size(r) for r in releases[:-2])

    # ensure the threshold is in the middle
    assert 0 < threshold < total_size

    storage = get_storage_with_buffer_config(
        min_batch_size={"release_bytes": threshold}
    )
    storage.storage = Mock(wraps=storage.storage)

    for release in releases:
        storage.release_add([release])
    storage.flush()

    # We should have called the underlying release_add at least twice, as
    # we have hit the threshold for number of parents on release n-2
    method_calls = Counter(c[0] for c in storage.storage.method_calls)
    assert method_calls["release_add"] >= 2


def test_buffering_proxy_storage_snapshot_threshold_not_hit(sample_data) -> None:
    snapshots = sample_data.snapshots
    threshold = 10

    assert len(snapshots) < threshold
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "snapshot": threshold,
        }  # configuration set
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
        min_batch_size={
            "snapshot": threshold,
        }  # configuration set
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
    storage = get_storage_with_buffer_config(
        min_batch_size={
            "snapshot": 2,
        }
    )

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
    """Clear operation on buffer"""
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
    metadata = sample_data.content_metadata + sample_data.origin_metadata
    assert 0 < len(metadata) < threshold

    storage = get_storage_with_buffer_config(
        min_batch_size={
            "content": threshold,
            "skipped_content": threshold,
            "directory": threshold,
            "revision": threshold,
            "release": threshold,
            "raw_extrinsic_metadata": threshold,
        }
    )

    storage.metadata_authority_add(sample_data.authorities)
    storage.metadata_fetcher_add(sample_data.fetchers)

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
    s = storage.raw_extrinsic_metadata_add(metadata)
    assert s == {}

    assert len(storage._objects["content"]) == len(contents)
    assert len(storage._objects["skipped_content"]) == len(skipped_contents)
    assert len(storage._objects["directory"]) == len(directories)
    assert len(storage._objects["revision"]) == len(revisions)
    assert len(storage._objects["release"]) == len(releases)
    assert len(storage._objects["snapshot"]) == len(snapshots)
    assert len(storage._objects["raw_extrinsic_metadata"]) == len(metadata)

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
    assert len(storage._objects["raw_extrinsic_metadata"]) == len(metadata)

    # clear current buffer from all object types
    s = storage.clear_buffers()
    assert s is None

    assert len(storage._objects["content"]) == 0
    assert len(storage._objects["skipped_content"]) == 0
    assert len(storage._objects["directory"]) == 0
    assert len(storage._objects["revision"]) == 0
    assert len(storage._objects["release"]) == 0
    assert len(storage._objects["snapshot"]) == 0
    assert len(storage._objects["raw_extrinsic_metadata"]) == 0


def test_buffer_proxy_with_default_args() -> None:
    storage = get_storage_with_buffer_config()

    assert storage is not None


def test_buffer_flush_stats(sample_data) -> None:
    storage = get_storage_with_buffer_config()

    storage.metadata_authority_add(sample_data.authorities)
    storage.metadata_fetcher_add(sample_data.fetchers)

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
    s = storage.raw_extrinsic_metadata_add(
        sample_data.content_metadata + sample_data.origin_metadata
    )
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
    assert s["cnt_metadata:add"] > 0
    assert s["ori_metadata:add"] > 0


def test_buffer_operation_order(sample_data) -> None:
    storage = get_storage_with_buffer_config()

    # not buffered (because they happen rarely)
    storage.metadata_authority_add(sample_data.authorities)
    storage.metadata_fetcher_add(sample_data.fetchers)

    # Wrap the inner storage in a mock to track all method calls.
    storage.storage = mocked_storage = Mock(wraps=storage.storage)

    # Simulate a loader: add origin metadata, contents, directories, revisions,
    # releases, snapshots, then content metadata.
    storage.raw_extrinsic_metadata_add(sample_data.origin_metadata)
    storage.content_add(sample_data.contents)
    storage.skipped_content_add(sample_data.skipped_contents)
    storage.directory_add(sample_data.directories)
    storage.revision_add(sample_data.revisions)
    storage.release_add(sample_data.releases)
    storage.snapshot_add(sample_data.snapshots)
    storage.raw_extrinsic_metadata_add(sample_data.content_metadata)

    # Check that nothing has been flushed yet
    assert mocked_storage.method_calls == []

    # Flush all the things
    storage.flush()

    methods_called = [c[0] for c in mocked_storage.method_calls]
    prev = -1
    for method in [
        "raw_extrinsic_metadata_add",
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


def test_buffer_empty_batches() -> None:
    "Flushing an empty buffer storage doesn't call any underlying _add method"
    storage = get_storage_with_buffer_config()
    storage.storage = mocked_storage = Mock(wraps=storage.storage)

    storage.flush()
    methods_called = {c[0] for c in mocked_storage.method_calls}
    assert methods_called == {"flush", "clear_buffers"}


def test_buffer_warning_when_flush_is_missing_flush_ok(sample_data) -> None:
    storage = get_storage_with_buffer_config()
    storage.content_add(sample_data.contents)
    # storage.flush() is intentionally missing

    with pytest.warns(UserWarning, match="during shutdown. A call"):
        del storage


def test_buffer_warning_when_flush_is_missing_flush_fails(mocker, sample_data) -> None:
    storage = get_storage_with_buffer_config()
    storage.content_add(sample_data.contents)
    # storage.flush() is intentionally missing

    # Make underlying flush() on the underlying storage crash
    # to test if we get the right warning.
    mocker.patch.object(storage.storage, "flush", side_effect=RuntimeError("KO"))

    with pytest.warns(UserWarning, match="They are now probably lost"):
        del storage
