# Copyright (C) 2020-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
from contextlib import contextmanager
from unittest.mock import patch

import attr
import pytest

from swh.model.model import Content, ModelObjectType, SkippedContent
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.storage import get_storage
from swh.storage.in_memory import InMemoryStorage
from swh.storage.proxies.tenacious import TenaciousProxyStorage
from swh.storage.tests.storage_data import StorageData
from swh.storage.tests.storage_tests import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)
from swh.storage.tests.storage_tests import TestStorage as _TestStorage  # noqa
from swh.storage.utils import now

data = StorageData()
collections = {
    "origin": data.origins,
    "content": data.contents,
    "skipped_content": data.skipped_contents,
    "revision": data.revisions,
    "directory": data.directories,
    "release": data.releases,
    "snapshot": data.snapshots,
}
# generic storage tests (using imported TestStorage* classes)


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": {
            "cls": "memory",
        },
    }


@pytest.fixture
def swh_storage(swh_storage_backend, swh_storage_backend_config):
    storage_config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "tenacious"},
            swh_storage_backend_config,
        ],
    }

    storage = get_storage(**storage_config)
    storage.storage = swh_storage_backend  # use the same instance of the in-mem backend
    storage.journal_writer = storage.storage.journal_writer
    return storage


class TestTenaciousStorage(_TestStorage):
    @pytest.mark.skip(
        'The "person" table of the pgsql is a legacy thing, and not '
        "supported by the cassandra/in-memory backend."
    )
    def test_person_fullname_unicity(self):
        pass

    @pytest.mark.skip(reason="No collision with the tenacious storage")
    def test_content_add_collision(self, swh_storage, sample_data):
        pass

    @pytest.mark.skip(reason="No collision with the tenacious storage")
    def test_content_add_metadata_collision(self, swh_storage, sample_data):
        pass

    @pytest.mark.skip("content_update is not implemented")
    def test_content_update(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra/InMemory storage")
    def test_origin_count(self):
        pass

    @pytest.mark.skip("in-memory backend has no timeout")
    def test_querytimeout(self):
        pass

    @pytest.mark.skip("test_types doesn't like our getattribute tricks")
    def test_types(self):
        pass


class TestTenaciousStorageGeneratedData(_TestStorageGeneratedData):
    @pytest.mark.skip("Not supported by Cassandra/InMemory")
    def test_origin_count(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra/InMemory")
    def test_origin_count_with_visit_no_visits(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra/InMemory")
    def test_origin_count_with_visit_with_visits_and_snapshot(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra/InMemory")
    def test_origin_count_with_visit_with_visits_no_snapshot(self):
        pass


# specific tests for the tenacious behavior


def get_tenacious_storage(**config):
    storage_config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "validate"},
            {"cls": "tenacious", **config},
            {"cls": "memory"},
        ],
    }

    return get_storage(**storage_config)


@contextmanager
def disabled_validators():
    attr.set_run_validators(False)
    yield
    attr.set_run_validators(True)


def popid(d):
    d.pop("id")
    return d


testdata = [
    pytest.param(
        "content",
        "content_add",
        list(TEST_OBJECTS[ModelObjectType.CONTENT]),
        attr.evolve(Content.from_data(data=b"too big"), length=1000),
        attr.evolve(Content.from_data(data=b"to fail"), length=1000),
        id="content",
    ),
    pytest.param(
        "content",
        "content_add_metadata",
        [
            attr.evolve(cnt, ctime=now())
            for cnt in TEST_OBJECTS[ModelObjectType.CONTENT]
            if isinstance(cnt, Content)  # to keep mypy happy
        ],
        attr.evolve(Content.from_data(data=b"too big"), length=1000, ctime=now()),
        attr.evolve(Content.from_data(data=b"to fail"), length=1000, ctime=now()),
        id="content_metadata",
    ),
    pytest.param(
        "skipped_content",
        "skipped_content_add",
        list(TEST_OBJECTS[ModelObjectType.SKIPPED_CONTENT]),
        attr.evolve(
            SkippedContent.from_data(data=b"too big", reason="too big"),
            length=1000,
        ),
        attr.evolve(
            SkippedContent.from_data(data=b"to fail", reason="to fail"),
            length=1000,
        ),
        id="skipped_content",
    ),
    pytest.param(
        "directory",
        "directory_add",
        list(TEST_OBJECTS[ModelObjectType.DIRECTORY]),
        data.directory,
        data.directory2,
        id="directory",
    ),
    pytest.param(
        "revision",
        "revision_add",
        list(TEST_OBJECTS[ModelObjectType.REVISION]),
        data.revision,
        data.revision2,
        id="revision",
    ),
    pytest.param(
        "release",
        "release_add",
        list(TEST_OBJECTS[ModelObjectType.RELEASE]),
        data.release,
        data.release2,
        id="release",
    ),
    pytest.param(
        "snapshot",
        "snapshot_add",
        list(TEST_OBJECTS[ModelObjectType.SNAPSHOT]),
        data.snapshot,
        data.complete_snapshot,
        id="snapshot",
    ),
    pytest.param(
        "origin",
        "origin_add",
        list(TEST_OBJECTS[ModelObjectType.ORIGIN]),
        data.origin,
        data.origin2,
        id="origin",
    ),
]


class LimitedInMemoryStorage(InMemoryStorage):
    # forbidden are 'bad1' and 'bad2' arguments of `testdata`
    forbidden = [x[0][3] for x in testdata] + [x[0][4] for x in testdata]

    def __init__(self, *args, **kw):
        self.add_calls = Counter()
        super().__init__(*args, **kw)

    def reset(self):
        super().reset()
        self.add_calls.clear()

    def content_add(self, contents):
        return self._maybe_add(super().content_add, "content", contents)

    def content_add_metadata(self, contents):
        return self._maybe_add(super().content_add_metadata, "content", contents)

    def skipped_content_add(self, skipped_contents):
        return self._maybe_add(
            super().skipped_content_add, "skipped_content", skipped_contents
        )

    def origin_add(self, origins):
        return self._maybe_add(super().origin_add, "origin", origins)

    def directory_add(self, directories):
        return self._maybe_add(super().directory_add, "directory", directories)

    def revision_add(self, revisions):
        return self._maybe_add(super().revision_add, "revision", revisions)

    def release_add(self, releases):
        return self._maybe_add(super().release_add, "release", releases)

    def snapshot_add(self, snapshots):
        return self._maybe_add(super().snapshot_add, "snapshot", snapshots)

    def _maybe_add(self, add_func, object_type, objects):
        self.add_calls[object_type] += 1
        if any(c in self.forbidden for c in objects):
            raise ValueError(
                f"{object_type} is forbidden",
                [c.unique_key() for c in objects if c in self.forbidden],
            )
        return add_func(objects)


@patch("swh.storage.in_memory.InMemoryStorage", LimitedInMemoryStorage)
@pytest.mark.parametrize("object_type, add_func_name, objects, bad1, bad2", testdata)
def test_tenacious_proxy_storage(object_type, add_func_name, objects, bad1, bad2):
    storage = get_tenacious_storage()
    tenacious = storage.storage
    in_memory = tenacious.storage
    assert isinstance(tenacious, TenaciousProxyStorage)
    assert isinstance(in_memory, LimitedInMemoryStorage)

    size = len(objects)

    add_func = getattr(storage, add_func_name)

    # Note: when checking the LimitedInMemoryStorage.add_calls counter, it's
    # hard to guess the exact number of calls in the end (depends on the size
    # of batch and the position of bad objects in this batch). So we will only
    # check a lower limit of the form (n + m), where n is the minimum expected
    # number of additions (due to the batch begin split), and m is the fact
    # that bad objects are tried (individually) several (3) times before giving
    # up. So for one bad object, m is 3; for 2 bad objects, m is 6, etc.

    s = add_func(objects)
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 0
    assert storage.add_calls[object_type] == (1 + 0)
    in_memory.reset()
    tenacious.reset()

    # bad1 is the last element
    s = add_func(objects + [bad1])
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 1

    assert storage.add_calls[object_type] >= (2 + 3)
    in_memory.reset()
    tenacious.reset()

    # bad1 and bad2 are the last elements
    s = add_func(objects + [bad1, bad2])
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 2
    assert storage.add_calls[object_type] >= (3 + 6)
    in_memory.reset()
    tenacious.reset()

    # bad1 is the first element
    s = add_func([bad1] + objects)
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 1
    assert storage.add_calls[object_type] >= (2 + 3)
    in_memory.reset()
    tenacious.reset()

    # bad1 and bad2 are the first elements
    s = add_func([bad1, bad2] + objects)
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 2
    assert storage.add_calls[object_type] >= (3 + 6)
    in_memory.reset()
    tenacious.reset()

    # bad1 is in the middle of the list of inserted elements
    s = add_func(objects[: size // 2] + [bad1] + objects[size // 2 :])
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 1
    assert storage.add_calls[object_type] >= (3 + 3)
    in_memory.reset()
    tenacious.reset()

    # bad1 and bad2 are together in the middle of the list of inserted elements
    s = add_func(objects[: size // 2] + [bad1, bad2] + objects[size // 2 :])
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 2
    assert storage.add_calls[object_type] >= (3 + 6)
    in_memory.reset()
    tenacious.reset()

    # bad1 and bad2 are spread in the middle of the list of inserted elements
    s = add_func(
        objects[: size // 3]
        + [bad1]
        + objects[size // 3 : 2 * (size // 3)]
        + [bad2]
        + objects[2 * (size // 3) :]
    )
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 2
    assert storage.add_calls[object_type] >= (3 + 6)
    in_memory.reset()
    tenacious.reset()

    # bad1 is the only element
    s = add_func([bad1])
    assert s.get(f"{object_type}:add", 0) == 0
    assert s.get(f"{object_type}:add:errors", 0) == 1
    assert storage.add_calls[object_type] == (0 + 3)
    in_memory.reset()
    tenacious.reset()

    # bad1 and bad2 are the only elements
    s = add_func([bad1, bad2])
    assert s.get(f"{object_type}:add", 0) == 0
    assert s.get(f"{object_type}:add:errors", 0) == 2
    assert storage.add_calls[object_type] == (1 + 6)
    in_memory.reset()
    tenacious.reset()


@patch("swh.storage.in_memory.InMemoryStorage", LimitedInMemoryStorage)
@pytest.mark.parametrize("object_type, add_func_name, objects, bad1, bad2", testdata)
def test_tenacious_proxy_storage_rate_limit(
    object_type, add_func_name, objects, bad1, bad2
):
    storage = get_tenacious_storage(error_rate_limit={"errors": 1, "window_size": 2})
    tenacious = storage.storage
    in_memory = tenacious.storage
    assert isinstance(tenacious, TenaciousProxyStorage)
    assert isinstance(in_memory, LimitedInMemoryStorage)

    size = len(objects)

    add_func = getattr(storage, add_func_name)

    # with no insertion failure, no impact
    s = add_func(objects)
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 0
    in_memory.reset()
    tenacious.reset()

    # with one insertion failure, no impact
    s = add_func([bad1] + objects)
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 1
    in_memory.reset()
    tenacious.reset()

    s = add_func(objects[: size // 2] + [bad1] + objects[size // 2 :])
    assert s.get(f"{object_type}:add", 0) == size
    assert s.get(f"{object_type}:add:errors", 0) == 1
    in_memory.reset()
    tenacious.reset()

    # with two consecutive insertion failures, exception is raised
    with pytest.raises(RuntimeError, match="Too many insertion errors"):
        add_func([bad1, bad2] + objects)
    in_memory.reset()
    tenacious.reset()

    if size > 2:
        # with two consecutive insertion failures, exception is raised
        # (errors not at the beginning)
        with pytest.raises(RuntimeError, match="Too many insertion errors"):
            add_func(objects[: size // 2] + [bad1, bad2] + objects[size // 2 :])
        in_memory.reset()
        tenacious.reset()

        # with two non-consecutive insertion failures, no impact
        # (errors are far enough to not reach the rate limit)
        s = add_func(
            objects[: size // 3]
            + [bad1]
            + objects[size // 3 : 2 * (size // 3)]
            + [bad2]
            + objects[2 * (size // 3) :]
        )
        assert s.get(f"{object_type}:add", 0) == size
        assert s.get(f"{object_type}:add:errors", 0) == 2
        in_memory.reset()
        tenacious.reset()
