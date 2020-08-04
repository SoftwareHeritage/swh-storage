# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import inspect
import itertools
import math
import queue
import random
import threading

from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import Mock

import attr
import pytest

from hypothesis import given, strategies, settings, HealthCheck

from typing import Any, ClassVar, Dict, Iterator, Optional

from swh.model import from_disk
from swh.model.hashutil import hash_to_bytes
from swh.model.identifiers import SWHID
from swh.model.model import (
    Content,
    Directory,
    MetadataTargetType,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Person,
    Release,
    Revision,
    Snapshot,
)
from swh.model.hypothesis_strategies import objects
from swh.storage import get_storage
from swh.storage.converters import origin_url_to_sha1 as sha1
from swh.storage.exc import HashCollision, StorageArgumentException
from swh.storage.interface import ListOrder, PagedResult, StorageInterface
from swh.storage.utils import content_hex_hashes, now


@contextmanager
def db_transaction(storage):
    with storage.db() as db:
        with db.transaction() as cur:
            yield db, cur


def transform_entries(
    storage: StorageInterface, dir_: Directory, *, prefix: bytes = b""
) -> Iterator[Dict[str, Any]]:
    """Iterate through a directory's entries, and yields the items 'directory_ls' is
       expected to return; including content metadata for file entries."""

    for ent in dir_.entries:
        if ent.type == "dir":
            yield {
                "dir_id": dir_.id,
                "type": ent.type,
                "target": ent.target,
                "name": prefix + ent.name,
                "perms": ent.perms,
                "status": None,
                "sha1": None,
                "sha1_git": None,
                "sha256": None,
                "length": None,
            }
        elif ent.type == "file":
            contents = storage.content_find({"sha1_git": ent.target})
            assert contents
            ent_dict = contents[0].to_dict()
            for key in ["ctime", "blake2s256"]:
                del ent_dict[key]
            ent_dict.update(
                {
                    "dir_id": dir_.id,
                    "type": ent.type,
                    "target": ent.target,
                    "name": prefix + ent.name,
                    "perms": ent.perms,
                }
            )
            yield ent_dict


def assert_contents_ok(
    expected_contents, actual_contents, keys_to_check={"sha1", "data"}
):
    """Assert that a given list of contents matches on a given set of keys.

    """
    for k in keys_to_check:
        expected_list = set([c.get(k) for c in expected_contents])
        actual_list = set([c.get(k) for c in actual_contents])
        assert actual_list == expected_list, k


def round_to_milliseconds(date):
    """Round datetime to milliseconds before insertion, so equality doesn't fail after a
    round-trip through a DB (eg. Cassandra)

    """
    return date.replace(microsecond=(date.microsecond // 1000) * 1000)


def test_round_to_milliseconds():
    date = now()

    for (ms, expected_ms) in [(0, 0), (1000, 1000), (555555, 555000), (999500, 999000)]:
        date = date.replace(microsecond=ms)
        actual_date = round_to_milliseconds(date)
        assert actual_date.microsecond == expected_ms


class LazyContent(Content):
    def with_data(self):
        return Content.from_dict({**self.to_dict(), "data": b"42\n"})


class TestStorage:
    """Main class for Storage testing.

    This class is used as-is to test local storage (see TestLocalStorage
    below) and remote storage (see TestRemoteStorage in
    test_remote_storage.py.

    We need to have the two classes inherit from this base class
    separately to avoid nosetests running the tests from the base
    class twice.
    """

    maxDiff = None  # type: ClassVar[Optional[int]]

    def test_types(self, swh_storage_backend_config):
        """Checks all methods of StorageInterface are implemented by this
        backend, and that they have the same signature."""
        # Create an instance of the protocol (which cannot be instantiated
        # directly, so this creates a subclass, then instantiates it)
        interface = type("_", (StorageInterface,), {})()
        storage = get_storage(**swh_storage_backend_config)

        assert "content_add" in dir(interface)

        missing_methods = []

        for meth_name in dir(interface):
            if meth_name.startswith("_"):
                continue
            interface_meth = getattr(interface, meth_name)
            try:
                concrete_meth = getattr(storage, meth_name)
            except AttributeError:
                if not getattr(interface_meth, "deprecated_endpoint", False):
                    # The backend is missing a (non-deprecated) endpoint
                    missing_methods.append(meth_name)
                continue

            expected_signature = inspect.signature(interface_meth)
            actual_signature = inspect.signature(concrete_meth)

            assert expected_signature == actual_signature, meth_name

        assert missing_methods == []

    def test_check_config(self, swh_storage):
        assert swh_storage.check_config(check_write=True)
        assert swh_storage.check_config(check_write=False)

    def test_content_add(self, swh_storage, sample_data):
        cont = sample_data.content

        insertion_start_time = now()
        actual_result = swh_storage.content_add([cont])
        insertion_end_time = now()

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont.length,
        }

        assert list(swh_storage.content_get([cont.sha1])) == [
            {"sha1": cont.sha1, "data": cont.data}
        ]

        expected_cont = attr.evolve(cont, data=None)

        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        for obj in contents:
            assert insertion_start_time <= obj.ctime
            assert obj.ctime <= insertion_end_time
            assert obj == expected_cont

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["content"] == 1

    def test_content_add_from_lazy_content(self, swh_storage, sample_data):
        cont = sample_data.content
        lazy_content = LazyContent.from_dict(cont.to_dict())

        insertion_start_time = now()

        actual_result = swh_storage.content_add([lazy_content])

        insertion_end_time = now()

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont.length,
        }

        # the fact that we retrieve the content object from the storage with
        # the correct 'data' field ensures it has been 'called'
        assert list(swh_storage.content_get([cont.sha1])) == [
            {"sha1": cont.sha1, "data": cont.data}
        ]

        expected_cont = attr.evolve(lazy_content, data=None, ctime=None)
        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        for obj in contents:
            assert insertion_start_time <= obj.ctime
            assert obj.ctime <= insertion_end_time
            assert attr.evolve(obj, ctime=None).to_dict() == expected_cont.to_dict()

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["content"] == 1

    def test_content_get_missing(self, swh_storage, sample_data):
        cont, cont2 = sample_data.contents[:2]

        swh_storage.content_add([cont])

        # Query a single missing content
        results = list(swh_storage.content_get([cont2.sha1]))
        assert results == [None]

        # Check content_get does not abort after finding a missing content
        results = list(swh_storage.content_get([cont.sha1, cont2.sha1]))
        assert results == [{"sha1": cont.sha1, "data": cont.data}, None]

        # Check content_get does not discard found countent when it finds
        # a missing content.
        results = list(swh_storage.content_get([cont2.sha1, cont.sha1]))
        assert results == [None, {"sha1": cont.sha1, "data": cont.data}]

    def test_content_add_different_input(self, swh_storage, sample_data):
        cont, cont2 = sample_data.contents[:2]

        actual_result = swh_storage.content_add([cont, cont2])
        assert actual_result == {
            "content:add": 2,
            "content:add:bytes": cont.length + cont2.length,
        }

    def test_content_add_twice(self, swh_storage, sample_data):
        cont, cont2 = sample_data.contents[:2]

        actual_result = swh_storage.content_add([cont])
        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont.length,
        }
        assert len(swh_storage.journal_writer.journal.objects) == 1

        actual_result = swh_storage.content_add([cont, cont2])
        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont2.length,
        }
        assert 2 <= len(swh_storage.journal_writer.journal.objects) <= 3

        assert len(swh_storage.content_find(cont.to_dict())) == 1
        assert len(swh_storage.content_find(cont2.to_dict())) == 1

    def test_content_add_collision(self, swh_storage, sample_data):
        cont1 = sample_data.content

        # create (corrupted) content with same sha1{,_git} but != sha256
        sha256_array = bytearray(cont1.sha256)
        sha256_array[0] += 1
        cont1b = attr.evolve(cont1, sha256=bytes(sha256_array))

        with pytest.raises(HashCollision) as cm:
            swh_storage.content_add([cont1, cont1b])

        exc = cm.value
        actual_algo = exc.algo
        assert actual_algo in ["sha1", "sha1_git", "blake2s256"]
        actual_id = exc.hash_id
        assert actual_id == getattr(cont1, actual_algo).hex()
        collisions = exc.args[2]
        assert len(collisions) == 2
        assert collisions == [
            content_hex_hashes(cont1.hashes()),
            content_hex_hashes(cont1b.hashes()),
        ]
        assert exc.colliding_content_hashes() == [
            cont1.hashes(),
            cont1b.hashes(),
        ]

    def test_content_add_duplicate(self, swh_storage, sample_data):
        cont = sample_data.content
        swh_storage.content_add([cont, cont])

        assert list(swh_storage.content_get([cont.sha1])) == [
            {"sha1": cont.sha1, "data": cont.data}
        ]

    def test_content_update(self, swh_storage, sample_data):
        cont1 = sample_data.content

        if hasattr(swh_storage, "journal_writer"):
            swh_storage.journal_writer.journal = None  # TODO, not supported

        swh_storage.content_add([cont1])

        # alter the sha1_git for example
        cont1b = attr.evolve(
            cont1, sha1_git=hash_to_bytes("3a60a5275d0333bf13468e8b3dcab90f4046e654")
        )

        swh_storage.content_update([cont1b.to_dict()], keys=["sha1_git"])

        results = swh_storage.content_get_metadata([cont1.sha1])

        expected_content = attr.evolve(cont1b, data=None).to_dict()
        del expected_content["ctime"]
        assert tuple(results[cont1.sha1]) == (expected_content,)

    def test_content_add_metadata(self, swh_storage, sample_data):
        cont = attr.evolve(sample_data.content, data=None, ctime=now())

        actual_result = swh_storage.content_add_metadata([cont])
        assert actual_result == {
            "content:add": 1,
        }

        expected_cont = cont.to_dict()
        del expected_cont["ctime"]

        assert tuple(swh_storage.content_get_metadata([cont.sha1])[cont.sha1]) == (
            expected_cont,
        )
        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        for obj in contents:
            obj = attr.evolve(obj, ctime=None)
            assert obj == cont

    def test_content_add_metadata_different_input(self, swh_storage, sample_data):
        contents = sample_data.contents[:2]
        cont = attr.evolve(contents[0], data=None, ctime=now())
        cont2 = attr.evolve(contents[1], data=None, ctime=now())

        actual_result = swh_storage.content_add_metadata([cont, cont2])
        assert actual_result == {
            "content:add": 2,
        }

    def test_content_add_metadata_collision(self, swh_storage, sample_data):
        cont1 = attr.evolve(sample_data.content, data=None, ctime=now())

        # create (corrupted) content with same sha1{,_git} but != sha256
        sha1_git_array = bytearray(cont1.sha256)
        sha1_git_array[0] += 1
        cont1b = attr.evolve(cont1, sha256=bytes(sha1_git_array))

        with pytest.raises(HashCollision) as cm:
            swh_storage.content_add_metadata([cont1, cont1b])

        exc = cm.value
        actual_algo = exc.algo
        assert actual_algo in ["sha1", "sha1_git", "blake2s256"]
        actual_id = exc.hash_id
        assert actual_id == getattr(cont1, actual_algo).hex()
        collisions = exc.args[2]
        assert len(collisions) == 2
        assert collisions == [
            content_hex_hashes(cont1.hashes()),
            content_hex_hashes(cont1b.hashes()),
        ]
        assert exc.colliding_content_hashes() == [
            cont1.hashes(),
            cont1b.hashes(),
        ]

    def test_skipped_content_add(self, swh_storage, sample_data):
        contents = sample_data.skipped_contents[:2]
        cont = contents[0]
        cont2 = attr.evolve(contents[1], blake2s256=None)

        contents_dict = [c.to_dict() for c in [cont, cont2]]

        missing = list(swh_storage.skipped_content_missing(contents_dict))

        assert missing == [cont.hashes(), cont2.hashes()]

        actual_result = swh_storage.skipped_content_add([cont, cont, cont2])

        assert 2 <= actual_result.pop("skipped_content:add") <= 3
        assert actual_result == {}

        missing = list(swh_storage.skipped_content_missing(contents_dict))
        assert missing == []

    def test_skipped_content_add_missing_hashes(self, swh_storage, sample_data):
        cont, cont2 = [
            attr.evolve(c, sha1_git=None) for c in sample_data.skipped_contents[:2]
        ]
        contents_dict = [c.to_dict() for c in [cont, cont2]]

        missing = list(swh_storage.skipped_content_missing(contents_dict))
        assert len(missing) == 2

        actual_result = swh_storage.skipped_content_add([cont, cont, cont2])

        assert 2 <= actual_result.pop("skipped_content:add") <= 3
        assert actual_result == {}

        missing = list(swh_storage.skipped_content_missing(contents_dict))
        assert missing == []

    def test_skipped_content_missing_partial_hash(self, swh_storage, sample_data):
        cont = sample_data.skipped_content
        cont2 = attr.evolve(cont, sha1_git=None)
        contents_dict = [c.to_dict() for c in [cont, cont2]]

        missing = list(swh_storage.skipped_content_missing(contents_dict))
        assert len(missing) == 2

        actual_result = swh_storage.skipped_content_add([cont])

        assert actual_result.pop("skipped_content:add") == 1
        assert actual_result == {}

        missing = list(swh_storage.skipped_content_missing(contents_dict))
        assert missing == [cont2.hashes()]

    @pytest.mark.property_based
    @settings(deadline=None)  # this test is very slow
    @given(
        strategies.sets(
            elements=strategies.sampled_from(["sha256", "sha1_git", "blake2s256"]),
            min_size=0,
        )
    )
    def test_content_missing(self, swh_storage, sample_data, algos):
        algos |= {"sha1"}
        content, missing_content = [sample_data.content2, sample_data.skipped_content]
        swh_storage.content_add([content])

        test_contents = [content.to_dict()]
        missing_per_hash = defaultdict(list)
        for i in range(256):
            test_content = missing_content.to_dict()
            for hash in algos:
                test_content[hash] = bytes([i]) + test_content[hash][1:]
                missing_per_hash[hash].append(test_content[hash])
            test_contents.append(test_content)

        assert set(swh_storage.content_missing(test_contents)) == set(
            missing_per_hash["sha1"]
        )

        for hash in algos:
            assert set(
                swh_storage.content_missing(test_contents, key_hash=hash)
            ) == set(missing_per_hash[hash])

    @pytest.mark.property_based
    @given(
        strategies.sets(
            elements=strategies.sampled_from(["sha256", "sha1_git", "blake2s256"]),
            min_size=0,
        )
    )
    def test_content_missing_unknown_algo(self, swh_storage, sample_data, algos):
        algos |= {"sha1"}
        content, missing_content = [sample_data.content2, sample_data.skipped_content]
        swh_storage.content_add([content])

        test_contents = [content.to_dict()]
        missing_per_hash = defaultdict(list)
        for i in range(16):
            test_content = missing_content.to_dict()
            for hash in algos:
                test_content[hash] = bytes([i]) + test_content[hash][1:]
                missing_per_hash[hash].append(test_content[hash])
            test_content["nonexisting_algo"] = b"\x00"
            test_contents.append(test_content)

        assert set(swh_storage.content_missing(test_contents)) == set(
            missing_per_hash["sha1"]
        )

        for hash in algos:
            assert set(
                swh_storage.content_missing(test_contents, key_hash=hash)
            ) == set(missing_per_hash[hash])

    def test_content_missing_per_sha1(self, swh_storage, sample_data):
        # given
        cont = sample_data.content
        missing_cont = sample_data.skipped_content
        swh_storage.content_add([cont])

        # when
        gen = swh_storage.content_missing_per_sha1([cont.sha1, missing_cont.sha1])
        # then
        assert list(gen) == [missing_cont.sha1]

    def test_content_missing_per_sha1_git(self, swh_storage, sample_data):
        cont, cont2 = sample_data.contents[:2]
        missing_cont = sample_data.skipped_content

        swh_storage.content_add([cont, cont2])

        contents = [cont.sha1_git, cont2.sha1_git, missing_cont.sha1_git]

        missing_contents = swh_storage.content_missing_per_sha1_git(contents)
        assert list(missing_contents) == [missing_cont.sha1_git]

    def test_content_get_partition(self, swh_storage, swh_contents):
        """content_get_partition paginates results if limit exceeded"""
        expected_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]

        actual_contents = []
        for i in range(16):
            actual_result = swh_storage.content_get_partition(i, 16)
            assert actual_result["next_page_token"] is None
            actual_contents.extend(actual_result["contents"])

        assert_contents_ok(expected_contents, actual_contents, ["sha1"])

    def test_content_get_partition_full(self, swh_storage, swh_contents):
        """content_get_partition for a single partition returns all available
        contents"""
        expected_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]

        actual_result = swh_storage.content_get_partition(0, 1)
        assert actual_result["next_page_token"] is None

        actual_contents = actual_result["contents"]
        assert_contents_ok(expected_contents, actual_contents, ["sha1"])

    def test_content_get_partition_empty(self, swh_storage, swh_contents):
        """content_get_partition when at least one of the partitions is
        empty"""
        expected_contents = {
            cont.sha1 for cont in swh_contents if cont.status != "absent"
        }
        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_partitions = 1 << math.floor(math.log2(len(swh_contents)) + 1)

        seen_sha1s = []

        for i in range(nb_partitions):
            actual_result = swh_storage.content_get_partition(
                i, nb_partitions, limit=len(swh_contents) + 1
            )

            for cont in actual_result["contents"]:
                seen_sha1s.append(cont["sha1"])

            # Limit is higher than the max number of results
            assert actual_result["next_page_token"] is None

        assert set(seen_sha1s) == expected_contents

    def test_content_get_partition_limit_none(self, swh_storage):
        """content_get_partition call with wrong limit input should fail"""
        with pytest.raises(StorageArgumentException) as e:
            swh_storage.content_get_partition(1, 16, limit=None)

        assert e.value.args == ("limit should not be None",)

    def test_generate_content_get_partition_pagination(self, swh_storage, swh_contents):
        """content_get_partition returns contents within range provided"""
        expected_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]

        # retrieve contents
        actual_contents = []
        for i in range(4):
            page_token = None
            while True:
                actual_result = swh_storage.content_get_partition(
                    i, 4, limit=3, page_token=page_token
                )
                actual_contents.extend(actual_result["contents"])
                page_token = actual_result["next_page_token"]

                if page_token is None:
                    break

        assert_contents_ok(expected_contents, actual_contents, ["sha1"])

    def test_content_get_metadata(self, swh_storage, sample_data):
        cont1, cont2 = sample_data.contents[:2]

        swh_storage.content_add([cont1, cont2])

        actual_md = swh_storage.content_get_metadata([cont1.sha1, cont2.sha1])

        # we only retrieve the metadata so no data nor ctime within
        expected_cont1, expected_cont2 = [
            attr.evolve(c, data=None).to_dict() for c in [cont1, cont2]
        ]
        expected_cont1.pop("ctime")
        expected_cont2.pop("ctime")

        assert tuple(actual_md[cont1.sha1]) == (expected_cont1,)
        assert tuple(actual_md[cont2.sha1]) == (expected_cont2,)
        assert len(actual_md.keys()) == 2

    def test_content_get_metadata_missing_sha1(self, swh_storage, sample_data):
        cont1, cont2 = sample_data.contents[:2]
        missing_cont = sample_data.skipped_content

        swh_storage.content_add([cont1, cont2])

        actual_contents = swh_storage.content_get_metadata([missing_cont.sha1])

        assert len(actual_contents) == 1
        assert tuple(actual_contents[missing_cont.sha1]) == ()

    def test_content_get_random(self, swh_storage, sample_data):
        cont, cont2, cont3 = sample_data.contents[:3]
        swh_storage.content_add([cont, cont2, cont3])

        assert swh_storage.content_get_random() in {
            cont.sha1_git,
            cont2.sha1_git,
            cont3.sha1_git,
        }

    def test_directory_add(self, swh_storage, sample_data):
        content = sample_data.content
        directory = sample_data.directories[1]
        assert directory.entries[0].target == content.sha1_git
        swh_storage.content_add([content])

        init_missing = list(swh_storage.directory_missing([directory.id]))
        assert [directory.id] == init_missing

        actual_result = swh_storage.directory_add([directory])
        assert actual_result == {"directory:add": 1}

        assert ("directory", directory) in list(
            swh_storage.journal_writer.journal.objects
        )

        actual_data = list(swh_storage.directory_ls(directory.id))
        expected_data = list(transform_entries(swh_storage, directory))

        for data in actual_data:
            assert data in expected_data

        after_missing = list(swh_storage.directory_missing([directory.id]))
        assert after_missing == []

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["directory"] == 1

    def test_directory_add_twice(self, swh_storage, sample_data):
        directory = sample_data.directories[1]

        actual_result = swh_storage.directory_add([directory])
        assert actual_result == {"directory:add": 1}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("directory", directory)
        ]

        actual_result = swh_storage.directory_add([directory])
        assert actual_result == {"directory:add": 0}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("directory", directory)
        ]

    def test_directory_ls_recursive(self, swh_storage, sample_data):
        # create consistent dataset regarding the directories we want to list
        content, content2 = sample_data.contents[:2]
        swh_storage.content_add([content, content2])
        dir1, dir2, dir3 = sample_data.directories[:3]

        dir_ids = [d.id for d in [dir1, dir2, dir3]]
        init_missing = list(swh_storage.directory_missing(dir_ids))
        assert init_missing == dir_ids

        actual_result = swh_storage.directory_add([dir1, dir2, dir3])
        assert actual_result == {"directory:add": 3}

        # List directory containing one file
        actual_data = list(swh_storage.directory_ls(dir1.id, recursive=True))
        expected_data = list(transform_entries(swh_storage, dir1))
        for data in actual_data:
            assert data in expected_data

        # List directory containing a file and an unknown subdirectory
        actual_data = list(swh_storage.directory_ls(dir2.id, recursive=True))
        expected_data = list(transform_entries(swh_storage, dir2))
        for data in actual_data:
            assert data in expected_data

        # List directory containing both a known and unknown subdirectory, entries
        # should be both those of the directory and of the known subdir (up to contents)
        actual_data = list(swh_storage.directory_ls(dir3.id, recursive=True))
        expected_data = list(
            itertools.chain(
                transform_entries(swh_storage, dir3),
                transform_entries(swh_storage, dir2, prefix=b"subdir/"),
            )
        )

        for data in actual_data:
            assert data in expected_data

    def test_directory_ls_non_recursive(self, swh_storage, sample_data):
        # create consistent dataset regarding the directories we want to list
        content, content2 = sample_data.contents[:2]
        swh_storage.content_add([content, content2])
        dir1, dir2, dir3, _, dir5 = sample_data.directories[:5]

        dir_ids = [d.id for d in [dir1, dir2, dir3, dir5]]
        init_missing = list(swh_storage.directory_missing(dir_ids))
        assert init_missing == dir_ids

        actual_result = swh_storage.directory_add([dir1, dir2, dir3, dir5])
        assert actual_result == {"directory:add": 4}

        # List directory containing a file and an unknown subdirectory
        actual_data = list(swh_storage.directory_ls(dir1.id))
        expected_data = list(transform_entries(swh_storage, dir1))
        for data in actual_data:
            assert data in expected_data

        # List directory containing a single file
        actual_data = list(swh_storage.directory_ls(dir2.id))
        expected_data = list(transform_entries(swh_storage, dir2))
        for data in actual_data:
            assert data in expected_data

        # List directory containing a known subdirectory, entries should
        # only be those of the parent directory, not of the subdir
        actual_data = list(swh_storage.directory_ls(dir3.id))
        expected_data = list(transform_entries(swh_storage, dir3))
        for data in actual_data:
            assert data in expected_data

    def test_directory_entry_get_by_path(self, swh_storage, sample_data):
        cont, content2 = sample_data.contents[:2]
        dir1, dir2, dir3, dir4, dir5 = sample_data.directories[:5]

        # given
        dir_ids = [d.id for d in [dir1, dir2, dir3, dir4, dir5]]
        init_missing = list(swh_storage.directory_missing(dir_ids))
        assert init_missing == dir_ids

        actual_result = swh_storage.directory_add([dir3, dir4])
        assert actual_result == {"directory:add": 2}

        expected_entries = [
            {
                "dir_id": dir3.id,
                "name": b"foo",
                "type": "file",
                "target": cont.sha1_git,
                "sha1": None,
                "sha1_git": None,
                "sha256": None,
                "status": None,
                "perms": from_disk.DentryPerms.content,
                "length": None,
            },
            {
                "dir_id": dir3.id,
                "name": b"subdir",
                "type": "dir",
                "target": dir2.id,
                "sha1": None,
                "sha1_git": None,
                "sha256": None,
                "status": None,
                "perms": from_disk.DentryPerms.directory,
                "length": None,
            },
            {
                "dir_id": dir3.id,
                "name": b"hello",
                "type": "file",
                "target": content2.sha1_git,
                "sha1": None,
                "sha1_git": None,
                "sha256": None,
                "status": None,
                "perms": from_disk.DentryPerms.content,
                "length": None,
            },
        ]

        # when (all must be found here)
        for entry, expected_entry in zip(dir3.entries, expected_entries):
            actual_entry = swh_storage.directory_entry_get_by_path(
                dir3.id, [entry.name]
            )
            assert actual_entry == expected_entry

        # same, but deeper
        for entry, expected_entry in zip(dir3.entries, expected_entries):
            actual_entry = swh_storage.directory_entry_get_by_path(
                dir4.id, [b"subdir1", entry.name]
            )
            expected_entry = expected_entry.copy()
            expected_entry["name"] = b"subdir1/" + expected_entry["name"]
            assert actual_entry == expected_entry

        # when (nothing should be found here since `dir` is not persisted.)
        for entry in dir2.entries:
            actual_entry = swh_storage.directory_entry_get_by_path(
                dir2.id, [entry.name]
            )
            assert actual_entry is None

    def test_directory_get_random(self, swh_storage, sample_data):
        dir1, dir2, dir3 = sample_data.directories[:3]
        swh_storage.directory_add([dir1, dir2, dir3])

        assert swh_storage.directory_get_random() in {
            dir1.id,
            dir2.id,
            dir3.id,
        }

    def test_revision_add(self, swh_storage, sample_data):
        revision = sample_data.revision
        init_missing = swh_storage.revision_missing([revision.id])
        assert list(init_missing) == [revision.id]

        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        end_missing = swh_storage.revision_missing([revision.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision)
        ]

        # already there so nothing added
        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 0}

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["revision"] == 1

    def test_revision_add_twice(self, swh_storage, sample_data):
        revision, revision2 = sample_data.revisions[:2]

        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision)
        ]

        actual_result = swh_storage.revision_add([revision, revision2])
        assert actual_result == {"revision:add": 1}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision),
            ("revision", revision2),
        ]

    def test_revision_add_name_clash(self, swh_storage, sample_data):
        revision, revision2 = sample_data.revisions[:2]

        revision1 = attr.evolve(
            revision,
            author=Person(
                fullname=b"John Doe <john.doe@example.com>",
                name=b"John Doe",
                email=b"john.doe@example.com",
            ),
        )
        revision2 = attr.evolve(
            revision2,
            author=Person(
                fullname=b"John Doe <john.doe@example.com>",
                name=b"John Doe ",
                email=b"john.doe@example.com ",
            ),
        )
        actual_result = swh_storage.revision_add([revision1, revision2])
        assert actual_result == {"revision:add": 2}

    def test_revision_get_order(self, swh_storage, sample_data):
        revision, revision2 = sample_data.revisions[:2]

        add_result = swh_storage.revision_add([revision, revision2])
        assert add_result == {"revision:add": 2}

        # order 1
        res1 = swh_storage.revision_get([revision.id, revision2.id])

        assert [Revision.from_dict(r) for r in res1] == [revision, revision2]

        # order 2
        res2 = swh_storage.revision_get([revision2.id, revision.id])
        assert [Revision.from_dict(r) for r in res2] == [revision2, revision]

    def test_revision_log(self, swh_storage, sample_data):
        revision1, revision2, revision3, revision4 = sample_data.revisions[:4]

        # rev4 -is-child-of-> rev3 -> rev1, (rev2 -> rev1)
        swh_storage.revision_add([revision1, revision2, revision3, revision4])

        # when
        results = list(swh_storage.revision_log([revision4.id]))

        # for comparison purposes
        actual_results = [Revision.from_dict(r) for r in results]
        assert len(actual_results) == 4  # rev4 -child-> rev3 -> rev1, (rev2 -> rev1)
        assert actual_results == [revision4, revision3, revision1, revision2]

    def test_revision_log_with_limit(self, swh_storage, sample_data):
        revision1, revision2, revision3, revision4 = sample_data.revisions[:4]

        # revision4 -is-child-of-> revision3
        swh_storage.revision_add([revision3, revision4])
        results = list(swh_storage.revision_log([revision4.id], 1))

        actual_results = [Revision.from_dict(r) for r in results]
        assert len(actual_results) == 1
        assert actual_results[0] == revision4

    def test_revision_log_unknown_revision(self, swh_storage, sample_data):
        revision = sample_data.revision
        rev_log = list(swh_storage.revision_log([revision.id]))
        assert rev_log == []

    def test_revision_shortlog(self, swh_storage, sample_data):
        revision1, revision2, revision3, revision4 = sample_data.revisions[:4]

        # rev4 -is-child-of-> rev3 -> (rev1, rev2); rev2 -> rev1
        swh_storage.revision_add([revision1, revision2, revision3, revision4])

        results = list(swh_storage.revision_shortlog([revision4.id]))
        actual_results = [[id, tuple(parents)] for (id, parents) in results]

        assert len(actual_results) == 4
        assert actual_results == [
            [revision4.id, revision4.parents],
            [revision3.id, revision3.parents],
            [revision1.id, revision1.parents],
            [revision2.id, revision2.parents],
        ]

    def test_revision_shortlog_with_limit(self, swh_storage, sample_data):
        revision1, revision2, revision3, revision4 = sample_data.revisions[:4]

        # revision4 -is-child-of-> revision3
        swh_storage.revision_add([revision1, revision2, revision3, revision4])
        results = list(swh_storage.revision_shortlog([revision4.id], 1))
        actual_results = [[id, tuple(parents)] for (id, parents) in results]

        assert len(actual_results) == 1
        assert list(actual_results[0]) == [revision4.id, revision4.parents]

    def test_revision_get(self, swh_storage, sample_data):
        revision, revision2 = sample_data.revisions[:2]

        swh_storage.revision_add([revision])

        actual_revisions = list(swh_storage.revision_get([revision.id, revision2.id]))

        assert len(actual_revisions) == 2
        assert Revision.from_dict(actual_revisions[0]) == revision
        assert actual_revisions[1] is None

    def test_revision_get_no_parents(self, swh_storage, sample_data):
        revision = sample_data.revision
        swh_storage.revision_add([revision])

        get = list(swh_storage.revision_get([revision.id]))

        assert len(get) == 1
        assert revision.parents == ()
        assert tuple(get[0]["parents"]) == ()  # no parents on this one

    def test_revision_get_random(self, swh_storage, sample_data):
        revision1, revision2, revision3 = sample_data.revisions[:3]

        swh_storage.revision_add([revision1, revision2, revision3])

        assert swh_storage.revision_get_random() in {
            revision1.id,
            revision2.id,
            revision3.id,
        }

    def test_release_add(self, swh_storage, sample_data):
        release, release2 = sample_data.releases[:2]

        init_missing = swh_storage.release_missing([release.id, release2.id])
        assert list(init_missing) == [release.id, release2.id]

        actual_result = swh_storage.release_add([release, release2])
        assert actual_result == {"release:add": 2}

        end_missing = swh_storage.release_missing([release.id, release2.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("release", release),
            ("release", release2),
        ]

        # already present so nothing added
        actual_result = swh_storage.release_add([release, release2])
        assert actual_result == {"release:add": 0}

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["release"] == 2

    def test_release_add_no_author_date(self, swh_storage, sample_data):
        full_release = sample_data.release

        release = attr.evolve(full_release, author=None, date=None)
        actual_result = swh_storage.release_add([release])
        assert actual_result == {"release:add": 1}

        end_missing = swh_storage.release_missing([release.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("release", release)
        ]

    def test_release_add_twice(self, swh_storage, sample_data):
        release, release2 = sample_data.releases[:2]

        actual_result = swh_storage.release_add([release])
        assert actual_result == {"release:add": 1}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("release", release)
        ]

        actual_result = swh_storage.release_add([release, release2, release, release2])
        assert actual_result == {"release:add": 1}

        assert set(swh_storage.journal_writer.journal.objects) == set(
            [("release", release), ("release", release2),]
        )

    def test_release_add_name_clash(self, swh_storage, sample_data):
        release, release2 = [
            attr.evolve(
                c,
                author=Person(
                    fullname=b"John Doe <john.doe@example.com>",
                    name=b"John Doe",
                    email=b"john.doe@example.com",
                ),
            )
            for c in sample_data.releases[:2]
        ]

        actual_result = swh_storage.release_add([release, release2])
        assert actual_result == {"release:add": 2}

    def test_release_get(self, swh_storage, sample_data):
        release, release2, release3 = sample_data.releases[:3]

        # given
        swh_storage.release_add([release, release2])

        # when
        releases = list(swh_storage.release_get([release.id, release2.id]))
        actual_releases = [Release.from_dict(r) for r in releases]

        # then
        assert actual_releases == [release, release2]

        unknown_releases = list(swh_storage.release_get([release3.id]))
        assert unknown_releases[0] is None

    def test_release_get_order(self, swh_storage, sample_data):
        release, release2 = sample_data.releases[:2]

        add_result = swh_storage.release_add([release, release2])
        assert add_result == {"release:add": 2}

        # order 1
        res1 = swh_storage.release_get([release.id, release2.id])
        assert list(res1) == [release.to_dict(), release2.to_dict()]

        # order 2
        res2 = swh_storage.release_get([release2.id, release.id])
        assert list(res2) == [release2.to_dict(), release.to_dict()]

    def test_release_get_random(self, swh_storage, sample_data):
        release, release2, release3 = sample_data.releases[:3]

        swh_storage.release_add([release, release2, release3])

        assert swh_storage.release_get_random() in {
            release.id,
            release2.id,
            release3.id,
        }

    def test_origin_add(self, swh_storage, sample_data):
        origins = list(sample_data.origins[:2])
        origin_urls = [o.url for o in origins]

        assert swh_storage.origin_get(origin_urls) == [None, None]

        stats = swh_storage.origin_add(origins)
        assert stats == {"origin:add": 2}

        actual_origins = swh_storage.origin_get(origin_urls)
        assert actual_origins == origins

        assert set(swh_storage.journal_writer.journal.objects) == set(
            [("origin", origins[0]), ("origin", origins[1]),]
        )

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["origin"] == 2

    def test_origin_add_twice(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        add1 = swh_storage.origin_add([origin, origin2])
        assert set(swh_storage.journal_writer.journal.objects) == set(
            [("origin", origin), ("origin", origin2),]
        )
        assert add1 == {"origin:add": 2}

        add2 = swh_storage.origin_add([origin, origin2])
        assert set(swh_storage.journal_writer.journal.objects) == set(
            [("origin", origin), ("origin", origin2),]
        )
        assert add2 == {"origin:add": 0}

    def test_origin_get(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        assert swh_storage.origin_get([origin.url]) == [None]
        swh_storage.origin_add([origin])

        actual_origins = swh_storage.origin_get([origin.url])
        assert actual_origins == [origin]

        actual_origins = swh_storage.origin_get([origin.url, "not://exists"])
        assert actual_origins == [origin, None]

    def _generate_random_visits(self, nb_visits=100, start=0, end=7):
        """Generate random visits within the last 2 months (to avoid
        computations)

        """
        visits = []
        today = now()
        for weeks in range(nb_visits, 0, -1):
            hours = random.randint(0, 24)
            minutes = random.randint(0, 60)
            seconds = random.randint(0, 60)
            days = random.randint(0, 28)
            weeks = random.randint(start, end)
            date_visit = today - timedelta(
                weeks=weeks, hours=hours, minutes=minutes, seconds=seconds, days=days
            )
            visits.append(date_visit)
        return visits

    def test_origin_visit_get__unknown_origin(self, swh_storage):
        actual_page = swh_storage.origin_visit_get("foo")
        assert actual_page.next_page_token is None
        assert actual_page.results == []
        assert actual_page == PagedResult()

    def test_origin_visit_get__validation_failure(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        with pytest.raises(
            StorageArgumentException, match="page_token must be a string"
        ):
            swh_storage.origin_visit_get(origin.url, page_token=10)  # not bytes

        with pytest.raises(
            StorageArgumentException, match="order must be a ListOrder value"
        ):
            swh_storage.origin_visit_get(origin.url, order="foobar")  # wrong order

    def test_origin_visit_get_all(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        ov1, ov2, ov3 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                ),
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit2,
                    type=sample_data.type_visit2,
                ),
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit2,
                    type=sample_data.type_visit2,
                ),
            ]
        )

        # order asc, no token, no limit
        actual_page = swh_storage.origin_visit_get(origin.url)
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov1, ov2, ov3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_get(origin.url, limit=2)
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ov1, ov2]

        # order asc, token, no limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_get(origin.url, limit=1)
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ov1]

        # order asc, token, no limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov2, ov3]

        # order asc, token, limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, limit=2
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov2, ov3]

        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, limit=1
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ov2]

        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, limit=1
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov3]

        # order desc, no token, no limit
        actual_page = swh_storage.origin_visit_get(origin.url, order=ListOrder.DESC)
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov3, ov2, ov1]

        # order desc, no token, limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, limit=2, order=ListOrder.DESC
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ov3, ov2]

        # order desc, token, no limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov1]

        # order desc, no token, limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, limit=1, order=ListOrder.DESC
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ov3]

        # order desc, token, no limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov2, ov1]

        # order desc, token, limit
        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, order=ListOrder.DESC, limit=1
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ov2]

        actual_page = swh_storage.origin_visit_get(
            origin.url, page_token=next_page_token, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ov1]

    def test_origin_visit_status_get__unknown_cases(self, swh_storage, sample_data):
        origin = sample_data.origin
        actual_page = swh_storage.origin_visit_status_get("foobar", 1)
        assert actual_page.next_page_token is None
        assert actual_page.results == []

        actual_page = swh_storage.origin_visit_status_get(origin.url, 1)
        assert actual_page.next_page_token is None
        assert actual_page.results == []

        origin = sample_data.origin
        swh_storage.origin_add([origin])
        ov1 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                ),
            ]
        )[0]
        actual_page = swh_storage.origin_visit_status_get(origin.url, ov1.visit + 10)
        assert actual_page.next_page_token is None
        assert actual_page.results == []

    def test_origin_visit_status_get_all(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        date_visit3 = round_to_milliseconds(now())
        date_visit1 = date_visit3 - datetime.timedelta(hours=2)
        date_visit2 = date_visit3 - datetime.timedelta(hours=1)
        assert date_visit1 < date_visit2 < date_visit3

        ov1 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url, date=date_visit1, type=sample_data.type_visit1,
                ),
            ]
        )[0]

        ovs1 = OriginVisitStatus(
            origin=origin.url,
            visit=ov1.visit,
            date=date_visit1,
            status="created",
            snapshot=None,
        )

        ovs2 = OriginVisitStatus(
            origin=origin.url,
            visit=ov1.visit,
            date=date_visit2,
            status="partial",
            snapshot=None,
        )

        ovs3 = OriginVisitStatus(
            origin=origin.url,
            visit=ov1.visit,
            date=date_visit3,
            status="full",
            snapshot=sample_data.snapshot.id,
            metadata={},
        )

        swh_storage.origin_visit_status_add([ovs2, ovs3])

        # order asc, no token, no limit
        actual_page = swh_storage.origin_visit_status_get(origin.url, ov1.visit)
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs1, ovs2, ovs3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, limit=2
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovs1, ovs2]

        # order asc, token, no limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, limit=1
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovs1]

        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs2, ovs3]

        # order asc, token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token, limit=2
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs2, ovs3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, limit=2
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovs1, ovs2]

        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token, limit=1
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs3]

        # order desc, no token, no limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs3, ovs2, ovs1]

        # order desc, no token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, limit=2, order=ListOrder.DESC
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovs3, ovs2]

        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs1]

        # order desc, no token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, order=ListOrder.DESC, limit=1
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovs3]

        # order desc, token, no limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs2, ovs1]

        # order desc, token, limit
        actual_page = swh_storage.origin_visit_status_get(
            origin.url,
            ov1.visit,
            page_token=next_page_token,
            order=ListOrder.DESC,
            limit=1,
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovs2]

        actual_page = swh_storage.origin_visit_status_get(
            origin.url, ov1.visit, page_token=next_page_token, order=ListOrder.DESC
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovs1]

    def test_origin_visit_status_get_random(self, swh_storage, sample_data):
        origins = sample_data.origins[:2]
        swh_storage.origin_add(origins)

        # Add some random visits within the selection range
        visits = self._generate_random_visits()
        visit_type = "git"

        # Add visits to those origins
        for origin in origins:
            for date_visit in visits:
                visit = swh_storage.origin_visit_add(
                    [OriginVisit(origin=origin.url, date=date_visit, type=visit_type,)]
                )[0]
                swh_storage.origin_visit_status_add(
                    [
                        OriginVisitStatus(
                            origin=origin.url,
                            visit=visit.visit,
                            date=now(),
                            status="full",
                            snapshot=None,
                        )
                    ]
                )

        swh_storage.refresh_stat_counters()

        stats = swh_storage.stat_counters()
        assert stats["origin"] == len(origins)
        assert stats["origin_visit"] == len(origins) * len(visits)

        random_ov, random_ovs = swh_storage.origin_visit_status_get_random(visit_type)
        assert random_ov and random_ovs
        assert random_ov.origin is not None
        assert random_ov.origin == random_ovs.origin
        assert random_ov.origin in [o.url for o in origins]

    def test_origin_visit_status_get_random_nothing_found(
        self, swh_storage, sample_data
    ):
        origins = sample_data.origins
        swh_storage.origin_add(origins)
        visit_type = "hg"
        # Add some visits outside of the random generation selection so nothing
        # will be found by the random selection
        visits = self._generate_random_visits(nb_visits=3, start=13, end=24)
        for origin in origins:
            for date_visit in visits:
                visit = swh_storage.origin_visit_add(
                    [OriginVisit(origin=origin.url, date=date_visit, type=visit_type,)]
                )[0]
                swh_storage.origin_visit_status_add(
                    [
                        OriginVisitStatus(
                            origin=origin.url,
                            visit=visit.visit,
                            date=now(),
                            status="full",
                            snapshot=None,
                        )
                    ]
                )

        random_origin_visit = swh_storage.origin_visit_status_get_random(visit_type)
        assert random_origin_visit is None

    def test_origin_get_by_sha1(self, swh_storage, sample_data):
        origin = sample_data.origin
        assert swh_storage.origin_get([origin.url])[0] is None
        swh_storage.origin_add([origin])

        origins = list(swh_storage.origin_get_by_sha1([sha1(origin.url)]))
        assert len(origins) == 1
        assert origins[0]["url"] == origin.url

    def test_origin_get_by_sha1_not_found(self, swh_storage, sample_data):
        origin = sample_data.origin
        assert swh_storage.origin_get([origin.url])[0] is None
        origins = list(swh_storage.origin_get_by_sha1([sha1(origin.url)]))
        assert len(origins) == 1
        assert origins[0] is None

    def test_origin_search_single_result(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        actual_page = swh_storage.origin_search(origin.url)
        assert actual_page.next_page_token is None
        assert actual_page.results == []

        actual_page = swh_storage.origin_search(origin.url, regexp=True)
        assert actual_page.next_page_token is None
        assert actual_page.results == []

        swh_storage.origin_add([origin])
        actual_page = swh_storage.origin_search(origin.url)
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin]

        actual_page = swh_storage.origin_search(f".{origin.url[1:-1]}.", regexp=True)
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin]

        swh_storage.origin_add([origin2])
        actual_page = swh_storage.origin_search(origin2.url)
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin2]

        actual_page = swh_storage.origin_search(f".{origin2.url[1:-1]}.", regexp=True)
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin2]

    def test_origin_search_no_regexp(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]
        swh_storage.origin_add([origin, origin2])

        # no pagination
        actual_page = swh_storage.origin_search("/")
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin, origin2]

        # offset=0
        actual_page = swh_storage.origin_search("/", page_token=None, limit=1)
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [origin]

        # offset=1
        actual_page = swh_storage.origin_search(
            "/", page_token=next_page_token, limit=1
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin2]

    def test_origin_search_regexp_substring(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        swh_storage.origin_add([origin, origin2])

        # no pagination
        actual_page = swh_storage.origin_search("/", regexp=True)
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin, origin2]

        # offset=0
        actual_page = swh_storage.origin_search(
            "/", page_token=None, limit=1, regexp=True
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [origin]

        # offset=1
        actual_page = swh_storage.origin_search(
            "/", page_token=next_page_token, limit=1, regexp=True
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin2]

    def test_origin_search_regexp_fullstring(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        swh_storage.origin_add([origin, origin2])

        # no pagination
        actual_page = swh_storage.origin_search(".*/.*", regexp=True)
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin, origin2]

        # offset=0
        actual_page = swh_storage.origin_search(
            ".*/.*", page_token=None, limit=1, regexp=True
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [origin]

        # offset=1
        actual_page = swh_storage.origin_search(
            ".*/.*", page_token=next_page_token, limit=1, regexp=True
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin2]

    def test_origin_visit_add(self, swh_storage, sample_data):
        origin1 = sample_data.origins[1]
        swh_storage.origin_add([origin1])

        date_visit = now()
        date_visit2 = date_visit + datetime.timedelta(minutes=1)

        date_visit = round_to_milliseconds(date_visit)
        date_visit2 = round_to_milliseconds(date_visit2)

        visit1 = OriginVisit(
            origin=origin1.url, date=date_visit, type=sample_data.type_visit1,
        )
        visit2 = OriginVisit(
            origin=origin1.url, date=date_visit2, type=sample_data.type_visit2,
        )

        # add once
        ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])
        # then again (will be ignored as they already exist)
        origin_visit1, origin_visit2 = swh_storage.origin_visit_add([ov1, ov2])
        assert ov1 == origin_visit1
        assert ov2 == origin_visit2

        ovs1 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov1.visit,
            date=date_visit,
            status="created",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov2.visit,
            date=date_visit2,
            status="created",
            snapshot=None,
        )

        actual_visits = swh_storage.origin_visit_get(origin1.url).results
        expected_visits = [ov1, ov2]
        assert len(expected_visits) == len(actual_visits)
        for visit in expected_visits:
            assert visit in actual_visits

        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = list(
            [("origin", origin1)]
            + [("origin_visit", visit) for visit in expected_visits] * 2
            + [("origin_visit_status", ovs) for ovs in [ovs1, ovs2]]
        )

        for obj in expected_objects:
            assert obj in actual_objects

    def test_origin_visit_add_validation(self, swh_storage, sample_data):
        """Unknown origin when adding visits should raise"""
        visit = attr.evolve(sample_data.origin_visit, origin="something-unknonw")
        with pytest.raises(StorageArgumentException, match="Unknown origin"):
            swh_storage.origin_visit_add([visit])

        objects = list(swh_storage.journal_writer.journal.objects)
        assert not objects

    def test_origin_visit_status_add_validation(self, swh_storage):
        """Wrong origin_visit_status input should raise storage argument error"""
        date_visit = now()
        visit_status1 = OriginVisitStatus(
            origin="unknown-origin-url",
            visit=10,
            date=date_visit,
            status="full",
            snapshot=None,
        )
        with pytest.raises(StorageArgumentException, match="Unknown origin"):
            swh_storage.origin_visit_status_add([visit_status1])

        objects = list(swh_storage.journal_writer.journal.objects)
        assert not objects

    def test_origin_visit_status_add(self, swh_storage, sample_data):
        """Correct origin visit statuses should add a new visit status

        """
        snapshot = sample_data.snapshot
        origin1 = sample_data.origins[1]
        origin2 = Origin(url="new-origin")
        swh_storage.origin_add([origin1, origin2])

        ov1, ov2 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin1.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                ),
                OriginVisit(
                    origin=origin2.url,
                    date=sample_data.date_visit2,
                    type=sample_data.type_visit2,
                ),
            ]
        )

        ovs1 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov1.visit,
            date=sample_data.date_visit1,
            status="created",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=origin2.url,
            visit=ov2.visit,
            date=sample_data.date_visit2,
            status="created",
            snapshot=None,
        )

        date_visit_now = round_to_milliseconds(now())
        visit_status1 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit_now,
            status="full",
            snapshot=snapshot.id,
        )

        date_visit_now = round_to_milliseconds(now())
        visit_status2 = OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=date_visit_now,
            status="ongoing",
            snapshot=None,
            metadata={"intrinsic": "something"},
        )
        swh_storage.origin_visit_status_add([visit_status1, visit_status2])

        visit = swh_storage.origin_visit_get_latest(origin1.url, require_snapshot=True)
        visit_status = swh_storage.origin_visit_status_get_latest(
            origin1.url, visit.visit, require_snapshot=True
        )
        assert visit_status == visit_status1

        visit = swh_storage.origin_visit_get_latest(origin2.url, require_snapshot=False)
        visit_status = swh_storage.origin_visit_status_get_latest(
            origin2.url, visit.visit, require_snapshot=False
        )
        assert origin2.url != origin1.url
        assert visit_status == visit_status2

        actual_objects = list(swh_storage.journal_writer.journal.objects)

        expected_origins = [origin1, origin2]
        expected_visits = [ov1, ov2]
        expected_visit_statuses = [ovs1, ovs2, visit_status1, visit_status2]

        expected_objects = (
            [("origin", o) for o in expected_origins]
            + [("origin_visit", v) for v in expected_visits]
            + [("origin_visit_status", ovs) for ovs in expected_visit_statuses]
        )

        for obj in expected_objects:
            assert obj in actual_objects

    def test_origin_visit_status_add_twice(self, swh_storage, sample_data):
        """Correct origin visit statuses should add a new visit status

        """
        snapshot = sample_data.snapshot
        origin1 = sample_data.origins[1]
        swh_storage.origin_add([origin1])
        ov1 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin1.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                ),
            ]
        )[0]

        ovs1 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov1.visit,
            date=sample_data.date_visit1,
            status="created",
            snapshot=None,
        )
        date_visit_now = round_to_milliseconds(now())
        visit_status1 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit_now,
            status="full",
            snapshot=snapshot.id,
        )

        swh_storage.origin_visit_status_add([visit_status1])
        # second call will ignore existing entries (will send to storage though)
        swh_storage.origin_visit_status_add([visit_status1])

        visit_status = swh_storage.origin_visit_status_get_latest(ov1.origin, ov1.visit)
        assert visit_status == visit_status1

        actual_objects = list(swh_storage.journal_writer.journal.objects)

        expected_origins = [origin1]
        expected_visits = [ov1]
        expected_visit_statuses = [ovs1, visit_status1, visit_status1]

        # write twice in the journal
        expected_objects = (
            [("origin", o) for o in expected_origins]
            + [("origin_visit", v) for v in expected_visits]
            + [("origin_visit_status", ovs) for ovs in expected_visit_statuses]
        )

        for obj in expected_objects:
            assert obj in actual_objects

    def test_origin_visit_find_by_date(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit2,
            type=sample_data.type_visit1,
        )
        visit2 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit3,
            type=sample_data.type_visit2,
        )
        visit3 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit2,
            type=sample_data.type_visit3,
        )
        ov1, ov2, ov3 = swh_storage.origin_visit_add([visit1, visit2, visit3])

        ovs1 = OriginVisitStatus(
            origin=origin.url,
            visit=ov1.visit,
            date=sample_data.date_visit2,
            status="ongoing",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=origin.url,
            visit=ov2.visit,
            date=sample_data.date_visit3,
            status="ongoing",
            snapshot=None,
        )
        ovs3 = OriginVisitStatus(
            origin=origin.url,
            visit=ov3.visit,
            date=sample_data.date_visit2,
            status="ongoing",
            snapshot=None,
        )
        swh_storage.origin_visit_status_add([ovs1, ovs2, ovs3])

        # Simple case
        actual_visit = swh_storage.origin_visit_find_by_date(
            origin.url, sample_data.date_visit3
        )
        assert actual_visit == ov2

        # There are two visits at the same date, the latest must be returned
        actual_visit = swh_storage.origin_visit_find_by_date(
            origin.url, sample_data.date_visit2
        )
        assert actual_visit == ov3

    def test_origin_visit_find_by_date__unknown_origin(self, swh_storage, sample_data):
        actual_visit = swh_storage.origin_visit_find_by_date(
            "foo", sample_data.date_visit2
        )
        assert actual_visit is None

    def test_origin_visit_get_by(self, swh_storage, sample_data):
        snapshot = sample_data.snapshot
        origins = sample_data.origins[:2]
        swh_storage.origin_add(origins)
        origin_url, origin_url2 = [o.url for o in origins]

        visit = OriginVisit(
            origin=origin_url,
            date=sample_data.date_visit2,
            type=sample_data.type_visit2,
        )
        origin_visit1 = swh_storage.origin_visit_add([visit])[0]

        swh_storage.snapshot_add([snapshot])
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin_url,
                    visit=origin_visit1.visit,
                    date=now(),
                    status="ongoing",
                    snapshot=snapshot.id,
                )
            ]
        )

        # Add some other {origin, visit} entries
        visit2 = OriginVisit(
            origin=origin_url,
            date=sample_data.date_visit3,
            type=sample_data.type_visit3,
        )
        visit3 = OriginVisit(
            origin=origin_url2,
            date=sample_data.date_visit3,
            type=sample_data.type_visit3,
        )
        swh_storage.origin_visit_add([visit2, visit3])

        # when
        visit1_metadata = {
            "contents": 42,
            "directories": 22,
        }

        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin_url,
                    visit=origin_visit1.visit,
                    date=now(),
                    status="full",
                    snapshot=snapshot.id,
                    metadata=visit1_metadata,
                )
            ]
        )

        actual_visit = swh_storage.origin_visit_get_by(origin_url, origin_visit1.visit)
        assert actual_visit == origin_visit1

    def test_origin_visit_get_by__no_result(self, swh_storage, sample_data):
        actual_visit = swh_storage.origin_visit_get_by("unknown", 10)  # unknown origin
        assert actual_visit is None

        origin = sample_data.origin
        swh_storage.origin_add([origin])
        actual_visit = swh_storage.origin_visit_get_by(origin.url, 999)  # unknown visit
        assert actual_visit is None

    def test_origin_visit_get_latest_edge_cases(self, swh_storage, sample_data):
        # unknown origin so no result
        assert swh_storage.origin_visit_get_latest("unknown-origin") is None

        # unknown type so no result
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        assert swh_storage.origin_visit_get_latest(origin.url, type="unknown") is None

        # unknown allowed statuses should raise
        with pytest.raises(StorageArgumentException, match="Unknown allowed statuses"):
            swh_storage.origin_visit_get_latest(
                origin.url, allowed_statuses=["unknown"]
            )

    def test_origin_visit_get_latest_filter_type(self, swh_storage, sample_data):
        """Filtering origin visit get latest with filter type should be ok

        """
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit1, type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit2, type="hg",
        )
        date_now = round_to_milliseconds(now())
        visit3 = OriginVisit(origin=origin.url, date=date_now, type="hg",)
        assert sample_data.date_visit1 < sample_data.date_visit2
        assert sample_data.date_visit2 < date_now

        ov1, ov2, ov3 = swh_storage.origin_visit_add([visit1, visit2, visit3])

        # Check type filter is ok
        actual_visit = swh_storage.origin_visit_get_latest(origin.url, type="git")
        assert actual_visit == ov1
        actual_visit = swh_storage.origin_visit_get_latest(origin.url, type="hg")
        assert actual_visit == ov3
        actual_visit_unknown_type = swh_storage.origin_visit_get_latest(
            origin.url, type="npm",  # no visit matching that type
        )
        assert actual_visit_unknown_type is None

    def test_origin_visit_get_latest(self, swh_storage, sample_data):
        empty_snapshot, complete_snapshot = sample_data.snapshots[1:3]
        origin = sample_data.origin

        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit1, type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit2, type="hg",
        )
        date_now = round_to_milliseconds(now())
        visit3 = OriginVisit(origin=origin.url, date=date_now, type="hg",)
        assert visit1.date < visit2.date
        assert visit2.date < visit3.date

        ov1, ov2, ov3 = swh_storage.origin_visit_add([visit1, visit2, visit3])

        # no filters, latest visit is the last one (whose date is most recent)
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov3

        # 3 visits, none has snapshot so nothing is returned
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, require_snapshot=True
        )
        assert actual_visit is None

        # visit are created with "created" status, so nothing will get returned
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["partial"]
        )
        assert actual_visit is None

        # visit are created with "created" status, so most recent again
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["created"]
        )
        assert actual_visit == ov3

        # Add snapshot to visit1; require_snapshot=True makes it return first visit
        swh_storage.snapshot_add([complete_snapshot])
        visit_status_with_snapshot = OriginVisitStatus(
            origin=origin.url,
            visit=ov1.visit,
            date=round_to_milliseconds(now()),
            status="ongoing",
            snapshot=complete_snapshot.id,
        )
        swh_storage.origin_visit_status_add([visit_status_with_snapshot])
        # only the first visit has a snapshot now
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, require_snapshot=True
        )
        assert actual_visit == ov1

        # only the first visit has a status ongoing now
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["ongoing"]
        )
        assert actual_visit == ov1

        actual_visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url, ov1.visit, require_snapshot=True
        )
        assert actual_visit_status == visit_status_with_snapshot

        # ... and require_snapshot=False (defaults) still returns latest visit (3rd)
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, require_snapshot=False
        )
        assert actual_visit == ov3
        # no specific filter, this returns as before the latest visit
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov3

        # Status filter: all three visits are status=ongoing, so no visit
        # returned
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["full"]
        )
        assert actual_visit is None

        visit_status1_full = OriginVisitStatus(
            origin=origin.url,
            visit=ov1.visit,
            date=round_to_milliseconds(now()),
            status="full",
            snapshot=complete_snapshot.id,
        )
        # Mark the first visit as completed and check status filter again
        swh_storage.origin_visit_status_add([visit_status1_full])

        # only the first visit has the full status
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["full"]
        )
        assert actual_visit == ov1

        actual_visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url, ov1.visit, allowed_statuses=["full"]
        )
        assert actual_visit_status == visit_status1_full

        # no specific filter, this returns as before the latest visit
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov3

        # Add snapshot to visit2 and check that the new snapshot is returned
        swh_storage.snapshot_add([empty_snapshot])

        visit_status2_full = OriginVisitStatus(
            origin=origin.url,
            visit=ov2.visit,
            date=round_to_milliseconds(now()),
            status="ongoing",
            snapshot=empty_snapshot.id,
        )
        swh_storage.origin_visit_status_add([visit_status2_full])
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, require_snapshot=True
        )
        # 2nd visit is most recent with a snapshot
        assert actual_visit == ov2
        actual_visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url, ov2.visit, require_snapshot=True
        )
        assert actual_visit_status == visit_status2_full

        # no specific filter, this returns as before the latest visit, 3rd one
        actual_origin = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_origin == ov3

        # full status is still the first visit
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["full"]
        )
        assert actual_visit == ov1

        # Add snapshot to visit3 (same date as visit2)
        visit_status3_with_snapshot = OriginVisitStatus(
            origin=origin.url,
            visit=ov3.visit,
            date=round_to_milliseconds(now()),
            status="ongoing",
            snapshot=complete_snapshot.id,
        )
        swh_storage.origin_visit_status_add([visit_status3_with_snapshot])

        # full status is still the first visit
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, allowed_statuses=["full"], require_snapshot=True,
        )
        assert actual_visit == ov1

        actual_visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url,
            visit=actual_visit.visit,
            allowed_statuses=["full"],
            require_snapshot=True,
        )
        assert actual_visit_status == visit_status1_full

        # most recent is still the 3rd visit
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov3

        # 3rd visit has a snapshot now, so it's elected
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url, require_snapshot=True
        )
        assert actual_visit == ov3

        actual_visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url, ov3.visit, require_snapshot=True
        )
        assert actual_visit_status == visit_status3_with_snapshot

    def test_origin_visit_get_latest__same_date(self, swh_storage, sample_data):
        empty_snapshot, complete_snapshot = sample_data.snapshots[1:3]
        origin = sample_data.origin

        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit1, type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit1, type="hg",
        )

        ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])

        # ties should be broken by using the visit id
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov2

    def test_origin_visit_status_get_latest__validation(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url, date=sample_data.date_visit1, type="git",
        )

        # unknown allowed statuses should raise
        with pytest.raises(StorageArgumentException, match="Unknown allowed statuses"):
            swh_storage.origin_visit_status_get_latest(
                origin.url, visit1.visit, allowed_statuses=["unknown"]
            )

    def test_origin_visit_status_get_latest(self, swh_storage, sample_data):
        snapshot = sample_data.snapshots[2]
        origin1 = sample_data.origin
        swh_storage.origin_add([origin1])

        # to have some reference visits

        ov1, ov2 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin1.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                ),
                OriginVisit(
                    origin=origin1.url,
                    date=sample_data.date_visit2,
                    type=sample_data.type_visit2,
                ),
            ]
        )
        swh_storage.snapshot_add([snapshot])

        date_now = round_to_milliseconds(now())
        assert sample_data.date_visit1 < sample_data.date_visit2
        assert sample_data.date_visit2 < date_now

        ovs1 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov1.visit,
            date=sample_data.date_visit1,
            status="partial",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov1.visit,
            date=sample_data.date_visit2,
            status="ongoing",
            snapshot=None,
        )
        ovs3 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov2.visit,
            date=sample_data.date_visit2
            + datetime.timedelta(minutes=1),  # to not be ignored
            status="ongoing",
            snapshot=None,
        )
        ovs4 = OriginVisitStatus(
            origin=origin1.url,
            visit=ov2.visit,
            date=date_now,
            status="full",
            snapshot=snapshot.id,
            metadata={"something": "wicked"},
        )

        swh_storage.origin_visit_status_add([ovs1, ovs2, ovs3, ovs4])

        # unknown origin so no result
        actual_origin_visit = swh_storage.origin_visit_status_get_latest(
            "unknown-origin", ov1.visit
        )
        assert actual_origin_visit is None

        # unknown visit so no result
        actual_origin_visit = swh_storage.origin_visit_status_get_latest(
            ov1.origin, ov1.visit + 10
        )
        assert actual_origin_visit is None

        # Two visits, both with no snapshot, take the most recent
        actual_origin_visit2 = swh_storage.origin_visit_status_get_latest(
            origin1.url, ov1.visit
        )
        assert isinstance(actual_origin_visit2, OriginVisitStatus)
        assert actual_origin_visit2 == ovs2
        assert ovs2.origin == origin1.url
        assert ovs2.visit == ov1.visit

        actual_origin_visit = swh_storage.origin_visit_status_get_latest(
            origin1.url, ov1.visit, require_snapshot=True
        )
        # there is no visit with snapshot yet for that visit
        assert actual_origin_visit is None

        actual_origin_visit2 = swh_storage.origin_visit_status_get_latest(
            origin1.url, ov1.visit, allowed_statuses=["partial", "ongoing"]
        )
        # visit status with partial status visit elected
        assert actual_origin_visit2 == ovs2
        assert actual_origin_visit2.status == "ongoing"

        actual_origin_visit4 = swh_storage.origin_visit_status_get_latest(
            origin1.url, ov2.visit, require_snapshot=True
        )
        assert actual_origin_visit4 == ovs4
        assert actual_origin_visit4.snapshot == snapshot.id

        actual_origin_visit = swh_storage.origin_visit_status_get_latest(
            origin1.url, ov2.visit, require_snapshot=True, allowed_statuses=["ongoing"]
        )
        # nothing matches so nothing
        assert actual_origin_visit is None  # there is no visit with status full

        actual_origin_visit3 = swh_storage.origin_visit_status_get_latest(
            origin1.url, ov2.visit, allowed_statuses=["ongoing"]
        )
        assert actual_origin_visit3 == ovs3

    def test_person_fullname_unicity(self, swh_storage, sample_data):
        revision, rev2 = sample_data.revisions[0:2]
        # create a revision with same committer fullname but wo name and email
        revision2 = attr.evolve(
            rev2,
            committer=Person(
                fullname=revision.committer.fullname, name=None, email=None
            ),
        )

        swh_storage.revision_add([revision, revision2])

        # when getting added revisions
        revisions = list(swh_storage.revision_get([revision.id, revision2.id]))

        # then check committers are the same
        assert revisions[0]["committer"] == revisions[1]["committer"]

    def test_snapshot_add_get_empty(self, swh_storage, sample_data):
        empty_snapshot = sample_data.snapshots[1]
        empty_snapshot_dict = empty_snapshot.to_dict()

        origin = sample_data.origin
        swh_storage.origin_add([origin])
        ov1 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                )
            ]
        )[0]

        actual_result = swh_storage.snapshot_add([empty_snapshot])
        assert actual_result == {"snapshot:add": 1}

        date_now = now()

        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=date_now,
                    status="full",
                    snapshot=empty_snapshot.id,
                )
            ]
        )

        by_id = swh_storage.snapshot_get(empty_snapshot.id)
        assert by_id == {**empty_snapshot_dict, "next_branch": None}

        by_ov = swh_storage.snapshot_get_by_origin_visit(origin.url, ov1.visit)
        assert by_ov == {**empty_snapshot_dict, "next_branch": None}

        ovs1 = OriginVisitStatus.from_dict(
            {
                "origin": origin.url,
                "date": sample_data.date_visit1,
                "visit": ov1.visit,
                "status": "created",
                "snapshot": None,
                "metadata": None,
            }
        )
        ovs2 = OriginVisitStatus.from_dict(
            {
                "origin": origin.url,
                "date": date_now,
                "visit": ov1.visit,
                "status": "full",
                "metadata": None,
                "snapshot": empty_snapshot.id,
            }
        )
        actual_objects = list(swh_storage.journal_writer.journal.objects)

        expected_objects = [
            ("origin", origin),
            ("origin_visit", ov1),
            ("origin_visit_status", ovs1,),
            ("snapshot", empty_snapshot),
            ("origin_visit_status", ovs2,),
        ]
        for obj in expected_objects:
            assert obj in actual_objects

    def test_snapshot_add_get_complete(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]
        complete_snapshot_dict = complete_snapshot.to_dict()
        origin = sample_data.origin

        swh_storage.origin_add([origin])
        visit = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type=sample_data.type_visit1,
        )
        origin_visit1 = swh_storage.origin_visit_add([visit])[0]
        visit_id = origin_visit1.visit

        actual_result = swh_storage.snapshot_add([complete_snapshot])
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=origin_visit1.visit,
                    date=now(),
                    status="ongoing",
                    snapshot=complete_snapshot.id,
                )
            ]
        )
        assert actual_result == {"snapshot:add": 1}

        by_id = swh_storage.snapshot_get(complete_snapshot.id)
        assert by_id == {**complete_snapshot_dict, "next_branch": None}

        by_ov = swh_storage.snapshot_get_by_origin_visit(origin.url, visit_id)
        assert by_ov == {**complete_snapshot_dict, "next_branch": None}

    def test_snapshot_add_many(self, swh_storage, sample_data):
        snapshot, _, complete_snapshot = sample_data.snapshots[:3]

        actual_result = swh_storage.snapshot_add([snapshot, complete_snapshot])
        assert actual_result == {"snapshot:add": 2}

        assert swh_storage.snapshot_get(complete_snapshot.id) == {
            **complete_snapshot.to_dict(),
            "next_branch": None,
        }

        assert swh_storage.snapshot_get(snapshot.id) == {
            **snapshot.to_dict(),
            "next_branch": None,
        }

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()["snapshot"] == 2

    def test_snapshot_add_many_incremental(self, swh_storage, sample_data):
        snapshot, _, complete_snapshot = sample_data.snapshots[:3]

        actual_result = swh_storage.snapshot_add([complete_snapshot])
        assert actual_result == {"snapshot:add": 1}

        actual_result2 = swh_storage.snapshot_add([snapshot, complete_snapshot])
        assert actual_result2 == {"snapshot:add": 1}

        assert swh_storage.snapshot_get(complete_snapshot.id) == {
            **complete_snapshot.to_dict(),
            "next_branch": None,
        }

        assert swh_storage.snapshot_get(snapshot.id) == {
            **snapshot.to_dict(),
            "next_branch": None,
        }

    def test_snapshot_add_twice(self, swh_storage, sample_data):
        snapshot, empty_snapshot = sample_data.snapshots[:2]

        actual_result = swh_storage.snapshot_add([empty_snapshot])
        assert actual_result == {"snapshot:add": 1}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("snapshot", empty_snapshot)
        ]

        actual_result = swh_storage.snapshot_add([snapshot])
        assert actual_result == {"snapshot:add": 1}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("snapshot", empty_snapshot),
            ("snapshot", snapshot),
        ]

    def test_snapshot_add_count_branches(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]

        actual_result = swh_storage.snapshot_add([complete_snapshot])
        assert actual_result == {"snapshot:add": 1}

        snp_size = swh_storage.snapshot_count_branches(complete_snapshot.id)

        expected_snp_size = {
            "alias": 1,
            "content": 1,
            "directory": 2,
            "release": 1,
            "revision": 1,
            "snapshot": 1,
            None: 1,
        }
        assert snp_size == expected_snp_size

    def test_snapshot_add_get_paginated(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]

        swh_storage.snapshot_add([complete_snapshot])

        snp_id = complete_snapshot.id
        branches = complete_snapshot.to_dict()["branches"]
        branch_names = list(sorted(branches))

        # Test branch_from
        snapshot = swh_storage.snapshot_get_branches(snp_id, branches_from=b"release")

        rel_idx = branch_names.index(b"release")
        expected_snapshot = {
            "id": snp_id,
            "branches": {name: branches[name] for name in branch_names[rel_idx:]},
            "next_branch": None,
        }

        assert snapshot == expected_snapshot

        # Test branches_count
        snapshot = swh_storage.snapshot_get_branches(snp_id, branches_count=1)

        expected_snapshot = {
            "id": snp_id,
            "branches": {branch_names[0]: branches[branch_names[0]],},
            "next_branch": b"content",
        }
        assert snapshot == expected_snapshot

        # test branch_from + branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, branches_from=b"directory", branches_count=3
        )

        dir_idx = branch_names.index(b"directory")
        expected_snapshot = {
            "id": snp_id,
            "branches": {
                name: branches[name] for name in branch_names[dir_idx : dir_idx + 3]
            },
            "next_branch": branch_names[dir_idx + 3],
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get_filtered(self, swh_storage, sample_data):
        origin = sample_data.origin
        complete_snapshot = sample_data.snapshots[2]

        swh_storage.origin_add([origin])
        visit = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type=sample_data.type_visit1,
        )
        origin_visit1 = swh_storage.origin_visit_add([visit])[0]

        swh_storage.snapshot_add([complete_snapshot])
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=origin_visit1.visit,
                    date=now(),
                    status="ongoing",
                    snapshot=complete_snapshot.id,
                )
            ]
        )

        snp_id = complete_snapshot.id
        branches = complete_snapshot.to_dict()["branches"]

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=["release", "revision"]
        )

        expected_snapshot = {
            "id": snp_id,
            "branches": {
                name: tgt
                for name, tgt in branches.items()
                if tgt and tgt["target_type"] in ["release", "revision"]
            },
            "next_branch": None,
        }

        assert snapshot == expected_snapshot

        snapshot = swh_storage.snapshot_get_branches(snp_id, target_types=["alias"])

        expected_snapshot = {
            "id": snp_id,
            "branches": {
                name: tgt
                for name, tgt in branches.items()
                if tgt and tgt["target_type"] == "alias"
            },
            "next_branch": None,
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get_filtered_and_paginated(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]

        swh_storage.snapshot_add([complete_snapshot])

        snp_id = complete_snapshot.id
        branches = complete_snapshot.to_dict()["branches"]
        branch_names = list(sorted(branches))

        # Test branch_from

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=["directory", "release"], branches_from=b"directory2"
        )

        expected_snapshot = {
            "id": snp_id,
            "branches": {name: branches[name] for name in (b"directory2", b"release")},
            "next_branch": None,
        }

        assert snapshot == expected_snapshot

        # Test branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=["directory", "release"], branches_count=1
        )

        expected_snapshot = {
            "id": snp_id,
            "branches": {b"directory": branches[b"directory"]},
            "next_branch": b"directory2",
        }
        assert snapshot == expected_snapshot

        # Test branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=["directory", "release"], branches_count=2
        )

        expected_snapshot = {
            "id": snp_id,
            "branches": {
                name: branches[name] for name in (b"directory", b"directory2")
            },
            "next_branch": b"release",
        }
        assert snapshot == expected_snapshot

        # test branch_from + branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id,
            target_types=["directory", "release"],
            branches_from=b"directory2",
            branches_count=1,
        )

        dir_idx = branch_names.index(b"directory2")
        expected_snapshot = {
            "id": snp_id,
            "branches": {branch_names[dir_idx]: branches[branch_names[dir_idx]],},
            "next_branch": b"release",
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get_branch_by_type(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]
        snapshot = complete_snapshot.to_dict()

        alias1 = b"alias1"
        alias2 = b"alias2"
        target1 = random.choice(list(snapshot["branches"].keys()))
        target2 = random.choice(list(snapshot["branches"].keys()))

        snapshot["branches"][alias2] = {
            "target": target2,
            "target_type": "alias",
        }

        snapshot["branches"][alias1] = {
            "target": target1,
            "target_type": "alias",
        }

        new_snapshot = Snapshot.from_dict(snapshot)
        swh_storage.snapshot_add([new_snapshot])

        branches = swh_storage.snapshot_get_branches(
            new_snapshot.id,
            target_types=["alias"],
            branches_from=alias1,
            branches_count=1,
        )["branches"]

        assert len(branches) == 1
        assert alias1 in branches

    def test_snapshot_add_get(self, swh_storage, sample_data):
        snapshot = sample_data.snapshot
        origin = sample_data.origin

        swh_storage.origin_add([origin])
        visit = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type=sample_data.type_visit1,
        )
        ov1 = swh_storage.origin_visit_add([visit])[0]

        swh_storage.snapshot_add([snapshot])
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=now(),
                    status="ongoing",
                    snapshot=snapshot.id,
                )
            ]
        )

        expected_snapshot = {**snapshot.to_dict(), "next_branch": None}

        by_id = swh_storage.snapshot_get(snapshot.id)
        assert by_id == expected_snapshot

        by_ov = swh_storage.snapshot_get_by_origin_visit(origin.url, ov1.visit)
        assert by_ov == expected_snapshot

        actual_visit = swh_storage.origin_visit_get_by(origin.url, ov1.visit)
        assert actual_visit == ov1

        visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url, ov1.visit, require_snapshot=True
        )
        assert visit_status.snapshot == snapshot.id

    def test_snapshot_add_twice__by_origin_visit(self, swh_storage, sample_data):
        snapshot = sample_data.snapshot
        origin = sample_data.origin

        swh_storage.origin_add([origin])
        ov1 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit1,
                    type=sample_data.type_visit1,
                )
            ]
        )[0]
        swh_storage.snapshot_add([snapshot])
        date_now2 = now()

        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=date_now2,
                    status="ongoing",
                    snapshot=snapshot.id,
                )
            ]
        )

        expected_snapshot = {**snapshot.to_dict(), "next_branch": None}

        by_ov1 = swh_storage.snapshot_get_by_origin_visit(origin.url, ov1.visit)
        assert by_ov1 == expected_snapshot

        ov2 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=sample_data.date_visit2,
                    type=sample_data.type_visit2,
                )
            ]
        )[0]

        date_now4 = now()
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov2.visit,
                    date=date_now4,
                    status="ongoing",
                    snapshot=snapshot.id,
                )
            ]
        )

        by_ov2 = swh_storage.snapshot_get_by_origin_visit(origin.url, ov2.visit)
        assert by_ov2 == expected_snapshot

        ovs1 = OriginVisitStatus.from_dict(
            {
                "origin": origin.url,
                "date": sample_data.date_visit1,
                "visit": ov1.visit,
                "status": "created",
                "metadata": None,
                "snapshot": None,
            }
        )
        ovs2 = OriginVisitStatus.from_dict(
            {
                "origin": origin.url,
                "date": date_now2,
                "visit": ov1.visit,
                "status": "ongoing",
                "metadata": None,
                "snapshot": snapshot.id,
            }
        )
        ovs3 = OriginVisitStatus.from_dict(
            {
                "origin": origin.url,
                "date": sample_data.date_visit2,
                "visit": ov2.visit,
                "status": "created",
                "metadata": None,
                "snapshot": None,
            }
        )
        ovs4 = OriginVisitStatus.from_dict(
            {
                "origin": origin.url,
                "date": date_now4,
                "visit": ov2.visit,
                "status": "ongoing",
                "metadata": None,
                "snapshot": snapshot.id,
            }
        )
        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = [
            ("origin", origin),
            ("origin_visit", ov1),
            ("origin_visit_status", ovs1),
            ("snapshot", snapshot),
            ("origin_visit_status", ovs2),
            ("origin_visit", ov2),
            ("origin_visit_status", ovs3),
            ("origin_visit_status", ovs4),
        ]

        for obj in expected_objects:
            assert obj in actual_objects

    def test_snapshot_get_random(self, swh_storage, sample_data):
        snapshot, empty_snapshot, complete_snapshot = sample_data.snapshots[:3]
        swh_storage.snapshot_add([snapshot, empty_snapshot, complete_snapshot])

        assert swh_storage.snapshot_get_random() in {
            snapshot.id,
            empty_snapshot.id,
            complete_snapshot.id,
        }

    def test_snapshot_missing(self, swh_storage, sample_data):
        snapshot, missing_snapshot = sample_data.snapshots[:2]
        snapshots = [snapshot.id, missing_snapshot.id]
        swh_storage.snapshot_add([snapshot])

        missing_snapshots = swh_storage.snapshot_missing(snapshots)

        assert list(missing_snapshots) == [missing_snapshot.id]

    def test_stat_counters(self, swh_storage, sample_data):
        origin = sample_data.origin
        snapshot = sample_data.snapshot
        revision = sample_data.revision
        release = sample_data.release
        directory = sample_data.directory
        content = sample_data.content

        expected_keys = ["content", "directory", "origin", "revision"]

        # Initially, all counters are 0

        swh_storage.refresh_stat_counters()
        counters = swh_storage.stat_counters()
        assert set(expected_keys) <= set(counters)
        for key in expected_keys:
            assert counters[key] == 0

        # Add a content. Only the content counter should increase.

        swh_storage.content_add([content])

        swh_storage.refresh_stat_counters()
        counters = swh_storage.stat_counters()

        assert set(expected_keys) <= set(counters)
        for key in expected_keys:
            if key != "content":
                assert counters[key] == 0
        assert counters["content"] == 1

        # Add other objects. Check their counter increased as well.

        swh_storage.origin_add([origin])
        visit = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit2,
            type=sample_data.type_visit2,
        )
        origin_visit1 = swh_storage.origin_visit_add([visit])[0]

        swh_storage.snapshot_add([snapshot])
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=origin_visit1.visit,
                    date=now(),
                    status="ongoing",
                    snapshot=snapshot.id,
                )
            ]
        )
        swh_storage.directory_add([directory])
        swh_storage.revision_add([revision])
        swh_storage.release_add([release])

        swh_storage.refresh_stat_counters()
        counters = swh_storage.stat_counters()
        assert counters["content"] == 1
        assert counters["directory"] == 1
        assert counters["snapshot"] == 1
        assert counters["origin"] == 1
        assert counters["origin_visit"] == 1
        assert counters["revision"] == 1
        assert counters["release"] == 1
        assert counters["snapshot"] == 1
        if "person" in counters:
            assert counters["person"] == 3

    def test_content_find_ctime(self, swh_storage, sample_data):
        origin_content = sample_data.content
        ctime = round_to_milliseconds(now())
        content = attr.evolve(origin_content, data=None, ctime=ctime)
        swh_storage.content_add_metadata([content])

        actually_present = swh_storage.content_find({"sha1": content.sha1})
        assert actually_present[0] == content

    def test_content_find_with_present_content(self, swh_storage, sample_data):
        content = sample_data.content
        expected_content = attr.evolve(content, data=None)

        # 1. with something to find
        swh_storage.content_add([content])

        actually_present = swh_storage.content_find({"sha1": content.sha1})
        assert 1 == len(actually_present)
        assert actually_present[0] == expected_content

        # 2. with something to find
        actually_present = swh_storage.content_find({"sha1_git": content.sha1_git})
        assert 1 == len(actually_present)
        assert actually_present[0] == expected_content

        # 3. with something to find
        actually_present = swh_storage.content_find({"sha256": content.sha256})
        assert 1 == len(actually_present)
        assert actually_present[0] == expected_content

        # 4. with something to find
        actually_present = swh_storage.content_find(content.hashes())
        assert 1 == len(actually_present)
        assert actually_present[0] == expected_content

    def test_content_find_with_non_present_content(self, swh_storage, sample_data):
        missing_content = sample_data.skipped_content
        # 1. with something that does not exist
        actually_present = swh_storage.content_find({"sha1": missing_content.sha1})
        assert actually_present == []

        # 2. with something that does not exist
        actually_present = swh_storage.content_find(
            {"sha1_git": missing_content.sha1_git}
        )
        assert actually_present == []

        # 3. with something that does not exist
        actually_present = swh_storage.content_find({"sha256": missing_content.sha256})
        assert actually_present == []

    def test_content_find_with_duplicate_input(self, swh_storage, sample_data):
        content = sample_data.content

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(content.sha1)
        sha1_array[0] += 1
        sha1git_array = bytearray(content.sha1_git)
        sha1git_array[0] += 1
        duplicated_content = attr.evolve(
            content, sha1=bytes(sha1_array), sha1_git=bytes(sha1git_array)
        )

        # Inject the data
        swh_storage.content_add([content, duplicated_content])

        actual_result = swh_storage.content_find(
            {
                "blake2s256": duplicated_content.blake2s256,
                "sha256": duplicated_content.sha256,
            }
        )

        expected_content = attr.evolve(content, data=None)
        expected_duplicated_content = attr.evolve(duplicated_content, data=None)

        for result in actual_result:
            assert result in [expected_content, expected_duplicated_content]

    def test_content_find_with_duplicate_sha256(self, swh_storage, sample_data):
        content = sample_data.content

        hashes = {}
        # Create fake data with colliding sha256
        for hashalgo in ("sha1", "sha1_git", "blake2s256"):
            value = bytearray(getattr(content, hashalgo))
            value[0] += 1
            hashes[hashalgo] = bytes(value)

        duplicated_content = attr.evolve(
            content,
            sha1=hashes["sha1"],
            sha1_git=hashes["sha1_git"],
            blake2s256=hashes["blake2s256"],
        )
        swh_storage.content_add([content, duplicated_content])

        actual_result = swh_storage.content_find({"sha256": duplicated_content.sha256})
        assert len(actual_result) == 2

        expected_content = attr.evolve(content, data=None)
        expected_duplicated_content = attr.evolve(duplicated_content, data=None)

        for result in actual_result:
            assert result in [expected_content, expected_duplicated_content]

        # Find with both sha256 and blake2s256
        actual_result = swh_storage.content_find(
            {
                "sha256": duplicated_content.sha256,
                "blake2s256": duplicated_content.blake2s256,
            }
        )

        assert len(actual_result) == 1
        assert actual_result == [expected_duplicated_content]

    def test_content_find_with_duplicate_blake2s256(self, swh_storage, sample_data):
        content = sample_data.content

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(content.sha1)
        sha1_array[0] += 1
        sha1git_array = bytearray(content.sha1_git)
        sha1git_array[0] += 1
        sha256_array = bytearray(content.sha256)
        sha256_array[0] += 1

        duplicated_content = attr.evolve(
            content,
            sha1=bytes(sha1_array),
            sha1_git=bytes(sha1git_array),
            sha256=bytes(sha256_array),
        )

        swh_storage.content_add([content, duplicated_content])

        actual_result = swh_storage.content_find(
            {"blake2s256": duplicated_content.blake2s256}
        )

        expected_content = attr.evolve(content, data=None)
        expected_duplicated_content = attr.evolve(duplicated_content, data=None)

        for result in actual_result:
            assert result in [expected_content, expected_duplicated_content]

        # Find with both sha256 and blake2s256
        actual_result = swh_storage.content_find(
            {
                "sha256": duplicated_content.sha256,
                "blake2s256": duplicated_content.blake2s256,
            }
        )

        assert actual_result == [expected_duplicated_content]

    def test_content_find_bad_input(self, swh_storage):
        # 1. with no hash to lookup
        with pytest.raises(StorageArgumentException):
            swh_storage.content_find({})  # need at least one hash

        # 2. with bad hash
        with pytest.raises(StorageArgumentException):
            swh_storage.content_find({"unknown-sha1": "something"})  # not the right key

    def test_object_find_by_sha1_git(self, swh_storage, sample_data):
        content = sample_data.content
        directory = sample_data.directory
        revision = sample_data.revision
        release = sample_data.release

        sha1_gits = [b"00000000000000000000"]
        expected = {
            b"00000000000000000000": [],
        }

        swh_storage.content_add([content])
        sha1_gits.append(content.sha1_git)

        expected[content.sha1_git] = [
            {"sha1_git": content.sha1_git, "type": "content",}
        ]

        swh_storage.directory_add([directory])
        sha1_gits.append(directory.id)
        expected[directory.id] = [{"sha1_git": directory.id, "type": "directory",}]

        swh_storage.revision_add([revision])
        sha1_gits.append(revision.id)
        expected[revision.id] = [{"sha1_git": revision.id, "type": "revision",}]

        swh_storage.release_add([release])
        sha1_gits.append(release.id)
        expected[release.id] = [{"sha1_git": release.id, "type": "release",}]

        ret = swh_storage.object_find_by_sha1_git(sha1_gits)

        assert expected == ret

    def test_metadata_fetcher_add_get(self, swh_storage, sample_data):
        fetcher = sample_data.metadata_fetcher
        actual_fetcher = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
        assert actual_fetcher is None  # does not exist

        swh_storage.metadata_fetcher_add([fetcher])

        res = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
        assert res == fetcher

        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = [
            ("metadata_fetcher", fetcher),
        ]

        for obj in expected_objects:
            assert obj in actual_objects

    def test_metadata_fetcher_add_zero(self, swh_storage, sample_data):
        fetcher = sample_data.metadata_fetcher
        actual_fetcher = swh_storage.metadata_fetcher_get(fetcher.name, fetcher.version)
        assert actual_fetcher is None  # does not exist

        swh_storage.metadata_fetcher_add([])

    def test_metadata_authority_add_get(self, swh_storage, sample_data):
        authority = sample_data.metadata_authority

        actual_authority = swh_storage.metadata_authority_get(
            authority.type, authority.url
        )
        assert actual_authority is None  # does not exist

        swh_storage.metadata_authority_add([authority])

        res = swh_storage.metadata_authority_get(authority.type, authority.url)
        assert res == authority

        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = [
            ("metadata_authority", authority),
        ]

        for obj in expected_objects:
            assert obj in actual_objects

    def test_metadata_authority_add_zero(self, swh_storage, sample_data):
        authority = sample_data.metadata_authority

        actual_authority = swh_storage.metadata_authority_get(
            authority.type, authority.url
        )
        assert actual_authority is None  # does not exist

        swh_storage.metadata_authority_add([])

    def test_content_metadata_add(self, swh_storage, sample_data):
        content = sample_data.content
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata = sample_data.content_metadata[:2]

        content_swhid = SWHID(
            object_type="content", object_id=hash_to_bytes(content.sha1_git)
        )

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add(content_metadata)

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content_swhid, authority
        )
        assert result.next_page_token is None
        assert list(sorted(result.results, key=lambda x: x.discovery_date,)) == list(
            content_metadata
        )

        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = [
            ("metadata_authority", authority),
            ("metadata_fetcher", fetcher),
        ] + [("raw_extrinsic_metadata", item) for item in content_metadata]

        for obj in expected_objects:
            assert obj in actual_objects

    def test_content_metadata_add_duplicate(self, swh_storage, sample_data):
        """Duplicates should be silently updated."""
        content = sample_data.content
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]
        content_swhid = SWHID(
            object_type="content", object_id=hash_to_bytes(content.sha1_git)
        )

        new_content_metadata2 = attr.evolve(
            content_metadata2, format="new-format", metadata=b"new-metadata",
        )

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])
        swh_storage.raw_extrinsic_metadata_add([new_content_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content_swhid, authority
        )
        assert result.next_page_token is None

        expected_results1 = (content_metadata, new_content_metadata2)
        expected_results2 = (content_metadata, content_metadata2)

        assert tuple(sorted(result.results, key=lambda x: x.discovery_date,)) in (
            expected_results1,  # cassandra
            expected_results2,  # postgresql
        )

    def test_content_metadata_get(self, swh_storage, sample_data):
        content, content2 = sample_data.contents[:2]
        fetcher, fetcher2 = sample_data.fetchers[:2]
        authority, authority2 = sample_data.authorities[:2]
        (
            content1_metadata1,
            content1_metadata2,
            content1_metadata3,
        ) = sample_data.content_metadata[:3]

        content1_swhid = SWHID(object_type="content", object_id=content.sha1_git)
        content2_swhid = SWHID(object_type="content", object_id=content2.sha1_git)
        content2_metadata = attr.evolve(content1_metadata2, id=content2_swhid)

        swh_storage.metadata_authority_add([authority, authority2])
        swh_storage.metadata_fetcher_add([fetcher, fetcher2])

        swh_storage.raw_extrinsic_metadata_add(
            [
                content1_metadata1,
                content1_metadata2,
                content1_metadata3,
                content2_metadata,
            ]
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content1_swhid, authority
        )
        assert result.next_page_token is None
        assert [content1_metadata1, content1_metadata2] == list(
            sorted(result.results, key=lambda x: x.discovery_date,)
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content1_swhid, authority2
        )
        assert result.next_page_token is None
        assert [content1_metadata3] == list(
            sorted(result.results, key=lambda x: x.discovery_date,)
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content2_swhid, authority
        )
        assert result.next_page_token is None
        assert [content2_metadata] == list(result.results,)

    def test_content_metadata_get_after(self, swh_storage, sample_data):
        content = sample_data.content
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]

        content_swhid = SWHID(object_type="content", object_id=content.sha1_git)

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT,
            content_swhid,
            authority,
            after=content_metadata.discovery_date - timedelta(seconds=1),
        )
        assert result.next_page_token is None
        assert [content_metadata, content_metadata2] == list(
            sorted(result.results, key=lambda x: x.discovery_date,)
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT,
            content_swhid,
            authority,
            after=content_metadata.discovery_date,
        )
        assert result.next_page_token is None
        assert result.results == [content_metadata2]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT,
            content_swhid,
            authority,
            after=content_metadata2.discovery_date,
        )
        assert result.next_page_token is None
        assert result.results == []

    def test_content_metadata_get_paginate(self, swh_storage, sample_data):
        content = sample_data.content
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]

        content_swhid = SWHID(object_type="content", object_id=content.sha1_git)

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])
        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])
        swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content_swhid, authority
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content_swhid, authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [content_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT,
            content_swhid,
            authority,
            limit=1,
            page_token=result.next_page_token,
        )
        assert result.next_page_token is None
        assert result.results == [content_metadata2]

    def test_content_metadata_get_paginate_same_date(self, swh_storage, sample_data):
        content = sample_data.content
        fetcher1, fetcher2 = sample_data.fetchers[:2]
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]

        content_swhid = SWHID(object_type="content", object_id=content.sha1_git)

        swh_storage.metadata_fetcher_add([fetcher1, fetcher2])
        swh_storage.metadata_authority_add([authority])

        new_content_metadata2 = attr.evolve(
            content_metadata2,
            discovery_date=content_metadata2.discovery_date,
            fetcher=attr.evolve(fetcher2, metadata=None),
        )

        swh_storage.raw_extrinsic_metadata_add(
            [content_metadata, new_content_metadata2]
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT, content_swhid, authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [content_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.CONTENT,
            content_swhid,
            authority,
            limit=1,
            page_token=result.next_page_token,
        )
        assert result.next_page_token is None
        assert result.results == [new_content_metadata2]

    def test_content_metadata_get__invalid_id(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])
        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])

        with pytest.raises(StorageArgumentException, match="SWHID"):
            swh_storage.raw_extrinsic_metadata_get(
                MetadataTargetType.CONTENT, origin.url, authority
            )

    def test_origin_metadata_add(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]

        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority
        )
        assert result.next_page_token is None
        assert list(sorted(result.results, key=lambda x: x.discovery_date)) == [
            origin_metadata,
            origin_metadata2,
        ]

        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = [
            ("metadata_authority", authority),
            ("metadata_fetcher", fetcher),
            ("raw_extrinsic_metadata", origin_metadata),
            ("raw_extrinsic_metadata", origin_metadata2),
        ]

        for obj in expected_objects:
            assert obj in actual_objects

    def test_origin_metadata_add_duplicate(self, swh_storage, sample_data):
        """Duplicates should be silently updated."""
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        new_origin_metadata2 = attr.evolve(
            origin_metadata2, format="new-format", metadata=b"new-metadata",
        )

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])
        swh_storage.raw_extrinsic_metadata_add([new_origin_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority
        )
        assert result.next_page_token is None

        # which of the two behavior happens is backend-specific.
        expected_results1 = (origin_metadata, new_origin_metadata2)
        expected_results2 = (origin_metadata, origin_metadata2)

        assert tuple(sorted(result.results, key=lambda x: x.discovery_date,)) in (
            expected_results1,  # cassandra
            expected_results2,  # postgresql
        )

    def test_origin_metadata_get(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]
        fetcher, fetcher2 = sample_data.fetchers[:2]
        authority, authority2 = sample_data.authorities[:2]
        (
            origin1_metadata1,
            origin1_metadata2,
            origin1_metadata3,
        ) = sample_data.origin_metadata[:3]

        assert swh_storage.origin_add([origin, origin2]) == {"origin:add": 2}

        origin2_metadata = attr.evolve(origin1_metadata2, id=origin2.url)

        swh_storage.metadata_authority_add([authority, authority2])
        swh_storage.metadata_fetcher_add([fetcher, fetcher2])

        swh_storage.raw_extrinsic_metadata_add(
            [origin1_metadata1, origin1_metadata2, origin1_metadata3, origin2_metadata]
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority
        )
        assert result.next_page_token is None
        assert [origin1_metadata1, origin1_metadata2] == list(
            sorted(result.results, key=lambda x: x.discovery_date,)
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority2
        )
        assert result.next_page_token is None
        assert [origin1_metadata3] == list(
            sorted(result.results, key=lambda x: x.discovery_date,)
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin2.url, authority
        )
        assert result.next_page_token is None
        assert [origin2_metadata] == list(result.results,)

    def test_origin_metadata_get_after(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]

        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])
        swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN,
            origin.url,
            authority,
            after=origin_metadata.discovery_date - timedelta(seconds=1),
        )
        assert result.next_page_token is None
        assert list(sorted(result.results, key=lambda x: x.discovery_date,)) == [
            origin_metadata,
            origin_metadata2,
        ]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN,
            origin.url,
            authority,
            after=origin_metadata.discovery_date,
        )
        assert result.next_page_token is None
        assert result.results == [origin_metadata2]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN,
            origin.url,
            authority,
            after=origin_metadata2.discovery_date,
        )
        assert result.next_page_token is None
        assert result.results == []

    def test_origin_metadata_get_paginate(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

        swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [origin_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN,
            origin.url,
            authority,
            limit=1,
            page_token=result.next_page_token,
        )
        assert result.next_page_token is None
        assert result.results == [origin_metadata2]

    def test_origin_metadata_get_paginate_same_date(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher1, fetcher2 = sample_data.fetchers[:2]
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher1, fetcher2])
        swh_storage.metadata_authority_add([authority])

        new_origin_metadata2 = attr.evolve(
            origin_metadata2,
            discovery_date=origin_metadata2.discovery_date,
            fetcher=attr.evolve(fetcher2, metadata=None),
        )

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, new_origin_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN, origin.url, authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [origin_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            MetadataTargetType.ORIGIN,
            origin.url,
            authority,
            limit=1,
            page_token=result.next_page_token,
        )
        assert result.next_page_token is None
        assert result.results == [new_origin_metadata2]

    def test_origin_metadata_add_missing_authority(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher])

        with pytest.raises(StorageArgumentException, match="authority"):
            swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

    def test_origin_metadata_add_missing_fetcher(self, swh_storage, sample_data):
        origin = sample_data.origin
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_authority_add([authority])

        with pytest.raises(StorageArgumentException, match="fetcher"):
            swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

    def test_origin_metadata_get__invalid_id_type(self, swh_storage, sample_data):
        origin = sample_data.origin
        authority = sample_data.metadata_authority
        fetcher = sample_data.metadata_fetcher
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        content_metadata = sample_data.content_metadata[0]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

        with pytest.raises(StorageArgumentException, match="SWHID"):
            swh_storage.raw_extrinsic_metadata_get(
                MetadataTargetType.ORIGIN, content_metadata.id, authority,
            )


class TestStorageGeneratedData:
    def test_generate_content_get(self, swh_storage, swh_contents):
        contents_with_data = [c.to_dict() for c in swh_contents if c.status != "absent"]
        # input the list of sha1s we want from storage
        get_sha1s = [c["sha1"] for c in contents_with_data]

        # retrieve contents
        actual_contents = list(swh_storage.content_get(get_sha1s))
        assert None not in actual_contents
        assert_contents_ok(contents_with_data, actual_contents)

    def test_generate_content_get_metadata(self, swh_storage, swh_contents):
        # input the list of sha1s we want from storage
        expected_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]
        get_sha1s = [c["sha1"] for c in expected_contents]

        # retrieve contents
        meta_contents = swh_storage.content_get_metadata(get_sha1s)

        assert len(list(meta_contents)) == len(get_sha1s)

        actual_contents = []
        for contents in meta_contents.values():
            actual_contents.extend(contents)

        keys_to_check = {"length", "status", "sha1", "sha1_git", "sha256", "blake2s256"}

        assert_contents_ok(
            expected_contents, actual_contents, keys_to_check=keys_to_check
        )

    def test_generate_content_get_range(self, swh_storage, swh_contents):
        """content_get_range returns complete range"""
        present_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]

        get_sha1s = sorted([c.sha1 for c in swh_contents if c.status != "absent"])
        start = get_sha1s[2]
        end = get_sha1s[-2]
        actual_result = swh_storage.content_get_range(start, end)

        assert actual_result["next"] is None

        actual_contents = actual_result["contents"]
        expected_contents = [c for c in present_contents if start <= c["sha1"] <= end]
        if expected_contents:
            assert_contents_ok(expected_contents, actual_contents, ["sha1"])
        else:
            assert actual_contents == []

    def test_generate_content_get_range_full(self, swh_storage, swh_contents):
        """content_get_range for a full range returns all available contents"""
        present_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]

        start = b"0" * 40
        end = b"f" * 40
        actual_result = swh_storage.content_get_range(start, end)
        assert actual_result["next"] is None

        actual_contents = actual_result["contents"]
        expected_contents = [c for c in present_contents if start <= c["sha1"] <= end]
        if expected_contents:
            assert_contents_ok(expected_contents, actual_contents, ["sha1"])
        else:
            assert actual_contents == []

    def test_generate_content_get_range_empty(self, swh_storage, swh_contents):
        """content_get_range for an empty range returns nothing"""
        start = b"0" * 40
        end = b"f" * 40
        actual_result = swh_storage.content_get_range(end, start)
        assert actual_result["next"] is None
        assert len(actual_result["contents"]) == 0

    def test_generate_content_get_range_limit_none(self, swh_storage):
        """content_get_range call with wrong limit input should fail"""
        with pytest.raises(StorageArgumentException) as e:
            swh_storage.content_get_range(start=None, end=None, limit=None)

        assert e.value.args == ("limit should not be None",)

    def test_generate_content_get_range_no_limit(self, swh_storage, swh_contents):
        """content_get_range returns contents within range provided"""
        # input the list of sha1s we want from storage
        get_sha1s = sorted([c.sha1 for c in swh_contents if c.status != "absent"])
        start = get_sha1s[0]
        end = get_sha1s[-1]

        # retrieve contents
        actual_result = swh_storage.content_get_range(start, end)

        actual_contents = actual_result["contents"]
        assert actual_result["next"] is None
        assert len(actual_contents) == len(get_sha1s)

        expected_contents = [c.to_dict() for c in swh_contents if c.status != "absent"]
        assert_contents_ok(expected_contents, actual_contents, ["sha1"])

    def test_generate_content_get_range_limit(self, swh_storage, swh_contents):
        """content_get_range paginates results if limit exceeded"""
        contents_map = {c.sha1: c.to_dict() for c in swh_contents}

        # input the list of sha1s we want from storage
        get_sha1s = sorted([c.sha1 for c in swh_contents if c.status != "absent"])
        start = get_sha1s[0]
        end = get_sha1s[-1]

        # retrieve contents limited to n-1 results
        limited_results = len(get_sha1s) - 1
        actual_result = swh_storage.content_get_range(start, end, limit=limited_results)

        actual_contents = actual_result["contents"]
        assert actual_result["next"] == get_sha1s[-1]
        assert len(actual_contents) == limited_results

        expected_contents = [contents_map[sha1] for sha1 in get_sha1s[:-1]]
        assert_contents_ok(expected_contents, actual_contents, ["sha1"])

        # retrieve next part
        actual_results2 = swh_storage.content_get_range(start=end, end=end)
        assert actual_results2["next"] is None
        actual_contents2 = actual_results2["contents"]
        assert len(actual_contents2) == 1

        assert_contents_ok([contents_map[get_sha1s[-1]]], actual_contents2, ["sha1"])

    @pytest.mark.parametrize("limit", [1, 7, 10, 100, 1000])
    def test_origin_list(self, swh_storage, swh_origins, limit):
        returned_origins = []

        page_token = None
        i = 0
        while True:
            actual_page = swh_storage.origin_list(page_token=page_token, limit=limit)
            assert len(actual_page.results) <= limit

            returned_origins.extend(actual_page.results)

            i += 1
            page_token = actual_page.next_page_token

            if page_token is None:
                assert i * limit >= len(swh_origins)
                break
            else:
                assert len(actual_page.results) == limit

        assert sorted(returned_origins) == sorted(swh_origins)

    def test_origin_count(self, swh_storage, sample_data):
        swh_storage.origin_add(sample_data.origins)

        assert swh_storage.origin_count("github") == 3
        assert swh_storage.origin_count("gitlab") == 2
        assert swh_storage.origin_count(".*user.*", regexp=True) == 5
        assert swh_storage.origin_count(".*user.*", regexp=False) == 0
        assert swh_storage.origin_count(".*user1.*", regexp=True) == 2
        assert swh_storage.origin_count(".*user1.*", regexp=False) == 0

    def test_origin_count_with_visit_no_visits(self, swh_storage, sample_data):
        swh_storage.origin_add(sample_data.origins)

        # none of them have visits, so with_visit=True => 0
        assert swh_storage.origin_count("github", with_visit=True) == 0
        assert swh_storage.origin_count("gitlab", with_visit=True) == 0
        assert swh_storage.origin_count(".*user.*", regexp=True, with_visit=True) == 0
        assert swh_storage.origin_count(".*user.*", regexp=False, with_visit=True) == 0
        assert swh_storage.origin_count(".*user1.*", regexp=True, with_visit=True) == 0
        assert swh_storage.origin_count(".*user1.*", regexp=False, with_visit=True) == 0

    def test_origin_count_with_visit_with_visits_no_snapshot(
        self, swh_storage, sample_data
    ):
        swh_storage.origin_add(sample_data.origins)

        origin_url = "https://github.com/user1/repo1"
        visit = OriginVisit(origin=origin_url, date=now(), type="git",)
        swh_storage.origin_visit_add([visit])

        assert swh_storage.origin_count("github", with_visit=False) == 3
        # it has a visit, but no snapshot, so with_visit=True => 0
        assert swh_storage.origin_count("github", with_visit=True) == 0

        assert swh_storage.origin_count("gitlab", with_visit=False) == 2
        # these gitlab origins have no visit
        assert swh_storage.origin_count("gitlab", with_visit=True) == 0

        assert (
            swh_storage.origin_count("github.*user1", regexp=True, with_visit=False)
            == 1
        )
        assert (
            swh_storage.origin_count("github.*user1", regexp=True, with_visit=True) == 0
        )
        assert swh_storage.origin_count("github", regexp=True, with_visit=True) == 0

    def test_origin_count_with_visit_with_visits_and_snapshot(
        self, swh_storage, sample_data
    ):
        snapshot = sample_data.snapshot
        swh_storage.origin_add(sample_data.origins)

        swh_storage.snapshot_add([snapshot])
        origin_url = "https://github.com/user1/repo1"
        visit = OriginVisit(origin=origin_url, date=now(), type="git",)
        visit = swh_storage.origin_visit_add([visit])[0]
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin_url,
                    visit=visit.visit,
                    date=now(),
                    status="ongoing",
                    snapshot=snapshot.id,
                )
            ]
        )

        assert swh_storage.origin_count("github", with_visit=False) == 3
        # github/user1 has a visit and a snapshot, so with_visit=True => 1
        assert swh_storage.origin_count("github", with_visit=True) == 1

        assert (
            swh_storage.origin_count("github.*user1", regexp=True, with_visit=False)
            == 1
        )
        assert (
            swh_storage.origin_count("github.*user1", regexp=True, with_visit=True) == 1
        )
        assert swh_storage.origin_count("github", regexp=True, with_visit=True) == 1

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(strategies.lists(objects(split_content=True), max_size=2))
    def test_add_arbitrary(self, swh_storage, objects):
        for (obj_type, obj) in objects:
            if obj.object_type == "origin_visit":
                swh_storage.origin_add([Origin(url=obj.origin)])
                visit = OriginVisit(origin=obj.origin, date=obj.date, type=obj.type,)
                swh_storage.origin_visit_add([visit])
            else:
                method = getattr(swh_storage, obj_type + "_add")
                try:
                    method([obj])
                except HashCollision:
                    pass


@pytest.mark.db
class TestLocalStorage:
    """Test the local storage"""

    # This test is only relevant on the local storage, with an actual
    # objstorage raising an exception
    def test_content_add_objstorage_exception(self, swh_storage, sample_data):
        content = sample_data.content

        swh_storage.objstorage.content_add = Mock(
            side_effect=Exception("mocked broken objstorage")
        )

        with pytest.raises(Exception, match="mocked broken"):
            swh_storage.content_add([content])

        missing = list(swh_storage.content_missing([content.hashes()]))
        assert missing == [content.sha1]


@pytest.mark.db
class TestStorageRaceConditions:
    @pytest.mark.xfail
    def test_content_add_race(self, swh_storage, sample_data):
        content = sample_data.content

        results = queue.Queue()

        def thread():
            try:
                with db_transaction(swh_storage) as (db, cur):
                    ret = swh_storage.content_add([content], db=db, cur=cur)
                results.put((threading.get_ident(), "data", ret))
            except Exception as e:
                results.put((threading.get_ident(), "exc", e))

        t1 = threading.Thread(target=thread)
        t2 = threading.Thread(target=thread)
        t1.start()
        # this avoids the race condition
        # import time
        # time.sleep(1)
        t2.start()
        t1.join()
        t2.join()

        r1 = results.get(block=False)
        r2 = results.get(block=False)

        with pytest.raises(queue.Empty):
            results.get(block=False)
        assert r1[0] != r2[0]
        assert r1[1] == "data", "Got exception %r in Thread%s" % (r1[2], r1[0])
        assert r2[1] == "data", "Got exception %r in Thread%s" % (r2[2], r2[0])


@pytest.mark.db
class TestPgStorage:
    """This class is dedicated for the rare case where the schema needs to
       be altered dynamically.

       Otherwise, the tests could be blocking when ran altogether.

    """

    def test_content_update_with_new_cols(self, swh_storage, sample_data):
        content, content2 = sample_data.contents[:2]

        swh_storage.journal_writer.journal = None  # TODO, not supported

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                """alter table content
                           add column test text default null,
                           add column test2 text default null"""
            )

        swh_storage.content_add([content])

        cont = content.to_dict()
        cont["test"] = "value-1"
        cont["test2"] = "value-2"

        swh_storage.content_update([cont], keys=["test", "test2"])
        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                """SELECT sha1, sha1_git, sha256, length, status,
                   test, test2
                   FROM content WHERE sha1 = %s""",
                (cont["sha1"],),
            )

            datum = cur.fetchone()

        assert datum == (
            cont["sha1"],
            cont["sha1_git"],
            cont["sha256"],
            cont["length"],
            "visible",
            cont["test"],
            cont["test2"],
        )

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                """alter table content drop column test,
                                               drop column test2"""
            )

    def test_content_add_db(self, swh_storage, sample_data):
        content = sample_data.content

        actual_result = swh_storage.content_add([content])

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": content.length,
        }

        if hasattr(swh_storage, "objstorage"):
            assert content.sha1 in swh_storage.objstorage.objstorage

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "SELECT sha1, sha1_git, sha256, length, status"
                " FROM content WHERE sha1 = %s",
                (content.sha1,),
            )
            datum = cur.fetchone()

        assert datum == (
            content.sha1,
            content.sha1_git,
            content.sha256,
            content.length,
            "visible",
        )

        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        assert contents[0] == attr.evolve(content, data=None)

    def test_content_add_metadata_db(self, swh_storage, sample_data):
        content = attr.evolve(sample_data.content, data=None, ctime=now())

        actual_result = swh_storage.content_add_metadata([content])

        assert actual_result == {
            "content:add": 1,
        }

        if hasattr(swh_storage, "objstorage"):
            assert content.sha1 not in swh_storage.objstorage.objstorage
        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "SELECT sha1, sha1_git, sha256, length, status"
                " FROM content WHERE sha1 = %s",
                (content.sha1,),
            )
            datum = cur.fetchone()
        assert datum == (
            content.sha1,
            content.sha1_git,
            content.sha256,
            content.length,
            "visible",
        )

        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        assert contents[0] == content

    def test_skipped_content_add_db(self, swh_storage, sample_data):
        content, cont2 = sample_data.skipped_contents[:2]
        content2 = attr.evolve(cont2, blake2s256=None)

        actual_result = swh_storage.skipped_content_add([content, content, content2])

        assert 2 <= actual_result.pop("skipped_content:add") <= 3
        assert actual_result == {}

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "SELECT sha1, sha1_git, sha256, blake2s256, "
                "length, status, reason "
                "FROM skipped_content ORDER BY sha1_git"
            )

            dbdata = cur.fetchall()

        assert len(dbdata) == 2
        assert dbdata[0] == (
            content.sha1,
            content.sha1_git,
            content.sha256,
            content.blake2s256,
            content.length,
            "absent",
            "Content too long",
        )

        assert dbdata[1] == (
            content2.sha1,
            content2.sha1_git,
            content2.sha256,
            content2.blake2s256,
            content2.length,
            "absent",
            "Content too long",
        )

    def test_clear_buffers(self, swh_storage):
        """Calling clear buffers on real storage does nothing

        """
        assert swh_storage.clear_buffers() is None

    def test_flush(self, swh_storage):
        """Calling clear buffers on real storage does nothing

        """
        assert swh_storage.flush() == {}
