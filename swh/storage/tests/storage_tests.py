# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import datetime
from datetime import timedelta
import inspect
import itertools
import math
import random
from typing import Any, ClassVar, Dict, Iterator, Optional
from unittest.mock import MagicMock

import attr
import cassandra
from hypothesis import HealthCheck, given, settings, strategies
import psycopg2.errors
import pytest

from swh.core.api import RemoteException
from swh.core.api.classes import stream_results
from swh.model import from_disk, hypothesis_strategies
from swh.model.hashutil import DEFAULT_ALGORITHMS, MultiHash, hash_to_bytes
from swh.model.model import (
    Directory,
    DirectoryEntry,
    ExtID,
    ModelObjectType,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Person,
    RawExtrinsicMetadata,
    Release,
    ReleaseTargetType,
    Revision,
    RevisionType,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID, ExtendedSWHID, ObjectType
from swh.storage.cassandra.storage import CassandraStorage
from swh.storage.common import origin_url_to_sha1 as sha1
from swh.storage.exc import (
    HashCollision,
    QueryTimeout,
    StorageArgumentException,
    UnknownMetadataAuthority,
    UnknownMetadataFetcher,
)
from swh.storage.in_memory import InMemoryStorage
from swh.storage.interface import (
    ListOrder,
    ObjectReference,
    OriginVisitWithStatuses,
    PagedResult,
    SnapshotBranchByNameResponse,
    StorageInterface,
)
from swh.storage.postgresql.storage import Storage as PostgreSQLStorage
from swh.storage.utils import (
    content_hex_hashes,
    now,
    remove_keys,
    round_to_milliseconds,
)

# list of hypothesis disabled health checks in some of the tests in TestStorage
disabled_health_checks = []
# we use getattr here to keep mypy happy regardless hypothesis version
if hasattr(HealthCheck, "function_scoped_fixture"):
    disabled_health_checks.append(HealthCheck.function_scoped_fixture)
if hasattr(HealthCheck, "differing_executors"):
    disabled_health_checks.append(HealthCheck.differing_executors)
# TODO: would probably require better fixes than just disabling this later health
#       check...


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
                ent_dict.pop(key, None)
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
    """Assert that a given list of contents matches on a given set of keys."""
    for k in keys_to_check:
        expected_list = set([c.get(k) for c in expected_contents])
        actual_list = set([c.get(k) for c in actual_contents])
        assert actual_list == expected_list, k


def filter_dict(d):
    "Filter None value from a dict"
    return {k: v for k, v in d.items() if v is not None}


class TestStorage:
    """Main class for Storage testing.

    This class is used as-is to test local storage (see TestLocalStorage
    below) and remote storage (see TestRemoteStorage in
    test_remote_storage.py.

    We need to have the two classes inherit from this base class
    separately to avoid nosetests running the tests from the base
    class twice.
    """

    maxDiff: ClassVar[Optional[int]] = None

    def test_types(self, swh_storage):
        """Checks all methods of StorageInterface are implemented by this
        backend, and that they have the same signature."""
        # Create an instance of the protocol (which cannot be instantiated
        # directly, so this creates a subclass, then instantiates it)
        interface = type("_", (StorageInterface,), {})()

        assert "content_add" in dir(interface)

        missing_methods = []

        for meth_name in dir(interface):
            if meth_name.startswith("_"):
                continue
            interface_meth = getattr(interface, meth_name)
            try:
                concrete_meth = getattr(swh_storage, meth_name)
            except AttributeError:
                if not getattr(interface_meth, "deprecated_endpoint", False):
                    # The backend is missing a (non-deprecated) endpoint
                    missing_methods.append(meth_name)
                continue

            expected_signature = inspect.signature(interface_meth)
            actual_signature = inspect.signature(concrete_meth)

            assert expected_signature == actual_signature, meth_name

        assert missing_methods == []

        # If all the assertions above succeed, then this one should too.
        # But there's no harm in double-checking.
        # And we could replace the assertions above by this one, but unlike
        # the assertions above, it doesn't explain what is missing.
        assert isinstance(swh_storage, StorageInterface)

    def test_check_config(self, swh_storage):
        assert swh_storage.check_config(check_write=True)
        assert swh_storage.check_config(check_write=False)

    def test_content_add(self, swh_storage, sample_data):
        # first insert only one item
        first_content = sample_data.content

        insertion_start_time = now()
        actual_result = swh_storage.content_add([first_content])
        insertion_end_time = now()

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": first_content.length,
        }

        assert (
            swh_storage.content_get_data({"sha1": first_content.sha1})
            == first_content.data
        )

        expected_cont = attr.evolve(first_content, data=None)

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

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            assert swh_storage.stat_counters()["content"] == 1

        # then insert all the content (first one already exists)
        contents = sample_data.contents

        actual_result = swh_storage.content_add(contents)
        assert actual_result == {
            "content:add": len(contents) - 1,
            "content:add:bytes": sum(len(c.data) for c in contents[1:]),
        }

        expected_contents = [attr.evolve(content, data=None) for content in contents]
        assert (
            swh_storage.content_get([content.sha1 for content in contents])
            == expected_contents
        )
        journal_contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(journal_contents) == len(contents)
        for obj, cont in zip(journal_contents, expected_contents):
            obj = attr.evolve(obj, ctime=None)
            assert obj == cont

    def test_content_add__legacy(self, swh_storage, sample_data):
        """content_add() with a single sha1 as param instead of a dict"""
        cont = sample_data.content

        insertion_start_time = now()
        actual_result = swh_storage.content_add([cont])
        insertion_end_time = now()

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont.length,
        }

        with pytest.warns(DeprecationWarning):
            assert swh_storage.content_get_data(cont.sha1) == cont.data

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

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            assert swh_storage.stat_counters()["content"] == 1

    def test_content_add_from_lazy_content(self, swh_storage, sample_data):
        cont = sample_data.content
        lazy_content = cont.evolve(data=None, get_data=lambda: cont.data)

        insertion_start_time = now()

        actual_result = swh_storage.content_add([lazy_content])

        insertion_end_time = now()

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont.length,
        }

        # the fact that we retrieve the content object from the storage with
        # the correct 'data' field ensures it has been 'called'
        assert swh_storage.content_get_data({"sha1": cont.sha1}) == cont.data

        expected_cont = cont.evolve(data=None, ctime=None, get_data=None)
        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        for obj in contents:
            assert insertion_start_time <= obj.ctime
            assert obj.ctime <= insertion_end_time
            assert obj.evolve(ctime=None).to_dict() == expected_cont.to_dict()

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            assert swh_storage.stat_counters()["content"] == 1

    @pytest.mark.parametrize("algo", sorted(DEFAULT_ALGORITHMS))
    def test_content_get_data_single_hash_dict(
        self, swh_storage, swh_storage_backend, sample_data, mocker, algo
    ):
        cont = sample_data.content

        swh_storage.content_add([cont])

        content_find = mocker.patch.object(
            swh_storage_backend, "content_find", wraps=swh_storage_backend.content_find
        )
        assert swh_storage.content_get_data({algo: cont.get_hash(algo)}) == cont.data

        assert len(content_find.mock_calls) == 1

    def test_content_get_data_two_hash_dict(
        self, swh_storage, swh_storage_backend, sample_data, mocker
    ):
        cont = sample_data.content

        swh_storage.content_add([cont])

        content_find = mocker.patch.object(
            swh_storage_backend, "content_find", wraps=swh_storage_backend.content_find
        )

        combinations = list(itertools.combinations(sorted(DEFAULT_ALGORITHMS), 2))
        for algo1, algo2 in combinations:
            assert (
                swh_storage.content_get_data(
                    {algo1: cont.get_hash(algo1), algo2: cont.get_hash(algo2)}
                )
                == cont.data
            )
        assert len(content_find.mock_calls) == len(combinations)

    def test_content_get_data_full_dict(
        self, swh_storage, swh_storage_backend, sample_data, mocker
    ):
        cont = sample_data.content

        swh_storage.content_add([cont])

        content_find = mocker.patch.object(
            swh_storage_backend, "content_find", wraps=swh_storage_backend.content_find
        )
        assert swh_storage.content_get_data(cont.hashes()) == cont.data
        assert len(content_find.mock_calls) == 0, (
            "content_get_data() needlessly called content_find(), "
            "as all hashes were provided as argument"
        )

    def test_content_get_data_missing(self, swh_storage, sample_data):
        cont, cont2 = sample_data.contents[:2]

        swh_storage.content_add([cont])

        # Query a single missing content
        actual_content_data = swh_storage.content_get_data({"sha1": cont2.sha1})
        assert actual_content_data is None

        # Check content_get does not abort after finding a missing content
        actual_content_data = swh_storage.content_get_data({"sha1": cont.sha1})
        assert actual_content_data == cont.data
        actual_content_data = swh_storage.content_get_data({"sha1": cont2.sha1})
        assert actual_content_data is None

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
        assert actual_algo in ["sha1", "sha1_git"]
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

        assert swh_storage.content_get_data({"sha1": cont.sha1}) == cont.data

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

        actual_contents = swh_storage.content_get([cont1.sha1])
        expected_content = attr.evolve(cont1b, data=None)
        assert actual_contents == [expected_content]

    def test_content_add_metadata(self, swh_storage, sample_data):
        # first insert only one item
        first_content = attr.evolve(sample_data.content, data=None, ctime=now())

        actual_result = swh_storage.content_add_metadata([first_content])
        assert actual_result == {
            "content:add": 1,
        }

        # then insert all the content (first one already exists)
        contents = [
            attr.evolve(content, data=None, ctime=now())
            for content in sample_data.contents
        ]

        actual_result = swh_storage.content_add_metadata(contents)
        assert actual_result == {
            "content:add": len(contents) - 1,
        }

        assert (
            swh_storage.content_get([content.sha1 for content in contents]) == contents
        )
        journal_contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(journal_contents) == len(contents)
        for obj, cont in zip(journal_contents, contents):
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

    def test_content_add_objstorage_first(
        self, swh_storage, swh_storage_backend, sample_data
    ):
        """Tests the objstorage is written to before the DB and journal"""
        cont = sample_data.content

        swh_storage_backend.objstorage.content_add = MagicMock(
            side_effect=Exception("Oops")
        )

        # Try to add, but the objstorage crashes
        try:
            swh_storage.content_add([cont])
        except Exception:
            pass

        # The DB must be written to after the objstorage, so the DB should be
        # unchanged if the objstorage crashed
        assert swh_storage.content_get_data({"sha1": cont.sha1}) is None

        # The journal too
        assert list(swh_storage.journal_writer.journal.objects) == []

    def test_skipped_content_add(self, swh_storage, sample_data):
        contents = sample_data.skipped_contents[:2]
        cont = contents[0]
        cont2 = attr.evolve(contents[1], blake2s256=None)

        contents_dict = [c.to_dict() for c in [cont, cont2]]

        missing = [
            filter_dict(c) for c in swh_storage.skipped_content_missing(contents_dict)
        ]

        assert missing == [filter_dict(c.hashes()) for c in [cont, cont2]]

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

    def test_skipped_content_find_with_results(self, swh_storage, sample_data):
        # XXX: We cannot test the origin part on postgresql due to
        # https://gitlab.softwareheritage.org/swh/devel/swh-storage/-/issues/4693
        if isinstance(swh_storage, PostgreSQLStorage):
            skipped_content = attr.evolve(sample_data.skipped_content, origin=None)
        else:
            # We configure an existing origin to see if we properly retrieve the origin URL
            origin = sample_data.origin
            skipped_content = attr.evolve(
                sample_data.skipped_content, origin=origin.url
            )
            swh_storage.origin_add([origin])
        swh_storage.skipped_content_add([skipped_content])

        # 1. with something to find
        actually_present = swh_storage.skipped_content_find(
            {"sha1": skipped_content.sha1}
        )
        assert 1 == len(actually_present)
        assert actually_present[0] == skipped_content

        # 2. with something to find
        actually_present = swh_storage.skipped_content_find(
            {"sha1_git": skipped_content.sha1_git}
        )
        assert 1 == len(actually_present)
        assert actually_present[0] == skipped_content

        # 3. with something to find
        actually_present = swh_storage.skipped_content_find(
            {"sha256": skipped_content.sha256}
        )
        assert 1 == len(actually_present)
        assert actually_present[0] == skipped_content

        # 4. with something to find
        actually_present = swh_storage.skipped_content_find(skipped_content.hashes())
        assert 1 == len(actually_present)
        assert actually_present[0] == skipped_content

    def test_skipped_content_find_with_no_results(self, swh_storage, sample_data):
        missing_content = sample_data.skipped_content

        # Please note that the database is left empty on purpose
        results = swh_storage.content_find({"sha1": missing_content.sha1})
        assert results == []
        results = swh_storage.content_find(missing_content.hashes())
        assert results == []

    def test_skipped_content_find_with_duplicate_input(self, swh_storage, sample_data):
        # use skipped_content2 as it does not reference an origin
        skipped_content = sample_data.skipped_content2

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(skipped_content.sha1)
        sha1_array[0] += 1
        sha1git_array = bytearray(skipped_content.sha1_git)
        sha1git_array[0] += 1
        duplicated = attr.evolve(
            skipped_content, sha1=bytes(sha1_array), sha1_git=bytes(sha1git_array)
        )

        # Inject the data
        swh_storage.skipped_content_add([skipped_content, duplicated])

        results = swh_storage.skipped_content_find(
            {
                "blake2s256": duplicated.blake2s256,
                "sha256": duplicated.sha256,
            }
        )
        assert set(results) == {skipped_content, duplicated}

    def test_skipped_content_find_with_duplicate_but_precise_search(
        self, swh_storage, sample_data
    ):
        # use skipped_content2 as it does not reference an origin
        skipped_content = sample_data.skipped_content2

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(skipped_content.sha1)
        sha1_array[0] += 1
        sha1git_array = bytearray(skipped_content.sha1_git)
        sha1git_array[0] += 1
        duplicated = attr.evolve(
            skipped_content, sha1=bytes(sha1_array), sha1_git=bytes(sha1git_array)
        )

        # Inject the data
        swh_storage.skipped_content_add([skipped_content, duplicated])

        # Search with collided hash should return both
        results = swh_storage.skipped_content_find(
            {
                "sha256": duplicated.sha256,
            }
        )
        assert len(results) == 2

        # Search with more precision should return only one
        results = swh_storage.skipped_content_find(
            {
                "sha256": duplicated.sha256,
                "sha1_git": skipped_content.sha1_git,
            }
        )
        assert results == [skipped_content]

    def test_skipped_content_find_bad_input(self, swh_storage):
        # 1. with no hash to lookup
        with pytest.raises(StorageArgumentException):
            swh_storage.skipped_content_find({})  # need at least one hash

        # 2. with bad hash
        with pytest.raises(StorageArgumentException):
            swh_storage.skipped_content_find(
                {"unknown-sha1": "something"}
            )  # not the right key

    def test_skipped_content_missing_partial_hash(self, swh_storage, sample_data):
        cont = sample_data.skipped_content
        cont2 = attr.evolve(cont, sha1_git=None)
        contents_dict = [c.to_dict() for c in [cont, cont2]]

        missing = list(swh_storage.skipped_content_missing(contents_dict))
        assert len(missing) == 2

        actual_result = swh_storage.skipped_content_add([cont])

        assert actual_result.pop("skipped_content:add") == 1
        assert actual_result == {}

        missing = [
            filter_dict(c) for c in swh_storage.skipped_content_missing(contents_dict)
        ]
        assert missing == [filter_dict(cont2.hashes())]

    @pytest.mark.property_based
    @settings(
        deadline=None,  # this test is very slow
        suppress_health_check=disabled_health_checks,
    )
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
    @settings(
        suppress_health_check=disabled_health_checks,
    )
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
        cont2 = sample_data.content2
        missing_cont = sample_data.skipped_content
        missing_cont2 = sample_data.skipped_content2
        swh_storage.content_add([cont, cont2])

        # when
        gen = swh_storage.content_missing_per_sha1(
            [cont.sha1, missing_cont.sha1, cont2.sha1, missing_cont2.sha1]
        )
        # then
        assert set(gen) == {missing_cont.sha1, missing_cont2.sha1}

    def test_content_missing_per_sha1_git(self, swh_storage, sample_data):
        cont, cont2 = sample_data.contents[:2]
        missing_cont = sample_data.skipped_content
        missing_cont2 = sample_data.skipped_content2

        swh_storage.content_add([cont, cont2])

        contents = [
            cont.sha1_git,
            cont2.sha1_git,
            missing_cont.sha1_git,
            missing_cont2.sha1_git,
        ]

        missing_contents = swh_storage.content_missing_per_sha1_git(contents)
        assert list(missing_contents) == [missing_cont.sha1_git, missing_cont2.sha1_git]

        missing_contents = swh_storage.content_missing_per_sha1_git([])
        assert list(missing_contents) == []

    def test_content_get_partition(self, swh_storage, swh_contents):
        """content_get_partition paginates results if limit exceeded"""
        expected_contents = [
            attr.evolve(c, data=None) for c in swh_contents if c.status != "absent"
        ]

        actual_contents = []
        for i in range(16):
            actual_result = swh_storage.content_get_partition(i, 16)
            assert actual_result.next_page_token is None
            actual_contents.extend(actual_result.results)

        assert len(actual_contents) == len(expected_contents)
        for content in actual_contents:
            assert content in expected_contents
            assert content.ctime is None

    def test_content_get_partition_full(self, swh_storage, swh_contents):
        """content_get_partition for a single partition returns all available contents"""
        expected_contents = [
            attr.evolve(c, data=None) for c in swh_contents if c.status != "absent"
        ]

        actual_result = swh_storage.content_get_partition(0, 1)
        assert actual_result.next_page_token is None

        actual_contents = actual_result.results
        assert len(actual_contents) == len(expected_contents)
        for content in actual_contents:
            assert content in expected_contents

    def test_content_get_partition_empty(self, swh_storage, swh_contents):
        """content_get_partition when at least one of the partitions is empty"""
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

            for content in actual_result.results:
                seen_sha1s.append(content.sha1)

            # Limit is higher than the max number of results
            assert actual_result.next_page_token is None

        assert set(seen_sha1s) == expected_contents

    def test_content_get_partition_limit_none(self, swh_storage):
        """content_get_partition call with wrong limit input should fail"""
        with pytest.raises(StorageArgumentException, match="limit should not be None"):
            swh_storage.content_get_partition(1, 16, limit=None)

    def test_content_get_partition_pagination_generate(self, swh_storage, swh_contents):
        """content_get_partition returns contents within range provided"""
        expected_contents = [
            attr.evolve(c, data=None) for c in swh_contents if c.status != "absent"
        ]

        # retrieve contents
        actual_contents = []
        for i in range(4):
            page_token = None
            while True:
                actual_result = swh_storage.content_get_partition(
                    i, 4, limit=3, page_token=page_token
                )
                actual_contents.extend(actual_result.results)
                page_token = actual_result.next_page_token

                if page_token is None:
                    break

        assert len(actual_contents) == len(expected_contents)
        for content in actual_contents:
            assert content in expected_contents

    @pytest.mark.parametrize("algo", sorted(DEFAULT_ALGORITHMS))
    def test_content_get(self, swh_storage, sample_data, algo):
        cont1, cont2 = sample_data.contents[:2]

        swh_storage.content_add([cont1, cont2])

        actual_contents = swh_storage.content_get(
            [getattr(cont1, algo), getattr(cont2, algo)], algo
        )

        # we only retrieve the metadata so no data nor ctime within
        expected_contents = [attr.evolve(c, data=None) for c in [cont1, cont2]]

        assert actual_contents == expected_contents
        for content in actual_contents:
            assert content.ctime is None

    @pytest.mark.parametrize("algo", sorted(DEFAULT_ALGORITHMS))
    def test_content_get_missing(self, swh_storage, sample_data, algo):
        cont1, cont2 = sample_data.contents[:2]
        assert cont1.sha1 != cont2.sha1
        missing_cont = sample_data.skipped_content

        swh_storage.content_add([cont1, cont2])

        actual_contents = swh_storage.content_get(
            [getattr(cont1, algo), getattr(cont2, algo), getattr(missing_cont, algo)],
            algo,
        )

        expected_contents = [
            attr.evolve(c, data=None) if c else None for c in [cont1, cont2, None]
        ]
        assert actual_contents == expected_contents

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
        directory = sample_data.directory
        assert directory.entries[0].target == content.sha1_git
        swh_storage.content_add([content])

        init_missing = set(swh_storage.directory_missing([directory.id]))
        assert init_missing == {directory.id}

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

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            assert swh_storage.stat_counters()["directory"] == 1

    def test_directory_add_all(self, swh_storage, sample_data):
        init_missing = set(
            swh_storage.directory_missing([d.id for d in sample_data.directories])
        )
        assert {d.id for d in sample_data.directories} == init_missing

        actual_result = swh_storage.directory_add(sample_data.directories)
        assert actual_result == {"directory:add": 7}

        for directory in sample_data.directories:
            assert ("directory", directory) in list(
                swh_storage.journal_writer.journal.objects
            )

    def test_directory_add_with_raw_manifest(self, swh_storage, sample_data):
        content = sample_data.content
        directory = sample_data.directory
        directory = attr.evolve(directory, raw_manifest=b"foo")
        directory = attr.evolve(directory, id=directory.compute_hash())

        assert directory.entries[0].target == content.sha1_git
        swh_storage.content_add([content])

        init_missing = list(swh_storage.directory_missing([directory.id]))
        assert [directory.id] == init_missing
        assert swh_storage.directory_get_raw_manifest([directory.id]) == {}

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

        assert swh_storage.directory_get_raw_manifest([directory.id]) == {
            directory.id: b"foo"
        }

        directory2 = attr.evolve(directory, raw_manifest=b"bar")
        directory2 = attr.evolve(directory2, id=directory2.compute_hash())
        swh_storage.directory_add([directory2])

        assert swh_storage.directory_get_raw_manifest(
            [directory.id, directory2.id]
        ) == {directory.id: b"foo", directory2.id: b"bar"}

    @settings(
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large]
        + disabled_health_checks,
    )
    @given(
        strategies.lists(
            hypothesis_strategies.directories(),
            min_size=1,
            max_size=10,
            unique_by=lambda directory: directory.id,
        )
    )
    def test_directory_add_get_arbitrary(self, swh_storage, directories):
        swh_storage.directory_add(directories)

        for directory in directories:
            actual_directory = Directory(
                id=directory.id,
                entries=tuple(
                    stream_results(swh_storage.directory_get_entries, directory.id)
                ),
                raw_manifest=swh_storage.directory_get_raw_manifest([directory.id])[
                    directory.id
                ],
            )
            if directory.raw_manifest is None:
                assert directory == actual_directory
            else:
                assert directory.raw_manifest == actual_directory.raw_manifest
                # we can't compare the other fields, because they become non-intrinsic,
                # so they may clash between hypothesis runs

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

    def test_directory_add_raw_manifest__different_entries(
        self, swh_storage, check_ls=True
    ):
        """Add two directories with the same raw_manifest (and therefore, same id)
        but different entries.
        """
        dir1 = Directory(
            entries=(
                DirectoryEntry(
                    name=b"name1", type="file", target=b"\x00" * 20, perms=0o100000
                ),
            ),
            raw_manifest=b"abc",
        )
        dir2 = Directory(
            entries=(
                DirectoryEntry(
                    name=b"name2", type="file", target=b"\x00" * 20, perms=0o100000
                ),
            ),
            raw_manifest=b"abc",
        )
        assert dir1.id == dir2.id  # because it is computed from the raw_manifest only

        assert swh_storage.directory_add([dir1])["directory:add"] == 1
        assert swh_storage.directory_add([dir2])["directory:add"] in (0, 1)

        if check_ls:
            # This assertion is skipped when running from
            # test_directory_add_raw_manifest__different_entries__allow_overwrite
            assert [entry["name"] for entry in swh_storage.directory_ls(dir1.id)] == (
                [b"name1"]
            )

        # used in TestCassandraStorage by
        # test_directory_add_raw_manifest__different_entries__allow_overwrite
        return dir1.id

    def test_directory_get_id_partition(self, swh_storage, sample_data):
        directories = list(sample_data.directories) + [
            Directory(
                entries=(
                    DirectoryEntry(
                        name=f"entry{i}".encode(),
                        type="file",
                        target=b"\x00" * 20,
                        perms=0,
                    ),
                )
            )
            for i in range(100)
        ]
        swh_storage.directory_add(directories)

        expected_results = {dir_.id for dir_ in directories}
        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_partitions = 1 << math.floor(math.log2(len(expected_results)) + 1)

        actual_results = []

        for i in range(nb_partitions):
            result = list(
                stream_results(swh_storage.directory_get_id_partition, i, nb_partitions)
            )

            # Technically not guaranteed, but it is statistically very unlikely
            # all directories are in the same partition
            assert len(result) < len(expected_results)

            actual_results.extend(result)

        assert set(actual_results) == expected_results

    def test_directory_ls_recursive(self, swh_storage, sample_data):
        # create consistent dataset regarding the directories we want to list
        content, content2 = sample_data.contents[:2]
        swh_storage.content_add([content, content2])
        dir1, dir2, dir3 = sample_data.directories[:3]

        dir_ids = [d.id for d in [dir1, dir2, dir3]]
        init_missing = set(swh_storage.directory_missing(dir_ids))
        assert init_missing == set(dir_ids)

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
        init_missing = set(swh_storage.directory_missing(dir_ids))
        assert init_missing == set(dir_ids)

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

    def test_directory_ls_missing_content(self, swh_storage, sample_data):
        swh_storage.directory_add([sample_data.directory2])
        assert list(swh_storage.directory_ls(sample_data.directory2.id)) == [
            {
                "dir_id": sample_data.directory2.id,
                "length": None,
                "name": b"oof",
                "perms": 33188,
                "sha1": None,
                "sha1_git": None,
                "sha256": None,
                "status": None,
                "target": sample_data.directory2.entries[0].target,
                "type": "file",
            },
        ]

    def test_directory_ls_skipped_content(self, swh_storage, sample_data):
        swh_storage.directory_add([sample_data.directory2])

        cont = SkippedContent(
            sha1_git=sample_data.directory2.entries[0].target,
            sha1=b"c" * 20,
            sha256=None,
            blake2s256=None,
            length=42,
            status="absent",
            reason="You need a premium subscription to access this content",
        )
        swh_storage.skipped_content_add([cont])

        assert list(swh_storage.directory_ls(sample_data.directory2.id)) == [
            {
                "dir_id": sample_data.directory2.id,
                "length": 42,
                "name": b"oof",
                "perms": 33188,
                "sha1": b"c" * 20,
                "sha1_git": sample_data.directory2.entries[0].target,
                "sha256": None,
                "status": "absent",
                "target": sample_data.directory2.entries[0].target,
                "type": "file",
            },
        ]

    def test_directory_entry_get_by_path(self, swh_storage, sample_data):
        cont, content2 = sample_data.contents[:2]
        dir1, dir2, dir3, dir4, dir5 = sample_data.directories[:5]

        # given
        dir_ids = [d.id for d in [dir1, dir2, dir3, dir4, dir5]]
        init_missing = set(swh_storage.directory_missing(dir_ids))
        assert init_missing == set(dir_ids)

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

    def test_directory_get_entries_pagination(self, swh_storage, sample_data):
        dir_ = sample_data.directory3
        entries = sorted(dir_.entries, key=lambda entry: entry.name)
        swh_storage.directory_add(sample_data.directories)

        # No pagination needed
        actual_data = swh_storage.directory_get_entries(dir_.id)
        assert sorted(actual_data.results) == sorted(entries)
        assert actual_data.next_page_token is None, actual_data

        # A little pagination
        actual_data = swh_storage.directory_get_entries(dir_.id, limit=2)
        assert len(actual_data.results) == 2, actual_data
        assert actual_data.next_page_token is not None, actual_data

        all_results = list(actual_data.results)

        actual_data = swh_storage.directory_get_entries(
            dir_.id, page_token=actual_data.next_page_token
        )
        assert len(actual_data.results) == len(entries) - 2, actual_data
        assert actual_data.next_page_token is None, actual_data

        all_results.extend(actual_data.results)
        assert sorted(all_results) == sorted(entries)

    @pytest.mark.parametrize("limit", [1, 2, 3, 4, 5])
    def test_directory_get_entries(self, swh_storage, sample_data, limit):
        dir_ = sample_data.directory3
        swh_storage.directory_add(sample_data.directories)

        actual_data = list(
            stream_results(
                swh_storage.directory_get_entries,
                dir_.id,
                limit=limit,
            )
        )
        assert sorted(actual_data) == sorted(dir_.entries)

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

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
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

    def test_revision_add_fractional_timezone(self, swh_storage, sample_data):
        # When reading a date from this time period on systems configured with
        # timezone Europe/Paris, postgresql returns them with UTC+00:09:21 as timezone,
        # but psycopg2 < 2.9.0 had to truncate them.
        # https://www.psycopg.org/docs/usage.html#time-zones-handling
        #
        # There is a workaround in swh.storage.postgresql.storage.Storage.get_db,
        # to set the timezone to UTC so it works on all psycopg2 versions.
        #
        # Therefore, this test always succeeds in tox (because psycopg2 >= 2.9.0)
        # and on the CI (both because psycopg2 >= 2.9.0 and TZ=UTC); but which means
        # this test is only useful on machines with older psycopg2 versions and
        # TZ=Europe/Paris. But the workaround is also only needed on this kind of
        # configuration, so this is good enough.
        revision = attr.evolve(
            sample_data.revision,
            date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=-1855958962, microseconds=0),
                offset_bytes=b"+0000",
            ),
        )
        init_missing = swh_storage.revision_missing([revision.id])
        assert list(init_missing) == [revision.id]

        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        end_missing = swh_storage.revision_missing([revision.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision)
        ]

        assert swh_storage.revision_get([revision.id])[0] == revision

    def test_revision_add_with_raw_manifest(self, swh_storage, sample_data):
        revision = sample_data.revision
        revision = attr.evolve(revision, raw_manifest=b"foo")
        revision = attr.evolve(revision, id=revision.compute_hash())
        init_missing = swh_storage.revision_missing([revision.id])
        assert list(init_missing) == [revision.id]

        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        end_missing = swh_storage.revision_missing([revision.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision)
        ]

        assert swh_storage.revision_get([revision.id]) == [revision]

    @settings(
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large]
        + disabled_health_checks,
    )
    @given(
        strategies.lists(
            hypothesis_strategies.revisions(),
            min_size=1,
            max_size=10,
            unique_by=lambda rev: rev.id,
        )
    )
    def test_revision_add_get_arbitrary(self, swh_storage, revisions):
        # remove non-intrinsic data, so releases inserted with different hypothesis
        # data can't clash with each other
        revisions = [
            attr.evolve(
                revision,
                synthetic=False,
                metadata=None,
                author=(
                    None
                    if revision.author is None
                    else Person.from_fullname(revision.author.fullname)
                ),
                committer=(
                    None
                    if revision.committer is None
                    else Person.from_fullname(revision.committer.fullname)
                ),
                type=RevisionType.GIT,
            )
            for revision in revisions
        ]

        swh_storage.revision_add(revisions)

        for revision in revisions:
            (rev,) = swh_storage.revision_get([revision.id])
            if rev.raw_manifest is None:
                assert rev == revision
            else:
                assert rev.raw_manifest == revision.raw_manifest
                # we can't compare the other fields, because they become non-intrinsic,
                # so they may clash between hypothesis runs

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
        actual_revisions = swh_storage.revision_get([revision.id, revision2.id])
        assert actual_revisions == [revision, revision2]

        # order 2
        actual_revisions2 = swh_storage.revision_get([revision2.id, revision.id])
        assert actual_revisions2 == [revision2, revision]

    def test_revision_get_partition(self, swh_storage, sample_data):
        revisions = list(sample_data.revisions) + [
            Revision(
                message=f"hello{i}".encode(),
                author=None,
                date=None,
                committer=None,
                committer_date=None,
                parents=(),
                type=RevisionType.GIT,
                directory=b"\x00" * 20,
                synthetic=True,
            )
            for i in range(100)
        ]
        swh_storage.revision_add(revisions)

        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_partitions = 1 << math.floor(math.log2(len(revisions)) + 1)

        actual_revisions = []

        for i in range(nb_partitions):
            result = list(
                stream_results(swh_storage.revision_get_partition, i, nb_partitions)
            )

            # Technically not guaranteed, but it is statistically very unlikely
            # all revisions are in the same partition
            assert len(result) < len(revisions)

            actual_revisions.extend(result)

        # TODO: use set equality once Revision.metadata is removed
        actual_revisions.sort(key=lambda rev: rev.id)
        revisions.sort(key=lambda rev: rev.id)
        assert actual_revisions == revisions

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
        results = list(swh_storage.revision_log([revision4.id], limit=1))

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

        actual_revisions = swh_storage.revision_get([revision.id, revision2.id])

        assert len(actual_revisions) == 2
        assert actual_revisions == [revision, None]

    def test_revision_get_no_parents(self, swh_storage, sample_data):
        revision = sample_data.revision
        swh_storage.revision_add([revision])

        actual_revision = swh_storage.revision_get([revision.id])[0]

        assert revision.parents == ()
        assert actual_revision.parents == ()  # no parents on this one

    def test_revision_get_random(self, swh_storage, sample_data):
        revision1, revision2, revision3 = sample_data.revisions[:3]

        swh_storage.revision_add([revision1, revision2, revision3])

        assert swh_storage.revision_get_random() in {
            revision1.id,
            revision2.id,
            revision3.id,
        }

    def test_revision_missing_many(self, swh_storage, sample_data):
        """Large number of revision ids to check can cause ScyllaDB to reject
        queries."""
        revision = sample_data.revision
        ids = [bytes([b1, b2]) * 10 for b1 in range(256) for b2 in range(10)]
        ids.append(revision.id)
        ids.sort()
        init_missing = swh_storage.revision_missing(ids)
        assert set(init_missing) == set(ids)

        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        end_missing = swh_storage.revision_missing(ids)
        assert set(end_missing) == set(ids) - {revision.id}

    def test_revision_add_no_author_or_date(self, swh_storage, sample_data):
        full_revision = sample_data.revision

        revision = attr.evolve(full_revision, author=None, date=None)
        revision = attr.evolve(revision, id=revision.compute_hash())
        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        end_missing = swh_storage.revision_missing([revision.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision)
        ]

        assert swh_storage.revision_get([revision.id]) == [revision]

    def test_revision_add_no_committer_or_date(self, swh_storage, sample_data):
        full_revision = sample_data.revision

        revision = attr.evolve(full_revision, committer=None, committer_date=None)
        revision = attr.evolve(revision, id=revision.compute_hash())
        actual_result = swh_storage.revision_add([revision])
        assert actual_result == {"revision:add": 1}

        end_missing = swh_storage.revision_missing([revision.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("revision", revision)
        ]

        assert swh_storage.revision_get([revision.id]) == [revision]

    def test_extid_add_git(self, swh_storage, sample_data):
        gitids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "git"
        ]
        extids = [
            ExtID(
                extid=gitid,
                extid_type="git",
                target=CoreSWHID(
                    object_id=gitid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for gitid in gitids
        ]

        assert swh_storage.extid_get_from_extid("git", gitids) == []
        assert swh_storage.extid_get_from_target(ObjectType.REVISION, gitids) == []

        summary = swh_storage.extid_add(extids)
        assert summary == {"extid:add": len(gitids)}

        assert swh_storage.extid_get_from_extid("git", gitids) == extids
        assert swh_storage.extid_get_from_target(ObjectType.REVISION, gitids) == extids

        assert swh_storage.extid_get_from_extid("hg", gitids) == []
        assert swh_storage.extid_get_from_target(ObjectType.RELEASE, gitids) == []

        # check ExtIDs have been added to the journal
        extids_in_journal = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "extid"
        ]
        assert extids == extids_in_journal

    def test_extid_add_hg(self, swh_storage, sample_data):
        def get_node(revision):
            node = None
            if revision.extra_headers:
                node = dict(revision.extra_headers).get(b"node")
            if node is None and revision.metadata:
                node = hash_to_bytes(revision.metadata.get("node"))
            return node

        swhids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "hg"
        ]
        extids = [
            get_node(revision)
            for revision in sample_data.revisions
            if revision.type.value == "hg"
        ]

        assert swh_storage.extid_get_from_extid("hg", extids) == []
        assert swh_storage.extid_get_from_target(ObjectType.REVISION, swhids) == []

        extid_objs = [
            ExtID(
                extid=hgid,
                extid_type="hg",
                extid_version=1,
                target=CoreSWHID(
                    object_id=swhid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for hgid, swhid in zip(extids, swhids)
        ]
        summary = swh_storage.extid_add(extid_objs)
        assert summary == {"extid:add": len(swhids)}

        assert swh_storage.extid_get_from_extid("hg", extids) == extid_objs
        assert (
            swh_storage.extid_get_from_target(ObjectType.REVISION, swhids) == extid_objs
        )

        assert swh_storage.extid_get_from_extid("git", extids) == []
        assert swh_storage.extid_get_from_target(ObjectType.RELEASE, swhids) == []

        # check ExtIDs have been added to the journal
        extids_in_journal = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "extid"
        ]
        assert extid_objs == extids_in_journal

    def test_extid_add_twice(self, swh_storage, sample_data):
        gitids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "git"
        ]

        extids = [
            ExtID(
                extid=gitid,
                extid_type="git",
                target=CoreSWHID(
                    object_id=gitid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for gitid in gitids
        ]
        summary = swh_storage.extid_add(extids)
        assert summary == {"extid:add": len(gitids)}

        # add them again, should be noop
        summary = swh_storage.extid_add(extids)
        # assert summary == {"extid:add": 0}
        assert swh_storage.extid_get_from_extid("git", gitids) == extids
        assert swh_storage.extid_get_from_target(ObjectType.REVISION, gitids) == extids

    def test_extid_add_extid_multicity(self, swh_storage, sample_data):
        ids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "git"
        ]

        extids = [
            ExtID(
                extid=extid,
                extid_type="git",
                extid_version=2,
                target=CoreSWHID(
                    object_id=extid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for extid in ids
        ]
        swh_storage.extid_add(extids)

        # try to add "modified-extid" versions, should be added
        extids2 = [
            ExtID(
                extid=extid,
                extid_type="hg",
                extid_version=2,
                target=CoreSWHID(
                    object_id=extid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for extid in ids
        ]
        swh_storage.extid_add(extids2)

        assert swh_storage.extid_get_from_extid("git", ids) == extids
        assert swh_storage.extid_get_from_extid("hg", ids) == extids2
        assert set(swh_storage.extid_get_from_target(ObjectType.REVISION, ids)) == {
            *extids,
            *extids2,
        }

    def test_extid_add_target_multicity(self, swh_storage, sample_data):
        ids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "git"
        ]

        extids = [
            ExtID(
                extid=extid,
                extid_type="git",
                target=CoreSWHID(
                    object_id=extid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for extid in ids
        ]
        swh_storage.extid_add(extids)

        # try to add "modified" versions, should be added
        extids2 = [
            ExtID(
                extid=extid,
                extid_type="git",
                target=CoreSWHID(
                    object_id=extid,
                    object_type=ObjectType.RELEASE,
                ),
            )
            for extid in ids
        ]
        swh_storage.extid_add(extids2)

        assert set(swh_storage.extid_get_from_extid("git", ids)) == {*extids, *extids2}
        assert swh_storage.extid_get_from_target(ObjectType.REVISION, ids) == extids
        assert swh_storage.extid_get_from_target(ObjectType.RELEASE, ids) == extids2

    def test_extid_version_behavior(self, swh_storage, sample_data):
        ids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "git"
        ]

        # Insert extids with several different versions
        extids = [
            ExtID(
                extid=extid,
                extid_type="git",
                extid_version=0,
                target=CoreSWHID(
                    object_id=extid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for extid in ids
        ] + [
            ExtID(
                extid=extid,
                extid_type="git",
                extid_version=1,
                target=CoreSWHID(
                    object_id=extid,
                    object_type=ObjectType.REVISION,
                ),
            )
            for extid in ids
        ]
        swh_storage.extid_add(extids)

        # Check that both versions get returned
        for git_id in ids:
            objs = swh_storage.extid_get_from_extid("git", [git_id])
            assert len(objs) == 2
            assert set(obj.extid_version for obj in objs) == {0, 1}
        for swhid in ids:
            objs = swh_storage.extid_get_from_target(ObjectType.REVISION, [swhid])
            assert len(objs) == 2
            assert set(obj.extid_version for obj in objs) == {0, 1}
        for version in [0, 1]:
            for git_id in ids:
                objs = swh_storage.extid_get_from_extid(
                    "git", [git_id], version=version
                )
                assert len(objs) == 1
                assert objs[0].extid_version == version
            for swhid in ids:
                objs = swh_storage.extid_get_from_target(
                    ObjectType.REVISION,
                    [swhid],
                    extid_version=version,
                    extid_type="git",
                )
                assert len(objs) == 1
                assert objs[0].extid_version == version
                assert objs[0].extid_type == "git"

    def test_extid_version_behavior_failure(self, swh_storage, sample_data):
        """Calls with wrong input should raise"""
        ids = [
            revision.id
            for revision in sample_data.revisions
            if revision.type.value == "git"
        ]

        # Other edge cases
        with pytest.raises(
            (ValueError, RemoteException), match="both extid_type and extid_version"
        ):
            swh_storage.extid_get_from_target(
                ObjectType.REVISION, [ids[0]], extid_version=0
            )

        with pytest.raises(
            (ValueError, RemoteException), match="both extid_type and extid_version"
        ):
            swh_storage.extid_get_from_target(
                ObjectType.REVISION, [ids[0]], extid_type="git"
            )

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

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            assert swh_storage.stat_counters()["release"] == 2

    def test_release_add_with_raw_manifest(self, swh_storage, sample_data):
        release = sample_data.releases[0]
        release = attr.evolve(release, raw_manifest=b"foo")
        release = attr.evolve(release, id=release.compute_hash())

        init_missing = swh_storage.release_missing([release.id])
        assert list(init_missing) == [release.id]

        actual_result = swh_storage.release_add([release])
        assert actual_result == {"release:add": 1}

        end_missing = swh_storage.release_missing([release.id])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ("release", release),
        ]

        assert swh_storage.release_get([release.id]) == [release]

    @settings(
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large]
        + disabled_health_checks,
    )
    @given(
        strategies.lists(
            hypothesis_strategies.releases(),
            min_size=1,
            max_size=10,
            unique_by=lambda rel: rel.id,
        )
    )
    def test_release_add_get_arbitrary(self, swh_storage, releases):
        # remove non-intrinsic data, so releases inserted with different hypothesis
        # data can't clash with each other
        releases = [
            attr.evolve(
                release,
                synthetic=False,
                metadata=None,
                author=(
                    Person.from_fullname(release.author.fullname)
                    if release.author
                    else None
                ),
            )
            for release in releases
        ]
        swh_storage.release_add(releases)

        for release in releases:
            (rev,) = swh_storage.release_get([release.id])
            if rev.raw_manifest is None:
                assert rev == release
            else:
                assert rev.raw_manifest == release.raw_manifest
                # we can't compare the other fields, because they become non-intrinsic,
                # so they may clash between hypothesis runs

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
            [
                ("release", release),
                ("release", release2),
            ]
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
        actual_releases = swh_storage.release_get([release.id, release2.id])

        # then
        assert actual_releases == [release, release2]

        unknown_releases = swh_storage.release_get([release3.id])
        assert unknown_releases[0] is None

    def test_release_get_order(self, swh_storage, sample_data):
        release, release2 = sample_data.releases[:2]

        add_result = swh_storage.release_add([release, release2])
        assert add_result == {"release:add": 2}

        # order 1
        actual_releases = swh_storage.release_get([release.id, release2.id])
        assert actual_releases == [release, release2]

        # order 2
        actual_releases2 = swh_storage.release_get([release2.id, release.id])
        assert actual_releases2 == [release2, release]

    def test_release_get_partition(self, swh_storage, sample_data):
        releases = list(sample_data.releases) + [
            Release(
                name=f"release{i}".encode(),
                message=f"hello{i}".encode(),
                author=None,
                date=None,
                target=b"\x00" * 20,
                target_type=ReleaseTargetType.REVISION,
                synthetic=True,
            )
            for i in range(100)
        ]
        swh_storage.release_add(releases)

        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_partitions = 1 << math.floor(math.log2(len(releases)) + 1)

        actual_releases = []

        for i in range(nb_partitions):
            result = list(
                stream_results(swh_storage.release_get_partition, i, nb_partitions)
            )

            # Technically not guaranteed, but it is statistically very unlikely
            # all releases are in the same partition
            assert len(result) < len(releases)

            actual_releases.extend(result)

        assert set(actual_releases) == set(releases)

    def test_release_get_random(self, swh_storage, sample_data):
        release, release2, release3 = sample_data.releases[:3]

        swh_storage.release_add([release, release2, release3])

        assert swh_storage.release_get_random() in {
            release.id,
            release2.id,
            release3.id,
        }

    def test_origin_add(self, swh_storage, sample_data):
        origins = list(sample_data.origins)
        origin_urls = [o.url for o in origins]

        assert swh_storage.origin_get(origin_urls) == [None] * len(origins)

        stats = swh_storage.origin_add(origins)
        assert stats == {"origin:add": len(origin_urls)}

        actual_origins = swh_storage.origin_get(origin_urls)
        assert actual_origins == origins

        assert set(swh_storage.journal_writer.journal.objects) == set(
            [("origin", origin) for origin in origins]
        )

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            assert swh_storage.stat_counters()["origin"] == len(origins)

    def test_origin_add_twice(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        add1 = swh_storage.origin_add([origin, origin2])
        assert set(swh_storage.journal_writer.journal.objects) == set(
            [
                ("origin", origin),
                ("origin", origin2),
            ]
        )
        assert add1 == {"origin:add": 2}

        add2 = swh_storage.origin_add([origin, origin2])
        assert set(swh_storage.journal_writer.journal.objects) == set(
            [
                ("origin", origin),
                ("origin", origin2),
            ]
        )
        assert add2 == {"origin:add": 0}

    def test_origin_add_twice_at_once(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]

        add1 = swh_storage.origin_add([origin, origin2, origin, origin2])
        assert set(swh_storage.journal_writer.journal.objects) == set(
            [
                ("origin", origin),
                ("origin", origin2),
            ]
        )
        assert add1 == {"origin:add": 2}

        add2 = swh_storage.origin_add([origin, origin2, origin, origin2])
        assert set(swh_storage.journal_writer.journal.objects) == set(
            [
                ("origin", origin),
                ("origin", origin2),
            ]
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

    @pytest.mark.parametrize(
        "allowed_statuses,require_snapshot",
        [
            ([], False),
            (["failed"], False),
            (["failed", "full"], False),
            ([], True),
            (["failed"], True),
            (["failed", "full"], True),
        ],
    )
    def test_origin_visit_get_with_statuses(
        self, swh_storage, sample_data, allowed_statuses, require_snapshot
    ):
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

        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=sample_data.date_visit1 + datetime.timedelta(hours=1),
                    type=sample_data.type_visit1,
                    status="failed",
                    snapshot=None,
                ),
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov2.visit,
                    date=sample_data.date_visit2 + datetime.timedelta(hours=1),
                    type=sample_data.type_visit2,
                    status="failed",
                    snapshot=None,
                ),
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov3.visit,
                    date=sample_data.date_visit2 + datetime.timedelta(hours=1),
                    type=sample_data.type_visit2,
                    status="failed",
                    snapshot=None,
                ),
            ]
        )

        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=sample_data.date_visit1 + datetime.timedelta(hours=2),
                    type=sample_data.type_visit1,
                    status="full",
                    snapshot=sample_data.snapshots[0].id,
                ),
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov2.visit,
                    date=sample_data.date_visit2 + datetime.timedelta(hours=2),
                    type=sample_data.type_visit2,
                    status="full",
                    snapshot=sample_data.snapshots[1].id,
                ),
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov3.visit,
                    date=sample_data.date_visit2 + datetime.timedelta(hours=2),
                    type=sample_data.type_visit2,
                    status="full",
                    snapshot=sample_data.snapshots[2].id,
                ),
            ]
        )

        ov1_statuses = swh_storage.origin_visit_status_get(
            origin.url, visit=ov1.visit
        ).results

        ov2_statuses = swh_storage.origin_visit_status_get(
            origin.url, visit=ov2.visit
        ).results

        ov3_statuses = swh_storage.origin_visit_status_get(
            origin.url, visit=ov3.visit
        ).results

        def _filter_statuses(ov_statuses):
            if allowed_statuses:
                ov_statuses = [
                    ovs for ovs in ov_statuses if ovs.status in allowed_statuses
                ]
                assert [ovs.status for ovs in ov_statuses] == allowed_statuses
            else:
                assert [ovs.status for ovs in ov_statuses] == [
                    "created",
                    "failed",
                    "full",
                ]
            if require_snapshot:
                ov_statuses = [ovs for ovs in ov_statuses if ovs.snapshot is not None]
            return ov_statuses

        ov1_statuses = _filter_statuses(ov1_statuses)
        ov2_statuses = _filter_statuses(ov2_statuses)
        ov3_statuses = _filter_statuses(ov3_statuses)

        ovws1 = OriginVisitWithStatuses(visit=ov1, statuses=ov1_statuses)
        ovws2 = OriginVisitWithStatuses(visit=ov2, statuses=ov2_statuses)
        ovws3 = OriginVisitWithStatuses(visit=ov3, statuses=ov3_statuses)

        # order asc, no token, no limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws1, ovws2, ovws3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            limit=2,
        )
        next_page_token = actual_page.next_page_token
        assert len(actual_page.results) == 2
        assert next_page_token is not None
        assert actual_page.results == [ovws1, ovws2]

        # order asc, token, no limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
        )

        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws3]

        # order asc, no token, limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            limit=1,
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovws1]

        # order asc, token, no limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws2, ovws3]

        # order asc, token, limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            limit=2,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws2, ovws3]

        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            limit=1,
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovws2]

        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            limit=1,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws3]

        # order desc, no token, no limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            order=ListOrder.DESC,
        )

        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws3, ovws2, ovws1]

        # order desc, no token, limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            limit=2,
            order=ListOrder.DESC,
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovws3, ovws2]

        # order desc, token, no limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            order=ListOrder.DESC,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws1]

        # order desc, no token, limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            limit=1,
            order=ListOrder.DESC,
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovws3]

        # order desc, token, no limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            order=ListOrder.DESC,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws2, ovws1]

        # order desc, token, limit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            order=ListOrder.DESC,
            limit=1,
        )
        next_page_token = actual_page.next_page_token
        assert next_page_token is not None
        assert actual_page.results == [ovws2]

        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=next_page_token,
            order=ListOrder.DESC,
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == [ovws1]

        # should return empty results if page_token is last visit
        actual_page = swh_storage.origin_visit_get_with_statuses(
            origin.url,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            page_token=str(ov3.visit),
        )
        assert actual_page.next_page_token is None
        assert actual_page.results == []

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

    def test_origin_visit_status_add_unknown_type(self, swh_storage, sample_data):
        ov = OriginVisit(
            origin=sample_data.origin.url,
            date=now(),
            type=sample_data.type_visit1,
            visit=0,
        )
        ovs = OriginVisitStatus(
            origin=ov.origin,
            visit=1,
            date=now(),
            status="created",
            snapshot=None,
        )

        with pytest.raises(StorageArgumentException):
            swh_storage.origin_visit_status_add([ovs])

        swh_storage.origin_add([sample_data.origin])

        with pytest.raises(StorageArgumentException):
            swh_storage.origin_visit_status_add([ovs])

        swh_storage.origin_visit_add([ov])

        swh_storage.origin_visit_status_add([ovs])

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
                    origin=origin.url,
                    date=date_visit1,
                    type=sample_data.type_visit1,
                ),
            ]
        )[0]

        ovs1 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit1,
            type=ov1.type,
            status="created",
            snapshot=None,
        )

        ovs2 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit2,
            type=ov1.type,
            status="partial",
            snapshot=None,
        )

        ovs3 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit3,
            type=ov1.type,
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
                    [
                        OriginVisit(
                            origin=origin.url,
                            date=date_visit,
                            type=visit_type,
                        )
                    ]
                )[0]
                swh_storage.origin_visit_status_add(
                    [
                        OriginVisitStatus(
                            origin=origin.url,
                            visit=visit.visit,
                            date=now(),
                            status="full",
                            snapshot=hash_to_bytes(
                                "9b922e6d8d5b803c1582aabe5525b7b91150788e"
                            ),
                        )
                    ]
                )

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
            swh_storage.refresh_stat_counters()
            stats = swh_storage.stat_counters()
            assert stats["origin"] == len(origins)
            assert stats["origin_visit"] == len(origins) * len(visits)

        random_ovs = swh_storage.origin_visit_status_get_random(visit_type)
        assert random_ovs
        assert random_ovs.origin is not None
        assert random_ovs.origin in [o.url for o in origins]
        assert random_ovs.type is not None

    def test_origin_visit_status_get_random_nothing_found(
        self, swh_storage, sample_data
    ):
        origins = sample_data.origins
        swh_storage.origin_add(origins)
        visit_type = "hg"
        # Add some visits older than 3 months so they are excluded from the random
        # selection
        visits = self._generate_random_visits(nb_visits=100, start=14, end=24)
        for origin in origins:
            for date_visit in visits:
                visit = swh_storage.origin_visit_add(
                    [
                        OriginVisit(
                            origin=origin.url,
                            date=date_visit,
                            type=visit_type,
                        )
                    ]
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

    def test_origin_snapshot_get_all(self, swh_storage, sample_data):
        origin = sample_data.origins[0]
        swh_storage.origin_add([origin])

        # add some random visits within the selection range
        visits = self._generate_random_visits()
        visit_type = "git"

        # set first visit to a null snapshot
        visit = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=visits[0],
                    type=visit_type,
                )
            ]
        )[0]
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=visit.visit,
                    date=now(),
                    status="created",
                    snapshot=None,
                )
            ]
        )

        # add visits to origin
        snapshots = set()
        for date_visit in visits[1:]:
            visit = swh_storage.origin_visit_add(
                [
                    OriginVisit(
                        origin=origin.url,
                        date=date_visit,
                        type=visit_type,
                    )
                ]
            )[0]
            # pick a random snapshot and keep track of it
            snapshot = random.choice(sample_data.snapshots).id
            snapshots.add(snapshot)
            swh_storage.origin_visit_status_add(
                [
                    OriginVisitStatus(
                        origin=origin.url,
                        visit=visit.visit,
                        date=now(),
                        status="full",
                        snapshot=snapshot,
                    )
                ]
            )

        # check expected snapshots are returned
        assert set(swh_storage.origin_snapshot_get_all(origin.url)) == snapshots

    def test_origin_get_by_sha1(self, swh_storage, sample_data):
        origin = sample_data.origin
        assert swh_storage.origin_get([origin.url])[0] is None
        swh_storage.origin_add([origin])

        origins = list(swh_storage.origin_get_by_sha1([sha1(origin.url)]))
        assert len(origins) == 1
        assert origins[0]["url"] == origin.url

    def test_origin_get_by_sha1_not_found(self, swh_storage, sample_data):
        unknown_origin = sample_data.origin
        assert swh_storage.origin_get([unknown_origin.url])[0] is None
        origins = list(swh_storage.origin_get_by_sha1([sha1(unknown_origin.url)]))
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

    def test_origin_search_no_visit_types(self, swh_storage, sample_data):
        origin = sample_data.origins[0]
        swh_storage.origin_add([origin])
        actual_page = swh_storage.origin_search(origin.url, visit_types=["git"])
        assert actual_page.next_page_token is None
        assert actual_page.results == []

    def test_origin_search_with_visit_types(self, swh_storage, sample_data):
        origin, origin2 = sample_data.origins[:2]
        swh_storage.origin_add([origin, origin2])
        swh_storage.origin_visit_add(
            [
                OriginVisit(origin=origin.url, date=now(), type="git"),
                OriginVisit(origin=origin2.url, date=now(), type="svn"),
            ]
        )
        actual_page = swh_storage.origin_search(origin.url, visit_types=["git"])
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin]

        actual_page = swh_storage.origin_search(origin2.url, visit_types=["svn"])
        assert actual_page.next_page_token is None
        assert actual_page.results == [origin2]

    def test_origin_search_multiple_visit_types(self, swh_storage, sample_data):
        origin = sample_data.origins[0]
        swh_storage.origin_add([origin])

        def _add_visit_type(visit_type):
            swh_storage.origin_visit_add(
                [OriginVisit(origin=origin.url, date=now(), type=visit_type)]
            )

        def _check_visit_types(visit_types):
            actual_page = swh_storage.origin_search(origin.url, visit_types=visit_types)
            assert actual_page.next_page_token is None
            assert actual_page.results == [origin]

        _add_visit_type("git")
        _check_visit_types(["git"])
        _check_visit_types(["git", "hg"])

        _add_visit_type("hg")
        _check_visit_types(["hg"])
        _check_visit_types(["git", "hg"])

    def test_origin_visit_add(self, swh_storage, sample_data):
        origin1 = sample_data.origins[1]
        swh_storage.origin_add([origin1])

        date_visit = now()
        date_visit2 = date_visit + datetime.timedelta(minutes=1)

        date_visit = round_to_milliseconds(date_visit)
        date_visit2 = round_to_milliseconds(date_visit2)

        visit1 = OriginVisit(
            origin=origin1.url,
            date=date_visit,
            type=sample_data.type_visit1,
        )
        visit2 = OriginVisit(
            origin=origin1.url,
            date=date_visit2,
            type=sample_data.type_visit2,
        )

        # add once
        ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])
        # then again (will be ignored as they already exist)
        origin_visit1, origin_visit2 = swh_storage.origin_visit_add([ov1, ov2])
        assert ov1 == origin_visit1
        assert ov2 == origin_visit2

        assert ov1.visit == 1
        assert ov2.visit == 2

        ovs1 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit,
            type=ov1.type,
            status="created",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=date_visit2,
            type=ov2.type,
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

    def test_origin_visit_add_replayed(self, swh_storage, sample_data):
        """Tests adding a visit with an id makes sure the next id is higher"""
        origin1 = sample_data.origins[1]
        swh_storage.origin_add([origin1])

        date_visit = now()
        date_visit2 = date_visit + datetime.timedelta(minutes=1)

        date_visit = round_to_milliseconds(date_visit)
        date_visit2 = round_to_milliseconds(date_visit2)

        visit1 = OriginVisit(
            origin=origin1.url, date=date_visit, type=sample_data.type_visit1, visit=42
        )
        visit2 = OriginVisit(
            origin=origin1.url,
            date=date_visit2,
            type=sample_data.type_visit2,
        )

        # add once
        ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])
        # then again (will be ignored as they already exist)
        origin_visit1, origin_visit2 = swh_storage.origin_visit_add([ov1, ov2])
        assert ov1 == origin_visit1
        assert ov2 == origin_visit2

        assert ov1.visit == 42
        assert ov2.visit == 43

        # check OriginVisitStatus objects
        ovs1 = swh_storage.origin_visit_status_get(ov1.origin, visit=ov1.visit).results
        assert not ovs1, f"There should be no OriginVisitStatus for visit {ov1}"
        ovs2 = swh_storage.origin_visit_status_get(ov2.origin, visit=ov2.visit).results
        assert len(ovs2) == 1
        assert ovs2[0].status == "created"
        assert ovs2[0].type == ov2.type

        visit3 = OriginVisit(
            origin=origin1.url, date=date_visit, type=sample_data.type_visit1, visit=12
        )
        visit4 = OriginVisit(
            origin=origin1.url,
            date=date_visit2,
            type=sample_data.type_visit2,
        )

        # add once
        ov3, ov4 = swh_storage.origin_visit_add([visit3, visit4])
        # then again (will be ignored as they already exist)
        origin_visit3, origin_visit4 = swh_storage.origin_visit_add([ov3, ov4])
        assert ov3 == origin_visit3
        assert ov4 == origin_visit4

        assert ov3.visit == 12
        assert ov4.visit == 44

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
        """Correct origin visit statuses should add a new visit status"""
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
            origin=ov1.origin,
            visit=ov1.visit,
            date=sample_data.date_visit1,
            type=ov1.type,
            status="created",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=sample_data.date_visit2,
            type=ov2.type,
            status="created",
            snapshot=None,
        )

        date_visit_now = round_to_milliseconds(now())
        visit_status1 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit_now,
            type=ov1.type,
            status="full",
            snapshot=snapshot.id,
        )

        date_visit_now = round_to_milliseconds(now())
        visit_status2 = OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=date_visit_now,
            type=ov2.type,
            status="ongoing",
            snapshot=None,
            metadata={"intrinsic": "something"},
        )
        stats = swh_storage.origin_visit_status_add([visit_status1, visit_status2])
        assert stats == {"origin_visit_status:add": 2}

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
        """Correct origin visit statuses should add a new visit status"""
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
            origin=ov1.origin,
            visit=ov1.visit,
            date=sample_data.date_visit1,
            type=ov1.type,
            status="created",
            snapshot=None,
        )
        date_visit_now = round_to_milliseconds(now())
        visit_status1 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=date_visit_now,
            type=ov1.type,
            status="full",
            snapshot=snapshot.id,
        )

        stats = swh_storage.origin_visit_status_add([visit_status1])
        assert stats == {"origin_visit_status:add": 1}
        # second call will ignore existing entries (will send to storage though)
        stats = swh_storage.origin_visit_status_add([visit_status1])
        # ...so the storage still returns it as an addition
        assert stats == {"origin_visit_status:add": 1}

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

    def _setup_origin_visit_tests_data(self, swh_storage, sample_data):
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

        return origin, ov1, ov2, ov3

    def test_origin_visit_find_by_date(self, swh_storage, sample_data):
        origin, _, origin_visit2, origin_visit3 = self._setup_origin_visit_tests_data(
            swh_storage, sample_data
        )

        # Simple case
        actual_visit = swh_storage.origin_visit_find_by_date(
            origin.url, sample_data.date_visit3
        )
        assert actual_visit == origin_visit2

        # There are two visits at the same date, the latest must be returned
        actual_visit = swh_storage.origin_visit_find_by_date(
            origin.url, sample_data.date_visit2
        )
        assert actual_visit == origin_visit3

    def test_origin_visit_find_by_date_and_type(self, swh_storage, sample_data):
        (
            origin,
            origin_visit1,
            origin_visit2,
            origin_visit3,
        ) = self._setup_origin_visit_tests_data(swh_storage, sample_data)

        # each visit has a different type
        for origin_visit in (origin_visit1, origin_visit2, origin_visit3):
            actual_visit = swh_storage.origin_visit_find_by_date(
                origin.url, sample_data.date_visit3, type=origin_visit.type
            )

            assert actual_visit == origin_visit

        # visit 1 and visit 3 have same date but different types
        for origin_visit in (origin_visit1, origin_visit3):
            actual_visit = swh_storage.origin_visit_find_by_date(
                origin.url, sample_data.date_visit2, type=origin_visit.type
            )

            assert actual_visit == origin_visit

    def test_origin_visit_find_by_date_latest_visit(self, swh_storage, sample_data):
        first_visit_date = sample_data.date_visit2
        second_visit_date = first_visit_date + timedelta(days=10)

        origin = sample_data.origin
        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url,
            date=first_visit_date,
            type=sample_data.type_visit2,
        )
        visit2 = OriginVisit(
            origin=origin.url,
            date=second_visit_date,
            type=sample_data.type_visit2,
        )
        visit1, visit2 = swh_storage.origin_visit_add([visit1, visit2])

        # should return the second visit
        actual_visit = swh_storage.origin_visit_find_by_date(
            origin.url, second_visit_date
        )
        assert actual_visit == visit2

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
        """Filtering origin visit get latest with filter type should be ok"""
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit2,
            type="hg",
        )
        date_now = round_to_milliseconds(now())
        visit3 = OriginVisit(
            origin=origin.url,
            date=date_now,
            type="hg",
        )
        assert sample_data.date_visit1 < sample_data.date_visit2
        assert sample_data.date_visit2 < date_now

        ov1, ov2, ov3 = swh_storage.origin_visit_add([visit1, visit2, visit3])

        # Check type filter is ok
        actual_visit = swh_storage.origin_visit_get_latest(origin.url, type="git")
        assert actual_visit == ov1
        actual_visit = swh_storage.origin_visit_get_latest(origin.url, type="hg")
        assert actual_visit == ov3
        actual_visit_unknown_type = swh_storage.origin_visit_get_latest(
            origin.url,
            type="npm",  # no visit matching that type
        )
        assert actual_visit_unknown_type is None

    def test_origin_visit_get_latest(self, swh_storage, sample_data):
        empty_snapshot, complete_snapshot = sample_data.snapshots[1:3]
        origin = sample_data.origin

        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit2,
            type="hg",
        )
        date_now = round_to_milliseconds(now())
        visit3 = OriginVisit(
            origin=origin.url,
            date=date_now,
            type="hg",
        )
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
            origin=ov1.origin,
            visit=ov1.visit,
            date=round_to_milliseconds(now()),
            type=ov1.type,
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
            origin=ov1.origin,
            visit=ov1.visit,
            date=round_to_milliseconds(now()),
            type=ov1.type,
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
            origin=ov2.origin,
            visit=ov2.visit,
            date=round_to_milliseconds(now()),
            type=ov2.type,
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
            origin=ov3.origin,
            visit=ov3.visit,
            date=round_to_milliseconds(now()),
            type=ov3.type,
            status="ongoing",
            snapshot=complete_snapshot.id,
        )
        swh_storage.origin_visit_status_add([visit_status3_with_snapshot])

        # full status is still the first visit
        actual_visit = swh_storage.origin_visit_get_latest(
            origin.url,
            allowed_statuses=["full"],
            require_snapshot=True,
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
            origin=origin.url,
            date=sample_data.date_visit1,
            type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type="hg",
        )

        ov1, ov2 = swh_storage.origin_visit_add([visit1, visit2])

        # ties should be broken by using the visit id
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov2

    def test_origin_visit_get_latest_order(self, swh_storage, sample_data):
        origin = sample_data.origin

        id1 = 2
        id2 = 1
        id3 = 3
        date1 = datetime.datetime(2021, 8, 2, tzinfo=datetime.timezone.utc)
        date2 = datetime.datetime(2021, 8, 3, tzinfo=datetime.timezone.utc)
        date3 = datetime.datetime(2021, 8, 1, tzinfo=datetime.timezone.utc)

        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url,
            visit=id1,
            date=date1,
            type="git",
        )
        visit2 = OriginVisit(
            origin=origin.url,
            visit=id2,
            date=date2,
            type="hg",
        )
        visit3 = OriginVisit(
            origin=origin.url,
            visit=id3,
            date=date3,
            type="tar",
        )

        ov1, ov2, ov3 = swh_storage.origin_visit_add([visit1, visit2, visit3])
        ovs = [
            OriginVisitStatus(
                origin=origin.url,
                visit=ov.visit,
                date=ov.date,
                type=ov.type,
                status="created",
                snapshot=None,
            )
            for ov in [ov1, ov2, ov3]
        ]
        swh_storage.origin_visit_status_add(ovs)

        # no filters
        actual_visit = swh_storage.origin_visit_get_latest(origin.url)
        assert actual_visit == ov3

    def test_origin_visit_get_latest__not_last(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])

        (date1, date2, date3, date4) = [
            datetime.datetime(2021, 8, i, tzinfo=datetime.timezone.utc)
            for i in range(1, 5)
        ]

        visit1 = OriginVisit(
            origin=origin.url,
            visit=0,
            date=date1,
            type="git",
        )

        swh_storage.origin_visit_add([visit1])
        ov1 = swh_storage.origin_visit_get_latest(origin.url)

        # Add a snapshot, but do not attach it to visit1 for now
        complete_snapshot = sample_data.snapshots[2]
        swh_storage.snapshot_add([complete_snapshot])

        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=date2,
                    status="partial",
                    snapshot=None,
                )
            ]
        )

        # no snapshot is associated to the visit, so None
        visit = swh_storage.origin_visit_get_latest(
            origin.url,
            allowed_statuses=["partial"],
            require_snapshot=True,
        )
        assert visit is None

        # attach the visit to the snapshot
        swh_storage.origin_visit_status_add(
            [
                OriginVisitStatus(
                    origin=origin.url,
                    visit=ov1.visit,
                    date=date3,
                    status="full",
                    snapshot=complete_snapshot.id,
                )
            ]
        )
        # and add a visit later on
        ov2 = swh_storage.origin_visit_add(
            [
                OriginVisit(
                    origin=origin.url,
                    date=date4,
                    type=visit1.type,
                )
            ]
        )[0]

        # so now the returned visit should be ov1 (because ov2 has no snapshot,
        # so it won't be returned when require_snapshot is True)
        visit = swh_storage.origin_visit_get_latest(origin.url, require_snapshot=True)
        assert visit == ov1
        # but without require_snapshot, ov2 is returned
        visit = swh_storage.origin_visit_get_latest(origin.url, require_snapshot=False)
        assert visit == ov2

    def test_origin_visit_status_get_latest__validation(self, swh_storage, sample_data):
        origin = sample_data.origin
        swh_storage.origin_add([origin])
        visit1 = OriginVisit(
            origin=origin.url,
            date=sample_data.date_visit1,
            type="git",
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
            origin=ov1.origin,
            visit=ov1.visit,
            date=sample_data.date_visit1,
            type=ov1.type,
            status="partial",
            snapshot=None,
        )
        ovs2 = OriginVisitStatus(
            origin=ov1.origin,
            visit=ov1.visit,
            date=sample_data.date_visit2,
            type=ov1.type,
            status="ongoing",
            snapshot=None,
        )
        ovs3 = OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=sample_data.date_visit2
            + datetime.timedelta(minutes=1),  # to not be ignored
            type=ov2.type,
            status="ongoing",
            snapshot=None,
        )
        ovs4 = OriginVisitStatus(
            origin=ov2.origin,
            visit=ov2.visit,
            date=date_now,
            type=ov2.type,
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
        revisions = swh_storage.revision_get([revision.id, revision2.id])

        # then check committers are the same
        assert revisions[0].committer == revisions[1].committer

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
                    origin=ov1.origin,
                    visit=ov1.visit,
                    date=date_now,
                    type=ov1.type,
                    status="full",
                    snapshot=empty_snapshot.id,
                )
            ]
        )

        by_id = swh_storage.snapshot_get(empty_snapshot.id)
        assert by_id == {**empty_snapshot_dict, "next_branch": None}

        ovs1 = OriginVisitStatus.from_dict(
            {
                "origin": ov1.origin,
                "date": sample_data.date_visit1,
                "type": ov1.type,
                "visit": ov1.visit,
                "status": "created",
                "snapshot": None,
                "metadata": None,
            }
        )
        ovs2 = OriginVisitStatus.from_dict(
            {
                "origin": ov1.origin,
                "date": date_now,
                "type": ov1.type,
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
            (
                "origin_visit_status",
                ovs1,
            ),
            ("snapshot", empty_snapshot),
            (
                "origin_visit_status",
                ovs2,
            ),
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

    @settings(
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large]
        + disabled_health_checks,
    )
    @given(
        strategies.lists(
            hypothesis_strategies.snapshots(),
            min_size=1,
            max_size=10,
            unique_by=lambda snp: snp.id,
        )
    )
    def test_snapshot_add_get_arbitrary(self, swh_storage, snapshots):
        swh_storage.snapshot_add(snapshots)

        for snapshot in snapshots:
            assert swh_storage.snapshot_get(snapshot.id) == {
                **snapshot.to_dict(),
                "next_branch": None,
            }

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

        if isinstance(swh_storage, InMemoryStorage) or not isinstance(
            swh_storage, CassandraStorage
        ):
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

    def test_snapshot_add_count_branches_with_filtering(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]

        actual_result = swh_storage.snapshot_add([complete_snapshot])
        assert actual_result == {"snapshot:add": 1}

        snp_size = swh_storage.snapshot_count_branches(
            complete_snapshot.id, branch_name_exclude_prefix=b"release"
        )

        expected_snp_size = {
            "alias": 1,
            "content": 1,
            "directory": 2,
            "revision": 1,
            "snapshot": 1,
            None: 1,
        }
        assert snp_size == expected_snp_size

    def test_snapshot_add_count_branches_with_filtering_edge_cases(
        self, swh_storage, sample_data
    ):
        snapshot = Snapshot(
            branches={
                b"\xaa\xff": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"\xaa\xff\x00": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"\xff\xff": SnapshotBranch(
                    target=sample_data.release.id,
                    target_type=SnapshotTargetType.RELEASE,
                ),
                b"\xff\xff\x00": SnapshotBranch(
                    target=sample_data.release.id,
                    target_type=SnapshotTargetType.RELEASE,
                ),
                b"dangling": None,
            },
        )

        swh_storage.snapshot_add([snapshot])

        assert swh_storage.snapshot_count_branches(
            snapshot.id, branch_name_exclude_prefix=b"\xaa\xff"
        ) == {None: 1, "release": 2}

        assert swh_storage.snapshot_count_branches(
            snapshot.id, branch_name_exclude_prefix=b"\xff\xff"
        ) == {None: 1, "revision": 2}

    def test_snapshot_add_get_paginated(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]

        swh_storage.snapshot_add([complete_snapshot])

        snp_id = complete_snapshot.id
        branches = complete_snapshot.branches
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
            "branches": {
                branch_names[0]: branches[branch_names[0]],
            },
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
        branches = complete_snapshot.branches

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=["release", "revision"]
        )

        expected_snapshot = {
            "id": snp_id,
            "branches": {
                name: tgt
                for name, tgt in branches.items()
                if tgt
                and tgt.target_type
                in [SnapshotTargetType.RELEASE, SnapshotTargetType.REVISION]
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
                if tgt and tgt.target_type == SnapshotTargetType.ALIAS
            },
            "next_branch": None,
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get_filtered_and_paginated(self, swh_storage, sample_data):
        complete_snapshot = sample_data.snapshots[2]

        swh_storage.snapshot_add([complete_snapshot])

        snp_id = complete_snapshot.id
        branches = complete_snapshot.branches
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
            "branches": {
                branch_names[dir_idx]: branches[branch_names[dir_idx]],
            },
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

    def test_snapshot_add_get_by_branches_name_pattern(self, swh_storage, sample_data):
        snapshot = Snapshot(
            branches={
                b"refs/heads/master": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"refs/heads/incoming": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"refs/pull/1": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"refs/pull/2": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"dangling": None,
                b"\xaa\xff": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"\xaa\xff\x00": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"\xff\xff": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"\xff\xff\x00": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
            },
        )
        swh_storage.snapshot_add([snapshot])

        for include_pattern, exclude_prefix, nb_results in (
            (b"pull", None, 2),
            (b"incoming", None, 1),
            (b"dangling", None, 1),
            (None, b"refs/heads/", 7),
            (b"refs", b"refs/heads/master", 3),
            (b"refs", b"refs/heads/master", 3),
            (None, b"\xaa\xff", 7),
            (None, b"\xff\xff", 7),
        ):
            branches = swh_storage.snapshot_get_branches(
                snapshot.id,
                branch_name_include_substring=include_pattern,
                branch_name_exclude_prefix=exclude_prefix,
            )["branches"]

            expected_branches = [
                branch_name
                for branch_name in snapshot.branches
                if (include_pattern is None or include_pattern in branch_name)
                and (
                    exclude_prefix is None or not branch_name.startswith(exclude_prefix)
                )
            ]
            assert sorted(branches) == sorted(expected_branches)
            assert len(branches) == nb_results

    def test_snapshot_add_get_by_branches_name_pattern_filtered_paginated(
        self, swh_storage, sample_data
    ):
        pattern = b"foo"
        nb_branches_by_target_type = 10
        branches = {}
        for i in range(nb_branches_by_target_type):
            branches[f"branch/directory/bar{i}".encode()] = SnapshotBranch(
                target=sample_data.directory.id,
                target_type=SnapshotTargetType.DIRECTORY,
            )
            branches[f"branch/revision/bar{i}".encode()] = SnapshotBranch(
                target=sample_data.revision.id,
                target_type=SnapshotTargetType.REVISION,
            )
            branches[f"branch/directory/{pattern}{i}".encode()] = SnapshotBranch(
                target=sample_data.directory.id,
                target_type=SnapshotTargetType.DIRECTORY,
            )
            branches[f"branch/revision/{pattern}{i}".encode()] = SnapshotBranch(
                target=sample_data.revision.id,
                target_type=SnapshotTargetType.REVISION,
            )

        snapshot = Snapshot(branches=branches)
        swh_storage.snapshot_add([snapshot])

        branches_count = nb_branches_by_target_type // 2

        for target_type in (
            SnapshotTargetType.DIRECTORY,
            SnapshotTargetType.REVISION,
        ):
            target_type_str = target_type.value
            partial_branches = swh_storage.snapshot_get_branches(
                snapshot.id,
                branch_name_include_substring=pattern,
                target_types=[target_type_str],
                branches_count=branches_count,
            )
            branches = partial_branches["branches"]

            expected_branches = [
                branch_name
                for branch_name, branch_data in snapshot.branches.items()
                if pattern in branch_name and branch_data.target_type == target_type
            ][:branches_count]

            assert sorted(branches) == sorted(expected_branches)
            assert (
                partial_branches["next_branch"]
                == f"branch/{target_type_str}/{pattern}{branches_count}".encode()
            )

            partial_branches = swh_storage.snapshot_get_branches(
                snapshot.id,
                branch_name_include_substring=pattern,
                target_types=[target_type_str],
                branches_from=partial_branches["next_branch"],
            )
            branches = partial_branches["branches"]

            expected_branches = [
                branch_name
                for branch_name, branch_data in snapshot.branches.items()
                if pattern in branch_name and branch_data.target_type == target_type
            ][branches_count:]

            assert sorted(branches) == sorted(expected_branches)
            assert partial_branches["next_branch"] is None

    def test_snapshot_get_branches_from_no_result(self, swh_storage, sample_data):
        snapshot = Snapshot(
            branches={
                b"refs/heads/master": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
            },
        )
        swh_storage.snapshot_add([snapshot])

        partial_branches = swh_storage.snapshot_get_branches(
            snapshot.id,
            branches_from=b"s",
        )

        assert partial_branches is not None
        assert partial_branches["branches"] == {}

    def test_snapshot_get_branches_correct_branch_count(self, swh_storage, sample_data):
        n = 20
        branches = {}
        for i in range(n):
            branches[f"refs/heads/head{i:02d}".encode()] = SnapshotBranch(
                target=sample_data.revision.id,
                target_type=SnapshotTargetType.REVISION,
            )
            branches[f"refs/tags/tag{i:02d}".encode()] = SnapshotBranch(
                target=sample_data.release.id,
                target_type=SnapshotTargetType.RELEASE,
            )
        for i in range(n):
            branches[f"refs/tags/tag{n+i:02d}".encode()] = SnapshotBranch(
                target=sample_data.release.id,
                target_type=SnapshotTargetType.RELEASE,
            )

        snapshot = Snapshot(branches=branches)
        swh_storage.snapshot_add([snapshot])

        partial_branches = swh_storage.snapshot_get_branches(
            snapshot.id, target_types=["release"], branches_count=n
        )

        assert len(partial_branches["branches"]) == n
        assert all(
            branch.target_type == SnapshotTargetType.RELEASE
            for branch in partial_branches["branches"].values()
        )
        assert partial_branches["next_branch"] == b"refs/tags/tag20"

    def test_snapshot_get_branches_from_after_exclude_prefix(
        self, swh_storage, sample_data
    ):
        snapshot = Snapshot(
            branches={
                b"refs/pulls/pull0": SnapshotBranch(
                    target=sample_data.revision.id,
                    target_type=SnapshotTargetType.REVISION,
                ),
                b"refs/tags/tag00": SnapshotBranch(
                    target=sample_data.release.id,
                    target_type=SnapshotTargetType.RELEASE,
                ),
                b"refs/tags/tag01": SnapshotBranch(
                    target=sample_data.release.id,
                    target_type=SnapshotTargetType.RELEASE,
                ),
            }
        )
        swh_storage.snapshot_add([snapshot])

        branches_from = b"refs/tags/tag01"
        partial_branches = swh_storage.snapshot_get_branches(
            snapshot.id,
            branch_name_exclude_prefix=b"refs/pulls",
            branches_from=branches_from,
        )

        assert len(partial_branches["branches"]) == 1
        assert partial_branches["next_branch"] is None

        assert partial_branches["branches"] == {
            branches_from: snapshot.branches[branches_from]
        }

    @settings(
        suppress_health_check=disabled_health_checks,
    )
    @given(hypothesis_strategies.snapshots(min_size=1))
    def test_snapshot_get_unknown_snapshot(self, swh_storage, unknown_snapshot):
        assert swh_storage.snapshot_get(unknown_snapshot.id) is None
        assert swh_storage.snapshot_get_branches(unknown_snapshot.id) is None

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

        actual_visit = swh_storage.origin_visit_get_by(origin.url, ov1.visit)
        assert actual_visit == ov1

        visit_status = swh_storage.origin_visit_status_get_latest(
            origin.url, ov1.visit, require_snapshot=True
        )
        assert visit_status.snapshot == snapshot.id

    def test_snapshot_get_id_partition(self, swh_storage, sample_data):
        snapshots = list(sample_data.snapshots) + [
            Snapshot(
                branches={
                    f"branch{i}".encode(): SnapshotBranch(
                        target=b"\x00" * 20,
                        target_type=SnapshotTargetType.REVISION,
                    ),
                },
            )
            for i in range(100)
        ]
        swh_storage.snapshot_add(snapshots)

        expected_results = {snp.id for snp in snapshots}
        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_partitions = 1 << math.floor(math.log2(len(expected_results)) + 1)

        actual_results = []

        for i in range(nb_partitions):
            result = list(
                stream_results(swh_storage.snapshot_get_id_partition, i, nb_partitions)
            )

            # Technically not guaranteed, but it is statistically very unlikely
            # all snapshots are in the same partition
            assert len(result) < len(expected_results)

            actual_results.extend(result)

        assert set(actual_results) == expected_results

    def test_snapshot_get_random(self, swh_storage, sample_data):
        snapshot, empty_snapshot, complete_snapshot = sample_data.snapshots[:3]
        swh_storage.snapshot_add([snapshot, empty_snapshot, complete_snapshot])

        assert swh_storage.snapshot_get_random() in {
            snapshot.id,
            empty_snapshot.id,
            complete_snapshot.id,
        }

    def test_snapshot_branch_get_by_name_missing_snapshot(self, swh_storage):
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=hash_to_bytes(
                "0e7f84ede9a254f2cd55649ad5240783f557e65f"
            ),  # non existing id
            branch_name=b"master",
        )
        assert branch is None

    def test_snapshot_branch_get_by_name_empty_snapshot(self, swh_storage, sample_data):
        snapshot = sample_data.snapshots[1]  # empty snapshot
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"master"
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=False, target=None, aliases_followed=[]
        )

    def test_snapshot_branch_get_by_name_missing_branch(self, swh_storage, sample_data):
        snapshot = sample_data.snapshots[0]
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"non-existing"
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=False, target=None, aliases_followed=[]
        )

    @pytest.mark.parametrize("branch", [b"directory", b"content", b"revision"])
    def test_snapshot_branch_get_by_name_direct_find(
        self, swh_storage, sample_data, branch
    ):
        snapshot = sample_data.snapshots[2]
        swh_storage.snapshot_add([snapshot])
        response_branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=branch
        )
        assert response_branch == SnapshotBranchByNameResponse(
            branch_found=True,
            target=snapshot.branches[branch],
            aliases_followed=[branch],
        )

    def test_snapshot_branch_get_by_name_dangling_branch(
        self, swh_storage, sample_data
    ):
        snapshot = sample_data.snapshots[2]
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"dangling"
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=True, target=None, aliases_followed=[b"dangling"]
        )

    def test_snapshot_branch_get_by_name_alias_chain(self, swh_storage, sample_data):
        snapshot = sample_data.snapshots[2]
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"alias"
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=True,
            target=snapshot.branches[b"revision"],
            aliases_followed=[b"alias", b"revision"],
        )

    def test_snapshot_branch_get_by_name_not_follow_alias_cahin(
        self, swh_storage, sample_data
    ):
        snapshot = sample_data.snapshots[2]
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"alias", follow_alias_chain=False
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=True,
            target=snapshot.branches[b"alias"],
            aliases_followed=[b"alias"],
        )

    def test_snapshot_branch_get_by_name_alias_chain_cycles(self, swh_storage):
        snapshot = Snapshot(
            id=hash_to_bytes("428893e6a864344e8be8e7bda6cb34fb1735a00e"),
            branches={
                b"HEAD1": SnapshotBranch(
                    target=b"HEAD2",
                    target_type=SnapshotTargetType.ALIAS,
                ),
                b"HEAD2": SnapshotBranch(
                    target=b"HEAD1",
                    target_type=SnapshotTargetType.ALIAS,
                ),
            },
        )
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"HEAD1"
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=True,
            target=None,
            aliases_followed=[b"HEAD1", b"HEAD2", b"HEAD1"],
        )

    def test_snapshot_branch_get_by_name_alias_chain_too_long(self, swh_storage):
        snapshot = Snapshot(
            id=hash_to_bytes("873893e6a864344e8be8e7bda6cb34fb1735a00e"),
            branches={
                b"first": SnapshotBranch(
                    target=b"second",
                    target_type=SnapshotTargetType.ALIAS,
                ),
                b"second": SnapshotBranch(
                    target=b"third",
                    target_type=SnapshotTargetType.ALIAS,
                ),
                b"third": SnapshotBranch(
                    target=b"forth",
                    target_type=SnapshotTargetType.ALIAS,
                ),
                b"forth": SnapshotBranch(
                    target=b"revision",
                    target_type=SnapshotTargetType.ALIAS,
                ),
            },
        )
        swh_storage.snapshot_add([snapshot])
        branch = swh_storage.snapshot_branch_get_by_name(
            snapshot_id=snapshot.id, branch_name=b"first", max_alias_chain_length=3
        )
        assert branch == SnapshotBranchByNameResponse(
            branch_found=True,
            target=None,
            aliases_followed=[b"first", b"second", b"third"],
        )

    def test_snapshot_missing(self, swh_storage, sample_data):
        snapshot, missing_snapshot = sample_data.snapshots[:2]
        snapshots = [snapshot.id, missing_snapshot.id]
        swh_storage.snapshot_add([snapshot])

        missing_snapshots = swh_storage.snapshot_missing(snapshots)

        assert list(missing_snapshots) == [missing_snapshot.id]

    def test_stat_counters(self, swh_storage, sample_data):
        if isinstance(swh_storage, CassandraStorage) and not isinstance(
            swh_storage, InMemoryStorage
        ):
            pytest.skip("Cassandra backend does not support stat counters")
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
        assert actually_present[0].ctime is not None
        assert actually_present[0].ctime.tzinfo is not None

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
            {
                "sha1_git": content.sha1_git,
                "type": "content",
            }
        ]

        swh_storage.directory_add([directory])
        sha1_gits.append(directory.id)
        expected[directory.id] = [
            {
                "sha1_git": directory.id,
                "type": "directory",
            }
        ]

        swh_storage.revision_add([revision])
        sha1_gits.append(revision.id)
        expected[revision.id] = [
            {
                "sha1_git": revision.id,
                "type": "revision",
            }
        ]

        swh_storage.release_add([release])
        sha1_gits.append(release.id)
        expected[release.id] = [
            {
                "sha1_git": release.id,
                "type": "release",
            }
        ]

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

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add(content_metadata)

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(), authority
        )
        assert result.next_page_token is None
        assert list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        ) == list(content_metadata)

        actual_objects = list(swh_storage.journal_writer.journal.objects)
        expected_objects = [
            ("metadata_authority", authority),
            ("metadata_fetcher", fetcher),
        ] + [("raw_extrinsic_metadata", item) for item in content_metadata]

        for obj in expected_objects:
            assert obj in actual_objects

    def test_content_metadata_add_duplicate(self, swh_storage, sample_data):
        """Duplicates should be silently ignored."""
        content = sample_data.content
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])
        swh_storage.raw_extrinsic_metadata_add([content_metadata2, content_metadata])

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(), authority
        )
        assert result.next_page_token is None

        expected_results = (content_metadata, content_metadata2)

        assert (
            tuple(
                sorted(
                    result.results,
                    key=lambda x: x.discovery_date,
                )
            )
            == expected_results
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

        content2_metadata = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(content1_metadata2.to_dict(), ("id",)),  # recompute id
                "target": str(content2.swhid()),
            }
        )

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
            content.swhid().to_extended(), authority
        )
        assert result.next_page_token is None
        assert [content1_metadata1, content1_metadata2] == list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(), authority2
        )
        assert result.next_page_token is None
        assert [content1_metadata3] == list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            content2.swhid().to_extended(), authority
        )
        assert result.next_page_token is None
        assert [content2_metadata] == list(
            result.results,
        )

    def test_content_metadata_get_after(self, swh_storage, sample_data):
        content = sample_data.content
        fetcher = sample_data.metadata_fetcher
        authority = sample_data.metadata_authority
        content_metadata, content_metadata2 = sample_data.content_metadata[:2]

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(),
            authority,
            after=content_metadata.discovery_date - timedelta(seconds=1),
        )
        assert result.next_page_token is None
        assert [content_metadata, content_metadata2] == list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(),
            authority,
            after=content_metadata.discovery_date,
        )
        assert result.next_page_token is None
        assert result.results == [content_metadata2]

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(),
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

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])
        swh_storage.raw_extrinsic_metadata_add([content_metadata, content_metadata2])
        swh_storage.raw_extrinsic_metadata_get(content.swhid().to_extended(), authority)

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(), authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [content_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(),
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

        swh_storage.metadata_fetcher_add([fetcher1, fetcher2])
        swh_storage.metadata_authority_add([authority])

        new_content_metadata2 = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(content_metadata2.to_dict(), ("id",)),  # recompute id
                "discovery_date": content_metadata2.discovery_date,
                "fetcher": attr.evolve(fetcher2, metadata=None).to_dict(),
            }
        )

        swh_storage.raw_extrinsic_metadata_add(
            [content_metadata, new_content_metadata2]
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(), authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [content_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            content.swhid().to_extended(),
            authority,
            limit=1,
            page_token=result.next_page_token,
        )
        assert result.next_page_token is None
        assert result.results[0].to_dict() == new_content_metadata2.to_dict()
        assert result.results == [new_content_metadata2]

    def test_content_metadata_get_by_ids(self, swh_storage, sample_data):
        content, content2 = sample_data.contents[:2]
        fetcher, fetcher2 = sample_data.fetchers[:2]
        authority, authority2 = sample_data.authorities[:2]
        (
            content1_metadata1,
            content1_metadata2,
            content1_metadata3,
        ) = sample_data.content_metadata[:3]

        content2_metadata = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(content1_metadata2.to_dict(), ("id",)),  # recompute id
                "target": str(content2.swhid()),
            }
        )

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

        assert set(
            swh_storage.raw_extrinsic_metadata_get_by_ids(
                [content1_metadata1.id, b"\x00" * 20, content2_metadata.id]
            )
        ) == {content1_metadata1, content2_metadata}

    def test_content_metadata_get_authorities(self, swh_storage, sample_data):
        content1, content2, content3 = sample_data.contents[:3]
        fetcher, fetcher2 = sample_data.fetchers[:2]
        authority, authority2 = sample_data.authorities[:2]
        (
            content1_metadata1,
            content1_metadata2,
            content1_metadata3,
        ) = sample_data.content_metadata[:3]

        content2_metadata = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(content1_metadata2.to_dict(), ("id",)),  # recompute id
                "target": str(content2.swhid()),
            }
        )

        content1_metadata2 = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(content1_metadata2.to_dict(), ("id",)),  # recompute id
                "authority": authority2.to_dict(),
            }
        )

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

        assert swh_storage.raw_extrinsic_metadata_get_authorities(
            content1.swhid().to_extended()
        ) in (
            [authority, authority2],
            [authority2, authority],
        )

        assert swh_storage.raw_extrinsic_metadata_get_authorities(
            content2.swhid().to_extended()
        ) == [authority]

        assert (
            swh_storage.raw_extrinsic_metadata_get_authorities(
                content3.swhid().to_extended()
            )
            == []
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
            Origin(origin.url).swhid(), authority
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

        swh_storage.metadata_fetcher_add([fetcher])
        swh_storage.metadata_authority_add([authority])

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])
        swh_storage.raw_extrinsic_metadata_add([origin_metadata2, origin_metadata])

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(), authority
        )
        assert result.next_page_token is None

        # which of the two behavior happens is backend-specific.
        expected_results = (origin_metadata, origin_metadata2)

        assert (
            tuple(
                sorted(
                    result.results,
                    key=lambda x: x.discovery_date,
                )
            )
            == expected_results
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

        origin2_metadata = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(origin1_metadata2.to_dict(), ("id",)),  # recompute id
                "target": str(Origin(origin2.url).swhid()),
            }
        )

        swh_storage.metadata_authority_add([authority, authority2])
        swh_storage.metadata_fetcher_add([fetcher, fetcher2])

        swh_storage.raw_extrinsic_metadata_add(
            [origin1_metadata1, origin1_metadata2, origin1_metadata3, origin2_metadata]
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(), authority
        )
        assert result.next_page_token is None
        assert [origin1_metadata1, origin1_metadata2] == list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(), authority2
        )
        assert result.next_page_token is None
        assert [origin1_metadata3] == list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        )

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin2.url).swhid(), authority
        )
        assert result.next_page_token is None
        assert [origin2_metadata] == list(
            result.results,
        )

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
            Origin(origin.url).swhid(),
            authority,
            after=origin_metadata.discovery_date - timedelta(seconds=1),
        )
        assert result.next_page_token is None
        assert list(
            sorted(
                result.results,
                key=lambda x: x.discovery_date,
            )
        ) == [
            origin_metadata,
            origin_metadata2,
        ]

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(),
            authority,
            after=origin_metadata.discovery_date,
        )
        assert result.next_page_token is None
        assert result.results == [origin_metadata2]

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(),
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

        swh_storage.raw_extrinsic_metadata_get(Origin(origin.url).swhid(), authority)

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(), authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [origin_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(),
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

        new_origin_metadata2 = RawExtrinsicMetadata.from_dict(
            {
                **remove_keys(origin_metadata2.to_dict(), ("id",)),  # recompute id
                "discovery_date": origin_metadata2.discovery_date,
                "fetcher": attr.evolve(fetcher2, metadata=None).to_dict(),
            }
        )

        swh_storage.raw_extrinsic_metadata_add([origin_metadata, new_origin_metadata2])

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(), authority, limit=1
        )
        assert result.next_page_token is not None
        assert result.results == [origin_metadata]

        result = swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(),
            authority,
            limit=1,
            page_token=result.next_page_token,
        )
        assert result.next_page_token is None
        assert result.results == [new_origin_metadata2]

    def test_origin_metadata_missing_authority(self, swh_storage, sample_data):
        origin = sample_data.origin
        fetcher = sample_data.metadata_fetcher
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_fetcher_add([fetcher])

        with pytest.raises(UnknownMetadataAuthority):
            swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

        assert swh_storage.raw_extrinsic_metadata_get(
            Origin(origin.url).swhid(), origin_metadata.authority
        ) == PagedResult(results=[], next_page_token=None)

    def test_origin_metadata_missing_fetcher(self, swh_storage, sample_data):
        origin = sample_data.origin
        authority = sample_data.metadata_authority
        origin_metadata, origin_metadata2 = sample_data.origin_metadata[:2]
        assert swh_storage.origin_add([origin]) == {"origin:add": 1}

        swh_storage.metadata_authority_add([authority])

        with pytest.raises(UnknownMetadataFetcher):
            swh_storage.raw_extrinsic_metadata_add([origin_metadata, origin_metadata2])

    def test_object_references_add_find(self, swh_storage):
        for src_type, target_types in (
            ("ori", ("snp",)),
            ("snp", ("snp", "rel", "rev", "dir", "cnt")),
            ("rel", ("rel", "rev", "dir", "cnt")),
            ("rev", ("rev", "dir", "cnt")),
            ("dir", ("dir", "cnt")),
        ):
            for target_type in target_types:
                src_obj_id = (
                    MultiHash(["sha1"])
                    .from_data(f"{src_type}-{target_type}".encode())
                    .hexdigest()["sha1"]
                )
                source = ExtendedSWHID.from_string(f"swh:1:{src_type}:{src_obj_id}")
                targets = [
                    ExtendedSWHID.from_string(
                        f"swh:1:{target_type}:{src_obj_id[0:38]}{i:02x}"
                    )
                    for i in range(20)
                ]

                for target in targets:
                    refs = swh_storage.object_find_recent_references(
                        target_swhid=target, limit=10
                    )
                    assert refs == []

                recorded = swh_storage.object_references_add(
                    [
                        ObjectReference(source=source, target=target)
                        for target in targets
                    ]
                )

                assert recorded["object_reference:add"] == len(targets)

                for target in targets:
                    refs = swh_storage.object_find_recent_references(
                        target_swhid=target, limit=10
                    )
                    assert refs == [source]

    def test_object_references_add_duplicate(self, swh_storage):
        """Ensure adding the same reference twice in the same call does not crash"""
        source = ExtendedSWHID.from_string(f"swh:1:dir:{0:040x}")
        target = ExtendedSWHID.from_string(f"swh:1:cnt:{1:040x}")

        assert swh_storage.object_references_add(
            [
                ObjectReference(source=source, target=target),
                ObjectReference(source=source, target=target),
            ]
        ) == {"object_reference:add": 1}

    def test_object_references_add_twice(self, swh_storage):
        """Ensure adding the same reference twice does not crash"""
        source = ExtendedSWHID.from_string(f"swh:1:dir:{0:040x}")
        target = ExtendedSWHID.from_string(f"swh:1:cnt:{1:040x}")

        assert swh_storage.object_references_add(
            [ObjectReference(source=source, target=target)]
        ) == {"object_reference:add": 1}

        assert swh_storage.object_references_add(
            [ObjectReference(source=source, target=target)]
        ) == {"object_reference:add": 1}

    def test_querytimeout(self, swh_storage, sample_data, mocker):
        origin_url = "https://example.org/"

        message = "too slow!"
        mocker.patch(
            "swh.storage.postgresql.db.Db.origin_visit_get_latest",
            side_effect=psycopg2.errors.QueryCanceled(message),
        )
        mocker.patch(
            "swh.storage.postgresql.db.Db.revision_missing_from_list",
            side_effect=psycopg2.errors.QueryCanceled(message),
        )
        mocker.patch(
            "swh.storage.cassandra.cql.CqlRunner._execute_with_retries_inner",
            side_effect=cassandra.ReadTimeout(message),
        )
        mocker.patch(
            "swh.storage.cassandra.cql.CqlRunner._execute_many_with_retries_inner",
            side_effect=cassandra.ReadTimeout(message),
        )

        # db_transaction on postgres, _execute_with_retries on cassandra
        with pytest.raises(QueryTimeout, match=message):
            swh_storage.origin_visit_get_latest(origin_url, require_snapshot=True)

        # db_transaction_generator on postgres, _execute_many_with_retries on cassandra
        with pytest.raises(QueryTimeout, match=message):
            list(swh_storage.revision_missing([b"\x00" * 20]))


class TestStorageGeneratedData:
    def test_generate_content_get_data(self, swh_storage, swh_contents):
        contents_with_data = [c for c in swh_contents if c.status != "absent"]

        # retrieve contents
        for content in contents_with_data:
            actual_content_data = swh_storage.content_get_data({"sha1": content.sha1})
            assert actual_content_data is not None
            assert actual_content_data == content.data

    def test_generate_content_get(self, swh_storage, swh_contents):
        expected_contents = [
            attr.evolve(c, data=None) for c in swh_contents if c.status != "absent"
        ]

        actual_contents = swh_storage.content_get([c.sha1 for c in expected_contents])

        assert len(actual_contents) == len(expected_contents)
        assert actual_contents == expected_contents

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
        visit = OriginVisit(
            origin=origin_url,
            date=now(),
            type="git",
        )
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
        visit = OriginVisit(
            origin=origin_url,
            date=now(),
            type="git",
        )
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

    @settings(
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large]
        + disabled_health_checks,
    )
    @given(
        strategies.lists(hypothesis_strategies.objects(split_content=True), max_size=2)
    )
    def test_add_arbitrary(self, swh_storage, objects):
        # Deduplicate based on ids if any
        objects = list(
            {obj.id: obj for obj in objects if hasattr(obj, "id")}.values()
        ) + [obj for obj in objects if not hasattr(obj, "id")]
        random.shuffle(objects)

        for obj_type, obj in objects:
            if obj_type == ModelObjectType.ORIGIN_VISIT:
                swh_storage.origin_add([Origin(url=obj.origin)])
                visit = OriginVisit(
                    origin=obj.origin,
                    date=obj.date,
                    type=obj.type,
                )
                swh_storage.origin_visit_add([visit])
            elif obj_type == ModelObjectType.RAW_EXTRINSIC_METADATA:
                swh_storage.metadata_authority_add([obj.authority])
                swh_storage.metadata_fetcher_add([obj.fetcher])
                swh_storage.raw_extrinsic_metadata_add([obj])
            else:
                method = getattr(swh_storage, f"{obj_type}_add")
                try:
                    method([obj])
                except HashCollision:
                    pass
