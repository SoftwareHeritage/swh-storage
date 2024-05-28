# Copyright (C) 2018-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dataclasses
import datetime
import itertools
import re
from typing import Any, Dict, Union

import attr
from cassandra.cluster import NoHostAvailable
import pytest

from swh.core.api.classes import stream_results
from swh.model import from_disk
from swh.model.model import (
    Directory,
    DirectoryEntry,
    ExtID,
    Person,
    Snapshot,
    SnapshotBranch,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID, ObjectType
from swh.storage import get_storage
from swh.storage.cassandra.cql import BATCH_INSERT_MAX_SIZE
import swh.storage.cassandra.model
from swh.storage.cassandra.model import BaseRow, ContentRow, ExtIDRow
from swh.storage.cassandra.schema import CREATE_TABLES_QUERIES, HASH_ALGORITHMS, TABLES
from swh.storage.cassandra.storage import (
    DIRECTORY_ENTRIES_INSERT_ALGOS,
    CassandraStorage,
)
from swh.storage.tests.storage_data import StorageData
from swh.storage.tests.storage_tests import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)
from swh.storage.tests.storage_tests import TestStorage as _TestStorage
from swh.storage.utils import now, remove_keys


@pytest.fixture
def swh_storage_backend_config(swh_storage_cassandra_backend_config):
    return swh_storage_cassandra_backend_config


def _python_type_to_cql_type_re(ty: type) -> str:
    if ty is bytes:
        return "blob"
    elif ty is str:
        return "(ascii|text)"
    elif ty is int:
        return "(small||big)int"
    elif ty is datetime.datetime:
        return "timestamp"
    elif ty is TimestampWithTimezone:
        return "microtimestamp_with_timezone"
    elif ty is Person:
        return "person"
    elif ty is bool:
        return "boolean"
    elif ty is dict:
        return "frozen<list <list<blob>> >"
    elif getattr(ty, "__origin__", None) is Union:
        if len(ty.__args__) == 2 and type(None) in ty.__args__:  # type: ignore
            (inner_type,) = [
                arg
                for arg in ty.__args__  # type: ignore
                if arg is not type(None)  # noqa
            ]
            # This is Optional[inner_type]. CQL does not support NOT NULL,
            # so the corresponding type is:
            return _python_type_to_cql_type_re(inner_type)

    assert False, f"Unsupported type: {ty}"


@pytest.mark.cassandra
def test_schema_matches_model():
    """Checks tables defined in :mod:`swh.storage.cassandra.schema` match
    the object model defined in :mod:`swh.storage.cassandra.model`.
    """

    models = {}  # {table_name: cls}
    for obj_name in dir(swh.storage.cassandra.model):
        cls = getattr(swh.storage.cassandra.model, obj_name)
        if isinstance(cls, type) and issubclass(cls, BaseRow) and hasattr(cls, "TABLE"):
            assert cls.TABLE not in models, f"Duplicate model for table {cls.TABLE}"
            models[cls.TABLE] = cls

    del models["object_count"]  # https://forge.softwareheritage.org/D6150

    statements = {}  # {table_name: statement}
    for statement in CREATE_TABLES_QUERIES:
        statement = re.sub(" +", " ", statement.strip())  # normalize whitespace
        if statement.startswith(
            (
                "CREATE TYPE ",
                "CREATE OR REPLACE FUNCTION",
                "CREATE OR REPLACE AGGREGATE",
                "-- Secondary table",
            )
        ):
            continue
        prefix = "CREATE TABLE IF NOT EXISTS "
        assert statement.startswith(prefix)
        table_name = statement[len(prefix) :].split(" ", 1)[0]
        assert (
            table_name not in statements
        ), f"Duplicate statement for table {table_name}"

        statements[table_name] = statement

    assert set(models) - set(statements) == set(), "Missing statements"
    assert set(statements) - set(models) == set(), "Missing models"

    mismatches = []
    for table_name in list(models):
        # Compute what we expect the statement to look like:
        model = models[table_name]
        expected_lines = [rf"CREATE TABLE IF NOT EXISTS {model.TABLE} \("]
        for field in dataclasses.fields(model):
            expected_lines.append(
                f" {field.name} {_python_type_to_cql_type_re(field.type)},"
            )

        partition_key = ", ".join(model.PARTITION_KEY)
        clustering_key = "".join(", " + col for col in model.CLUSTERING_KEY)
        expected_lines.append(rf" PRIMARY KEY \(\({partition_key}\){clustering_key}\)")

        if table_name == "origin_visit_status":
            # we need to special-case this one
            expected_lines.append(r"\)")
            expected_lines.append(r"WITH CLUSTERING ORDER BY \(visit DESC, date DESC\)")
            expected_lines.append(r";")
        else:
            expected_lines.append(r"\);")

        statement = statements[table_name]
        actual_lines = statement.split("\n")

        mismatches.extend(
            (table_name, expected_line, actual_line)
            for (expected_line, actual_line) in zip(expected_lines, actual_lines)
            if not re.match(expected_line, actual_line)
        )

    assert not mismatches, "\n" + "\n".join(
        f"{table_name}:\t{actual_line!r} did not match {expected_line!r}"
        for (table_name, expected_line, actual_line) in mismatches
    )


# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below


@pytest.mark.cassandra
class TestCassandraStorage(_TestStorage):
    def test_config_wrong_consistency_should_raise(self):
        storage_config = dict(
            cls="cassandra",
            hosts=["first"],
            port=9999,
            keyspace="any",
            consistency_level="fake",
            journal_writer={"cls": "memory"},
            objstorage={"cls": "memory"},
        )

        with pytest.raises(ValueError, match="Unknown consistency"):
            get_storage(**storage_config)

    def test_config_consistency_used(self, swh_storage_backend_config):
        config_with_consistency = dict(
            swh_storage_backend_config, **{"consistency_level": "THREE"}
        )

        storage = get_storage(**config_with_consistency)

        with pytest.raises(NoHostAvailable):
            storage.content_get_random()

    def test_no_authentication(self, swh_storage_backend_config):
        config_without_authentication = swh_storage_backend_config.copy()
        config_without_authentication.pop("auth_provider")

        with pytest.raises(NoHostAvailable):
            get_storage(**config_without_authentication)

    def test_no_authenticator_class(self, swh_storage_backend_config):
        config_without_authentication = swh_storage_backend_config.copy()
        authenticator_config = config_without_authentication.pop("auth_provider").copy()
        authenticator_config.pop("cls")
        config_without_authentication["auth_provider"] = authenticator_config

        with pytest.raises(ValueError, match="cls property"):
            get_storage(**config_without_authentication)

    def test_content_add_murmur3_collision(self, swh_storage, mocker, sample_data):
        """The Murmur3 token is used as link from index tables to the main
        table; and non-matching contents with colliding murmur3-hash
        are filtered-out when reading the main table.
        This test checks the content methods do filter out these collision.
        """
        called = 0

        cont, cont2 = sample_data.contents[:2]

        # always return a token
        def mock_cgtfsa(algo, hashes):
            nonlocal called
            called += 1
            assert algo in ("sha1", "sha1_git")
            return [123456]

        mocker.patch.object(
            swh_storage._cql_runner,
            "content_get_tokens_from_single_algo",
            mock_cgtfsa,
        )

        # For all tokens, always return cont
        def mock_cgft(tokens):
            nonlocal called
            called += 1
            return [
                ContentRow(
                    length=10,
                    ctime=datetime.datetime.now(),
                    status="present",
                    **{algo: getattr(cont, algo) for algo in HASH_ALGORITHMS},
                )
            ]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_from_tokens", mock_cgft
        )

        actual_result = swh_storage.content_add([cont2])

        assert called == 4
        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": cont2.length,
        }

    def test_content_get_metadata_murmur3_collision(
        self, swh_storage, mocker, sample_data
    ):
        """The Murmur3 token is used as link from index tables to the main
        table; and non-matching contents with colliding murmur3-hash
        are filtered-out when reading the main table.
        This test checks the content methods do filter out these collisions.
        """
        called = 0

        cont, cont2 = [attr.evolve(c, ctime=now()) for c in sample_data.contents[:2]]

        # always return a token
        def mock_cgtfsa(algo, hashes):
            nonlocal called
            called += 1
            assert algo in ("sha1", "sha1_git")
            return [123456]

        mocker.patch.object(
            swh_storage._cql_runner,
            "content_get_tokens_from_single_algo",
            mock_cgtfsa,
        )

        # For all tokens, always return cont and cont2
        cols = list(set(cont.to_dict()) - {"data"})

        def mock_cgft(tokens):
            nonlocal called
            called += 1
            return [
                ContentRow(
                    **{col: getattr(cont, col) for col in cols},
                )
                for cont in [cont, cont2]
            ]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_from_tokens", mock_cgft
        )

        actual_result = swh_storage.content_get([cont.sha1])
        assert called == 2

        # dropping extra column not returned
        expected_cont = attr.evolve(cont, data=None)

        # but cont2 should be filtered out
        assert actual_result == [expected_cont]

    def test_content_find_murmur3_collision(self, swh_storage, mocker, sample_data):
        """The Murmur3 token is used as link from index tables to the main
        table; and non-matching contents with colliding murmur3-hash
        are filtered-out when reading the main table.
        This test checks the content methods do filter out these collisions.
        """
        called = 0

        cont, cont2 = [attr.evolve(c, ctime=now()) for c in sample_data.contents[:2]]

        # always return a token
        def mock_cgtfsa(algo, hashes):
            nonlocal called
            called += 1
            assert algo in ("sha1", "sha1_git")
            return [123456]

        mocker.patch.object(
            swh_storage._cql_runner,
            "content_get_tokens_from_single_algo",
            mock_cgtfsa,
        )

        # For all tokens, always return cont and cont2
        cols = list(set(cont.to_dict()) - {"data"})

        def mock_cgft(tokens):
            nonlocal called
            called += 1
            return [
                ContentRow(**{col: getattr(cont, col) for col in cols})
                for cont in [cont, cont2]
            ]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_from_tokens", mock_cgft
        )

        expected_content = attr.evolve(cont, data=None)

        actual_result = swh_storage.content_find({"sha1": cont.sha1})

        assert called == 2

        # but cont2 should be filtered out
        assert actual_result == [expected_content]

    def test_content_get_partition_murmur3_collision(
        self,
        swh_storage: CassandraStorage,
        mocker,
        sample_data,
    ):
        """The Murmur3 token is used as link from index tables to the main table; and
        non-matching contents with colliding murmur3-hash are filtered-out when reading
        the main table.

        This test checks the content_get_partition endpoints return all contents, even
        the collisions.

        """
        called = 0

        rows: Dict[int, Dict] = {}
        for tok, content in enumerate(sample_data.contents):
            cont = attr.evolve(content, data=None, ctime=now())
            row_d = {**cont.to_dict(), "tok": tok}
            rows[tok] = row_d

        # For all tokens, always return cont

        def mock_content_get_token_range(range_start, range_end, limit):
            nonlocal called
            called += 1

            for tok in list(rows.keys()) * 3:  # yield multiple times the same tok
                row_d = dict(rows[tok].items())
                row_d.pop("tok")
                yield (tok, ContentRow(**row_d))

        mocker.patch.object(
            swh_storage._cql_runner,
            "content_get_token_range",
            mock_content_get_token_range,
        )

        actual_results = list(
            stream_results(
                swh_storage.content_get_partition, partition_id=0, nb_partitions=1
            )
        )

        assert called > 0

        # everything is listed, even collisions
        assert len(actual_results) == 3 * len(sample_data.contents)
        # as we duplicated the returned results, dropping duplicate should yield
        # the original length
        assert len(set(actual_results)) == len(sample_data.contents)

    @pytest.mark.skip("content_update is not yet implemented for Cassandra")
    def test_content_update(self):
        pass

    def test_extid_murmur3_collision(self, swh_storage, mocker, sample_data):
        """The Murmur3 token is used as link from index table to the main
        table; and non-matching extid with colliding murmur3-hash
        are filtered-out when reading the main table.
        This test checks the extid methods do filter out these collision.
        """
        swh_storage.extid_add(sample_data.extids)

        # For any token, always return all extids, i.e. make as if all tokens
        # for all extid entries collide
        def mock_egft(token):
            return [
                ExtIDRow(
                    extid_type=extid.extid_type,
                    extid=extid.extid,
                    extid_version=extid.extid_version,
                    target_type=extid.target.object_type.value,
                    target=extid.target.object_id,
                )
                for extid in sample_data.extids
            ]

        mocker.patch.object(
            swh_storage._cql_runner,
            "extid_get_from_token",
            mock_egft,
        )

        for extid in sample_data.extids:
            extids = swh_storage.extid_get_from_target(
                target_type=extid.target.object_type, ids=[extid.target.object_id]
            )
            assert extids == [extid]

    def _directory_with_entries(self, sample_data, nb_entries):
        """Returns a dir with ``nb_entries``, all pointing to
        the same content"""
        return Directory(
            entries=tuple(
                DirectoryEntry(
                    name=f"file{i:10}".encode(),
                    type="file",
                    target=sample_data.content.sha1_git,
                    perms=from_disk.DentryPerms.directory,
                )
                for i in range(nb_entries)
            )
        )

    @pytest.mark.parametrize(
        "insert_algo,nb_entries",
        [
            ("one-by-one", 10),
            ("concurrent", 10),
            ("batch", 1),
            ("batch", 2),
            ("batch", BATCH_INSERT_MAX_SIZE - 1),
            ("batch", BATCH_INSERT_MAX_SIZE),
            ("batch", BATCH_INSERT_MAX_SIZE + 1),
            ("batch", BATCH_INSERT_MAX_SIZE * 2),
        ],
    )
    def test_directory_add_algos(
        self,
        swh_storage,
        sample_data,
        mocker,
        insert_algo,
        nb_entries,
    ):
        mocker.patch.object(swh_storage, "_directory_entries_insert_algo", insert_algo)

        class new_sample_data:
            content = sample_data.content
            directory = self._directory_with_entries(sample_data, nb_entries)

        self.test_directory_add(swh_storage, new_sample_data)

    @pytest.mark.parametrize("insert_algo", DIRECTORY_ENTRIES_INSERT_ALGOS)
    def test_directory_add_atomic(self, swh_storage, sample_data, mocker, insert_algo):
        """Checks that a crash occurring after some directory entries were written
        does not cause the directory to be (partially) visible.
        ie. checks directories are added somewhat atomically."""
        # Disable the journal writer, it would detect the CrashyEntry exception too
        # early for this test to be relevant
        swh_storage.journal_writer.journal = None
        mocker.patch.object(swh_storage, "_directory_entries_insert_algo", insert_algo)

        class CrashyEntry(DirectoryEntry):
            def __init__(self):
                super().__init__(**{**directory.entries[0].to_dict(), "name": b"crash"})

            def to_dict(self):
                return {**super().to_dict(), "perms": "abcde"}

        directory = self._directory_with_entries(sample_data, BATCH_INSERT_MAX_SIZE)
        entries = directory.entries
        directory = attr.evolve(directory, entries=entries + (CrashyEntry(),))

        with pytest.raises(TypeError):
            swh_storage.directory_add([directory])

        # Usually, this writes all entries but the crashy one in the database;
        # let's check this. (If this assertion fails, then the test is useless;
        # but it does not affect the actual functionality)
        # However, because they are inserted simultaneously, the backend may crash
        # before the last handful of entries; so we allow them to be missing
        # without failing the test (which would make it flaky).
        entry_rows = swh_storage._cql_runner.directory_entry_get([directory.id])
        assert (
            {entry.name for entry in entries[0:-100]}
            <= {row.name for row in entry_rows}
            <= {entry.name for entry in entries}
        )

        # BUT, because not all the entries were written, the directory should
        # be considered not written.
        assert swh_storage.directory_missing([directory.id]) == [directory.id]
        assert list(swh_storage.directory_ls(directory.id)) == []
        assert swh_storage.directory_get_entries(directory.id) is None

    def test_directory_add_raw_manifest__different_entries__allow_overwrite(
        self, swh_storage
    ):
        """This test demonstrates a shortcoming of the Cassandra storage backend's
        design:

        1. add a directory with an entry named "name1" and raw_manifest="abc"
        2. add a directory with an entry named "name2" and the same raw_manifest
        3. the directories' id is computed only from the raw_manifest, so both
           directories have the same id, which causes their entries to be
           "additive" in the database; so directory_ls returns both entries

        However, by default, the Cassandra storage has allow_overwrite=False,
        which "accidentally" avoids this issue most of the time, by skipping insertion
        if an object with the same id is already in the database.

        This can still be an issue when either allow_overwrite=True or when inserting
        both directories at about the same time (because of the lack of
        transactionality); but the likelihood of two clients inserting two different
        objects with the same manifest at the same time is very low, it could only
        happen if loaders running in parallel used different (or nondeterministic)
        parsers on corrupt objects.
        """
        assert (
            swh_storage._allow_overwrite is False
        ), "Unexpected default _allow_overwrite value"
        swh_storage._allow_overwrite = True

        # Run the other test, but skip its last assertion
        dir_id = self.test_directory_add_raw_manifest__different_entries(
            swh_storage, check_ls=False
        )
        assert [entry["name"] for entry in swh_storage.directory_ls(dir_id)] == [
            b"name1",
            b"name2",
        ]

    def test_snapshot_add_atomic(self, swh_storage, sample_data, mocker):
        """Checks that a crash occurring after some snapshot branches were written
        does not cause the snapshot to be (partially) visible.
        ie. checks snapshots are added somewhat atomically."""
        # Disable the journal writer, it would detect the CrashyBranch exception too
        # early for this test to be relevant
        swh_storage.journal_writer.journal = None

        class MyException(Exception):
            pass

        class CrashyBranch(SnapshotBranch):
            def __getattribute__(self, name):
                if name == "target" and should_raise:
                    raise MyException()
                else:
                    return super().__getattribute__(name)

        snapshot = sample_data.complete_snapshot
        branches = snapshot.branches

        should_raise = False  # just so that we can construct the object
        crashy_branch = CrashyBranch.from_dict(branches[b"directory"].to_dict())
        should_raise = True

        snapshot = attr.evolve(
            snapshot,
            branches={
                **branches,
                b"crashy": crashy_branch,
            },
        )

        with pytest.raises(MyException):
            swh_storage.snapshot_add([snapshot])

        # This should have written some of the branches to the database:
        branch_rows = swh_storage._cql_runner.snapshot_branch_get(snapshot.id, b"", 10)
        assert {row.name for row in branch_rows} == set(branches)

        # BUT, because not all the branches were written, the snapshot should
        # be considered not written.
        assert swh_storage.snapshot_missing([snapshot.id]) == [snapshot.id]
        assert swh_storage.snapshot_get(snapshot.id) is None
        assert swh_storage.snapshot_count_branches(snapshot.id) is None
        assert swh_storage.snapshot_get_branches(snapshot.id) is None

    @pytest.mark.skip(
        'The "person" table of the pgsql is a legacy thing, and not '
        "supported by the cassandra backend."
    )
    def test_person_fullname_unicity(self):
        pass

    @pytest.mark.skip(
        'The "person" table of the pgsql is a legacy thing, and not '
        "supported by the cassandra backend."
    )
    def test_person_get(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count(self):
        pass

    def test_object_delete(self, swh_storage, sample_data):
        # For our sanity checks
        affected_tables = set(TABLES) - {
            "metadata_authority",
            "metadata_fetcher",
            "extid",
            "extid_by_target",
            "object_references",
        }
        # XXX: Not ideal…
        cql_runner = getattr(swh_storage, "_cql_runner")
        execute_query = getattr(cql_runner, "_execute_with_retries")

        swh_storage.content_add(sample_data.contents)
        swh_storage.skipped_content_add(sample_data.skipped_contents)
        swh_storage.directory_add(sample_data.directories)
        swh_storage.revision_add(sample_data.revisions)
        swh_storage.release_add(sample_data.releases)
        swh_storage.snapshot_add(sample_data.snapshots)
        swh_storage.origin_add(sample_data.origins)
        swh_storage.origin_visit_add(sample_data.origin_visits)
        swh_storage.origin_visit_status_add(sample_data.origin_visit_statuses)
        swh_storage.metadata_authority_add(sample_data.authorities)
        swh_storage.metadata_fetcher_add(sample_data.fetchers)
        swh_storage.raw_extrinsic_metadata_add(sample_data.content_metadata)
        swh_storage.raw_extrinsic_metadata_add(sample_data.origin_metadata)
        swhids = (
            [content.swhid().to_extended() for content in sample_data.contents]
            + [
                skipped_content.swhid().to_extended()
                for skipped_content in sample_data.skipped_contents
            ]
            + [directory.swhid().to_extended() for directory in sample_data.directories]
            + [revision.swhid().to_extended() for revision in sample_data.revisions]
            + [release.swhid().to_extended() for release in sample_data.releases]
            + [snapshot.swhid().to_extended() for snapshot in sample_data.snapshots]
            + [origin.swhid() for origin in sample_data.origins]
            + [emd.swhid() for emd in sample_data.content_metadata]
            + [emd.swhid() for emd in sample_data.origin_metadata]
        )

        # Do we have something in every affected tables?
        for table in affected_tables:
            row = execute_query(
                f"SELECT COUNT(*) AS count FROM {cql_runner.keyspace}.{table}", []
            ).one()
            assert row["count"] >= 1, f"nothing in table {table}"

        result = swh_storage.object_delete(swhids)
        assert result == {
            "content:delete": 3,
            "content:delete:bytes": 0,
            "skipped_content:delete": 2,
            "directory:delete": 7,
            "revision:delete": 8,
            "release:delete": 3,
            "snapshot:delete": 3,
            "origin:delete": 7,
            "origin_visit:delete": 3,
            "origin_visit_status:delete": 3,
            "cnt_metadata:delete": 3,
            "ori_metadata:delete": 3,
        }

        # Have we cleaned every affected tables?
        for table in affected_tables:
            row = execute_query(
                f"SELECT COUNT(*) AS count FROM {cql_runner.keyspace}.{table}", []
            ).one()
            assert row["count"] == 0, f"something in table {table}"

    def test_extid_delete_for_target(self, swh_storage, sample_data):
        swh_storage.revision_add([sample_data.revision, sample_data.hg_revision])
        swh_storage.directory_add([sample_data.directory, sample_data.directory2])
        extid_for_same_target = ExtID(
            target=CoreSWHID(
                object_type=ObjectType.REVISION, object_id=sample_data.revision.id
            ),
            extid_type="drink_some",
            extid=bytes.fromhex("c0ffee"),
        )
        result = swh_storage.extid_add(sample_data.extids + (extid_for_same_target,))
        assert result == {"extid:add": 5}

        result = swh_storage.extid_delete_for_target(
            [sample_data.revision.swhid(), sample_data.directory2.swhid()]
        )
        assert result == {"extid:delete": 3}

        extids = swh_storage.extid_get_from_target(
            target_type=ObjectType.REVISION, ids=[sample_data.hg_revision.id]
        )
        assert extids == [sample_data.extid2]
        extids = swh_storage.extid_get_from_target(
            target_type=ObjectType.DIRECTORY, ids=[sample_data.directory.id]
        )
        assert extids == [sample_data.extid3]

        result = swh_storage.extid_delete_for_target(
            [sample_data.hg_revision.swhid(), sample_data.directory.swhid()]
        )
        assert result == {"extid:delete": 2}


@pytest.mark.cassandra
class TestCassandraStorageGeneratedData(_TestStorageGeneratedData):
    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_no_visits(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_with_visits_and_snapshot(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_with_visits_no_snapshot(self):
        pass


@pytest.mark.cassandra
@pytest.mark.parametrize(
    "allow_overwrite,object_type",
    itertools.product(
        [False, True],
        # Note the absence of "content", it's tested above.
        ["directory", "revision", "release", "snapshot", "origin", "extid"],
    ),
)
def test_allow_overwrite(
    allow_overwrite: bool, object_type: str, swh_storage_backend_config
):
    if object_type in ("origin", "extid"):
        pytest.skip(
            f"test_disallow_overwrite not implemented for {object_type} objects, "
            f"because all their columns are in the primary key."
        )
    swh_storage = get_storage(
        allow_overwrite=allow_overwrite, **swh_storage_backend_config
    )

    # directory_ls joins with content and directory table, and needs those to return
    # non-None entries:
    if object_type == "directory":
        swh_storage.directory_add([StorageData.directory5])
        swh_storage.content_add([StorageData.content, StorageData.content2])

    obj1: Any
    obj2: Any

    # Get two test objects
    if object_type == "directory":
        (obj1, obj2, *_) = StorageData.directories
    elif object_type == "snapshot":
        # StorageData.snapshots[1] is the empty snapshot, which is the corner case
        # that makes this test succeed for the wrong reasons
        obj1 = StorageData.snapshot
        obj2 = StorageData.complete_snapshot
    else:
        (obj1, obj2, *_) = getattr(StorageData, (object_type + "s"))

    # Let's make both objects have the same hash, but different content
    obj1 = attr.evolve(obj1, id=obj2.id)

    # Get the methods used to add and get these objects
    add = getattr(swh_storage, object_type + "_add")
    if object_type == "directory":

        def get(ids):
            return [
                Directory(
                    id=ids[0],
                    entries=tuple(
                        map(
                            lambda entry: DirectoryEntry(
                                name=entry["name"],
                                type=entry["type"],
                                target=entry["sha1_git"],
                                perms=entry["perms"],
                            ),
                            swh_storage.directory_ls(ids[0]),
                        )
                    ),
                )
            ]

    elif object_type == "snapshot":

        def get(ids):
            return [
                Snapshot.from_dict(
                    remove_keys(swh_storage.snapshot_get(ids[0]), ("next_branch",))
                )
            ]

    else:
        get = getattr(swh_storage, object_type + "_get")

    # Add the first object
    add([obj1])

    # It should be returned as-is
    assert get([obj1.id]) == [obj1]

    # Add the second object
    add([obj2])

    if allow_overwrite:
        # obj1 was overwritten by obj2
        expected = obj2
    else:
        # obj2 was not written, because obj1 already exists and has the same hash
        expected = obj1

    if allow_overwrite and object_type in ("directory", "snapshot"):
        # TODO
        pytest.xfail(
            "directory entries and snapshot branches are concatenated "
            "instead of being replaced"
        )
    assert get([obj1.id]) == [expected]
