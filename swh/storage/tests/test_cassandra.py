# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import itertools
import os
import resource
import signal
import socket
import subprocess
import time
from typing import Any, Dict

import attr
from cassandra.cluster import NoHostAvailable
import pytest

from swh.core.api.classes import stream_results
from swh.model import from_disk
from swh.model.model import Directory, DirectoryEntry, Snapshot, SnapshotBranch
from swh.storage import get_storage
from swh.storage.cassandra import create_keyspace
from swh.storage.cassandra.cql import BATCH_INSERT_MAX_SIZE
from swh.storage.cassandra.model import ContentRow, ExtIDRow
from swh.storage.cassandra.schema import HASH_ALGORITHMS, TABLES
from swh.storage.cassandra.storage import DIRECTORY_ENTRIES_INSERT_ALGOS
from swh.storage.tests.storage_data import StorageData
from swh.storage.tests.storage_tests import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)
from swh.storage.tests.storage_tests import TestStorage as _TestStorage
from swh.storage.utils import now, remove_keys

CONFIG_TEMPLATE = """
data_file_directories:
    - {data_dir}/data
commitlog_directory: {data_dir}/commitlog
hints_directory: {data_dir}/hints
saved_caches_directory: {data_dir}/saved_caches

commitlog_sync: periodic
commitlog_sync_period_in_ms: 1000000
partitioner: org.apache.cassandra.dht.Murmur3Partitioner
endpoint_snitch: SimpleSnitch
seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          - seeds: "127.0.0.1"

storage_port: {storage_port}
native_transport_port: {native_transport_port}
start_native_transport: true
listen_address: 127.0.0.1

enable_user_defined_functions: true

# speed-up by disabling period saving to disk
key_cache_save_period: 0
row_cache_save_period: 0
trickle_fsync: false
commitlog_sync_period_in_ms: 100000
"""

SCYLLA_EXTRA_CONFIG_TEMPLATE = """
experimental_features:
    - udf
view_hints_directory: {data_dir}/view_hints
prometheus_port: 0  # disable prometheus server
start_rpc: false  # disable thrift server
api_port: {api_port}
"""


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for_peer(addr, port):
    wait_until = time.time() + 60
    while time.time() < wait_until:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
        except ConnectionRefusedError:
            time.sleep(0.1)
        else:
            sock.close()
            return True
    return False


@pytest.fixture(scope="session")
def cassandra_cluster(tmpdir_factory):
    cassandra_conf = tmpdir_factory.mktemp("cassandra_conf")
    cassandra_data = tmpdir_factory.mktemp("cassandra_data")
    cassandra_log = tmpdir_factory.mktemp("cassandra_log")
    native_transport_port = free_port()
    storage_port = free_port()
    jmx_port = free_port()
    api_port = free_port()

    use_scylla = bool(os.environ.get("SWH_USE_SCYLLADB", ""))

    cassandra_bin = os.environ.get(
        "SWH_CASSANDRA_BIN", "/usr/bin/scylla" if use_scylla else "/usr/sbin/cassandra"
    )

    if use_scylla:
        os.makedirs(cassandra_conf.join("conf"))
        config_path = cassandra_conf.join("conf/scylla.yaml")
        config_template = CONFIG_TEMPLATE + SCYLLA_EXTRA_CONFIG_TEMPLATE
    else:
        config_path = cassandra_conf.join("cassandra.yaml")
        config_template = CONFIG_TEMPLATE

    with open(str(config_path), "w") as fd:
        fd.write(
            config_template.format(
                data_dir=str(cassandra_data),
                storage_port=storage_port,
                native_transport_port=native_transport_port,
                api_port=api_port,
            )
        )

    if os.environ.get("SWH_CASSANDRA_LOG"):
        stdout = stderr = None
    else:
        stdout = stderr = subprocess.DEVNULL

    env = {
        "MAX_HEAP_SIZE": "300M",
        "HEAP_NEWSIZE": "50M",
        "JVM_OPTS": "-Xlog:gc=error:file=%s/gc.log" % cassandra_log,
    }
    if "JAVA_HOME" in os.environ:
        env["JAVA_HOME"] = os.environ["JAVA_HOME"]

    if use_scylla:
        env = {
            **env,
            "SCYLLA_HOME": cassandra_conf,
        }
        # prevent "NOFILE rlimit too low (recommended setting 200000,
        # minimum setting 10000; refusing to start."
        resource.setrlimit(resource.RLIMIT_NOFILE, (200000, 200000))

        proc = subprocess.Popen(
            [
                cassandra_bin,
                "--developer-mode=1",
            ],
            start_new_session=True,
            env=env,
            stdout=stdout,
            stderr=stderr,
        )
    else:
        proc = subprocess.Popen(
            [
                cassandra_bin,
                "-Dcassandra.config=file://%s/cassandra.yaml" % cassandra_conf,
                "-Dcassandra.logdir=%s" % cassandra_log,
                "-Dcassandra.jmx.local.port=%d" % jmx_port,
                "-Dcassandra-foreground=yes",
            ],
            start_new_session=True,
            env=env,
            stdout=stdout,
            stderr=stderr,
        )

    listening = wait_for_peer("127.0.0.1", native_transport_port)

    if listening:
        yield (["127.0.0.1"], native_transport_port)

    if not listening or os.environ.get("SWH_CASSANDRA_LOG"):
        debug_log_path = str(cassandra_log.join("debug.log"))
        if os.path.exists(debug_log_path):
            with open(debug_log_path) as fd:
                print(fd.read())

    if not listening:
        if proc.poll() is None:
            raise Exception("cassandra process unexpectedly not listening.")
        else:
            raise Exception("cassandra process unexpectedly stopped.")

    pgrp = os.getpgid(proc.pid)
    os.killpg(pgrp, signal.SIGKILL)


class RequestHandler:
    def on_request(self, rf):
        if hasattr(rf.message, "query"):
            print()
            print(rf.message.query)


@pytest.fixture(scope="session")
def keyspace(cassandra_cluster):
    (hosts, port) = cassandra_cluster
    keyspace = os.urandom(10).hex()

    create_keyspace(hosts, keyspace, port)

    return keyspace


# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below


@pytest.fixture
def swh_storage_backend_config(cassandra_cluster, keyspace):
    (hosts, port) = cassandra_cluster

    storage_config = dict(
        cls="cassandra",
        hosts=hosts,
        port=port,
        keyspace=keyspace,
        journal_writer={"cls": "memory"},
        objstorage={"cls": "memory"},
    )

    yield storage_config

    storage = get_storage(**storage_config)

    for table in TABLES:
        storage._cql_runner._session.execute('TRUNCATE TABLE "%s"' % table)

    storage._cql_runner._cluster.shutdown()


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
        self, swh_storage, mocker, sample_data
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

        # This should have written some of the entries to the database:
        entry_rows = swh_storage._cql_runner.directory_entry_get([directory.id])
        assert {row.name for row in entry_rows} == {entry.name for entry in entries}

        # BUT, because not all the entries were written, the directory should
        # be considered not written.
        assert swh_storage.directory_missing([directory.id]) == [directory.id]
        assert list(swh_storage.directory_ls(directory.id)) == []
        assert swh_storage.directory_get_entries(directory.id) is None

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
