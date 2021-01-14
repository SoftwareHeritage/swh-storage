# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import signal
import socket
import subprocess
import time
from typing import Dict

import attr
import pytest

from swh.core.api.classes import stream_results
from swh.storage import get_storage
from swh.storage.cassandra import create_keyspace
from swh.storage.cassandra.model import ContentRow
from swh.storage.cassandra.schema import HASH_ALGORITHMS, TABLES
from swh.storage.tests.storage_tests import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)
from swh.storage.tests.storage_tests import TestStorage as _TestStorage
from swh.storage.utils import now

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


def free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for_peer(addr, port):
    wait_until = time.time() + 20
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

    with open(str(cassandra_conf.join("cassandra.yaml")), "w") as fd:
        fd.write(
            CONFIG_TEMPLATE.format(
                data_dir=str(cassandra_data),
                storage_port=storage_port,
                native_transport_port=native_transport_port,
            )
        )

    if os.environ.get("SWH_CASSANDRA_LOG"):
        stdout = stderr = None
    else:
        stdout = stderr = subprocess.DEVNULL

    cassandra_bin = os.environ.get("SWH_CASSANDRA_BIN", "/usr/sbin/cassandra")
    env = {
        "MAX_HEAP_SIZE": "300M",
        "HEAP_NEWSIZE": "50M",
        "JVM_OPTS": "-Xlog:gc=error:file=%s/gc.log" % cassandra_log,
    }
    if "JAVA_HOME" in os.environ:
        env["JAVA_HOME"] = os.environ["JAVA_HOME"]

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

    running = wait_for_peer("127.0.0.1", native_transport_port)

    if running:
        yield (["127.0.0.1"], native_transport_port)

    if not running or os.environ.get("SWH_CASSANDRA_LOG"):
        debug_log_path = str(cassandra_log.join("debug.log"))
        if os.path.exists(debug_log_path):
            with open(debug_log_path) as fd:
                print(fd.read())

    if not running:
        raise Exception("cassandra process stopped unexpectedly.")

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
    def test_content_add_murmur3_collision(self, swh_storage, mocker, sample_data):
        """The Murmur3 token is used as link from index tables to the main
        table; and non-matching contents with colliding murmur3-hash
        are filtered-out when reading the main table.
        This test checks the content methods do filter out these collision.
        """
        called = 0

        cont, cont2 = sample_data.contents[:2]

        # always return a token
        def mock_cgtfsh(algo, hash_):
            nonlocal called
            called += 1
            assert algo in ("sha1", "sha1_git")
            return [123456]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_tokens_from_single_hash", mock_cgtfsh,
        )

        # For all tokens, always return cont
        def mock_cgft(token):
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
            swh_storage._cql_runner, "content_get_from_token", mock_cgft
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
        def mock_cgtfsh(algo, hash_):
            nonlocal called
            called += 1
            assert algo in ("sha1", "sha1_git")
            return [123456]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_tokens_from_single_hash", mock_cgtfsh,
        )

        # For all tokens, always return cont and cont2
        cols = list(set(cont.to_dict()) - {"data"})

        def mock_cgft(token):
            nonlocal called
            called += 1
            return [
                ContentRow(**{col: getattr(cont, col) for col in cols},)
                for cont in [cont, cont2]
            ]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_from_token", mock_cgft
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
        def mock_cgtfsh(algo, hash_):
            nonlocal called
            called += 1
            assert algo in ("sha1", "sha1_git")
            return [123456]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_tokens_from_single_hash", mock_cgtfsh,
        )

        # For all tokens, always return cont and cont2
        cols = list(set(cont.to_dict()) - {"data"})

        def mock_cgft(token):
            nonlocal called
            called += 1
            return [
                ContentRow(**{col: getattr(cont, col) for col in cols})
                for cont in [cont, cont2]
            ]

        mocker.patch.object(
            swh_storage._cql_runner, "content_get_from_token", mock_cgft
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
