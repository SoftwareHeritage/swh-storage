# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from functools import partial
import os
import resource
import signal
import socket
import subprocess
import time
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import pytest
from pytest_postgresql import factories
from pytest_shared_session_scope import (
    CleanupToken,
    SetupToken,
    shared_session_scope_json,
)

from swh.core.db.db_utils import initialize_database_for_module
from swh.storage import get_storage
from swh.storage.postgresql.storage import Storage as StorageDatastore
from swh.storage.tests.storage_data import StorageData

os.environ["LC_ALL"] = "C.UTF-8"


_CASSANDRA_CONFIG_TEMPLATE = """
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
authenticator: PasswordAuthenticator
"""

_SCYLLA_EXTRA_CONFIG_TEMPLATE = """
experimental_features:
    - udf
view_hints_directory: {data_dir}/view_hints
prometheus_port: 0  # disable prometheus server
start_rpc: false  # disable thrift server
api_port: {api_port}
"""


def _free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _wait_for_peer(addr, port):
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
def cassandra_auth_provider_config():
    return {
        "cls": "cassandra.auth.PlainTextAuthProvider",
        "username": "cassandra",
        "password": "cassandra",
    }


@pytest.fixture(scope="session")
def session_uuid():
    return os.environ.get("PYTEST_XDIST_TESTRUNUID", str(uuid.uuid4))


@shared_session_scope_json(
    params=[
        pytest.param("", marks=pytest.mark.cassandra),
    ]
)
def swh_storage_cassandra_cluster(tmpdir_factory, tmp_path_factory, session_uuid):
    cassandra_conf = tmpdir_factory.mktemp("cassandra_conf")
    cassandra_data = tmpdir_factory.mktemp("cassandra_data")
    cassandra_log = tmpdir_factory.mktemp("cassandra_log")
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    proc = None

    data = yield

    if data == SetupToken.FIRST:
        # first pytest-xdist worker to execute that session scope fixture
        # spawns the cassandra process

        native_transport_port = _free_port()
        storage_port = _free_port()
        jmx_port = _free_port()
        api_port = _free_port()

        use_scylla = bool(os.environ.get("SWH_USE_SCYLLADB", ""))

        cassandra_bin = os.environ.get(
            "SWH_CASSANDRA_BIN",
            "/usr/bin/scylla" if use_scylla else "/usr/sbin/cassandra",
        )

        if use_scylla:
            os.makedirs(cassandra_conf.join("conf"))
            config_path = cassandra_conf.join("conf/scylla.yaml")
            config_template = _CASSANDRA_CONFIG_TEMPLATE + _SCYLLA_EXTRA_CONFIG_TEMPLATE
        else:
            config_path = cassandra_conf.join("cassandra.yaml")
            config_template = _CASSANDRA_CONFIG_TEMPLATE

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
            "CASSANDRA_LOG_DIR": cassandra_log,
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

        listening = _wait_for_peer("127.0.0.1", native_transport_port)

        if listening:
            # Wait for initialization
            auth_provider = PlainTextAuthProvider(
                username="cassandra", password="cassandra"
            )
            with Cluster(
                ["127.0.0.1"],
                port=native_transport_port,
                auth_provider=auth_provider,
                connect_timeout=30,
                control_connection_timeout=30,
            ) as cluster:

                session = None
                retry = 0
                while (not session) and retry < 10:
                    try:
                        session = cluster.connect()
                    except Exception:
                        time.sleep(1)
                        retry += 1

        data = (["127.0.0.1"], native_transport_port)

    token: CleanupToken = yield data

    if token == CleanupToken.LAST:
        # last pytest-xdist worker tearing down that session scope fixture
        # informs the worker that spawned cassandra it can be shutdowned
        (root_tmp_dir / session_uuid).touch()

    if proc is not None:
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

        while not (root_tmp_dir / session_uuid).exists():
            # wait until all pytest-xdist workers executed their test suites
            time.sleep(1)

        # kill cassandra process
        pgrp = os.getpgid(proc.pid)
        os.killpg(pgrp, signal.SIGKILL)


@pytest.fixture(scope="session")
def swh_storage_cassandra_keyspace(
    swh_storage_cassandra_cluster, cassandra_auth_provider_config
):
    from swh.storage.cassandra import create_keyspace
    from swh.storage.cassandra.cql import CqlRunner

    (hosts, port) = swh_storage_cassandra_cluster
    keyspace = "test" + os.urandom(10).hex()

    cql_runner = CqlRunner(
        hosts,
        keyspace,
        port,
        auth_provider=cassandra_auth_provider_config,
        consistency_level="ONE",
        register_user_types=False,
    )

    create_keyspace(cql_runner)

    cql_runner._cluster.shutdown()

    return keyspace


@pytest.fixture
def swh_storage_cassandra_backend_config(
    swh_storage_cassandra_cluster,
    swh_storage_cassandra_keyspace,
    cassandra_auth_provider_config,
):
    from swh.storage.cassandra.cql import CqlRunner, mark_all_migrations_completed

    (hosts, port) = swh_storage_cassandra_cluster

    keyspace = swh_storage_cassandra_keyspace

    storage_config = dict(
        cls="cassandra",
        hosts=hosts,
        port=port,
        keyspace=keyspace,
        journal_writer={"cls": "memory"},
        objstorage={"cls": "memory"},
        auth_provider=cassandra_auth_provider_config,
    )

    cql_runner = CqlRunner(
        hosts,
        keyspace,
        port,
        auth_provider=cassandra_auth_provider_config,
        consistency_level="ONE",
    )

    mark_all_migrations_completed(cql_runner)

    yield storage_config

    cql_runner._cluster.shutdown()


swh_storage_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="storage",
            version=StorageDatastore.current_version,
        ),
    ],
)


swh_storage_postgresql = factories.postgresql(
    "swh_storage_postgresql_proc",
)


@pytest.fixture
def swh_storage_postgresql_backend_config(swh_storage_postgresql):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    yield {
        "cls": "postgresql",
        "db": swh_storage_postgresql.info.dsn,
        "objstorage": {"cls": "memory"},
        "check_config": {"check_write": True},
        "max_pool_conns": 100,
    }


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql_backend_config):
    """Configuration to use for :func:`swh_storage_backend`.

    Defaults to :func:`swh_storage_postgresql_backend_config`.

    """
    return swh_storage_postgresql_backend_config


@pytest.fixture
def swh_storage_backend(swh_storage_backend_config):
    """
    By default, this fixture aliases ``swh_storage``. However, when ``swh_storage``
    is overridden to be a proxy storage, this fixture returns the storage instance
    behind all proxies.

    This is useful to introspect the state of backends from proxy tests"""
    storage = get_storage(**swh_storage_backend_config)

    backend = storage

    # handle storage pipeline for backward-compatibility as
    # object_references_create_partition is only available on
    # real storage backend, not on proxies.
    while hasattr(backend, "storage"):
        backend = backend.storage

    backend.object_references_create_partition(
        *datetime.date.today().isocalendar()[0:2]
    )

    yield storage

    if hasattr(backend, "_cql_runner") and hasattr(backend._cql_runner, "_cluster"):
        from swh.storage.cassandra.schema import TABLES

        for partition in backend.object_references_list_partitions():
            backend.object_references_drop_partition(partition)

        keyspace = backend._keyspace
        for table in TABLES:
            table_rows = backend._cql_runner.execute_with_retries(
                f"SELECT * from {keyspace}.{table} LIMIT 1", args=[]
            )
            if table_rows.one() is not None:
                backend._cql_runner.execute_with_retries(
                    f"TRUNCATE TABLE {keyspace}.{table}", args=[]
                )
        backend._cql_runner._cluster.shutdown()
    if hasattr(backend, "_pool"):
        backend._pool.close()


@pytest.fixture
def swh_storage(swh_storage_backend):
    return swh_storage_backend


@pytest.fixture
def sample_data() -> StorageData:
    """Pre-defined sample storage object data to manipulate

    Returns:
        StorageData whose attribute keys are data model objects. Either multiple
        objects: contents, directories, revisions, releases, ... or simple ones:
        content, directory, revision, release, ...

    """
    return StorageData()
