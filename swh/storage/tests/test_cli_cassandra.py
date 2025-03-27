# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
import os

from cassandra import InvalidRequest
import pytest

from swh.model.model import Origin
from swh.storage import get_storage
from swh.storage.cassandra.migrations import MIGRATIONS, Migration

logger = logging.getLogger(__name__)


@pytest.fixture
def swh_storage_backend_config(swh_storage_cassandra_backend_config):
    return swh_storage_cassandra_backend_config


@pytest.fixture
def invoke(swh_storage_cassandra_backend_config):
    from .test_cli import invoke

    @functools.wraps(invoke)
    def newf(*args, **kwargs):
        assert "local_config" not in kwargs
        return invoke(
            *args,
            local_config={"storage": swh_storage_cassandra_backend_config},
            **kwargs,
        )

    return newf


@pytest.mark.cassandra
class TestCassandraCli:
    def test_init(
        self,
        swh_storage_cassandra_cluster,
        cassandra_auth_provider_config,
    ):
        # not using the the invoke fixture because it uses the keyspace that is already
        # initialized, which would make this test pointless
        from .test_cli import invoke

        (hosts, port) = swh_storage_cassandra_cluster
        keyspace = "test" + os.urandom(10).hex()

        storage_config = dict(
            cls="cassandra",
            hosts=hosts,
            port=port,
            keyspace=keyspace,
            journal_writer={"cls": "memory"},
            objstorage={"cls": "memory"},
            auth_provider=cassandra_auth_provider_config,
        )

        result = invoke("cassandra", "init", local_config={"storage": storage_config})
        assert result.exit_code == 0, result.output
        assert result.output == "Done.\n"

        # Check we can write and read to it
        storage = get_storage(**storage_config)
        origin = Origin(url="http://example.org")
        storage.origin_add([origin])
        assert storage.origin_get([origin.url]) == [origin]

    def test_list_migrations(self, swh_storage, invoke, mocker):
        # keep only the first migration
        mocker.patch("swh.storage.cassandra.migrations.MIGRATIONS", MIGRATIONS[0:1])

        result = invoke("cassandra", "list-migrations")
        assert result.exit_code == 0, result.output
        assert result.output == (
            "2024-12-12_init (required): completed\n"
            "    Dummy migration that represents the database schema as of v2.9.0\n"
            "\n"
        )

    def test_list_migrations_pending(self, swh_storage, invoke, mocker):
        new_migration = Migration(
            id="2024-12-19_test_migration",
            dependencies=set(),
            min_read_version="2.9.0",
            script=None,
            help="Test migration",
            required=False,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS",
            [MIGRATIONS[0], new_migration],
        )

        result = invoke("cassandra", "list-migrations")
        assert result.exit_code == 0, result.output
        assert result.output == (
            "2024-12-12_init (required): completed\n"
            "    Dummy migration that represents the database schema as of v2.9.0\n"
            "\n"
            "2024-12-19_test_migration: pending\n"
            "    Test migration\n"
            "\n"
        )

    def test_upgrade_all(self, swh_storage, invoke, mocker):
        def migration_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table (abc blob PRIMARY KEY);", []
            )

        new_migration = Migration(
            id="2024-12-19_test_migration",
            dependencies=set(),
            min_read_version="2.9.0",
            script=migration_script,
            help="Test migration",
            required=False,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS", [*MIGRATIONS, new_migration]
        )

        try:
            # create the table
            result = invoke("cassandra", "upgrade")
            assert result.exit_code == 0, result.output
            assert result.output == "Done.\n"

            # check the table exists
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table", []
            )
        finally:
            swh_storage._cql_runner.execute_with_retries(
                f"DROP TABLE {swh_storage.keyspace}.test_table", []
            )

    def test_upgrade_all_from_v2_9(self, swh_storage, invoke, mocker):
        """Tests upgrading from v2.9.x, which did not have a 'migrations' table."""
        assert len(MIGRATIONS) == 1, (
            "This test won't work correctly after we make more changes to the schema, "
            "as it relies on the schema being v2.9's plus only the migrations table."
        )
        cql_runner = swh_storage._cql_runner

        cql_runner.execute_with_retries(
            f"DROP TABLE {cql_runner.keyspace}.migration", []
        )

        def migration_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table (abc blob PRIMARY KEY);", []
            )

        new_migration = Migration(
            id="2024-12-19_test_migration",
            dependencies=set(),
            min_read_version="2.9.0",
            script=migration_script,
            help="Test migration",
            required=False,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS", [*MIGRATIONS, new_migration]
        )

        try:
            # create the table
            result = invoke("cassandra", "upgrade")
            assert result.exit_code == 0, result.output
            assert result.output == "Done.\n"

            # check the table exists
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table", []
            )
        finally:
            swh_storage._cql_runner.execute_with_retries(
                f"DROP TABLE {swh_storage.keyspace}.test_table", []
            )

    def test_upgrade_crashing(self, swh_storage, invoke, mocker):
        class TestException(Exception):
            pass

        def migration_script(cql_runner):
            raise TestException("Oh no, a crash!")

        new_migration = Migration(
            id="2024-12-19_test_migration",
            dependencies=set(),
            min_read_version="2.9.0",
            script=migration_script,
            help="Test migration",
            required=False,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS",
            [MIGRATIONS[0], new_migration],
        )

        # run the buggy migration
        result = invoke("cassandra", "upgrade")
        assert result.exit_code == 1, result.output
        assert result.output == ""  # raised an exception, traceback on stderr

        result = invoke("cassandra", "list-migrations")
        assert result.exit_code == 0, result.output
        assert result.output == (
            "2024-12-12_init (required): completed\n"
            "    Dummy migration that represents the database schema as of v2.9.0\n"
            "\n"
            "2024-12-19_test_migration: running\n"
            "    Test migration\n"
            "\n"
        )

    def test_upgrade_partial(self, swh_storage, invoke, mocker):
        def create_test_table_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table (abc blob PRIMARY KEY);", []
            )

        def drop_test_table_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries("DROP TABLE test_table;", [])

        new_migration1 = Migration(
            id="2024-12-19_create_test_table",
            dependencies=set(),
            min_read_version="2.9.0",
            script=create_test_table_script,
            help="Test migration",
            required=False,
        )
        new_migration2 = Migration(
            id="2024-12-19_drop_test_table",
            dependencies={"2024-12-19_create_test_table"},
            min_read_version="2.9.0",
            script=drop_test_table_script,
            help="Test migration",
            required=False,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS",
            [*MIGRATIONS, new_migration1, new_migration2],
        )

        # create the table
        result = invoke(
            "cassandra", "upgrade", "--migration", "2024-12-19_create_test_table"
        )
        assert result.exit_code == 0, result.output
        assert result.output == "Done.\n"

        # check the table exists
        swh_storage._cql_runner.execute_with_retries(
            f"SELECT * FROM {swh_storage.keyspace}.test_table", []
        )

        # drop the table
        result = invoke(
            "cassandra", "upgrade", "--migration", "2024-12-19_drop_test_table"
        )
        assert result.exit_code == 0, result.output
        assert result.output == "Done.\n"

        # check the table does not exist anymore
        with pytest.raises(InvalidRequest):
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table", []
            )

    @pytest.mark.parametrize("required", [True, False])
    def test_upgrade_manual(self, swh_storage, invoke, mocker, required):
        """Tries to apply a migration that cannot run automatically"""
        new_migration1 = Migration(
            id="2025-03-18_manual1",
            dependencies=set(),
            min_read_version="2.9.0",
            script=None,
            help="Test migration",
            required=False,
        )
        new_migration2 = Migration(
            id="2025-03-18_manual2",
            dependencies=set(),
            min_read_version="2.9.0",
            script=None,
            help="Test migration",
            required=required,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS",
            [*MIGRATIONS, new_migration1, new_migration2],
        )

        # run migration before its dependency
        result = invoke(
            "cassandra",
            "upgrade",
            "--migration",
            "2025-03-18_manual1",
            "--migration",
            "2025-03-18_manual2",
        )
        if required:
            assert result.exit_code == 4, result.output
            assert result.output == (
                "Some migrations need to be manually applied: "
                "2025-03-18_manual1, 2025-03-18_manual2\n"
                "Including these required migrations: "
                "2025-03-18_manual2\n"
            )
        else:
            assert result.exit_code == 5, result.output
            assert result.output == (
                "Some migrations need to be manually applied: "
                "2025-03-18_manual1, 2025-03-18_manual2\n"
            )

        # check the migration was not marked as applied
        result = invoke("cassandra", "list-migrations")
        assert result.exit_code == 0, result.output
        assert result.output == (
            "2024-12-12_init (required): completed\n"
            "    Dummy migration that represents the database schema as of v2.9.0\n"
            "\n"
            "2025-03-18_manual1: running\n"
            "    Test migration\n"
            "\n"
            f"2025-03-18_manual2{' (required)' if required else ''}: running\n"
            "    Test migration\n"
            "\n"
        )

        # check the tables still do not exist
        with pytest.raises(InvalidRequest):
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table1", []
            )
        with pytest.raises(InvalidRequest):
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table2", []
            )

    @pytest.mark.parametrize(
        "migration1_manual,migration2_required",
        [
            pytest.param(True, True, id="migration1=manual,migration2=required"),
            pytest.param(True, False, id="migration1=manual,migration2=optional"),
            pytest.param(False, True, id="migration1=scripted,migration2=required"),
            pytest.param(False, False, id="migration1=scripted,migration2=optional"),
        ],
    )
    def test_upgrade_disordered(
        self,
        swh_storage,
        invoke,
        mocker,
        migration1_manual,
        migration2_required,
    ):
        """Tries to apply a migration before its dependency"""

        def create_test_table1_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table1 (abc blob PRIMARY KEY);", []
            )

        def create_test_table2_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table1 (abc blob PRIMARY KEY);", []
            )

        new_migration1 = Migration(
            id="2024-12-19_create_test_table1",
            dependencies=set(),
            min_read_version="2.9.0",
            script=None if migration1_manual else create_test_table1_script,
            help="Test migration",
            required=False,
        )
        new_migration2 = Migration(
            id="2024-12-19_create_test_table2",
            dependencies={"2024-12-19_create_test_table1"},
            min_read_version="2.9.0",
            script=create_test_table2_script,
            help="Test migration",
            required=migration2_required,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS",
            [*MIGRATIONS, new_migration1, new_migration2],
        )

        # run migration before its dependency
        result = invoke(
            "cassandra", "upgrade", "--migration", "2024-12-19_create_test_table2"
        )
        if migration2_required:
            assert result.exit_code == 6, result.output
            assert result.output == (
                "Some migrations could not be applied because a dependency is missing: "
                "2024-12-19_create_test_table2\n"
                "Including these required migrations: "
                "2024-12-19_create_test_table2\n"
            )
        else:
            assert result.exit_code == 7, result.output
            assert result.output == (
                "Some migrations could not be applied because a dependency is missing: "
                "2024-12-19_create_test_table2\n"
            )

        # check the migration was not marked as applied
        result = invoke("cassandra", "list-migrations")
        assert result.exit_code == 0, result.output
        assert result.output == (
            "2024-12-12_init (required): completed\n"
            "    Dummy migration that represents the database schema as of v2.9.0\n"
            "\n"
            "2024-12-19_create_test_table1: pending\n"
            "    Test migration\n"
            "\n"
            "2024-12-19_create_test_table2"
            f"{' (required)' if migration2_required else ''}: pending\n"
            "    Test migration\n"
            "\n"
        )

        # check the tables still do not exist
        with pytest.raises(InvalidRequest):
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table1", []
            )
        with pytest.raises(InvalidRequest):
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table2", []
            )

    def test_mark_upgraded(self, swh_storage, invoke, mocker):
        def create_test_table1_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table1 (abc blob PRIMARY KEY);", []
            )

        def create_test_table2_script(cql_runner):
            cql_runner.execute_with_retries(f"USE {cql_runner.keyspace}", [])
            cql_runner.execute_with_retries(
                "CREATE TABLE IF NOT EXISTS test_table2 (abc blob PRIMARY KEY);", []
            )

        new_migration1 = Migration(
            id="2024-12-19_create_test_table1",
            dependencies=set(),
            min_read_version="2.9.0",
            script=create_test_table1_script,
            help="Test migration",
            required=False,
        )
        new_migration2 = Migration(
            id="2024-12-19_create_test_table2",
            dependencies={"2024-12-19_create_test_table1"},
            min_read_version="2.9.0",
            script=create_test_table2_script,
            help="Test migration",
            required=False,
        )
        mocker.patch(
            "swh.storage.cassandra.migrations.MIGRATIONS",
            [*MIGRATIONS, new_migration1, new_migration2],
        )
        # Pretend the first migration was applied
        result = invoke(
            "cassandra", "mark-upgraded", "--migration", "2024-12-19_create_test_table1"
        )
        assert result.exit_code == 0, result.output
        assert (
            result.output
            == "Migrations 2024-12-19_create_test_table1 marked as complete.\n"
        )

        # run dependent migration
        try:
            result = invoke(
                "cassandra", "upgrade", "--migration", "2024-12-19_create_test_table2"
            )
            assert result.exit_code == 0, result.output
            assert result.output == "Done.\n"

            # check the first table still does not exist
            with pytest.raises(InvalidRequest):
                swh_storage._cql_runner.execute_with_retries(
                    f"SELECT * FROM {swh_storage.keyspace}.test_table1", []
                )

            # check the second table now exists
            swh_storage._cql_runner.execute_with_retries(
                f"SELECT * FROM {swh_storage.keyspace}.test_table2", []
            )
        finally:
            try:
                swh_storage._cql_runner.execute_with_retries(
                    f"DROP TABLE {swh_storage.keyspace}.test_table2", []
                )
            except BaseException:
                pass
