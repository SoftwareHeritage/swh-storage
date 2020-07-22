# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob

from os import path, environ
from typing import Union

import pytest

import swh.storage

from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor, psycopg2, Version

from swh.core.utils import numfile_sortkey as sortkey
from swh.storage import get_storage

from swh.storage.tests.storage_data import StorageData


SQL_DIR = path.join(path.dirname(swh.storage.__file__), "sql")

environ["LC_ALL"] = "C.UTF-8"

DUMP_FILES = path.join(SQL_DIR, "*.sql")


@pytest.fixture
def swh_storage_backend_config(postgresql_proc, swh_storage_postgresql):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    yield {
        "cls": "local",
        "db": "postgresql://{user}@{host}:{port}/{dbname}".format(
            host=postgresql_proc.host,
            port=postgresql_proc.port,
            user="postgres",
            dbname="tests",
        ),
        "objstorage": {"cls": "memory", "args": {}},
    }


@pytest.fixture
def swh_storage(swh_storage_backend_config):
    return get_storage(**swh_storage_backend_config)


# the postgres_fact factory fixture below is mostly a copy of the code
# from pytest-postgresql. We need a custom version here to be able to
# specify our version of the DBJanitor we use.
def postgresql_fact(process_fixture_name, db_name=None, dump_files=DUMP_FILES):
    @pytest.fixture
    def postgresql_factory(request):
        """
        Fixture factory for PostgreSQL.

        :param FixtureRequest request: fixture request object
        :rtype: psycopg2.connection
        :returns: postgresql client
        """
        config = factories.get_config(request)
        if not psycopg2:
            raise ImportError("No module named psycopg2. Please install it.")
        proc_fixture = request.getfixturevalue(process_fixture_name)

        # _, config = try_import('psycopg2', request)
        pg_host = proc_fixture.host
        pg_port = proc_fixture.port
        pg_user = proc_fixture.user
        pg_options = proc_fixture.options
        pg_db = db_name or config["dbname"]
        with SwhDatabaseJanitor(
            pg_user,
            pg_host,
            pg_port,
            pg_db,
            proc_fixture.version,
            dump_files=dump_files,
        ):
            connection = psycopg2.connect(
                dbname=pg_db,
                user=pg_user,
                host=pg_host,
                port=pg_port,
                options=pg_options,
            )
            yield connection
            connection.close()

    return postgresql_factory


swh_storage_postgresql = postgresql_fact("postgresql_proc")


# This version of the DatabaseJanitor implement a different setup/teardown
# behavior than than the stock one: instead of dropping, creating and
# initializing the database for each test, it create and initialize the db only
# once, then it truncate the tables. This is needed to have acceptable test
# performances.
class SwhDatabaseJanitor(DatabaseJanitor):
    def __init__(
        self,
        user: str,
        host: str,
        port: str,
        db_name: str,
        version: Union[str, float, Version],
        dump_files: str = DUMP_FILES,
    ) -> None:
        super().__init__(user, host, port, db_name, version)
        self.dump_files = sorted(glob.glob(dump_files), key=sortkey)

    def db_setup(self):
        with psycopg2.connect(
            dbname=self.db_name, user=self.user, host=self.host, port=self.port,
        ) as cnx:
            with cnx.cursor() as cur:
                for fname in self.dump_files:
                    with open(fname) as fobj:
                        sql = fobj.read().replace("concurrently", "").strip()
                        if sql:
                            cur.execute(sql)
            cnx.commit()

    def db_reset(self):
        with psycopg2.connect(
            dbname=self.db_name, user=self.user, host=self.host, port=self.port,
        ) as cnx:
            with cnx.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s",
                    ("public",),
                )
                tables = set(table for (table,) in cur.fetchall())
                for table in tables:
                    cur.execute("truncate table %s cascade" % table)

                cur.execute(
                    "SELECT sequence_name FROM information_schema.sequences "
                    "WHERE sequence_schema = %s",
                    ("public",),
                )
                seqs = set(seq for (seq,) in cur.fetchall())
                for seq in seqs:
                    cur.execute("ALTER SEQUENCE %s RESTART;" % seq)
            cnx.commit()

    def init(self):
        with self.cursor() as cur:
            cur.execute(
                "SELECT COUNT(1) FROM pg_database WHERE datname=%s;", (self.db_name,)
            )
            db_exists = cur.fetchone()[0] == 1
            if db_exists:
                cur.execute(
                    "UPDATE pg_database SET datallowconn=true " "WHERE datname = %s;",
                    (self.db_name,),
                )

        if db_exists:
            self.db_reset()
        else:
            with self.cursor() as cur:
                cur.execute('CREATE DATABASE "{}";'.format(self.db_name))
            self.db_setup()

    def drop(self):
        pid_column = "pid"
        with self.cursor() as cur:
            cur.execute(
                "UPDATE pg_database SET datallowconn=false " "WHERE datname = %s;",
                (self.db_name,),
            )
            cur.execute(
                "SELECT pg_terminate_backend(pg_stat_activity.{})"
                "FROM pg_stat_activity "
                "WHERE pg_stat_activity.datname = %s;".format(pid_column),
                (self.db_name,),
            )


@pytest.fixture
def sample_data() -> StorageData:
    """Pre-defined sample storage object data to manipulate

    Returns:
        StorageData whose attribute keys are data model objects. Either multiple
        objects: contents, directories, revisions, releases, ... or simple ones:
        content, directory, revision, release, ...

    """
    return StorageData()
