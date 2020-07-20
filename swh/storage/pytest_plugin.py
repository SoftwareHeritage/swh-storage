# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob

from os import path, environ
from typing import Dict, Iterable, Union

import pytest

import swh.storage

from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor, psycopg2, Version

from swh.core.utils import numfile_sortkey as sortkey
from swh.model.model import (
    BaseModel,
    Content,
    Directory,
    MetadataAuthority,
    MetadataFetcher,
    Origin,
    OriginVisit,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)
from swh.storage import get_storage
from swh.storage.tests.storage_data import data


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
def sample_data() -> Dict:
    """Pre-defined sample storage object data to manipulate

    Returns:
        Dict of data (keys: content, directory, revision, release, person,
        origin)

    """
    return {
        "content": [data.cont, data.cont2],
        "content_no_data": [data.cont3],
        "skipped_content": [data.skipped_cont, data.skipped_cont2],
        "person": [data.person],
        "directory": [data.dir2, data.dir, data.dir3, data.dir4],
        "revision": [data.revision, data.revision2, data.revision3, data.revision4],
        "release": [data.release, data.release2, data.release3],
        "snapshot": [data.snapshot, data.empty_snapshot, data.complete_snapshot],
        "origin": [data.origin, data.origin2],
        "origin_visit": [data.origin_visit, data.origin_visit2, data.origin_visit3],
        "fetcher": [
            m.to_dict() for m in [data.metadata_fetcher, data.metadata_fetcher2]
        ],
        "authority": [
            m.to_dict() for m in [data.metadata_authority, data.metadata_authority2]
        ],
        "origin_metadata": [
            m.to_dict() for m in [data.origin_metadata, data.origin_metadata2,]
        ],
        "content_metadata": [
            m.to_dict()
            for m in [
                data.content_metadata,
                data.content_metadata2,
                data.content_metadata3,
            ]
        ],
    }


# FIXME: Add the metadata keys when we can (right now, we cannot as the data model
# changed but not the endpoints yet)
OBJECT_FACTORY = {
    "content": Content.from_dict,
    "content_no_data": Content.from_dict,
    "skipped_content": SkippedContent.from_dict,
    "person": Person.from_dict,
    "directory": Directory.from_dict,
    "revision": Revision.from_dict,
    "release": Release.from_dict,
    "snapshot": Snapshot.from_dict,
    "origin": Origin.from_dict,
    "origin_visit": OriginVisit.from_dict,
    "fetcher": MetadataFetcher.from_dict,
    "authority": MetadataAuthority.from_dict,
    "origin_metadata": RawExtrinsicMetadata.from_dict,
    "content_metadata": RawExtrinsicMetadata.from_dict,
}


@pytest.fixture
def sample_data_model(sample_data) -> Dict[str, Iterable[BaseModel]]:
    """Pre-defined sample storage object model to manipulate

    Returns:
        Dict of data (keys: content, directory, revision, release, person, origin, ...)
        values list of object data model with the corresponding types

    """
    return {
        object_type: [convert_fn(obj) for obj in sample_data[object_type]]
        for object_type, convert_fn in OBJECT_FACTORY.items()
    }
