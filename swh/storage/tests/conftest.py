# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import pytest

from typing import Union

from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor, psycopg2, Version

from os import path, environ
from hypothesis import settings
from typing import Dict

import swh.storage

from swh.core.utils import numfile_sortkey as sortkey

from swh.model.tests.generate_testdata import gen_contents, gen_origins


SQL_DIR = path.join(path.dirname(swh.storage.__file__), 'sql')

environ['LC_ALL'] = 'C.UTF-8'

DUMP_FILES = path.join(SQL_DIR, '*.sql')

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)


@pytest.fixture
def swh_storage(postgresql_proc, swh_storage_postgresql):
    storage_config = {
        'cls': 'local',
        'args': {
            'db': 'postgresql://{user}@{host}:{port}/{dbname}'.format(
                host=postgresql_proc.host,
                port=postgresql_proc.port,
                user='postgres',
                dbname='tests'),
            'objstorage': {
                'cls': 'memory',
                'args': {}
            },
            'journal_writer': {
                'cls': 'memory',
            },
        },
    }
    storage = swh.storage.get_storage(**storage_config)
    return storage


@pytest.fixture
def swh_contents(swh_storage):
    contents = gen_contents(n=20)
    swh_storage.content_add(contents)
    return contents


@pytest.fixture
def swh_origins(swh_storage):
    origins = gen_origins(n=100)
    swh_storage.origin_add(origins)
    return origins


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
            raise ImportError(
                'No module named psycopg2. Please install it.'
            )
        proc_fixture = request.getfixturevalue(process_fixture_name)

        # _, config = try_import('psycopg2', request)
        pg_host = proc_fixture.host
        pg_port = proc_fixture.port
        pg_user = proc_fixture.user
        pg_options = proc_fixture.options
        pg_db = db_name or config['dbname']
        with SwhDatabaseJanitor(
                pg_user, pg_host, pg_port, pg_db, proc_fixture.version,
                dump_files=dump_files
        ):
            connection = psycopg2.connect(
                dbname=pg_db,
                user=pg_user,
                host=pg_host,
                port=pg_port,
                options=pg_options
            )
            yield connection
            connection.close()

    return postgresql_factory


swh_storage_postgresql = postgresql_fact('postgresql_proc')


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
            dump_files: str = DUMP_FILES
    ) -> None:
        super().__init__(user, host, port, db_name, version)
        self.dump_files = sorted(
            glob.glob(dump_files), key=sortkey)

    def db_setup(self):
        with psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            host=self.host,
            port=self.port,
        ) as cnx:
            with cnx.cursor() as cur:
                for fname in self.dump_files:
                    with open(fname) as fobj:
                        sql = fobj.read().replace('concurrently', '').strip()
                        if sql:
                            cur.execute(sql)
            cnx.commit()

    def db_reset(self):
        with psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            host=self.host,
            port=self.port,
        ) as cnx:
            with cnx.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s", ('public',))
                tables = set(table for (table,) in cur.fetchall())
                for table in tables:
                    cur.execute('truncate table %s cascade' % table)

                cur.execute(
                    "SELECT sequence_name FROM information_schema.sequences "
                    "WHERE sequence_schema = %s", ('public',))
                seqs = set(seq for (seq,) in cur.fetchall())
                for seq in seqs:
                    cur.execute('ALTER SEQUENCE %s RESTART;' % seq)
            cnx.commit()

    def init(self):
        with self.cursor() as cur:
            cur.execute(
                "SELECT COUNT(1) FROM pg_database WHERE datname=%s;",
                (self.db_name,))
            db_exists = cur.fetchone()[0] == 1
            if db_exists:
                cur.execute(
                    'UPDATE pg_database SET datallowconn=true '
                    'WHERE datname = %s;',
                    (self.db_name,))

        if db_exists:
            self.db_reset()
        else:
            with self.cursor() as cur:
                cur.execute('CREATE DATABASE "{}";'.format(self.db_name))
            self.db_setup()

    def drop(self):
        pid_column = 'pid'
        with self.cursor() as cur:
            cur.execute(
                'UPDATE pg_database SET datallowconn=false '
                'WHERE datname = %s;', (self.db_name,))
            cur.execute(
                'SELECT pg_terminate_backend(pg_stat_activity.{})'
                'FROM pg_stat_activity '
                'WHERE pg_stat_activity.datname = %s;'.format(pid_column),
                (self.db_name,))


@pytest.fixture
def sample_data() -> Dict:
    """Pre-defined sample storage object data to manipulate

    Returns:
        Dict of data (keys: content, directory, revision, person)

    """
    sample_content = {
        'blake2s256': b'\xbf?\x05\xed\xc1U\xd2\xc5\x168Xm\x93\xde}f(HO@\xd0\xacn\x04\x1e\x9a\xb9\xfa\xbf\xcc\x08\xc7',  # noqa
        'sha1': b'g\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'sha1_git': b'\xf2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
        'sha256': b"\x87\x022\xedZN\x84\xe8za\xf8'(oA\xc9k\xb1\x80c\x80\xe7J\x06\xea\xd2\xd5\xbeB\x19\xb8\xce",  # noqa
        'length': 48,
        'data': b'temp file for testing content storage conversion',
        'status': 'visible',
    }

    sample_content2 = {
        'blake2s256': b'\xbf?\x05\xed\xc1U\xd2\xc5\x168Xm\x93\xde}f(HO@\xd0\xacn\x04\x1e\x9a\xb9\xfa\xbf\xcc\x08\xc7',  # noqa
        'sha1': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'sha1_git': b'\xc2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
        'sha256': b"\x77\x022\xedZN\x84\xe8za\xf8'(oA\xc9k\xb1\x80c\x80\xe7J\x06\xea\xd2\xd5\xbeB\x19\xb8\xce",  # noqa
        'length': 50,
        'data': b'temp file for testing content storage conversion 2',
        'status': 'visible',
    }

    sample_directory = {
        'id': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'entries': []
    }

    sample_person = {
        'name': b'John Doe',
        'email': b'john.doe@institute.org',
        'fullname': b'John Doe <john.doe@institute.org>'
    }

    sample_revision = {
        'id': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'message': b'something',
        'author': sample_person,
        'committer': sample_person,
        'date': 1567591673,
        'committer_date': 1567591673,
        'type': 'tar',
        'directory': b'\xc2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
        'synthetic': False,
        'metadata': {},
        'parents': [],
    }

    return {
        'content': [sample_content, sample_content2],
        'person': [sample_person],
        'directory': [sample_directory],
        'revision': [sample_revision],
    }
