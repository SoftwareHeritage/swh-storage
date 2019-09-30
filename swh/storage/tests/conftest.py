from os import path, environ
import glob
import pytest

from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor, psycopg2
from hypothesis import strategies
from swh.model.hypothesis_strategies import origins, contents

from swh.core.utils import numfile_sortkey as sortkey
import swh.storage

SQL_DIR = path.join(path.dirname(swh.storage.__file__), 'sql')

environ['LC_ALL'] = 'C.UTF-8'

DUMP_FILES = path.join(SQL_DIR, '*.sql')


@pytest.fixture
def swh_storage(postgresql_proc, swh_storage_postgresql, tmp_path):

    objdir = tmp_path / 'objstorage'
    objdir.mkdir()
    storage_config = {
        'cls': 'local',
        'args': {
            'db': 'postgresql://{user}@{host}:{port}/{dbname}'.format(
                host=postgresql_proc.host,
                port=postgresql_proc.port,
                user='postgres',
                dbname='tests'),
            'objstorage': {
                'cls': 'pathslicing',
                'args': {
                    'root': str(objdir),
                    'slicing': '0:1/1:5',
                },
            },
            'journal_writer': {
                'cls': 'memory',
            },
        },
    }
    storage = swh.storage.get_storage(**storage_config)
    return storage


def gen_origins(n=20):
    return strategies.lists(
        origins().map(lambda x: x.to_dict()),
        unique_by=lambda x: x['url'],
        min_size=n, max_size=n).example()


def gen_contents(n=20):
    return strategies.lists(
        contents().map(lambda x: x.to_dict()),
        unique_by=lambda x: (x['sha1'], x['sha1_git']),
        min_size=n, max_size=n).example()


@pytest.fixture
def swh_contents(swh_storage):
    contents = gen_contents()
    swh_storage.content_add(contents)
    return contents


@pytest.fixture
def swh_origins(swh_storage):
    origins = gen_origins()
    swh_storage.origin_add(origins)
    return origins


# the postgres_fact factory fixture below is mostly a copy of the code
# from pytest-postgresql. We need a custom version here to be able to
# specify our version of the DBJanitor we use.
def postgresql_fact(process_fixture_name, db_name=None):
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
                pg_user, pg_host, pg_port, pg_db, proc_fixture.version
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
# behavior than than the stock one: instead of droping, creating and
# initializing the database for each test, it create and initialize the db only
# once, then it truncate the tables. This is needed to have acceptable test
# performances.
class SwhDatabaseJanitor(DatabaseJanitor):
    def db_setup(self):
        with psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            host=self.host,
            port=self.port,
        ) as cnx:
            with cnx.cursor() as cur:
                all_dump_files = sorted(
                    glob.glob(DUMP_FILES), key=sortkey)
                for fname in all_dump_files:
                    with open(fname) as fobj:
                        sql = fobj.read().replace('concurrently', '')
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
