# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import psycopg2
import subprocess


TEST_DB_NAME = 'softwareheritage-test'
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(TEST_DIR, '../../../sql')
TEST_DB_DUMP = os.path.join(SQL_DIR, 'swh.dump')


def pg_restore(dbname, dumpfile):
    subprocess.check_call(['pg_restore', '--no-owner', '--no-privileges',
                           '--dbname', dbname, dumpfile])


def pg_dump(dbname, dumpfile):
    subprocess.check_call(['pg_dump', '--no-owner', '--no-privileges', '-Fc',
                           '-f', dumpfile, dbname])


def pg_dropdb(dbname):
    subprocess.check_call(['dropdb', dbname])


def pg_createdb(dbname):
    subprocess.check_call(['createdb', dbname])


def db_create(test_subj, dbname=TEST_DB_NAME, dbdump=TEST_DB_DUMP):
    """create the test DB and load the test data dump into it

    context: setUpClass

    """
    try:
        pg_createdb(dbname)
    except subprocess.CalledProcessError:  # try recovering once, in case
        pg_dropdb(dbname)                  # the db already existed
        pg_createdb(dbname)
    pg_restore(dbname, dbdump)
    test_subj.dbname = dbname


def db_destroy(test_subj):
    """destroy the test DB

    context: tearDownClass

    """
    pg_dropdb(test_subj.dbname)


def db_connect(test_subj):
    """connect to the test DB and open a cursor

    context: setUp

    """
    test_subj.conn = psycopg2.connect('dbname=' + test_subj.dbname)
    test_subj.cursor = test_subj.conn.cursor()


def db_close(test_subj):
    """rollback current transaction and disconnet from the test DB

    context: tearDown

    """
    test_subj.conn.rollback()
    test_subj.conn.close()


class DbTestFixture():
    """Mix this in a test subject class to get DB testing support.

    The test case class will then have the following attributes, accessible via
    self:

        dbname: name of the test database
        conn: psycopg2 connection object
        cursor: open psycopg2 cursor to the DB

    To ensure test isolation, each test method of the test case class will
    execute in its own connection, cursor, and transaction.

    To ensure setup/teardown methods are called, in case of multiple
    inheritance DbTestFixture should be the first class in the inheritance
    hierarchy.

    Note that if you want to define setup/teardown methods, you need to
    explicitly call super() to ensure that the fixture setup/teardown methods
    are invoked. Here is an example where all setup/teardown methods are
    defined in a test case:

        class TestDb(DbTestFixture, unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                super().setUpClass()
                # your class setup code here

            def setUp(self):
                super().setUp()
                # your instance setup code here

            def tearDown(self):
                # your instance teardown code here
                super().tearDown()

            @classmethod
            def tearDownClass(cls):
                # your class teardown code here
                super().tearDownClass()

    """

    @classmethod
    def setUpClass(cls):
        db_create(cls)
        super().setUpClass()

    def setUp(self):
        db_connect(self)
        super().setUp()

    def tearDown(self):
        super().tearDown()
        db_close(self)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        db_destroy(cls)
