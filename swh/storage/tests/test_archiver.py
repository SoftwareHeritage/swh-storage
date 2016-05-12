# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest
import os

from nose.tools import istest
from nose.plugins.attrib import attr
from datetime import datetime, timedelta

from swh.core import hashutil
from swh.core.tests.db_testing import DbTestFixture
from server_testing import ServerTestFixture

from swh.storage import Storage
from swh.storage.exc import ObjNotFoundError
from swh.storage.archiver import ArchiverDirector
from swh.storage.objstorage.api.client import RemoteObjStorage
from swh.storage.objstorage.api.server import app

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


@attr('db')
class TestArchiver(DbTestFixture, ServerTestFixture,
                   unittest.TestCase):
    """ Test the objstorage archiver.
    """

    TEST_DB_DUMP = os.path.join(TEST_DATA_DIR, 'dumps/swh.dump')

    def setUp(self):
        # Launch the backup server
        self.backup_objroot = tempfile.mkdtemp()
        self.config = {'storage_base': self.backup_objroot,
                       'storage_depth': 3}
        self.app = app
        super().setUp()

        # Launch a client to check objects presence
        print("url", self.url())
        self.remote_objstorage = RemoteObjStorage(self.url())
        # Create the local storage.
        self.objroot = tempfile.mkdtemp()
        self.storage = Storage(self.conn, self.objroot)
        # Initializes and fill the tables.
        self.initialize_tables()
        # Create the archiver
        self.archiver = self.__create_director()

    def tearDown(self):
        self.empty_tables()
        super().tearDown()

    def initialize_tables(self):
        """ Initializes the database with a sample of items.
        """
        # Add an archive
        self.cursor.execute("""INSERT INTO archives(id, url)
                               VALUES('Local', 'http://localhost:{}/')
                            """.format(self.port))
        self.conn.commit()

    def empty_tables(self):
        # Remove all content
        self.cursor.execute('DELETE FROM content_archive')
        self.cursor.execute('DELETE FROM archives')
        self.conn.commit()

    def __add_content(self, content_data, status='missing', date='now()'):
        # Add the content
        content = hashutil.hashdata(content_data)
        content.update({'data': content_data})
        self.storage.content_add([content])
        # Then update database
        content_id = r'\x' + hashutil.hash_to_hex(content['sha1'])
        self.cursor.execute("""INSERT INTO content_archive
                               VALUES('%s'::sha1, 'Local', '%s', %s)
                            """ % (content_id, status, date))
        return content['sha1']

    def __get_missing(self):
        self.cursor.execute("""SELECT content_id
                            FROM content_archive
                            WHERE status='missing'""")
        return self.cursor.fetchall()

    def __create_director(self, batch_size=5000, archival_max_age=3600,
                          retention_policy=1, asynchronous=False):
        config = {
            'objstorage_path': self.objroot,
            'batch_max_size': batch_size,
            'archival_max_age': archival_max_age,
            'retention_policy': retention_policy,
            'asynchronous': asynchronous  # Avoid depending on queue for tests.
        }
        director = ArchiverDirector(self.conn, config)
        return director

    @istest
    def archive_missing_content(self):
        """ Run archiver on a missing content should archive it.
        """
        content_data = b'archive_missing_content'
        id = self.__add_content(content_data)
        # After the run, the content should be in the archive.
        self.archiver.run()
        remote_data = self.remote_objstorage.content_get(id)
        self.assertEquals(content_data, remote_data)

    @istest
    def archive_present_content(self):
        """ A content that is not 'missing' shouldn't be archived.
        """
        id = self.__add_content(b'archive_present_content', status='present')
        # After the run, the content should NOT be in the archive.*
        self.archiver.run()
        with self.assertRaises(ObjNotFoundError):
            self.remote_objstorage.content_get(id)

    @istest
    def archive_ongoing_remaining(self):
        """ A content that is ongoing and still have some time
        to be archived should not be rescheduled.
        """
        id = self.__add_content(b'archive_ongoing_remaining', status='ongoing')
        items = [x for batch in self.archiver.get_unarchived_content()
                 for x in batch]
        id = r'\x' + hashutil.hash_to_hex(id)
        self.assertNotIn(id, items)

    @istest
    def archive_ongoing_elapsed(self):
        """ A content that is ongoing but with elapsed time should
        be rescheduled.
        """
        # Create an ongoing archive content with time elapsed by 1s.
        id = self.__add_content(
            b'archive_ongoing_elapsed',
            status='ongoing',
            date="'%s'" % (datetime.now() - timedelta(
                seconds=self.archiver.archival_max_age + 1
            ))
        )
        items = [x for batch in self.archiver.get_unarchived_content()
                 for x in batch]
        id = r'\x' + hashutil.hash_to_hex(id)
        self.assertIn(id, items)

    @istest
    def archive_already_enough(self):
        """ A content missing should not be archived if there
        is already enough copies.
        """
        id = self.__add_content(b'archive_alread_enough')
        director = self.__create_director(retention_policy=0)
        director.run()
        with self.assertRaises(ObjNotFoundError):
            self.remote_objstorage.content_get(id)
