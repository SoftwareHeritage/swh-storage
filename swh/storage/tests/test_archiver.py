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
from swh.storage.archiver import ArchiverDirector, ArchiverWorker
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
        self.backup_objroot = tempfile.mkdtemp(prefix='remote')
        self.config = {'storage_base': self.backup_objroot,
                       'storage_slicing': '0:2/2:4/4:6'}
        self.app = app
        super().setUp()

        # Launch a client to check objects presence
        self.remote_objstorage = RemoteObjStorage(self.url())
        # Create the local storage.
        self.objroot = tempfile.mkdtemp(prefix='local')
        self.storage = Storage(self.conn, self.objroot)
        # Initializes and fill the tables.
        self.initialize_tables()
        # Create the archiver
        self.archiver = self.__create_director()

        self.storage_data = ('Local', 'http://localhost:%s/' % self.port)

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

    def __create_worker(self, batch={}, config={}):
        mstorage_args = [self.archiver.master_storage.db.conn,
                         self.objroot]
        slaves = [self.storage_data]
        if not config:
            config = self.archiver.config
        return ArchiverWorker(batch, mstorage_args, slaves, config)

    # Integration test

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
    def archive_already_enough(self):
        """ A content missing with enough copies shouldn't be archived.
        """
        id = self.__add_content(b'archive_alread_enough')
        director = self.__create_director(retention_policy=0)
        director.run()
        with self.assertRaises(ObjNotFoundError):
            self.remote_objstorage.content_get(id)

    # Unit test for ArchiverDirector

    def vstatus(self, status, mtime):
        return self.archiver.get_virtual_status(status, mtime)

    @istest
    def vstatus_present(self):
        self.assertEquals(
            self.vstatus('present', None),
            'present'
        )

    @istest
    def vstatus_missing(self):
        self.assertEquals(
            self.vstatus('missing', None),
            'missing'
        )

    @istest
    def vstatus_ongoing_remaining(self):
        current_time = datetime.now()
        self.assertEquals(
            self.vstatus('ongoing', current_time),
            'present'
        )

    @istest
    def vstatus_ongoing_elapsed(self):
        past_time = datetime.now() - timedelta(
            seconds=self.archiver.config['archival_max_age'] + 1
        )
        self.assertEquals(
            self.vstatus('ongoing', past_time),
            'missing'
        )

    # Unit tests for archive worker

    @istest
    def need_archival_missing(self):
        """ A content should still need archival when it is missing.
        """
        id = self.__add_content(b'need_archival_missing', status='missing')
        id = r'\x' + hashutil.hash_to_hex(id)
        worker = self.__create_worker()
        self.assertEqual(worker.need_archival(id, self.storage_data), True)

    @istest
    def need_archival_present(self):
        """ A content should still need archival when it is missing
        """
        id = self.__add_content(b'need_archival_missing', status='present')
        id = r'\x' + hashutil.hash_to_hex(id)
        worker = self.__create_worker()
        self.assertEqual(worker.need_archival(id, self.storage_data), False)

    @istest
    def need_archival_ongoing_remaining(self):
        """ An ongoing archival with remaining time shouldnt need archival.
        """
        id = self.__add_content(b'need_archival_ongoing_remaining',
                                status='ongoing', date="'%s'" % datetime.now())
        id = r'\x' + hashutil.hash_to_hex(id)
        worker = self.__create_worker()
        self.assertEqual(worker.need_archival(id, self.storage_data), False)

    @istest
    def need_archival_ongoing_elasped(self):
        """ An ongoing archival with elapsed time should be scheduled again.
        """
        id = self.__add_content(
            b'archive_ongoing_elapsed',
            status='ongoing',
            date="'%s'" % (datetime.now() - timedelta(
                seconds=self.archiver.config['archival_max_age'] + 1
            ))
        )
        id = r'\x' + hashutil.hash_to_hex(id)
        worker = self.__create_worker()
        self.assertEqual(worker.need_archival(id, self.storage_data), True)

    @istest
    def content_sorting_by_archiver(self):
        """ Check that the content is correctly sorted.
        """
        batch = {
            'id1': {
                'present': [('slave1', 'slave1_url')],
                'missing': []
            },
            'id2': {
                'present': [],
                'missing': [('slave1', 'slave1_url')]
            }
        }
        worker = self.__create_worker(batch=batch)
        mapping = worker.sort_content_by_archive()
        self.assertNotIn('id1', mapping[('slave1', 'slave1_url')])
        self.assertIn('id2', mapping[('slave1', 'slave1_url')])
