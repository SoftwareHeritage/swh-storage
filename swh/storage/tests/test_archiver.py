# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest
import os
import time
import json

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core import hashutil
from swh.core.tests.db_testing import DbsTestFixture
from server_testing import ServerTestFixture

from swh.storage import Storage
from swh.storage.archiver import ArchiverDirector, ArchiverWorker
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.api.client import RemoteObjStorage
from swh.objstorage.api.server import app

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


@attr('db')
class TestArchiver(DbsTestFixture, ServerTestFixture,
                   unittest.TestCase):
    """ Test the objstorage archiver.
    """

    TEST_DB_NAMES = [
        'softwareheritage-test',
        'softwareheritage-archiver-test',
    ]
    TEST_DB_DUMPS = [
        os.path.join(TEST_DATA_DIR, 'dumps/swh.dump'),
        os.path.join(TEST_DATA_DIR, 'dumps/swh-archiver.dump'),
    ]
    TEST_DB_DUMP_TYPES = [
        'pg_dump',
        'pg_dump',
    ]

    def setUp(self):
        # Launch the backup server
        self.backup_objroot = tempfile.mkdtemp(prefix='remote')
        self.config = {
            'storage_base': self.backup_objroot,
            'storage_slicing': '0:2/2:4/4:6'
        }
        self.app = app
        super().setUp()

        # Retrieve connection (depends on the order in TEST_DB_NAMES)
        self.conn_storage = self.conns[0]  # db connection to storage
        self.conn = self.conns[1]          # archiver db's connection
        self.cursor = self.cursors[1]
        # a reader storage to check content has been archived
        self.remote_objstorage = RemoteObjStorage(self.url())
        # Create the local storage.
        self.objroot = tempfile.mkdtemp(prefix='local')
        # a writer storage to store content before archiving
        self.storage = Storage(self.conn_storage, self.objroot)
        # Initializes and fill the tables.
        self.initialize_tables()
        # Create the archiver
        self.archiver = self.__create_director()

        self.storage_data = ('banco', 'http://localhost:%s/' % self.port)

    def tearDown(self):
        self.empty_tables()
        super().tearDown()

    def initialize_tables(self):
        """ Initializes the database with a sample of items.
        """
        # Add an  archive (update  existing one for  technical reason,
        # altering enum cannot run in a transaction...)
        self.cursor.execute("""UPDATE archive
                               SET url='{}'
                               WHERE id='banco'
                            """.format(self.url()))
        self.conn.commit()

    def empty_tables(self):
        # Remove all content
        self.cursor.execute('DELETE FROM content_archive')
        self.conn.commit()

    def __add_content(self, content_data, status='missing', date=None):
        # Add the content to the storage
        content = hashutil.hashdata(content_data)
        content.update({'data': content_data})
        self.storage.content_add([content])
        # Then update database
        content_id = r'\x' + hashutil.hash_to_hex(content['sha1'])
        copies = {'banco': {
            'status': status,
            'mtime': date or int(time.time())  # if date is None, use now()
        }}
        self.cursor.execute("""INSERT INTO content_archive
                               VALUES('%s'::sha1, '%s')
                            """ % (content_id, json.dumps(copies)))
        return content['sha1']

    def __get_missing(self):
        self.cursor.execute("""SELECT content_id
                            FROM content_archive
                            WHERE status='missing'""")
        return self.cursor.fetchall()

    def __create_director(self, batch_size=5000, archival_max_age=3600,
                          retention_policy=1, asynchronous=False):
        config = {
            'objstorage_type': 'local_objstorage',
            'objstorage_path': self.objroot,
            'objstorage_slicing': '0:2/2:4/4:6',

            'batch_max_size': batch_size,
            'archival_max_age': archival_max_age,
            'retention_policy': retention_policy,
            'asynchronous': asynchronous  # Avoid depending on queue for tests.
        }
        director = ArchiverDirector(db_conn_archiver=self.conn,
                                    config=config)
        return director

    def __create_worker(self, batch={}, config={}):
        mobjstorage_args = self.archiver.master_objstorage_args
        if not config:
            config = self.archiver.config
        return ArchiverWorker(batch,
                              archiver_args=self.conn,
                              master_objstorage_args=mobjstorage_args,
                              slave_objstorages=[self.storage_data],
                              config=config)

    # Integration test
    @istest
    def archive_missing_content(self):
        """ Run archiver on a missing content should archive it.
        """
        content_data = b'archive_missing_content'
        content_id = self.__add_content(content_data)
        # before, the content should not be there
        try:
            self.remote_objstorage.content_get(content_id)
        except ObjNotFoundError:
            pass
        else:
            self.fail('Content should not be present before archival')
        self.archiver.run()
        # now the content should be present on remote objstorage
        remote_data = self.remote_objstorage.content_get(content_id)
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
        self.assertEquals(
            self.vstatus('ongoing', int(time.time())),
            'present'
        )

    @istest
    def vstatus_ongoing_elapsed(self):
        past_time = (
            int(time.time()) - self.archiver.config['archival_max_age'] - 1
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
                                status='ongoing')
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
            date=(
                int(time.time()) - self.archiver.config['archival_max_age'] - 1
            )
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
