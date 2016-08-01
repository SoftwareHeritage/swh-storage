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

from swh.storage.archiver import ArchiverDirector, ArchiverWorker
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.api.server import app

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


@attr('db')
class TestArchiver(DbsTestFixture, ServerTestFixture,
                   unittest.TestCase):
    """ Test the objstorage archiver.
    """

    TEST_DB_NAMES = [
        'softwareheritage-archiver-test',
    ]
    TEST_DB_DUMPS = [
        os.path.join(TEST_DATA_DIR, 'dumps/swh-archiver.dump'),
    ]
    TEST_DB_DUMP_TYPES = [
        'pg_dump',
    ]

    def setUp(self):
        # Launch the backup server
        dest_root = tempfile.mkdtemp(prefix='remote')
        self.config = {
            'storage_base': dest_root,
            'storage_slicing': '0:2/2:4/4:6'
        }
        self.app = app
        super().setUp()

        # Retrieve connection (depends on the order in TEST_DB_NAMES)
        self.conn = self.conns[0]          # archiver db's connection
        self.cursor = self.cursors[0]

        # Create source storage
        src_root = tempfile.mkdtemp()
        src_config = {'cls': 'pathslicing',
                      'args': {'root': src_root,
                               'slicing': '0:2/2:4/4:6'}}
        self.src_storage = get_objstorage(**src_config)

        # Create destination storage
        dest_config = {'cls': 'remote',
                       'args': {'base_url': self.url()}}
        self.dest_storage = get_objstorage(**dest_config)

        # Keep mapped the id to the storages
        self.storages = {'uffizi': self.src_storage,
                         'banco': self.dest_storage}

        # Create the archiver itself
        src_archiver_conf = {'host': 'uffizi'}
        dest_archiver_conf = {'host': 'banco'}
        src_archiver_conf.update(src_config)
        dest_archiver_conf.update(dest_config)
        self.archiver_storages = [src_archiver_conf, dest_archiver_conf]
        self.archiver = self._create_director(
            retention_policy=2,
            storages=self.archiver_storages
        )
        # Create a base worker
        self.archiver_worker = self._create_worker()

        # Initializes and fill the tables.
        self.initialize_tables()

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

    def _create_director(self, storages, batch_size=5000,
                         archival_max_age=3600, retention_policy=2,
                         asynchronous=False):
        config = {
            'dbconn': ('str', self.conn),
            'batch_max_size': ('int', batch_size),
            'archival_max_age': ('int', archival_max_age),
            'retention_policy': ('int', retention_policy),
            'asynchronous': ('bool', asynchronous),
            'storages': ('dict', self.archiver_storages)
        }
        return ArchiverDirector(config)

    def _create_worker(self, batch={}, retention_policy=2,
                       archival_max_age=3600):
        config = {
            'retention_policy': ('int', retention_policy),
            'archival_max_age': ('int', archival_max_age),
            'dbconn': ('str', self.conn),
            'storages': ('dict', self.archiver_storages)
        }
        return ArchiverWorker(batch, config)

    def _add_content(self, storage_name, content_data):
        """ Add really a content to the given objstorage

        This put an empty status for the added content.
        """
        # Add the content to the storage
        obj_id = self.storages[storage_name].add(content_data)
        db_obj_id = r'\x' + hashutil.hash_to_hex(obj_id)
        self.cursor.execute(""" INSERT INTO content_archive
                                VALUES('%s', '{}')
                            """ % (db_obj_id))
        return obj_id

    def _update_status(self, obj_id, storage_name, status, date=None):
        """ Update the db status for the given id/storage_name.

        This does not create the content in the storage.
        """
        db_obj_id = r'\x' + hashutil.hash_to_hex(obj_id)
        self.archiver.archiver_storage.content_archive_update(
            db_obj_id, storage_name, status
        )

    def _add_dated_content(self, obj_id, copies={}):
        """ Fully erase the previous copies field for the given content id

        This does not alter the contents into the objstorages.
        """
        db_obj_id = r'\x' + hashutil.hash_to_hex(obj_id)
        self.cursor.execute(""" UPDATE TABLE content_archive
                                SET copies='%s'
                                WHERE content_id='%s'
                            """ % (json.dumps(copies), db_obj_id))

    # Integration test
    @istest
    def archive_missing_content(self):
        """ Run archiver on a missing content should archive it.
        """
        obj_data = b'archive_missing_content'
        obj_id = self._add_content('uffizi', obj_data)
        self._update_status(obj_id, 'uffizi', 'present')
        # Content is missing on banco (entry not present in the db)
        try:
            self.dest_storage.get(obj_id)
        except ObjNotFoundError:
            pass
        else:
            self.fail('Content should not be present before archival')
        self.archiver.run()
        # now the content should be present on remote objstorage
        remote_data = self.dest_storage.get(obj_id)
        self.assertEquals(obj_data, remote_data)

    @istest
    def archive_present_content(self):
        """ A content that is not 'missing' shouldn't be archived.
        """
        obj_id = self._add_content('uffizi', b'archive_present_content')
        self._update_status(obj_id, 'uffizi', 'present')
        self._update_status(obj_id, 'banco', 'present')
        # After the run, the content should NOT be in the archive.
        # As the archiver believe it was already in.
        self.archiver.run()
        with self.assertRaises(ObjNotFoundError):
            self.dest_storage.get(obj_id)

    @istest
    def archive_already_enough(self):
        """ A content missing with enough copies shouldn't be archived.
        """
        obj_id = self._add_content('uffizi', b'archive_alread_enough')
        self._update_status(obj_id, 'uffizi', 'present')
        director = self._create_director(self.archiver_storages,
                                         retention_policy=1)
        # Obj is present in only one archive but only one copy is required.
        director.run()
        with self.assertRaises(ObjNotFoundError):
            self.dest_storage.get(obj_id)

    # Unit tests for archive worker

    def vstatus(self, status, mtime):
        return self.archiver_worker._get_virtual_status(status, mtime)

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
            self.vstatus('ongoing', time.time()),
            'present'
        )

    @istest
    def vstatus_ongoing_elapsed(self):
        past_time = (
            time.time() - self.archiver_worker.archival_max_age
        )
        self.assertEquals(
            self.vstatus('ongoing', past_time),
            'missing'
        )

    def _status(self, status, mtime=None):
        """ Get a dict that match the copies structure
        """
        return {'status': status, 'mtime': mtime or time.time()}

    @istest
    def need_archival_missing(self):
        """ A content should need archival when it is missing.
        """
        status_copies = {'present': ['uffizi'], 'missing': ['banco']}
        worker = self._create_worker({}, retention_policy=2)
        self.assertEqual(worker._need_archival(status_copies),
                         True)

    @istest
    def need_archival_present(self):
        """ A content present everywhere shouldn't need archival
        """
        status_copies = {'present': ['uffizi', 'banco']}
        worker = self._create_worker({}, retention_policy=2)
        self.assertEqual(worker._need_archival(status_copies),
                         False)

    def _compute_copies_status(self, status):
        """ A content with a given status should be detected correctly
        """
        obj_id = self._add_content(
            'banco', b'compute_copies_' + bytes(status, 'utf8'))
        self._update_status(obj_id, 'banco', status)
        worker = self._create_worker()
        self.assertIn('banco', worker._compute_copies(obj_id)[status])

    @istest
    def compute_copies_present(self):
        """ A present content should be detected with correct status
        """
        self._compute_copies_status('present')

    @istest
    def compute_copies_missing(self):
        """ A missing content should be detected with correct status
        """
        self._compute_copies_status('missing')

    def _get_backups(self, present, missing):
        """ Return a list of the pair src/dest from the present and missing
        """
        worker = self._create_worker()
        return list(worker._choose_backup_servers(present, missing))

    @istest
    def choose_backup_servers(self):
        self.assertEqual(len(self._get_backups(['uffizi', 'banco'], [])), 0)
        self.assertEqual(len(self._get_backups(['uffizi'], ['banco'])), 1)
        # Even with more possible destinations, do not take more than the
        # retention_policy require
        self.assertEqual(
            len(self._get_backups(['uffizi'], ['banco', 's3'])),
            1
        )
