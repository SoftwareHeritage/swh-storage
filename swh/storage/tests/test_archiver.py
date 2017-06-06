# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import tempfile
import shutil
import unittest
import os
import time
import json

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.model import hashutil
from swh.core.tests.db_testing import DbsTestFixture

from swh.storage.archiver.storage import get_archiver_storage

from swh.storage.archiver import ArchiverWithRetentionPolicyDirector
from swh.storage.archiver import ArchiverWithRetentionPolicyWorker
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError

try:
    # objstorage > 0.17
    from swh.objstorage.api.server import make_app as app
    from server_testing import ServerTestFixtureAsync as ServerTestFixture
    MIGRATED = True
except ImportError:
    # objstorage <= 0.17
    from swh.objstorage.api.server import app
    from server_testing import ServerTestFixture
    MIGRATED = False

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
        self.dest_root = tempfile.mkdtemp(prefix='remote')
        self.config = {
            'cls': 'pathslicing',
            'args': {
                'root': self.dest_root,
                'slicing': '0:2/2:4/4:6',
            }
        }
        if MIGRATED:
            self.app = app(self.config)
        else:
            self.app = app
        super().setUp()

        # Retrieve connection (depends on the order in TEST_DB_NAMES)
        self.conn = self.conns[0]          # archiver db's connection
        self.cursor = self.cursors[0]

        # Create source storage
        self.src_root = tempfile.mkdtemp()
        src_config = {
            'cls': 'pathslicing',
            'args': {
                'root': self.src_root,
                'slicing': '0:2/2:4/4:6'
            }
        }
        self.src_storage = get_objstorage(**src_config)

        # Create destination storage
        dest_config = {
            'cls': 'remote',
            'args': {
                'url': self.url()
            }
        }
        self.dest_storage = get_objstorage(**dest_config)

        # Keep mapped the id to the storages
        self.storages = {
            'uffizi': self.src_storage,
            'banco': self.dest_storage
        }

        # Override configurations
        src_archiver_conf = {'host': 'uffizi'}
        dest_archiver_conf = {'host': 'banco'}
        src_archiver_conf.update(src_config)
        dest_archiver_conf.update(dest_config)
        self.archiver_storages = [src_archiver_conf, dest_archiver_conf]
        self._override_director_config()
        self._override_worker_config()
        # Create the base archiver
        self.archiver = self._create_director()

    def tearDown(self):
        self.empty_tables()
        shutil.rmtree(self.src_root)
        shutil.rmtree(self.dest_root)
        super().tearDown()

    def empty_tables(self):
        # Remove all content
        self.cursor.execute('DELETE FROM content_archive')
        self.conn.commit()

    def _override_director_config(self, retention_policy=2):
        """ Override the default config of the Archiver director
        to allow the tests to use the *-test db instead of the default one as
        there is no configuration file for now.
        """
        ArchiverWithRetentionPolicyDirector.parse_config_file = lambda obj, additional_configs: {  # noqa
            'archiver_storage': {
                'cls': 'db',
                'args': {
                    'dbconn': self.conn,
                },
            },
            'batch_max_size': 5000,
            'archival_max_age': 3600,
            'retention_policy': retention_policy,
            'asynchronous': False,
        }

    def _override_worker_config(self):
        """ Override the default config of the Archiver worker
        to allow the tests to use the *-test db instead of the default one as
        there is no configuration file for now.
        """
        ArchiverWithRetentionPolicyWorker.parse_config_file = lambda obj, additional_configs: {  # noqa
            'retention_policy': 2,
            'archival_max_age': 3600,
            'archiver_storage': {
                'cls': 'db',
                'args': {
                    'dbconn': self.conn,
                },
            },
            'storages': self.archiver_storages,
            'source': 'uffizi',
        }

    def _create_director(self):
        return ArchiverWithRetentionPolicyDirector()

    def _create_worker(self, batch={}):
        return ArchiverWithRetentionPolicyWorker(batch)

    def _add_content(self, storage_name, content_data):
        """ Add really a content to the given objstorage

        This put an empty status for the added content.

        Args:
            storage_name: the concerned storage
            content_data: the data to insert
            with_row_insert: to insert a row entry in the db or not

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
        self._override_director_config(retention_policy=1)
        director = self._create_director()
        # Obj is present in only one archive but only one copy is required.
        director.run()
        with self.assertRaises(ObjNotFoundError):
            self.dest_storage.get(obj_id)

    # Unit tests for archive worker

    def archival_elapsed(self, mtime):
        return self._create_worker()._is_archival_delay_elapsed(mtime)

    @istest
    def vstatus_ongoing_remaining(self):
        self.assertFalse(self.archival_elapsed(time.time()))

    @istest
    def vstatus_ongoing_elapsed(self):
        past_time = (
            time.time() - self._create_worker().archival_max_age
        )
        self.assertTrue(self.archival_elapsed(past_time))

    def _status(self, status, mtime=None):
        """ Get a dict that match the copies structure
        """
        return {'status': status, 'mtime': mtime or time.time()}

    @istest
    def need_archival_missing(self):
        """ A content should need archival when it is missing.
        """
        status_copies = {'present': ['uffizi'], 'missing': ['banco']}
        worker = self._create_worker()
        self.assertEqual(worker.need_archival(status_copies),
                         True)

    @istest
    def need_archival_present(self):
        """ A content present everywhere shouldn't need archival
        """
        status_copies = {'present': ['uffizi', 'banco']}
        worker = self._create_worker()
        self.assertEqual(worker.need_archival(status_copies),
                         False)

    def _compute_copies_status(self, status):
        """ A content with a given status should be detected correctly
        """
        obj_id = self._add_content(
            'banco', b'compute_copies_' + bytes(status, 'utf8'))
        self._update_status(obj_id, 'banco', status)
        worker = self._create_worker()
        self.assertIn('banco', worker.compute_copies(
            set(worker.objstorages), obj_id)[status])

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

    @istest
    def compute_copies_extra_archive(self):
        obj_id = self._add_content('banco', b'foobar')
        self._update_status(obj_id, 'banco', 'present')
        self._update_status(obj_id, 'random_archive', 'present')
        worker = self._create_worker()
        copies = worker.compute_copies(set(worker.objstorages), obj_id)
        self.assertEqual(copies['present'], {'banco'})
        self.assertEqual(copies['missing'], {'uffizi'})

    def _get_backups(self, present, missing):
        """ Return a list of the pair src/dest from the present and missing
        """
        worker = self._create_worker()
        return list(worker.choose_backup_servers(present, missing))

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


class TestArchiverStorageStub(unittest.TestCase):
    def setUp(self):
        self.src_root = tempfile.mkdtemp(prefix='swh.storage.archiver.local')
        self.dest_root = tempfile.mkdtemp(prefix='swh.storage.archiver.remote')
        self.log_root = tempfile.mkdtemp(prefix='swh.storage.archiver.log')

        src_config = {
            'cls': 'pathslicing',
            'args': {
                'root': self.src_root,
                'slicing': '0:2/2:4/4:6'
            }
        }
        self.src_storage = get_objstorage(**src_config)

        # Create destination storage
        dest_config = {
            'cls': 'pathslicing',
            'args': {
                'root': self.dest_root,
                'slicing': '0:2/2:4/4:6'
            }
        }
        self.dest_storage = get_objstorage(**dest_config)

        self.config = {
            'cls': 'stub',
            'args': {
                'archives': {
                    'present_archive': 'http://uffizi:5003',
                    'missing_archive': 'http://banco:5003',
                },
                'present': ['present_archive'],
                'missing': ['missing_archive'],
                'logfile_base': os.path.join(self.log_root, 'log_'),
            }
        }

        # Generated with:
        #
        # id_length = 20
        # random.getrandbits(8 * id_length).to_bytes(id_length, 'big')
        #
        self.content_ids = [
            b"\xc7\xc9\x8dlk!'k\x81+\xa9\xc1lg\xc2\xcbG\r`f",
            b'S\x03:\xc9\xd0\xa7\xf2\xcc\x8f\x86v$0\x8ccq\\\xe3\xec\x9d',
            b'\xca\x1a\x84\xcbi\xd6co\x14\x08\\8\x9e\xc8\xc2|\xd0XS\x83',
            b'O\xa9\xce(\xb4\x95_&\xd2\xa2e\x0c\x87\x8fw\xd0\xdfHL\xb2',
            b'\xaaa \xd1vB\x15\xbd\xf2\xf0 \xd7\xc4_\xf4\xb9\x8a;\xb4\xcc',
        ]

        self.archiver_storage = get_archiver_storage(**self.config)
        super().setUp()

    def tearDown(self):
        shutil.rmtree(self.src_root)
        shutil.rmtree(self.dest_root)
        shutil.rmtree(self.log_root)
        super().tearDown()

    @istest
    def archive_ls(self):
        self.assertCountEqual(
            self.archiver_storage.archive_ls(),
            self.config['args']['archives'].items()
        )

    @istest
    def content_archive_get(self):
        for content_id in self.content_ids:
            self.assertEqual(
                self.archiver_storage.content_archive_get(content_id),
                (content_id, set(self.config['args']['present']), {}),
            )

    @istest
    def content_archive_get_copies(self):
        self.assertCountEqual(
            self.archiver_storage.content_archive_get_copies(),
            [],
        )

    @istest
    def content_archive_get_unarchived_copies(self):
        retention_policy = 2
        self.assertCountEqual(
            self.archiver_storage.content_archive_get_unarchived_copies(
                retention_policy),
            [],
        )

    @istest
    def content_archive_get_missing(self):
        self.assertCountEqual(
            self.archiver_storage.content_archive_get_missing(
                self.content_ids,
                'missing_archive'
            ),
            self.content_ids,
        )

        self.assertCountEqual(
            self.archiver_storage.content_archive_get_missing(
                self.content_ids,
                'present_archive'
            ),
            [],
        )

        with self.assertRaises(ValueError):
            list(self.archiver_storage.content_archive_get_missing(
                self.content_ids,
                'unknown_archive'
            ))

    @istest
    def content_archive_get_unknown(self):
        self.assertCountEqual(
            self.archiver_storage.content_archive_get_unknown(
                self.content_ids,
            ),
            [],
        )

    @istest
    def content_archive_update(self):
        for content_id in self.content_ids:
            self.archiver_storage.content_archive_update(
                content_id, 'present_archive', 'present')
            self.archiver_storage.content_archive_update(
                content_id, 'missing_archive', 'present')

        self.archiver_storage.close_logfile()

        # Make sure we created a logfile
        files = glob.glob('%s*' % self.config['args']['logfile_base'])
        self.assertEqual(len(files), 1)

        # make sure the logfile contains all our lines
        lines = open(files[0]).readlines()
        self.assertEqual(len(lines), 2 * len(self.content_ids))
