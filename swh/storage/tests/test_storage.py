# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os
import psycopg2
import shutil
import tempfile
import unittest
from uuid import UUID

from unittest.mock import patch

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core.tests.db_testing import DbTestFixture
from swh.core.hashutil import hex_to_hash
from swh.storage import Storage


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


@attr('db')
class AbstractTestStorage(DbTestFixture):
    """Base class for Storage testing.

    This class is used as-is to test local storage (see TestStorage
    below) and remote storage (see TestRemoteStorage in
    test_remote_storage.py.

    We need to have the two classes inherit from this base class
    separately to avoid nosetests running the tests from the base
    class twice.

    """
    TEST_DB_DUMP = os.path.join(TEST_DATA_DIR, 'dumps/swh.dump')

    def setUp(self):
        super().setUp()
        self.maxDiff = None
        self.objroot = tempfile.mkdtemp()
        self.storage = Storage(self.conn, self.objroot)

        self.cont = {
            'data': b'42\n',
            'length': 3,
            'sha1': hex_to_hash(
                '34973274ccef6ab4dfaaf86599792fa9c3fe4689'),
            'sha1_git': hex_to_hash(
                'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd'),
            'sha256': hex_to_hash(
                '673650f936cb3b0a2f93ce09d81be107'
                '48b1b203c19e8176b4eefc1964a0cf3a'),
        }

        self.cont2 = {
            'data': b'4242\n',
            'length': 5,
            'sha1': hex_to_hash(
                '61c2b3a30496d329e21af70dd2d7e097046d07b7'),
            'sha1_git': hex_to_hash(
                '36fade77193cb6d2bd826161a0979d64c28ab4fa'),
            'sha256': hex_to_hash(
                '859f0b154fdb2d630f45e1ecae4a8629'
                '15435e663248bb8461d914696fc047cd'),
        }

        self.missing_cont = {
            'data': b'missing\n',
            'length': 8,
            'sha1': hex_to_hash(
                'f9c24e2abb82063a3ba2c44efd2d3c797f28ac90'),
            'sha1_git': hex_to_hash(
                '33e45d56f88993aae6a0198013efa80716fd8919'),
            'sha256': hex_to_hash(
                '6bbd052ab054ef222c1c87be60cd191a'
                'ddedd24cc882d1f5f7f7be61dc61bb3a'),
        }

        self.skipped_cont = {
            'length': 1024 * 1024 * 200,
            'sha1_git': hex_to_hash(
                '33e45d56f88993aae6a0198013efa80716fd8920'),
            'reason': 'Content too long',
            'status': 'absent',
        }

        self.skipped_cont2 = {
            'length': 1024 * 1024 * 300,
            'sha1_git': hex_to_hash(
                '33e45d56f88993aae6a0198013efa80716fd8921'),
            'reason': 'Content too long',
            'status': 'absent',
        }

        self.dir = {
            'id': b'4\x013\x422\x531\x000\xf51\xe62\xa73\xff7\xc3\xa90',
            'entries': [
                {
                    'name': b'foo',
                    'type': 'file',
                    'target': self.cont['sha1_git'],
                    'perms': 0o644,
                },
                {
                    'name': b'bar\xc3',
                    'type': 'dir',
                    'target': b'12345678901234567890',
                    'perms': 0o2000,
                },
            ],
        }

        self.dir2 = {
            'id': b'4\x013\x422\x531\x000\xf51\xe62\xa73\xff7\xc3\xa95',
            'entries': [
                {
                    'name': b'oof',
                    'type': 'file',
                    'target': self.cont2['sha1_git'],
                    'perms': 0o644,
                }
            ],
        }

        self.dir3 = {
            'id': hex_to_hash('33e45d56f88993aae6a0198013efa80716fd8921'),
            'entries': [
                {
                    'name': b'foo',
                    'type': 'file',
                    'target': self.cont['sha1_git'],
                    'perms': 0o644,
                },
                {
                    'name': b'bar',
                    'type': 'dir',
                    'target': b'12345678901234560000',
                    'perms': 0o2000,
                },
                {
                    'name': b'hello',
                    'type': 'file',
                    'target': b'12345678901234567890',
                    'perms': 0o644,
                },

            ],
        }

        self.minus_offset = datetime.timezone(datetime.timedelta(minutes=-120))
        self.plus_offset = datetime.timezone(datetime.timedelta(minutes=120))

        self.revision = {
            'id': b'56789012345678901234',
            'message': b'hello',
            'author': {
                'name': b'Nicolas Dandrimont',
                'email': b'nicolas@example.com',
                'fullname': b'Nicolas Dandrimont <nicolas@example.com> ',
            },
            'date': {
                'timestamp': 1234567890,
                'offset': 120,
                'negative_utc': None,
            },
            'committer': {
                'name': b'St\xc3fano Zacchiroli',
                'email': b'stefano@example.com',
                'fullname': b'St\xc3fano Zacchiroli <stefano@example.com>'
            },
            'committer_date': {
                'timestamp': 1123456789,
                'offset': 0,
                'negative_utc': True,
            },
            'parents': [b'01234567890123456789', b'23434512345123456789'],
            'type': 'git',
            'directory': self.dir['id'],
            'metadata': {
                'checksums': {
                    'sha1': 'tarball-sha1',
                    'sha256': 'tarball-sha256',
                },
                'signed-off-by': 'some-dude',
                'extra_headers': [
                    ['gpgsig', b'test123'],
                    ['mergetags', [b'foo\\bar', b'\x22\xaf\x89\x80\x01\x00']],
                ],
            },
            'synthetic': True
        }

        self.revision2 = {
            'id': b'87659012345678904321',
            'message': b'hello again',
            'author': {
                'name': b'Roberto Dicosmo',
                'email': b'roberto@example.com',
                'fullname': b'Roberto Dicosmo <roberto@example.com>',
            },
            'date': {
                'timestamp': 1234567843.22,
                'offset': -720,
                'negative_utc': None,
            },
            'committer': {
                'name': b'tony',
                'email': b'ar@dumont.fr',
                'fullname': b'tony <ar@dumont.fr>',
            },
            'committer_date': {
                'timestamp': 1123456789,
                'offset': 0,
                'negative_utc': False,
            },
            'parents': [b'01234567890123456789'],
            'type': 'git',
            'directory': self.dir2['id'],
            'metadata': None,
            'synthetic': False
        }

        self.revision3 = {
            'id': hex_to_hash('7026b7c1a2af56521e951c01ed20f255fa054238'),
            'message': b'a simple revision with no parents this time',
            'author': {
                'name': b'Roberto Dicosmo',
                'email': b'roberto@example.com',
                'fullname': b'Roberto Dicosmo <roberto@example.com>',
            },
            'date': {
                'timestamp': 1234567843.22,
                'offset': -720,
                'negative_utc': None,
            },
            'committer': {
                'name': b'tony',
                'email': b'ar@dumont.fr',
                'fullname': b'tony <ar@dumont.fr>',
            },
            'committer_date': {
                'timestamp': 1127351742,
                'offset': 0,
                'negative_utc': False,
            },
            'parents': [],
            'type': 'git',
            'directory': self.dir2['id'],
            'metadata': None,
            'synthetic': True
        }

        self.revision4 = {
            'id': hex_to_hash('368a48fe15b7db2383775f97c6b247011b3f14f4'),
            'message': b'parent of self.revision2',
            'author': {
                'name': b'me',
                'email': b'me@soft.heri',
                'fullname': b'me <me@soft.heri>',
            },
            'date': {
                'timestamp': 1244567843.22,
                'offset': -720,
                'negative_utc': None,
            },
            'committer': {
                'name': b'committer-dude',
                'email': b'committer@dude.com',
                'fullname': b'committer-dude <committer@dude.com>',
            },
            'committer_date': {
                'timestamp': 1244567843.22,
                'offset': -720,
                'negative_utc': None,
            },
            'parents': [self.revision3['id']],
            'type': 'git',
            'directory': self.dir['id'],
            'metadata': None,
            'synthetic': False
        }

        self.origin = {
            'url': 'file:///dev/null',
            'type': 'git',
        }

        self.origin2 = {
            'url': 'file:///dev/zero',
            'type': 'git',
        }

        self.occurrence = {
            'branch': b'master',
            'target': b'67890123456789012345',
            'target_type': 'revision',
            'date': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
        }

        self.occurrence2 = {
            'branch': b'master',
            'target': self.revision2['id'],
            'target_type': 'revision',
            'date': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
        }

        self.release = {
            'id': b'87659012345678901234',
            'name': b'v0.0.1',
            'author': {
                'name': b'olasd',
                'email': b'nic@olasd.fr',
                'fullname': b'olasd <nic@olasd.fr>',
            },
            'date': {
                'timestamp': 1234567890,
                'offset': 42,
                'negative_utc': None,
            },
            'target': b'43210987654321098765',
            'target_type': 'revision',
            'message': b'synthetic release',
            'synthetic': True,
        }

        self.release2 = {
            'id': b'56789012348765901234',
            'name': b'v0.0.2',
            'author': {
                'name': b'tony',
                'email': b'ar@dumont.fr',
                'fullname': b'tony <ar@dumont.fr>',
            },
            'date': {
                'timestamp': 1634366813,
                'offset': -120,
                'negative_utc': None,
            },
            'target': b'432109\xa9765432\xc309\x00765',
            'target_type': 'revision',
            'message': b'v0.0.2\nMisc performance improvments + bug fixes',
            'synthetic': False
        }

        self.release3 = {
            'id': b'87659012345678904321',
            'name': b'v0.0.2',
            'author': {
                'name': b'tony',
                'email': b'tony@ardumont.fr',
                'fullname': b'tony <tony@ardumont.fr>',
            },
            'date': {
                'timestamp': 1634336813,
                'offset': 0,
                'negative_utc': False,
            },
            'target': self.revision2['id'],
            'target_type': 'revision',
            'message': b'yet another synthetic release',
            'synthetic': True,
        }

        self.fetch_history_date = datetime.datetime(
            2015, 1, 2, 21, 0, 0,
            tzinfo=datetime.timezone.utc)
        self.fetch_history_end = datetime.datetime(
            2015, 1, 2, 23, 0, 0,
            tzinfo=datetime.timezone.utc)

        self.fetch_history_duration = (self.fetch_history_end -
                                       self.fetch_history_date)

        self.fetch_history_data = {
            'status': True,
            'result': {'foo': 'bar'},
            'stdout': 'blabla',
            'stderr': 'blablabla',
        }

        self.entity1 = {
            'uuid': UUID('f96a7ec1-0058-4920-90cc-7327e4b5a4bf'),
            # GitHub users
            'parent': UUID('ad6df473-c1d2-4f40-bc58-2b091d4a750e'),
            'name': 'github:user:olasd',
            'type': 'person',
            'description': 'Nicolas Dandrimont',
            'homepage': 'http://example.com',
            'active': True,
            'generated': True,
            'lister_metadata': {
                # swh.lister.github
                'lister': '34bd6b1b-463f-43e5-a697-785107f598e4',
                'id': 12877,
                'type': 'user',
                'last_activity': '2015-11-03',
            },
            'metadata': None,
            'validity': [
                datetime.datetime(2015, 11, 3, 11, 0, 0,
                                  tzinfo=datetime.timezone.utc),
            ]
        }

        self.entity1_query = {
            'lister': '34bd6b1b-463f-43e5-a697-785107f598e4',
            'id': 12877,
            'type': 'user',
        }

        self.entity2 = {
            'uuid': UUID('3903d075-32d6-46d4-9e29-0aef3612c4eb'),
            # GitHub users
            'parent': UUID('ad6df473-c1d2-4f40-bc58-2b091d4a750e'),
            'name': 'github:user:zacchiro',
            'type': 'person',
            'description': 'Stefano Zacchiroli',
            'homepage': 'http://example.com',
            'active': True,
            'generated': True,
            'lister_metadata': {
                # swh.lister.github
                'lister': '34bd6b1b-463f-43e5-a697-785107f598e4',
                'id': 216766,
                'type': 'user',
                'last_activity': '2015-11-03',
            },
            'metadata': None,
            'validity': [
                datetime.datetime(2015, 11, 3, 11, 0, 0,
                                  tzinfo=datetime.timezone.utc),
            ]
        }

        self.entity3 = {
            'uuid': UUID('111df473-c1d2-4f40-bc58-2b091d4a7111'),
            # GitHub users
            'parent': UUID('222df473-c1d2-4f40-bc58-2b091d4a7222'),
            'name': 'github:user:ardumont',
            'type': 'person',
            'description': 'Antoine R. Dumont a.k.a tony',
            'homepage': 'https://ardumont.github.io',
            'active': True,
            'generated': True,
            'lister_metadata': {
                'lister': '34bd6b1b-463f-43e5-a697-785107f598e4',
                'id': 666,
                'type': 'user',
                'last_activity': '2016-01-15',
            },
            'metadata': None,
            'validity': [
                datetime.datetime(2015, 11, 3, 11, 0, 0,
                                  tzinfo=datetime.timezone.utc),
            ]
        }

        self.entity4 = {
            'uuid': UUID('222df473-c1d2-4f40-bc58-2b091d4a7222'),
            # GitHub users
            'parent': None,
            'name': 'github:user:ToNyX',
            'type': 'person',
            'description': 'ToNyX',
            'homepage': 'https://ToNyX.github.io',
            'active': True,
            'generated': True,
            'lister_metadata': {
                'lister': '34bd6b1b-463f-43e5-a697-785107f598e4',
                'id': 999,
                'type': 'user',
                'last_activity': '2015-12-24',
            },
            'metadata': None,
            'validity': [
                datetime.datetime(2015, 11, 3, 11, 0, 0,
                                  tzinfo=datetime.timezone.utc),
            ]
        }

        self.entity2_query = {
            'lister_metadata': {
                'lister': '34bd6b1b-463f-43e5-a697-785107f598e4',
                'id': 216766,
                'type': 'user',
            },
        }

    def tearDown(self):
        shutil.rmtree(self.objroot)

        self.cursor.execute("""SELECT table_name FROM information_schema.tables
                               WHERE table_schema = %s""", ('public',))

        tables = set(table for (table,) in self.cursor.fetchall())
        tables -= {'dbversion', 'entity', 'entity_history', 'listable_entity'}

        for table in tables:
            self.cursor.execute('truncate table %s cascade' % table)

        self.cursor.execute('delete from entity where generated=true')
        self.cursor.execute('delete from entity_history where generated=true')
        self.conn.commit()

        super().tearDown()

    @istest
    def content_add(self):
        cont = self.cont

        self.storage.content_add([cont])
        if hasattr(self.storage, 'objstorage'):
            self.assertIn(cont['sha1'], self.storage.objstorage)
        self.cursor.execute('SELECT sha1, sha1_git, sha256, length, status'
                            ' FROM content WHERE sha1 = %s',
                            (cont['sha1'],))
        datum = self.cursor.fetchone()
        self.assertEqual(
            (datum[0].tobytes(), datum[1].tobytes(), datum[2].tobytes(),
             datum[3], datum[4]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible'))

    @istest
    def content_add_collision(self):
        cont1 = self.cont

        # create (corrupted) content with same sha1{,_git} but != sha256
        cont1b = cont1.copy()
        sha256_array = bytearray(cont1b['sha256'])
        sha256_array[0] += 1
        cont1b['sha256'] = bytes(sha256_array)

        with self.assertRaises(psycopg2.IntegrityError):
            self.storage.content_add([cont1, cont1b])

    @istest
    def skipped_content_add(self):
        cont = self.skipped_cont
        cont2 = self.skipped_cont2

        self.storage.content_add([cont])
        self.storage.content_add([cont2])

        self.cursor.execute('SELECT sha1, sha1_git, sha256, length, status,'
                            'reason FROM skipped_content ORDER BY sha1_git')

        datum = self.cursor.fetchone()
        self.assertEqual(
            (datum[0], datum[1].tobytes(), datum[2],
             datum[3], datum[4], datum[5]),
            (None, cont['sha1_git'], None,
             cont['length'], 'absent', 'Content too long'))

        datum2 = self.cursor.fetchone()
        self.assertEqual(
            (datum2[0], datum2[1].tobytes(), datum2[2],
             datum2[3], datum2[4], datum2[5]),
            (None, cont2['sha1_git'], None,
             cont2['length'], 'absent', 'Content too long'))

    @istest
    def content_missing(self):
        cont2 = self.cont2
        missing_cont = self.missing_cont
        self.storage.content_add([cont2])
        gen = self.storage.content_missing([cont2, missing_cont])

        self.assertEqual(list(gen), [missing_cont['sha1']])

    @istest
    def content_missing_per_sha1(self):
        # given
        cont2 = self.cont2
        missing_cont = self.missing_cont
        self.storage.content_add([cont2])
        # when
        gen = self.storage.content_missing_per_sha1([cont2['sha1'],
                                                     missing_cont['sha1']])

        # then
        self.assertEqual(list(gen), [missing_cont['sha1']])

    @istest
    def directory_get(self):
        # given
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        self.storage.directory_add([self.dir])

        # when
        actual_dirs = list(self.storage.directory_get([self.dir['id']]))

        self.assertEqual(len(actual_dirs), 1)

        dir0 = actual_dirs[0]
        self.assertEqual(dir0['id'], self.dir['id'])
        # ids are generated so non deterministic value
        self.assertEqual(len(dir0['file_entries']), 1)
        self.assertEqual(len(dir0['dir_entries']), 1)
        self.assertIsNone(dir0['rev_entries'])

        after_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([], after_missing)

    @istest
    def directory_add(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        self.storage.directory_add([self.dir])

        stored_data = list(self.storage.directory_ls(self.dir['id']))

        data_to_store = [{
                 'dir_id': self.dir['id'],
                 'type': ent['type'],
                 'target': ent['target'],
                 'name': ent['name'],
                 'perms': ent['perms'],
                 'status': None,
                 'sha1': None,
                 'sha1_git': None,
                 'sha256': None,
            }
            for ent in sorted(self.dir['entries'], key=lambda ent: ent['name'])
        ]

        self.assertEqual(data_to_store, stored_data)

        after_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([], after_missing)

    @istest
    def directory_entry_get_by_path(self):
        # given
        init_missing = list(self.storage.directory_missing([self.dir3['id']]))
        self.assertEqual([self.dir3['id']], init_missing)

        self.storage.directory_add([self.dir3])

        expected_entries = [
            {
                'dir_id': self.dir3['id'],
                'name': b'foo',
                'type': 'file',
                'target': self.cont['sha1_git'],
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'status': None,
                'perms': 0o644,
            },
            {
                'dir_id': self.dir3['id'],
                'name': b'bar',
                'type': 'dir',
                'target': b'12345678901234560000',
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'status': None,
                'perms': 0o2000,
            },
            {
                'dir_id': self.dir3['id'],
                'name': b'hello',
                'type': 'file',
                'target': b'12345678901234567890',
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'status': None,
                'perms': 0o644,
            },
        ]

        # when (all must be found here)
        for entry, expected_entry in zip(self.dir3['entries'],
                                         expected_entries):
            actual_entry = self.storage.directory_entry_get_by_path(
                self.dir3['id'],
                [entry['name']])
            self.assertEqual(actual_entry, expected_entry)

        # when (nothing should be found here since self.dir is not persisted.)
        for entry in self.dir['entries']:
            actual_entry = self.storage.directory_entry_get_by_path(
                self.dir['id'],
                [entry['name']])
            self.assertIsNone(actual_entry)

    @istest
    def revision_add(self):
        init_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([self.revision['id']], list(init_missing))

        self.storage.revision_add([self.revision])

        end_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([], list(end_missing))

    @istest
    def revision_log(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])

        # when
        actual_results = list(self.storage.revision_log(
            [self.revision4['id']]))

        # hack: ids generated
        for actual_result in actual_results:
            del actual_result['author']['id']
            del actual_result['committer']['id']

        self.assertEqual(len(actual_results), 2)  # rev4 -child-> rev3
        self.assertEquals(actual_results[0], self.revision4)
        self.assertEquals(actual_results[1], self.revision3)

    @istest
    def revision_log_with_limit(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])
        actual_results = list(self.storage.revision_log(
            [self.revision4['id']], 1))

        # hack: ids generated
        for actual_result in actual_results:
            del actual_result['author']['id']
            del actual_result['committer']['id']

        self.assertEqual(len(actual_results), 1)
        self.assertEquals(actual_results[0], self.revision4)

    @staticmethod
    def _short_revision(revision):
        return [revision['id'], revision['parents']]

    @istest
    def revision_shortlog(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])

        # when
        actual_results = list(self.storage.revision_shortlog(
            [self.revision4['id']]))

        self.assertEqual(len(actual_results), 2)  # rev4 -child-> rev3
        self.assertEquals(list(actual_results[0]),
                          self._short_revision(self.revision4))
        self.assertEquals(list(actual_results[1]),
                          self._short_revision(self.revision3))

    @istest
    def revision_shortlog_with_limit(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])
        actual_results = list(self.storage.revision_shortlog(
            [self.revision4['id']], 1))

        self.assertEqual(len(actual_results), 1)
        self.assertEquals(list(actual_results[0]),
                          self._short_revision(self.revision4))

    @istest
    def revision_get(self):
        self.storage.revision_add([self.revision])

        actual_revisions = list(self.storage.revision_get(
            [self.revision['id'], self.revision2['id']]))

        # when
        del actual_revisions[0]['author']['id']  # hack: ids are generated
        del actual_revisions[0]['committer']['id']

        self.assertEqual(len(actual_revisions), 2)
        self.assertEqual(actual_revisions[0], self.revision)
        self.assertIsNone(actual_revisions[1])

    @istest
    def revision_get_no_parents(self):
        self.storage.revision_add([self.revision3])

        get = list(self.storage.revision_get([self.revision3['id']]))

        self.assertEqual(len(get), 1)
        self.assertEqual(get[0]['parents'], [])  # no parents on this one

    @istest
    def revision_get_by(self):
        # given
        self.storage.content_add([self.cont2])
        self.storage.directory_add([self.dir2])  # point to self.cont
        self.storage.revision_add([self.revision2])  # points to self.dir
        origin_id = self.storage.origin_add_one(self.origin2)

        # occurrence2 points to 'revision2' with branch 'master', we
        # need to point to the right origin
        occurrence2 = self.occurrence2.copy()
        occurrence2.update({'origin': origin_id})
        self.storage.occurrence_add([occurrence2])

        # we want only revision 2
        expected_revisions = list(self.storage.revision_get(
            [self.revision2['id']]))

        # when
        actual_results = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            None))

        self.assertEqual(actual_results[0], expected_revisions[0])

        # when (with no branch filtering, it's still ok)
        actual_results = list(self.storage.revision_get_by(
            origin_id,
            None,
            None))

        self.assertEqual(actual_results[0], expected_revisions[0])

    @istest
    def revision_get_by_multiple_occurrence(self):
        # 2 occurrences pointing to 2 different revisions
        # each occurence have 1 hour delta
        # the api must return the revision whose occurrence is the nearest.

        # given
        self.storage.content_add([self.cont2])
        self.storage.directory_add([self.dir2])
        self.storage.revision_add([self.revision2, self.revision3])
        origin_id = self.storage.origin_add_one(self.origin2)

        # occurrence2 points to 'revision2' with branch 'master', we
        # need to point to the right origin
        occurrence2 = self.occurrence2.copy()
        occurrence2.update({'origin': origin_id,
                            'date': occurrence2['date']})

        dt = datetime.timedelta(days=1)

        occurrence3 = self.occurrence2.copy()
        occurrence3.update({'origin': origin_id,
                            'date': occurrence3['date'] + dt,
                            'target': self.revision3['id']})

        # 2 occurrences on same revision with lower validity date with 1h delta
        self.storage.occurrence_add([occurrence2])
        self.storage.occurrence_add([occurrence3])

        # when
        actual_results0 = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            occurrence2['date']))

        # hack: ids are generated
        del actual_results0[0]['author']['id']
        del actual_results0[0]['committer']['id']

        self.assertEquals(len(actual_results0), 1)
        self.assertEqual(actual_results0, [self.revision2])

        # when
        actual_results1 = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            occurrence2['date'] + dt/3))  # closer to occurrence2

        # hack: ids are generated
        del actual_results1[0]['author']['id']
        del actual_results1[0]['committer']['id']

        self.assertEquals(len(actual_results1), 1)
        self.assertEqual(actual_results1, [self.revision2])

        # when
        actual_results2 = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            occurrence2['date'] + 2*dt/3))  # closer to occurrence3

        del actual_results2[0]['author']['id']
        del actual_results2[0]['committer']['id']

        self.assertEquals(len(actual_results2), 1)
        self.assertEqual(actual_results2, [self.revision3])

        # when
        actual_results3 = list(self.storage.revision_get_by(
            origin_id,
            occurrence3['branch'],
            occurrence3['date']))

        # hack: ids are generated
        del actual_results3[0]['author']['id']
        del actual_results3[0]['committer']['id']

        self.assertEquals(len(actual_results3), 1)
        self.assertEqual(actual_results3, [self.revision3])

        # when
        actual_results4 = list(self.storage.revision_get_by(
            origin_id,
            None,
            None))

        for actual_result in actual_results4:
            del actual_result['author']['id']
            del actual_result['committer']['id']

        self.assertEquals(len(actual_results4), 2)
        self.assertCountEqual(actual_results4,
                              [self.revision3, self.revision2])

    @istest
    def release_add(self):
        init_missing = self.storage.release_missing([self.release['id'],
                                                     self.release2['id']])
        self.assertEqual([self.release['id'], self.release2['id']],
                         list(init_missing))

        self.storage.release_add([self.release, self.release2])

        end_missing = self.storage.release_missing([self.release['id'],
                                                    self.release2['id']])
        self.assertEqual([], list(end_missing))

    @istest
    def release_get(self):
        # given
        self.storage.release_add([self.release, self.release2])

        # when
        actual_releases = list(self.storage.release_get([self.release['id'],
                                                         self.release2['id']]))

        # then
        for actual_release in actual_releases:
            del actual_release['author']['id']  # hack: ids are generated

        self.assertEquals([self.release, self.release2],
                          [actual_releases[0], actual_releases[1]])

    @istest
    def release_get_by(self):
        # given
        self.storage.revision_add([self.revision2])  # points to self.dir
        self.storage.release_add([self.release3])
        origin_id = self.storage.origin_add_one(self.origin2)

        # occurrence2 points to 'revision2' with branch 'master', we
        # need to point to the right origin
        occurrence2 = self.occurrence2.copy()
        occurrence2.update({'origin': origin_id})
        self.storage.occurrence_add([occurrence2])

        # we want only revision 2
        expected_releases = list(self.storage.release_get(
            [self.release3['id']]))

        # when
        actual_results = list(self.storage.release_get_by(
            occurrence2['origin']))

        # then
        self.assertEqual(actual_results[0], expected_releases[0])

    @istest
    def origin_add_one(self):
        origin0 = self.storage.origin_get(self.origin)
        self.assertIsNone(origin0)

        id = self.storage.origin_add_one(self.origin)

        actual_origin = self.storage.origin_get({'url': self.origin['url'],
                                                 'type': self.origin['type']})
        self.assertEqual(actual_origin['id'], id)

        id2 = self.storage.origin_add_one(self.origin)

        self.assertEqual(id, id2)

    @istest
    def origin_get(self):
        self.assertIsNone(self.storage.origin_get(self.origin))
        id = self.storage.origin_add_one(self.origin)

        # lookup per type and url (returns id)
        actual_origin0 = self.storage.origin_get({'url': self.origin['url'],
                                                  'type': self.origin['type']})
        self.assertEqual(actual_origin0['id'], id)

        # lookup per id (returns dict)
        actual_origin1 = self.storage.origin_get({'id': id})

        self.assertEqual(actual_origin1, {'id': id,
                                          'type': self.origin['type'],
                                          'url': self.origin['url'],
                                          'lister': None,
                                          'project': None})

    @istest
    def occurrence_add(self):
        origin_id = self.storage.origin_add_one(self.origin2)

        revision = self.revision.copy()
        revision['id'] = self.occurrence['target']
        self.storage.revision_add([revision])

        occur = self.occurrence
        occur['origin'] = origin_id
        self.storage.occurrence_add([occur])
        self.storage.occurrence_add([occur])

        test_query = '''
        with indiv_occurrences as (
          select origin, branch, target, target_type, unnest(visits) as visit
          from occurrence_history
        )
        select origin, branch, target, target_type, date
        from indiv_occurrences
        left join origin_visit using(origin, visit)
        order by origin, date'''

        self.cursor.execute(test_query)
        ret = self.cursor.fetchall()
        self.assertEqual(len(ret), 1)
        self.assertEqual(
            (ret[0][0], ret[0][1].tobytes(), ret[0][2].tobytes(),
             ret[0][3], ret[0][4]),
            (occur['origin'], occur['branch'], occur['target'],
             occur['target_type'], occur['date']))

        orig_date = occur['date']
        occur['date'] += datetime.timedelta(hours=10)
        self.storage.occurrence_add([occur])

        self.cursor.execute(test_query)
        ret = self.cursor.fetchall()
        self.assertEqual(len(ret), 2)
        self.assertEqual(
            (ret[0][0], ret[0][1].tobytes(), ret[0][2].tobytes(),
             ret[0][3], ret[0][4]),
            (occur['origin'], occur['branch'], occur['target'],
             occur['target_type'], orig_date))
        self.assertEqual(
            (ret[1][0], ret[1][1].tobytes(), ret[1][2].tobytes(),
             ret[1][3], ret[1][4]),
            (occur['origin'], occur['branch'], occur['target'],
             occur['target_type'], occur['date']))

    @istest
    def occurrence_get(self):
        # given
        origin_id = self.storage.origin_add_one(self.origin2)

        revision = self.revision.copy()
        revision['id'] = self.occurrence['target']
        self.storage.revision_add([revision])

        occur = self.occurrence
        occur['origin'] = origin_id
        self.storage.occurrence_add([occur])
        self.storage.occurrence_add([occur])

        # when
        actual_occurrence = list(self.storage.occurrence_get(origin_id))

        # then
        expected_occur = occur.copy()
        del expected_occur['date']

        self.assertEquals(len(actual_occurrence), 1)
        self.assertEquals(actual_occurrence[0], expected_occur)

    @istest
    def content_find_occurrence_with_present_content(self):
        # 1. with something to find
        # given
        self.storage.content_add([self.cont2])
        self.storage.directory_add([self.dir2])  # point to self.cont
        self.storage.revision_add([self.revision2])  # points to self.dir
        origin_id = self.storage.origin_add_one(self.origin2)
        occurrence = self.occurrence2
        occurrence.update({'origin': origin_id})
        self.storage.occurrence_add([occurrence])

        # when
        occ = self.storage.content_find_occurrence(
            {'sha1': self.cont2['sha1']})

        # then
        self.assertEquals(occ['origin_type'], self.origin2['type'])
        self.assertEquals(occ['origin_url'], self.origin2['url'])
        self.assertEquals(occ['branch'], self.occurrence2['branch'])
        self.assertEquals(occ['target'], self.revision2['id'])
        self.assertEquals(occ['target_type'], self.occurrence2['target_type'])
        self.assertEquals(occ['path'], self.dir2['entries'][0]['name'])

        occ2 = self.storage.content_find_occurrence(
            {'sha1_git': self.cont2['sha1_git']})

        self.assertEquals(occ2['origin_type'], self.origin2['type'])
        self.assertEquals(occ2['origin_url'], self.origin2['url'])
        self.assertEquals(occ2['branch'], self.occurrence2['branch'])
        self.assertEquals(occ2['target'], self.revision2['id'])
        self.assertEquals(occ2['target_type'], self.occurrence2['target_type'])
        self.assertEquals(occ2['path'], self.dir2['entries'][0]['name'])

        occ3 = self.storage.content_find_occurrence(
            {'sha256': self.cont2['sha256']})

        self.assertEquals(occ3['origin_type'], self.origin2['type'])
        self.assertEquals(occ3['origin_url'], self.origin2['url'])
        self.assertEquals(occ3['branch'], self.occurrence2['branch'])
        self.assertEquals(occ3['target'], self.revision2['id'])
        self.assertEquals(occ3['target_type'], self.occurrence2['target_type'])
        self.assertEquals(occ3['path'], self.dir2['entries'][0]['name'])

    @istest
    def content_find_occurrence_with_non_present_content(self):
        # 1. with something that does not exist
        missing_cont = self.missing_cont

        occ = self.storage.content_find_occurrence(
            {'sha1': missing_cont['sha1']})

        self.assertEquals(occ, None,
                          "Content does not exist so no occurrence")

        # 2. with something that does not exist
        occ = self.storage.content_find_occurrence(
            {'sha1_git': missing_cont['sha1_git']})

        self.assertEquals(occ, None,
                          "Content does not exist so no occurrence")

        # 3. with something that does not exist
        occ = self.storage.content_find_occurrence(
            {'sha256': missing_cont['sha256']})

        self.assertEquals(occ, None,
                          "Content does not exist so no occurrence")

    @istest
    def content_find_occurrence_bad_input(self):
        # 1. with bad input
        with self.assertRaises(ValueError) as cm:
            self.storage.content_find_occurrence({})  # empty is bad
        self.assertIn('content keys', cm.exception.args[0])

        # 2. with bad input
        with self.assertRaises(ValueError) as cm:
            self.storage.content_find_occurrence(
                {'unknown-sha1': 'something'})  # not the right key
        self.assertIn('content keys', cm.exception.args[0])

    @istest
    def entity_get_from_lister_metadata(self):
        self.storage.entity_add([self.entity1])

        fetched_entities = list(
            self.storage.entity_get_from_lister_metadata(
                [self.entity1_query, self.entity2_query]))

        # Entity 1 should have full metadata, with last_seen/last_id instead
        # of validity
        entity1 = self.entity1.copy()
        entity1['last_seen'] = entity1['validity'][0]
        del fetched_entities[0]['last_id']
        del entity1['validity']
        # Entity 2 should have no metadata
        entity2 = {
            'uuid': None,
            'lister_metadata': self.entity2_query.copy(),
        }

        self.assertEquals(fetched_entities, [entity1, entity2])

    @istest
    def entity_get_from_lister_metadata_twice(self):
        self.storage.entity_add([self.entity1])

        fetched_entities1 = list(
            self.storage.entity_get_from_lister_metadata(
                [self.entity1_query]))
        fetched_entities2 = list(
            self.storage.entity_get_from_lister_metadata(
                [self.entity1_query]))

        self.assertEquals(fetched_entities1, fetched_entities2)

    @istest
    def entity_get(self):
        # given
        self.storage.entity_add([self.entity4])
        self.storage.entity_add([self.entity3])

        # when: entity3 -child-of-> entity4
        actual_entity3 = list(self.storage.entity_get(self.entity3['uuid']))

        self.assertEquals(len(actual_entity3), 2)
        # remove dynamic data (modified by db)
        entity3 = self.entity3.copy()
        entity4 = self.entity4.copy()
        del entity3['validity']
        del entity4['validity']
        del actual_entity3[0]['last_seen']
        del actual_entity3[0]['last_id']
        del actual_entity3[1]['last_seen']
        del actual_entity3[1]['last_id']
        self.assertEquals(actual_entity3, [entity3, entity4])

        # when: entity4 only child
        actual_entity4 = list(self.storage.entity_get(self.entity4['uuid']))

        self.assertEquals(len(actual_entity4), 1)
        # remove dynamic data (modified by db)
        entity4 = self.entity4.copy()
        del entity4['validity']
        del actual_entity4[0]['last_id']
        del actual_entity4[0]['last_seen']

        self.assertEquals(actual_entity4, [entity4])

    @istest
    def entity_get_one(self):
        # given
        self.storage.entity_add([self.entity3, self.entity4])

        # when: entity3 -child-of-> entity4
        actual_entity3 = self.storage.entity_get_one(self.entity3['uuid'])

        # remove dynamic data (modified by db)
        entity3 = self.entity3.copy()
        del entity3['validity']
        del actual_entity3['last_seen']
        del actual_entity3['last_id']
        self.assertEquals(actual_entity3, entity3)

    @istest
    def stat_counters(self):
        expected_keys = ['content', 'directory', 'directory_entry_dir',
                         'occurrence', 'origin', 'person', 'revision']
        counters = self.storage.stat_counters()

        self.assertTrue(set(expected_keys) <= set(counters))
        self.assertIsInstance(counters[expected_keys[0]], int)

    @istest
    def content_find_with_present_content(self):
        # 1. with something to find
        cont = self.cont
        self.storage.content_add([cont])

        actually_present = self.storage.content_find({'sha1': cont['sha1']})

        actually_present.pop('ctime')
        self.assertEqual(actually_present, {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'length': cont['length'],
            'status': 'visible'
        })

        # 2. with something to find
        actually_present = self.storage.content_find(
            {'sha1_git': cont['sha1_git']})

        actually_present.pop('ctime')
        self.assertEqual(actually_present, {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'length': cont['length'],
            'status': 'visible'
        })

        # 3. with something to find
        actually_present = self.storage.content_find(
            {'sha256': cont['sha256']})

        actually_present.pop('ctime')
        self.assertEqual(actually_present, {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'length': cont['length'],
            'status': 'visible'
        })

        # 4. with something to find
        actually_present = self.storage.content_find(
            {'sha1': cont['sha1'],
             'sha1_git': cont['sha1_git'],
             'sha256': cont['sha256']})

        actually_present.pop('ctime')
        self.assertEqual(actually_present, {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'length': cont['length'],
            'status': 'visible'
        })

    @istest
    def content_find_with_non_present_content(self):
        # 1. with something that does not exist
        missing_cont = self.missing_cont

        actually_present = self.storage.content_find(
            {'sha1': missing_cont['sha1']})

        self.assertIsNone(actually_present)

        # 2. with something that does not exist
        actually_present = self.storage.content_find(
            {'sha1_git': missing_cont['sha1_git']})

        self.assertIsNone(actually_present)

        # 3. with something that does not exist
        actually_present = self.storage.content_find(
            {'sha256': missing_cont['sha256']})

        self.assertIsNone(actually_present)

    @istest
    def content_find_bad_input(self):
        # 1. with bad input
        with self.assertRaises(ValueError):
            self.storage.content_find({})  # empty is bad

        # 2. with bad input
        with self.assertRaises(ValueError):
            self.storage.content_find(
                {'unknown-sha1': 'something'})  # not the right key

    @istest
    def object_find_by_sha1_git(self):
        sha1_gits = [b'00000000000000000000']
        expected = {
            b'00000000000000000000': [],
        }

        self.storage.content_add([self.cont])
        sha1_gits.append(self.cont['sha1_git'])
        expected[self.cont['sha1_git']] = [{
            'sha1_git': self.cont['sha1_git'],
            'type': 'content',
            'id': self.cont['sha1'],
        }]

        self.storage.directory_add([self.dir])
        sha1_gits.append(self.dir['id'])
        expected[self.dir['id']] = [{
            'sha1_git': self.dir['id'],
            'type': 'directory',
            'id': self.dir['id'],
        }]

        self.storage.revision_add([self.revision])
        sha1_gits.append(self.revision['id'])
        expected[self.revision['id']] = [{
            'sha1_git': self.revision['id'],
            'type': 'revision',
            'id': self.revision['id'],
        }]

        self.storage.release_add([self.release])
        sha1_gits.append(self.release['id'])
        expected[self.release['id']] = [{
            'sha1_git': self.release['id'],
            'type': 'release',
            'id': self.release['id'],
        }]

        ret = self.storage.object_find_by_sha1_git(sha1_gits)
        for val in ret.values():
            for obj in val:
                del obj['object_id']

        self.assertEqual(expected, ret)


class TestStorage(AbstractTestStorage, unittest.TestCase):
    """Test the local storage"""

    # Can only be tested with local storage as you can't mock
    # datetimes for the remote server
    @istest
    def fetch_history(self):
        origin = self.storage.origin_add_one(self.origin)
        with patch('datetime.datetime'):
            datetime.datetime.now.return_value = self.fetch_history_date
            fetch_history_id = self.storage.fetch_history_start(origin)
            datetime.datetime.now.assert_called_with(tz=datetime.timezone.utc)

        with patch('datetime.datetime'):
            datetime.datetime.now.return_value = self.fetch_history_end
            self.storage.fetch_history_end(fetch_history_id,
                                           self.fetch_history_data)

        fetch_history = self.storage.fetch_history_get(fetch_history_id)
        expected_fetch_history = self.fetch_history_data.copy()

        expected_fetch_history['id'] = fetch_history_id
        expected_fetch_history['origin'] = origin
        expected_fetch_history['date'] = self.fetch_history_date
        expected_fetch_history['duration'] = self.fetch_history_duration

        self.assertEqual(expected_fetch_history, fetch_history)

    @istest
    def person_get(self):
        # given
        person0 = {
            'fullname': b'bob <alice@bob>',
            'name': b'bob',
            'email': b'alice@bob',
        }
        id0 = self.storage._person_add(person0)

        person1 = {
            'fullname': b'tony <tony@bob>',
            'name': b'tony',
            'email': b'tony@bob',
        }
        id1 = self.storage._person_add(person1)

        # when
        actual_persons = self.storage.person_get([id0, id1])

        # given (person injection through release for example)
        self.assertEqual(
            list(actual_persons), [
                {
                    'id': id0,
                    'fullname': person0['fullname'],
                    'name': person0['name'],
                    'email': person0['email'],
                },
                {
                    'id': id1,
                    'fullname': person1['fullname'],
                    'name': person1['name'],
                    'email': person1['email'],
                },
            ])
