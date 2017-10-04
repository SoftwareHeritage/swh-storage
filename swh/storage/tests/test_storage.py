# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
from operator import itemgetter
import psycopg2
import unittest
from uuid import UUID

from unittest.mock import patch

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.model import from_disk, identifiers
from swh.model.hashutil import hash_to_bytes
from swh.storage.db import cursor_to_bytes
from swh.core.tests.db_testing import DbTestFixture
from swh.storage.tests.storage_testing import StorageTestFixture


@attr('db')
class BaseTestStorage(StorageTestFixture, DbTestFixture):
    def setUp(self):
        super().setUp()

        db = self.test_db[self.TEST_STORAGE_DB_NAME]
        self.conn = db.conn
        self.cursor = db.cursor

        self.maxDiff = None

        self.cont = {
            'data': b'42\n',
            'length': 3,
            'sha1': hash_to_bytes(
                '34973274ccef6ab4dfaaf86599792fa9c3fe4689'),
            'sha1_git': hash_to_bytes(
                'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd'),
            'sha256': hash_to_bytes(
                '673650f936cb3b0a2f93ce09d81be107'
                '48b1b203c19e8176b4eefc1964a0cf3a'),
            'blake2s256': hash_to_bytes('d5fe1939576527e42cfd76a9455a2'
                                        '432fe7f56669564577dd93c4280e76d661d'),
            'status': 'visible',
        }

        self.cont2 = {
            'data': b'4242\n',
            'length': 5,
            'sha1': hash_to_bytes(
                '61c2b3a30496d329e21af70dd2d7e097046d07b7'),
            'sha1_git': hash_to_bytes(
                '36fade77193cb6d2bd826161a0979d64c28ab4fa'),
            'sha256': hash_to_bytes(
                '859f0b154fdb2d630f45e1ecae4a8629'
                '15435e663248bb8461d914696fc047cd'),
            'blake2s256': hash_to_bytes('849c20fad132b7c2d62c15de310adfe87be'
                                        '94a379941bed295e8141c6219810d'),
            'status': 'visible',
        }

        self.cont3 = {
            'data': b'424242\n',
            'length': 7,
            'sha1': hash_to_bytes(
                '3e21cc4942a4234c9e5edd8a9cacd1670fe59f13'),
            'sha1_git': hash_to_bytes(
                'c932c7649c6dfa4b82327d121215116909eb3bea'),
            'sha256': hash_to_bytes(
                '92fb72daf8c6818288a35137b72155f5'
                '07e5de8d892712ab96277aaed8cf8a36'),
            'blake2s256': hash_to_bytes('76d0346f44e5a27f6bafdd9c2befd304af'
                                        'f83780f93121d801ab6a1d4769db11'),
            'status': 'visible',
        }

        self.missing_cont = {
            'data': b'missing\n',
            'length': 8,
            'sha1': hash_to_bytes(
                'f9c24e2abb82063a3ba2c44efd2d3c797f28ac90'),
            'sha1_git': hash_to_bytes(
                '33e45d56f88993aae6a0198013efa80716fd8919'),
            'sha256': hash_to_bytes(
                '6bbd052ab054ef222c1c87be60cd191a'
                'ddedd24cc882d1f5f7f7be61dc61bb3a'),
            'blake2s256': hash_to_bytes('306856b8fd879edb7b6f1aeaaf8db9bbecc9'
                                        '93cd7f776c333ac3a782fa5c6eba'),
            'status': 'absent',
        }

        self.skipped_cont = {
            'length': 1024 * 1024 * 200,
            'sha1_git': hash_to_bytes(
                '33e45d56f88993aae6a0198013efa80716fd8920'),
            'sha1': hash_to_bytes(
                '43e45d56f88993aae6a0198013efa80716fd8920'),
            'sha256': hash_to_bytes(
                '7bbd052ab054ef222c1c87be60cd191a'
                'ddedd24cc882d1f5f7f7be61dc61bb3a'),
            'blake2s256': hash_to_bytes(
                'ade18b1adecb33f891ca36664da676e1'
                '2c772cc193778aac9a137b8dc5834b9b'),
            'reason': 'Content too long',
            'status': 'absent',
        }

        self.skipped_cont2 = {
            'length': 1024 * 1024 * 300,
            'sha1_git': hash_to_bytes(
                '44e45d56f88993aae6a0198013efa80716fd8921'),
            'sha1': hash_to_bytes(
                '54e45d56f88993aae6a0198013efa80716fd8920'),
            'sha256': hash_to_bytes(
                '8cbd052ab054ef222c1c87be60cd191a'
                'ddedd24cc882d1f5f7f7be61dc61bb3a'),
            'blake2s256': hash_to_bytes(
                '9ce18b1adecb33f891ca36664da676e1'
                '2c772cc193778aac9a137b8dc5834b9b'),
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
                    'perms': from_disk.DentryPerms.content,
                },
                {
                    'name': b'bar\xc3',
                    'type': 'dir',
                    'target': b'12345678901234567890',
                    'perms': from_disk.DentryPerms.directory,
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
                    'perms': from_disk.DentryPerms.content,
                }
            ],
        }

        self.dir3 = {
            'id': hash_to_bytes('33e45d56f88993aae6a0198013efa80716fd8921'),
            'entries': [
                {
                    'name': b'foo',
                    'type': 'file',
                    'target': self.cont['sha1_git'],
                    'perms': from_disk.DentryPerms.content,
                },
                {
                    'name': b'bar',
                    'type': 'dir',
                    'target': b'12345678901234560000',
                    'perms': from_disk.DentryPerms.directory,
                },
                {
                    'name': b'hello',
                    'type': 'file',
                    'target': b'12345678901234567890',
                    'perms': from_disk.DentryPerms.content,
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
                'timestamp': {
                    'seconds': 1234567843,
                    'microseconds': 220000,
                },
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
            'id': hash_to_bytes('7026b7c1a2af56521e951c01ed20f255fa054238'),
            'message': b'a simple revision with no parents this time',
            'author': {
                'name': b'Roberto Dicosmo',
                'email': b'roberto@example.com',
                'fullname': b'Roberto Dicosmo <roberto@example.com>',
            },
            'date': {
                'timestamp': {
                    'seconds': 1234567843,
                    'microseconds': 220000,
                },
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
            'id': hash_to_bytes('368a48fe15b7db2383775f97c6b247011b3f14f4'),
            'message': b'parent of self.revision2',
            'author': {
                'name': b'me',
                'email': b'me@soft.heri',
                'fullname': b'me <me@soft.heri>',
            },
            'date': {
                'timestamp': {
                    'seconds': 1244567843,
                    'microseconds': 220000,
                },
                'offset': -720,
                'negative_utc': None,
            },
            'committer': {
                'name': b'committer-dude',
                'email': b'committer@dude.com',
                'fullname': b'committer-dude <committer@dude.com>',
            },
            'committer_date': {
                'timestamp': {
                    'seconds': 1244567843,
                    'microseconds': 220000,
                },
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

        self.origin_metadata = {
            'origin': self.origin,
            'date': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'provenance': 'deposit-hal',
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
             }
        }

        self.origin_metadata2 = {
            'origin': self.origin,
            'date': datetime.datetime(2017, 1, 1, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'provenance': 'lister-github',
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
             }
        }

        self.date_visit1 = datetime.datetime(2015, 1, 1, 23, 0, 0,
                                             tzinfo=datetime.timezone.utc)

        self.occurrence = {
            'branch': b'master',
            'target': b'67890123456789012345',
            'target_type': 'revision',
        }

        self.date_visit2 = datetime.datetime(2015, 1, 1, 23, 0, 0,
                                             tzinfo=datetime.timezone.utc)

        self.occurrence2 = {
            'branch': b'master',
            'target': self.revision2['id'],
            'target_type': 'revision',
        }

        self.date_visit3 = datetime.datetime(2015, 1, 1, 23, 0, 0,
                                             tzinfo=datetime.timezone.utc)

        # template occurrence to be filled in test (cf. revision_log_by)
        self.occurrence3 = {
            'branch': b'master',
            'target_type': 'revision',
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
        self.reset_storage_tables()
        super().tearDown()

    def fetch_tools(self):
        tools = {}
        self.cursor.execute('''select tool_name, id, tool_version,
                                      tool_configuration
                               from indexer_configuration''')
        for row in self.cursor.fetchall():
            tools[row[0]] = {
                'id': row[1],
                'name': row[0],
                'version': row[2],
                'configuration': row[3]
            }

        return tools


class CommonTestStorage(BaseTestStorage):
    """Base class for Storage testing.

    This class is used as-is to test local storage (see TestStorage
    below) and remote storage (see TestRemoteStorage in
    test_remote_storage.py.

    We need to have the two classes inherit from this base class
    separately to avoid nosetests running the tests from the base
    class twice.

    """

    @staticmethod
    def normalize_entity(entity):
        entity = copy.deepcopy(entity)
        for key in ('date', 'committer_date'):
            if key in entity:
                entity[key] = identifiers.normalize_timestamp(entity[key])

        return entity

    @istest
    def check_config(self):
        self.assertTrue(self.storage.check_config(check_write=True))
        self.assertTrue(self.storage.check_config(check_write=False))

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
        cont = self.skipped_cont.copy()
        cont2 = self.skipped_cont2.copy()
        cont2['blake2s256'] = None

        self.storage.content_add([cont, cont, cont2])

        self.cursor.execute('SELECT sha1, sha1_git, sha256, blake2s256, '
                            'length, status, reason '
                            'FROM skipped_content ORDER BY sha1_git')

        datums = self.cursor.fetchall()

        self.assertEquals(2, len(datums))
        datum = datums[0]
        self.assertEqual(
            (datum[0].tobytes(), datum[1].tobytes(), datum[2].tobytes(),
             datum[3].tobytes(), datum[4], datum[5], datum[6]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['blake2s256'], cont['length'], 'absent',
             'Content too long')
        )

        datum2 = datums[1]
        self.assertEqual(
            (datum2[0].tobytes(), datum2[1].tobytes(), datum2[2].tobytes(),
             datum2[3], datum2[4], datum2[5], datum2[6]),
            (cont2['sha1'], cont2['sha1_git'], cont2['sha256'],
             cont2['blake2s256'], cont2['length'], 'absent',
             'Content too long')
        )

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
    def content_get_metadata(self):
        cont1 = self.cont.copy()
        cont2 = self.cont2.copy()

        self.storage.content_add([cont1, cont2])

        gen = self.storage.content_get_metadata([cont1['sha1'], cont2['sha1']])

        # we only retrieve the metadata
        cont1.pop('data')
        cont2.pop('data')

        self.assertEqual(list(gen), [cont1, cont2])

    @istest
    def content_get_metadata_missing_sha1(self):
        cont1 = self.cont.copy()
        cont2 = self.cont2.copy()

        missing_cont = self.missing_cont.copy()

        self.storage.content_add([cont1, cont2])

        gen = self.storage.content_get_metadata([missing_cont['sha1']])

        # All the metadata keys are None
        missing_cont.pop('data')
        for key in list(missing_cont):
            if key != 'sha1':
                missing_cont[key] = None

        self.assertEqual(list(gen), [missing_cont])

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

        data_to_store = []
        for ent in sorted(self.dir['entries'], key=itemgetter('name')):
            data_to_store.append({
                'dir_id': self.dir['id'],
                'type': ent['type'],
                'target': ent['target'],
                'name': ent['name'],
                'perms': ent['perms'],
                'status': None,
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'length': None,
            })

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
                'perms': from_disk.DentryPerms.content,
                'length': None,
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
                'perms': from_disk.DentryPerms.directory,
                'length': None,
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
                'perms': from_disk.DentryPerms.content,
                'length': None,
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

    def cache_content_revision_objects(self):
        self.storage.content_add([self.cont, self.cont2, self.cont3])
        directory = {
            'id': b'4\x013\x422\x531\x000\xf51\xe62\xa73\xff7\xc3\xa90',
            'entries': [
                {
                    'name': b'bar',
                    'type': 'file',
                    'target': self.cont2['sha1_git'],
                    'perms': from_disk.DentryPerms.content,
                },
                {
                    'name': b'foo',
                    'type': 'file',
                    'target': self.cont['sha1_git'],
                    'perms': from_disk.DentryPerms.content,
                },
                {
                    'name': b'bar\xc3',
                    'type': 'dir',
                    'target': b'12345678901234567890',
                    'perms': from_disk.DentryPerms.directory,
                },
            ],
        }
        directory2 = copy.deepcopy(directory)
        directory2['id'] = (directory2['id'][:-1] +
                            bytes([(directory2['id'][-1] + 1) % 256]))
        directory2['entries'][1] = {
            'name': b'foo',
            'type': 'file',
            'target': self.cont3['sha1_git'],
            'perms': from_disk.DentryPerms.content,
        }

        self.storage.directory_add([directory, directory2])
        revision = self.revision.copy()
        revision['directory'] = directory['id']
        revision2 = copy.deepcopy(revision)
        revision2['parents'] = [revision['id']]
        revision2['directory'] = directory2['id']
        revision2['id'] = (revision2['id'][:-1] +
                           bytes([(revision2['id'][-1] + 1) % 256]))
        self.storage.revision_add([revision, revision2])
        return (directory, directory2, revision, revision2)

    @istest
    def cache_content_revision_add(self):
        # Create a real arborescence tree (contents + directory) and a
        # revision targeting that directory.
        # Assert the cache is empty for that revision
        # Then create that revision
        # Trigger the cache population for that revision
        # Assert the cache now contains information for that revision
        # Trigger again the cache population for that revision
        # Assert the cache is not modified

        # given ()
        (directory, directory2,
         revision, revision2) = self.cache_content_revision_objects()

        # assert nothing in cache yet
        count_query = '''select count(*)
                         from cache_content_revision'''
        self.cursor.execute(count_query)
        ret = self.cursor.fetchone()
        self.assertEqual(ret, (0, ))

        # when, triggered the first time, we cache the revision
        self.storage.cache_content_revision_add([revision['id']])
        # the second time, we do nothing as this is already done
        self.storage.cache_content_revision_add([revision['id']])

        # then
        self.cursor.execute(count_query)
        ret = self.cursor.fetchone()
        # only 2 contents exists for that revision (the second call to
        # revision_cache discards as the revision is already cached)
        self.assertEqual(ret, (2, ))

        self.cursor.execute('select * from cache_content_revision')
        ret = self.cursor.fetchall()

        expected_cache_entries = [
            (directory['entries'][0]['target'], False,
             [[revision['id'], directory['entries'][0]['name']]]),
            (directory['entries'][1]['target'], False,
             [[revision['id'], directory['entries'][1]['name']]])
        ]
        for i, expected_entry in enumerate(expected_cache_entries):
            ret_entry = (ret[i][0].tobytes(), ret[i][1],
                         [[ret[i][2][0][0].tobytes(),
                           ret[i][2][0][1].tobytes()]])
            self.assertEquals(ret_entry, expected_entry)

    @istest
    def cache_content_revision_add_twice(self):
        # given ()
        (directory, directory2,
         revision, revision2) = self.cache_content_revision_objects()

        # assert nothing in cache yet
        count_query = '''select count(*)
                         from cache_content_revision'''
        self.cursor.execute(count_query)
        ret = self.cursor.fetchone()
        self.assertEqual(ret, (0, ))

        # when, triggered the first time, we cache the revision
        self.storage.cache_content_revision_add([revision['id']])
        # the second time, we do nothing as this is already done
        self.storage.cache_content_revision_add([revision2['id']])

        # then
        self.cursor.execute('select * from cache_content_revision')
        cache_entries = {
            content.tobytes(): [[rev.tobytes(), path.tobytes()]
                                for rev, path in rev_paths]
            for content, blacklisted, rev_paths in self.cursor.fetchall()
        }

        self.assertEquals(len(cache_entries), 3)
        self.assertEquals(len(cache_entries[self.cont['sha1_git']]), 1)
        self.assertEquals(len(cache_entries[self.cont2['sha1_git']]), 2)
        self.assertEquals(len(cache_entries[self.cont3['sha1_git']]), 1)

    @istest
    def cache_content_get_all(self):
        # given
        (directory, directory2,
         revision, revision2) = self.cache_content_revision_objects()

        # assert nothing in cache yet
        test_query = '''select sha1, sha1_git, sha256, ccr.revision_paths
                        from cache_content_revision ccr
                        inner join content c on c.sha1_git=ccr.content'''

        self.storage.cache_content_revision_add([revision['id']])
        self.cursor.execute(test_query, (revision['id'],))
        ret = list(cursor_to_bytes(self.cursor))

        self.assertEqual(len(ret), 2)

        expected_contents = []
        for entry in ret:
            expected_contents.append(dict(
                zip(['sha1', 'sha1_git', 'sha256', 'revision_paths'], entry)))

        # 1. default filters gives everything
        actual_cache_contents = list(self.storage.cache_content_get_all())

        self.assertEquals(actual_cache_contents, expected_contents)

    @istest
    def cache_content_get(self):
        # given
        (directory, directory2,
         revision, revision2) = self.cache_content_revision_objects()

        # assert nothing in cache yet
        test_query = '''select c.sha1, c.sha1_git, c.sha256, ccr.revision_paths
                        from cache_content_revision ccr
                        inner join content c on c.sha1_git=ccr.content
                        where ccr.content=%s'''

        self.storage.cache_content_revision_add([revision['id']])
        self.cursor.execute(test_query, (self.cont2['sha1_git'],))
        ret = list(cursor_to_bytes(self.cursor))[0]

        self.assertIsNotNone(ret)

        expected_content = dict(
            zip(['sha1', 'sha1_git', 'sha256', 'revision_paths'], ret))

        # when
        actual_cache_content = self.storage.cache_content_get(self.cont2)

        # then
        self.assertEquals(actual_cache_content, expected_content)

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
        self.assertEquals(actual_results[0],
                          self.normalize_entity(self.revision4))
        self.assertEquals(actual_results[1],
                          self.normalize_entity(self.revision3))

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

    @istest
    def revision_log_by(self):
        # given
        origin_id = self.storage.origin_add_one(self.origin2)
        self.storage.revision_add([self.revision3,
                                   self.revision4])

        # occurrence3 targets 'revision4'
        # with branch 'master' and origin origin_id
        occurrence3 = self.occurrence3.copy()
        date_visit1 = self.date_visit3
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      date_visit1)
        occurrence3.update({
            'origin': origin_id,
            'target': self.revision4['id'],
            'visit': origin_visit1['visit'],
        })

        self.storage.occurrence_add([occurrence3])

        # self.revision4 -is-child-of-> self.revision3
        # when
        actual_results = list(self.storage.revision_log_by(
            origin_id,
            branch_name=occurrence3['branch'],
            timestamp=date_visit1))

        # hack: ids generated
        for actual_result in actual_results:
            del actual_result['author']['id']
            del actual_result['committer']['id']

        self.assertEqual(len(actual_results), 2)
        self.assertEquals(actual_results[0],
                          self.normalize_entity(self.revision4))
        self.assertEquals(actual_results[1],
                          self.normalize_entity(self.revision3))

        # when - 2
        actual_results = list(self.storage.revision_log_by(
            origin_id,
            branch_name=None,
            timestamp=None,
            limit=1))

        # then
        for actual_result in actual_results:
            del actual_result['author']['id']
            del actual_result['committer']['id']

        self.assertEqual(len(actual_results), 1)
        self.assertEquals(actual_results[0], self.revision4)

        # when - 3 (revision not found)

        actual_res = list(self.storage.revision_log_by(
            origin_id,
            branch_name='inexistant-branch',
            timestamp=None))

        self.assertEquals(actual_res, [])

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
        self.assertEqual(actual_revisions[0],
                         self.normalize_entity(self.revision))
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
        date_visit1 = self.date_visit2
        origin_visit1 = self.storage.origin_visit_add(origin_id, date_visit1)
        occurrence2.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
        })
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
        # each occurence have 1 day delta
        # the api must return the revision whose occurrence is the nearest.

        # given
        self.storage.content_add([self.cont2])
        self.storage.directory_add([self.dir2])
        self.storage.revision_add([self.revision2, self.revision3])
        origin_id = self.storage.origin_add_one(self.origin2)

        # occurrence2 points to 'revision2' with branch 'master', we
        # need to point to the right origin
        date_visit1 = self.date_visit2
        origin_visit1 = self.storage.origin_visit_add(origin_id, date_visit1)
        occurrence2 = self.occurrence2.copy()
        occurrence2.update({
            'origin': origin_id,
            'visit': origin_visit1['visit']
        })

        dt = datetime.timedelta(days=1)
        date_visit2 = date_visit1 + dt
        origin_visit2 = self.storage.origin_visit_add(origin_id, date_visit2)
        occurrence3 = self.occurrence2.copy()
        occurrence3.update({
            'origin': origin_id,
            'visit': origin_visit2['visit'],
            'target': self.revision3['id'],
        })
        # 2 occurrences on same revision with lower validity date with 1 day
        # delta
        self.storage.occurrence_add([occurrence2])
        self.storage.occurrence_add([occurrence3])

        # when
        actual_results0 = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            date_visit1))

        # hack: ids are generated
        del actual_results0[0]['author']['id']
        del actual_results0[0]['committer']['id']

        self.assertEquals(len(actual_results0), 1)
        self.assertEqual(actual_results0,
                         [self.normalize_entity(self.revision2)])

        # when
        actual_results1 = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            date_visit1 + dt/3))  # closer to first visit

        # hack: ids are generated
        del actual_results1[0]['author']['id']
        del actual_results1[0]['committer']['id']

        self.assertEquals(len(actual_results1), 1)
        self.assertEqual(actual_results1,
                         [self.normalize_entity(self.revision2)])

        # when
        actual_results2 = list(self.storage.revision_get_by(
            origin_id,
            occurrence2['branch'],
            date_visit1 + 2*dt/3))  # closer to second visit

        del actual_results2[0]['author']['id']
        del actual_results2[0]['committer']['id']

        self.assertEquals(len(actual_results2), 1)
        self.assertEqual(actual_results2,
                         [self.normalize_entity(self.revision3)])

        # when
        actual_results3 = list(self.storage.revision_get_by(
            origin_id,
            occurrence3['branch'],
            date_visit2))

        # hack: ids are generated
        del actual_results3[0]['author']['id']
        del actual_results3[0]['committer']['id']

        self.assertEquals(len(actual_results3), 1)
        self.assertEqual(actual_results3,
                         [self.normalize_entity(self.revision3)])

        # when
        actual_results4 = list(self.storage.revision_get_by(
            origin_id,
            None,
            None))

        for actual_result in actual_results4:
            del actual_result['author']['id']
            del actual_result['committer']['id']

        self.assertEquals(len(actual_results4), 1)
        self.assertCountEqual(actual_results4,
                              [self.normalize_entity(self.revision3)])

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

        self.assertEquals([self.normalize_entity(self.release),
                           self.normalize_entity(self.release2)],
                          [actual_releases[0], actual_releases[1]])

    @istest
    def release_get_by(self):
        # given
        self.storage.revision_add([self.revision2])  # points to self.dir
        self.storage.release_add([self.release3])
        origin_id = self.storage.origin_add_one(self.origin2)

        # occurrence2 points to 'revision2' with branch 'master', we
        # need to point to the right origin
        origin_visit = self.storage.origin_visit_add(origin_id,
                                                     self.date_visit2)
        occurrence2 = self.occurrence2.copy()
        occurrence2.update({
            'origin': origin_id,
            'visit': origin_visit['visit'],
        })

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
    def origin_add(self):
        origin0 = self.storage.origin_get(self.origin)
        self.assertIsNone(origin0)

        id1, id2 = self.storage.origin_add([self.origin, self.origin2])

        actual_origin = self.storage.origin_get({
            'url': self.origin['url'],
            'type': self.origin['type'],
        })
        self.assertEqual(actual_origin['id'], id1)

        actual_origin2 = self.storage.origin_get({
            'url': self.origin2['url'],
            'type': self.origin2['type'],
        })
        self.assertEqual(actual_origin2['id'], id2)

    @istest
    def origin_add_twice(self):
        add1 = self.storage.origin_add([self.origin, self.origin2])
        add2 = self.storage.origin_add([self.origin, self.origin2])

        self.assertEqual(add1, add2)

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
    def origin_visit_add(self):
        # given
        self.assertIsNone(self.storage.origin_get(self.origin2))

        origin_id = self.storage.origin_add_one(self.origin2)
        self.assertIsNotNone(origin_id)

        # when
        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            ts=self.date_visit2)

        # then
        self.assertEquals(origin_visit1['origin'], origin_id)
        self.assertIsNotNone(origin_visit1['visit'])
        self.assertTrue(origin_visit1['visit'] > 0)

        actual_origin_visits = list(self.storage.origin_visit_get(origin_id))
        self.assertEquals(actual_origin_visits,
                          [{
                              'origin': origin_id,
                              'date': self.date_visit2,
                              'visit': origin_visit1['visit'],
                              'status': 'ongoing',
                              'metadata': None,
                          }])

    @istest
    def origin_visit_update(self):
        # given
        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id2 = self.storage.origin_add_one(self.origin)

        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            ts=self.date_visit2)

        origin_visit2 = self.storage.origin_visit_add(
            origin_id,
            ts=self.date_visit3)

        origin_visit3 = self.storage.origin_visit_add(
            origin_id2,
            ts=self.date_visit3)

        # when
        visit1_metadata = {
            'contents': 42,
            'directories': 22,
        }
        self.storage.origin_visit_update(
            origin_id, origin_visit1['visit'], status='full',
            metadata=visit1_metadata)
        self.storage.origin_visit_update(origin_id2, origin_visit3['visit'],
                                         status='partial')

        # then
        actual_origin_visits = list(self.storage.origin_visit_get(origin_id))
        self.assertEquals(actual_origin_visits,
                          [{
                              'origin': origin_visit2['origin'],
                              'date': self.date_visit2,
                              'visit': origin_visit1['visit'],
                              'status': 'full',
                              'metadata': visit1_metadata,
                          },
                           {
                               'origin': origin_visit2['origin'],
                               'date': self.date_visit3,
                               'visit': origin_visit2['visit'],
                               'status': 'ongoing',
                               'metadata': None,
                           }])

        actual_origin_visits_bis = list(self.storage.origin_visit_get(
            origin_id, limit=1))
        self.assertEquals(actual_origin_visits_bis,
                          [{
                              'origin': origin_visit2['origin'],
                              'date': self.date_visit2,
                              'visit': origin_visit1['visit'],
                              'status': 'full',
                              'metadata': visit1_metadata,
                          }])

        actual_origin_visits_ter = list(self.storage.origin_visit_get(
            origin_id, last_visit=origin_visit1['visit']))
        self.assertEquals(actual_origin_visits_ter,
                          [{
                               'origin': origin_visit2['origin'],
                               'date': self.date_visit3,
                               'visit': origin_visit2['visit'],
                               'status': 'ongoing',
                               'metadata': None,
                           }])

        actual_origin_visits2 = list(self.storage.origin_visit_get(origin_id2))
        self.assertEquals(actual_origin_visits2,
                          [{
                              'origin': origin_visit3['origin'],
                              'date': self.date_visit3,
                              'visit': origin_visit3['visit'],
                              'status': 'partial',
                              'metadata': None,
                          }])

    @istest
    def origin_visit_get_by(self):
        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id2 = self.storage.origin_add_one(self.origin)

        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            ts=self.date_visit2)

        occurrence2 = self.occurrence2.copy()
        occurrence2.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
        })

        self.storage.occurrence_add([occurrence2])

        # Add some other {origin, visit} entries
        self.storage.origin_visit_add(origin_id, ts=self.date_visit3)
        self.storage.origin_visit_add(origin_id2, ts=self.date_visit3)

        # when
        visit1_metadata = {
            'contents': 42,
            'directories': 22,
        }

        self.storage.origin_visit_update(
            origin_id, origin_visit1['visit'], status='full',
            metadata=visit1_metadata)

        expected_origin_visit = origin_visit1.copy()
        expected_origin_visit.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
            'date': self.date_visit2,
            'metadata': visit1_metadata,
            'status': 'full',
            'occurrences': {
                occurrence2['branch']: {
                    'target': occurrence2['target'],
                    'target_type': occurrence2['target_type'],
                }
            }
        })

        # when
        actual_origin_visit1 = self.storage.origin_visit_get_by(
            origin_visit1['origin'], origin_visit1['visit'])

        # then
        self.assertEquals(actual_origin_visit1, expected_origin_visit)

    @istest
    def origin_visit_get_by_no_result(self):
        # No result
        actual_origin_visit = self.storage.origin_visit_get_by(
            10, 999)

        self.assertIsNone(actual_origin_visit)

    @istest
    def occurrence_add(self):
        occur = self.occurrence.copy()

        origin_id = self.storage.origin_add_one(self.origin2)
        date_visit1 = self.date_visit1
        origin_visit1 = self.storage.origin_visit_add(origin_id, date_visit1)

        revision = self.revision.copy()
        revision['id'] = occur['target']
        self.storage.revision_add([revision])

        occur.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
        })
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
             occur['target_type'], self.date_visit1))

        date_visit2 = date_visit1 + datetime.timedelta(hours=10)

        origin_visit2 = self.storage.origin_visit_add(origin_id, date_visit2)
        occur2 = occur.copy()
        occur2.update({
            'visit': origin_visit2['visit'],
        })
        self.storage.occurrence_add([occur2])

        self.cursor.execute(test_query)
        ret = self.cursor.fetchall()
        self.assertEqual(len(ret), 2)
        self.assertEqual(
            (ret[0][0], ret[0][1].tobytes(), ret[0][2].tobytes(),
             ret[0][3], ret[0][4]),
            (occur['origin'], occur['branch'], occur['target'],
             occur['target_type'], date_visit1))
        self.assertEqual(
            (ret[1][0], ret[1][1].tobytes(), ret[1][2].tobytes(),
             ret[1][3], ret[1][4]),
            (occur2['origin'], occur2['branch'], occur2['target'],
             occur2['target_type'], date_visit2))

    @istest
    def occurrence_get(self):
        # given
        occur = self.occurrence.copy()
        origin_id = self.storage.origin_add_one(self.origin2)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)

        revision = self.revision.copy()
        revision['id'] = occur['target']
        self.storage.revision_add([revision])

        occur.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
        })
        self.storage.occurrence_add([occur])
        self.storage.occurrence_add([occur])

        # when
        actual_occurrence = list(self.storage.occurrence_get(origin_id))

        # then
        expected_occurrence = self.occurrence.copy()
        expected_occurrence.update({
            'origin': origin_id
        })
        self.assertEquals(len(actual_occurrence), 1)
        self.assertEquals(actual_occurrence[0], expected_occurrence)

    def _trigger_cache_provenance(self, origin_visit):
        """Trigger cache population for cache_content_revision.

        """
        ret = list(self.storage.cache_revision_origin_add(
                origin_visit['origin'],
                origin_visit['visit'],
        ))

        for revision_id in ret:
            self.storage.cache_content_revision_add([revision_id])

        return ret

    @istest
    def content_find_provenance_with_present_content(self):
        # 1. with something to find
        # given
        origin_id = self.storage.origin_add_one(self.origin2)
        self.storage.content_add([self.cont2])
        self.storage.directory_add([self.dir2])  # point to self.cont
        self.storage.revision_add([self.revision3])  # points to self.dir

        occurrence = self.occurrence3.copy()
        occurrence['target'] = self.revision3['id']
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit2)
        occurrence.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
        })

        self.storage.occurrence_add([occurrence])

        # Trigger cache population for cache_content_revision
        cached_revisions = self._trigger_cache_provenance(origin_visit1)

        self.assertIn(self.revision3['id'], cached_revisions)

        # when
        occs = list(self.storage.content_find_provenance(
            {'sha1': self.cont2['sha1']}))

        # then
        self.assertEquals(len(occs), 1)
        self.assertEquals(occs[0]['origin'], origin_visit1['origin'])
        self.assertEquals(occs[0]['visit'], origin_visit1['visit'])
        self.assertEquals(occs[0]['revision'], self.revision3['id'])
        self.assertEquals(occs[0]['path'], self.dir2['entries'][0]['name'])

        occs2 = list(self.storage.content_find_provenance(
            {'sha1_git': self.cont2['sha1_git']}))

        self.assertEquals(len(occs2), 1)
        self.assertEquals(occs2[0]['origin'], origin_visit1['origin'])
        self.assertEquals(occs2[0]['visit'], origin_visit1['visit'])
        self.assertEquals(occs2[0]['revision'], self.revision3['id'])
        self.assertEquals(occs2[0]['path'], self.dir2['entries'][0]['name'])

        occs3 = list(self.storage.content_find_provenance(
            {'sha256': self.cont2['sha256']}))

        self.assertEquals(len(occs3), 1)
        self.assertEquals(occs3[0]['origin'], origin_visit1['origin'])
        self.assertEquals(occs3[0]['visit'], origin_visit1['visit'])
        self.assertEquals(occs3[0]['revision'], self.revision3['id'])
        self.assertEquals(occs3[0]['path'], self.dir2['entries'][0]['name'])

    @istest
    def content_find_provenance_with_non_present_content(self):
        # 1. with something that does not exist
        missing_cont = self.missing_cont

        occ = list(self.storage.content_find_provenance(
            {'sha1': missing_cont['sha1']}))

        self.assertEquals(occ, [],
                          "Content does not exist so no occurrence")

        # 2. with something that does not exist
        occ = list(self.storage.content_find_provenance(
            {'sha1_git': missing_cont['sha1_git']}))

        self.assertEquals(occ, [],
                          "Content does not exist so no occurrence")

        # 3. with something that does not exist
        occ = list(self.storage.content_find_provenance(
            {'sha256': missing_cont['sha256']}))

        self.assertEquals(occ, [],
                          "Content does not exist so no occurrence")

    @istest
    def content_find_occurrence_bad_input(self):
        # 1. with bad input
        with self.assertRaises(ValueError) as cm:
            list(self.storage.content_find_provenance({}))  # empty is bad
        self.assertIn('content keys', cm.exception.args[0])

        # 2. with bad input
        with self.assertRaises(ValueError) as cm:
            list(self.storage.content_find_provenance(
                {'unknown-sha1': 'something'}))  # not the right key
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

        for key in expected_keys:
            self.cursor.execute('select * from swh_update_counter(%s)', (key,))
        self.conn.commit()

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
            'blake2s256': cont['blake2s256'],
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
            'blake2s256': cont['blake2s256'],
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
            'blake2s256': cont['blake2s256'],
            'length': cont['length'],
            'status': 'visible'
        })

        # 4. with something to find
        actually_present = self.storage.content_find({
            'sha1': cont['sha1'],
            'sha1_git': cont['sha1_git'],
            'sha256': cont['sha256'],
            'blake2s256': cont['blake2s256'],
        })

        actually_present.pop('ctime')
        self.assertEqual(actually_present, {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'blake2s256': cont['blake2s256'],
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

    @istest
    def content_mimetype_missing(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['file']['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        mimetypes = [
            {
                'id': self.cont2['sha1'],
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.missing_cont['sha1'],
                'indexer_configuration_id': tool_id,
            }]

        # when
        actual_missing = self.storage.content_mimetype_missing(mimetypes)

        # then
        self.assertEqual(list(actual_missing), [
            self.cont2['sha1'],
            self.missing_cont['sha1']
        ])

        # given
        self.storage.content_mimetype_add([{
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'indexer_configuration_id': tool_id,
        }])

        # when
        actual_missing = self.storage.content_mimetype_missing(mimetypes)

        # then
        self.assertEqual(list(actual_missing), [self.missing_cont['sha1']])

    @istest
    def content_mimetype_add__drop_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['file']['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        mimetype_v1 = {
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_mimetype_add([mimetype_v1])

        # when
        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.cont2['sha1']]))

        # then
        expected_mimetypes_v1 = [{
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'tool': tools['file'],
        }]
        self.assertEqual(actual_mimetypes, expected_mimetypes_v1)

        # given
        mimetype_v2 = mimetype_v1.copy()
        mimetype_v2.update({
            'mimetype': b'text/html',
            'encoding': b'us-ascii',
        })

        self.storage.content_mimetype_add([mimetype_v2])

        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.cont2['sha1']]))

        # mimetype did not change as the v2 was dropped.
        self.assertEqual(actual_mimetypes, expected_mimetypes_v1)

    @istest
    def content_mimetype_add__update_in_place_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['file']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        mimetype_v1 = {
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_mimetype_add([mimetype_v1])

        # when
        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.cont2['sha1']]))

        expected_mimetypes_v1 = [{
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'tool': tools['file'],
        }]

        # then
        self.assertEqual(actual_mimetypes, expected_mimetypes_v1)

        # given
        mimetype_v2 = mimetype_v1.copy()
        mimetype_v2.update({
            'mimetype': b'text/html',
            'encoding': b'us-ascii',
        })

        self.storage.content_mimetype_add([mimetype_v2], conflict_update=True)

        actual_mimetypes = list(self.storage.content_mimetype_get(
            [self.cont2['sha1']]))

        expected_mimetypes_v2 = [{
            'id': self.cont2['sha1'],
            'mimetype': b'text/html',
            'encoding': b'us-ascii',
            'tool': {
                'id': 2,
                'name': 'file',
                'version': '5.22',
                'configuration': {'command_line': 'file --mime <filepath>'}
            }
        }]

        # mimetype did change as the v2 was used to overwrite v1
        self.assertEqual(actual_mimetypes, expected_mimetypes_v2)

    @istest
    def content_mimetype_get(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['file']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        mimetypes = [self.cont2['sha1'], self.missing_cont['sha1']]

        mimetype1 = {
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_mimetype_add([mimetype1])

        # then
        actual_mimetypes = list(self.storage.content_mimetype_get(mimetypes))

        # then
        expected_mimetypes = [{
            'id': self.cont2['sha1'],
            'mimetype': b'text/plain',
            'encoding': b'utf-8',
            'tool': tools['file']
        }]

        self.assertEqual(actual_mimetypes, expected_mimetypes)

    @istest
    def content_language_missing(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['pygments']['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        languages = [
            {
                'id': self.cont2['sha1'],
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.missing_cont['sha1'],
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = list(self.storage.content_language_missing(languages))

        # then
        self.assertEqual(list(actual_missing), [
            self.cont2['sha1'],
            self.missing_cont['sha1'],
        ])

        # given
        self.storage.content_language_add([{
            'id': self.cont2['sha1'],
            'lang': 'haskell',
            'indexer_configuration_id': tool_id,
        }])

        # when
        actual_missing = list(self.storage.content_language_missing(languages))

        # then
        self.assertEqual(actual_missing, [self.missing_cont['sha1']])

    @istest
    def content_language_get(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['pygments']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        language1 = {
            'id': self.cont2['sha1'],
            'lang': 'common-lisp',
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_language_add([language1])

        # then
        actual_languages = list(self.storage.content_language_get(
            [self.cont2['sha1'], self.missing_cont['sha1']]))

        # then
        expected_languages = [{
            'id': self.cont2['sha1'],
            'lang': 'common-lisp',
            'tool': tools['pygments']
        }]

        self.assertEqual(actual_languages, expected_languages)

    @istest
    def content_language_add__drop_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['pygments']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        language_v1 = {
            'id': self.cont2['sha1'],
            'lang': 'emacslisp',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_language_add([language_v1])

        # when
        actual_languages = list(self.storage.content_language_get(
            [self.cont2['sha1']]))

        # then
        expected_languages_v1 = [{
            'id': self.cont2['sha1'],
            'lang': 'emacslisp',
            'tool': tools['pygments']
        }]
        self.assertEqual(actual_languages, expected_languages_v1)

        # given
        language_v2 = language_v1.copy()
        language_v2.update({
            'lang': 'common-lisp',
        })

        self.storage.content_language_add([language_v2])

        actual_languages = list(self.storage.content_language_get(
            [self.cont2['sha1']]))

        # language did not change as the v2 was dropped.
        self.assertEqual(actual_languages, expected_languages_v1)

    @istest
    def content_language_add__update_in_place_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['pygments']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        language_v1 = {
            'id': self.cont2['sha1'],
            'lang': 'common-lisp',
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_language_add([language_v1])

        # when
        actual_languages = list(self.storage.content_language_get(
            [self.cont2['sha1']]))

        # then
        expected_languages_v1 = [{
            'id': self.cont2['sha1'],
            'lang': 'common-lisp',
            'tool': tools['pygments']
        }]
        self.assertEqual(actual_languages, expected_languages_v1)

        # given
        language_v2 = language_v1.copy()
        language_v2.update({
            'lang': 'emacslisp',
        })

        self.storage.content_language_add([language_v2], conflict_update=True)

        actual_languages = list(self.storage.content_language_get(
            [self.cont2['sha1']]))

        # language did not change as the v2 was dropped.
        expected_languages_v2 = [{
            'id': self.cont2['sha1'],
            'lang': 'emacslisp',
            'tool': tools['pygments']
        }]

        # language did change as the v2 was used to overwrite v1
        self.assertEqual(actual_languages, expected_languages_v2)

    @istest
    def content_ctags_missing(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['universal-ctags']['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        ctags = [
            {
                'id': self.cont2['sha1'],
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.missing_cont['sha1'],
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = self.storage.content_ctags_missing(ctags)

        # then
        self.assertEqual(list(actual_missing), [
            self.cont2['sha1'],
            self.missing_cont['sha1']
        ])

        # given
        self.storage.content_ctags_add([
            {
                'id': self.cont2['sha1'],
                'indexer_configuration_id': tool_id,
                'ctags': [{
                    'name': 'done',
                    'kind': 'variable',
                    'line': 119,
                    'lang': 'OCaml',
                }]
            },
        ])

        # when
        actual_missing = self.storage.content_ctags_missing(ctags)

        # then
        self.assertEqual(list(actual_missing), [self.missing_cont['sha1']])

    @istest
    def content_ctags_get(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['universal-ctags']['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        ctags = [self.cont2['sha1'], self.missing_cont['sha1']]

        ctag1 = {
            'id': self.cont2['sha1'],
            'indexer_configuration_id': tool_id,
            'ctags': [
                {
                    'name': 'done',
                    'kind': 'variable',
                    'line': 100,
                    'lang': 'Python',
                },
                {
                    'name': 'main',
                    'kind': 'function',
                    'line': 119,
                    'lang': 'Python',
                }]
        }

        # when
        self.storage.content_ctags_add([ctag1])

        # then
        actual_ctags = list(self.storage.content_ctags_get(ctags))

        # then

        expected_ctags = [
            {
                'id': self.cont2['sha1'],
                'tool': tools['universal-ctags'],
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Python',
            },
            {
                'id': self.cont2['sha1'],
                'tool': tools['universal-ctags'],
                'name': 'main',
                'kind': 'function',
                'line': 119,
                'lang': 'Python',
            }
        ]

        self.assertEqual(actual_ctags, expected_ctags)

    @istest
    def content_ctags_search(self):
        # 1. given
        tools = self.fetch_tools()
        tool = tools['universal-ctags']
        tool_id = tool['id']

        cont = self.cont
        cont2 = self.cont2
        self.storage.content_add([cont, cont2])

        ctag1 = {
            'id': cont['sha1'],
            'indexer_configuration_id': tool_id,
            'ctags': [
                {
                    'name': 'hello',
                    'kind': 'function',
                    'line': 133,
                    'lang': 'Python',
                },
                {
                    'name': 'counter',
                    'kind': 'variable',
                    'line': 119,
                    'lang': 'Python',
                },
            ]
        }

        ctag2 = {
            'id': cont2['sha1'],
            'indexer_configuration_id': tool_id,
            'ctags': [
                {
                    'name': 'hello',
                    'kind': 'variable',
                    'line': 100,
                    'lang': 'C',
                },
            ]
        }

        self.storage.content_ctags_add([ctag1, ctag2])

        # 1. when
        actual_ctags = list(self.storage.content_ctags_search('hello',
                                                              limit=1))

        # 1. then
        self.assertEqual(actual_ctags, [
            {
                'id': ctag1['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'function',
                'line': 133,
                'lang': 'Python',
            }
        ])

        # 2. when
        actual_ctags = list(self.storage.content_ctags_search(
            'hello',
            limit=1,
            last_sha1=ctag1['id']))

        # 2. then
        self.assertEqual(actual_ctags, [
            {
                'id': ctag2['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'variable',
                'line': 100,
                'lang': 'C',
            }
        ])

        # 3. when
        actual_ctags = list(self.storage.content_ctags_search('hello'))

        # 3. then
        self.assertEqual(actual_ctags, [
            {
                'id': ctag1['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'function',
                'line': 133,
                'lang': 'Python',
            },
            {
                'id': ctag2['id'],
                'tool': tool,
                'name': 'hello',
                'kind': 'variable',
                'line': 100,
                'lang': 'C',
            },
        ])

        # 4. when
        actual_ctags = list(self.storage.content_ctags_search('counter'))

        # then
        self.assertEqual(actual_ctags, [{
            'id': ctag1['id'],
            'tool': tool,
            'name': 'counter',
            'kind': 'variable',
            'line': 119,
            'lang': 'Python',
        }])

    @istest
    def content_ctags_search_no_result(self):
        actual_ctags = list(self.storage.content_ctags_search('counter'))

        self.assertEquals(actual_ctags, [])

    @istest
    def content_ctags_add__add_new_ctags_added(self):
        # given
        tools = self.fetch_tools()
        tool = tools['universal-ctags']
        tool_id = tool['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        ctag_v1 = {
            'id': self.cont2['sha1'],
            'indexer_configuration_id': tool_id,
            'ctags': [{
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
            }]
        }

        # given
        self.storage.content_ctags_add([ctag_v1])
        self.storage.content_ctags_add([ctag_v1])  # conflict does nothing

        # when
        actual_ctags = list(self.storage.content_ctags_get(
            [self.cont2['sha1']]))

        # then
        expected_ctags = [{
            'id': self.cont2['sha1'],
            'name': 'done',
            'kind': 'variable',
            'line': 100,
            'lang': 'Scheme',
            'tool': tool,
        }]

        self.assertEqual(actual_ctags, expected_ctags)

        # given
        ctag_v2 = ctag_v1.copy()
        ctag_v2.update({
            'ctags': [
                {
                    'name': 'defn',
                    'kind': 'function',
                    'line': 120,
                    'lang': 'Scheme',
                }
            ]
        })

        self.storage.content_ctags_add([ctag_v2])

        expected_ctags = [
            {
                'id': self.cont2['sha1'],
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
                'tool': tool,
            }, {
                'id': self.cont2['sha1'],
                'name': 'defn',
                'kind': 'function',
                'line': 120,
                'lang': 'Scheme',
                'tool': tool,
            }
        ]

        actual_ctags = list(self.storage.content_ctags_get(
            [self.cont2['sha1']]))

        self.assertEqual(actual_ctags, expected_ctags)

    @istest
    def content_ctags_add__update_in_place(self):
        # given
        tools = self.fetch_tools()
        tool = tools['universal-ctags']
        tool_id = tool['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        ctag_v1 = {
            'id': self.cont2['sha1'],
            'indexer_configuration_id': tool_id,
            'ctags': [{
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
            }]
        }

        # given
        self.storage.content_ctags_add([ctag_v1])

        # when
        actual_ctags = list(self.storage.content_ctags_get(
            [self.cont2['sha1']]))

        # then
        expected_ctags = [
            {
                'id': self.cont2['sha1'],
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
                'tool': tool
            }
        ]
        self.assertEqual(actual_ctags, expected_ctags)

        # given
        ctag_v2 = ctag_v1.copy()
        ctag_v2.update({
            'ctags': [
                {
                    'name': 'done',
                    'kind': 'variable',
                    'line': 100,
                    'lang': 'Scheme',
                },
                {
                    'name': 'defn',
                    'kind': 'function',
                    'line': 120,
                    'lang': 'Scheme',
                }
            ]
        })

        self.storage.content_ctags_add([ctag_v2], conflict_update=True)

        actual_ctags = list(self.storage.content_ctags_get(
            [self.cont2['sha1']]))

        # ctag did change as the v2 was used to overwrite v1
        expected_ctags = [
            {
                'id': self.cont2['sha1'],
                'name': 'done',
                'kind': 'variable',
                'line': 100,
                'lang': 'Scheme',
                'tool': tool,
            },
            {
                'id': self.cont2['sha1'],
                'name': 'defn',
                'kind': 'function',
                'line': 120,
                'lang': 'Scheme',
                'tool': tool,
            }
        ]
        self.assertEqual(actual_ctags, expected_ctags)

    @istest
    def content_fossology_license_get(self):
        # given
        tools = self.fetch_tools()
        tool = tools['nomos']
        tool_id = tool['id']

        cont = self.cont
        self.storage.content_add([cont])

        license1 = {
            'id': cont['sha1'],
            'licenses': ['GPL-2.0+'],
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_fossology_license_add([license1])

        # then
        actual_licenses = list(self.storage.content_fossology_license_get(
            [cont['sha1'], self.missing_cont['sha1']]))

        expected_license = {
            'id': cont['sha1'],
            'licenses': ['GPL-2.0+'],
            'tool': tool,
        }

        # then
        self.assertEqual(actual_licenses, [expected_license])

    @istest
    def content_fossology_license_add__new_license_added(self):
        # given
        tools = self.fetch_tools()
        tool = tools['nomos']
        tool_id = tool['id']

        cont = self.cont
        self.storage.content_add([cont])

        license_v1 = {
            'id': cont['sha1'],
            'licenses': ['Apache-2.0'],
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_fossology_license_add([license_v1])
        # conflict does nothing
        self.storage.content_fossology_license_add([license_v1])

        # when
        actual_licenses = list(self.storage.content_fossology_license_get(
            [cont['sha1']]))

        # then
        expected_license = {
            'id': cont['sha1'],
            'licenses': ['Apache-2.0'],
            'tool': tool,
        }
        self.assertEqual(actual_licenses, [expected_license])

        # given
        license_v2 = license_v1.copy()
        license_v2.update({
            'licenses': ['BSD-2-Clause'],
        })

        self.storage.content_fossology_license_add([license_v2])

        actual_licenses = list(self.storage.content_fossology_license_get(
            [cont['sha1']]))

        expected_license.update({
            'licenses': ['Apache-2.0', 'BSD-2-Clause'],
        })

        # license did not change as the v2 was dropped.
        self.assertEqual(actual_licenses, [expected_license])

    @istest
    def content_fossology_license_add__update_in_place_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool = tools['nomos']
        tool_id = tool['id']

        cont = self.cont
        self.storage.content_add([cont])

        license_v1 = {
            'id': cont['sha1'],
            'licenses': ['CECILL'],
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_fossology_license_add([license_v1])
        # conflict does nothing
        self.storage.content_fossology_license_add([license_v1])

        # when
        actual_licenses = list(self.storage.content_fossology_license_get(
            [cont['sha1']]))

        # then
        expected_license = {
            'id': cont['sha1'],
            'licenses': ['CECILL'],
            'tool': tool,
        }
        self.assertEqual(actual_licenses, [expected_license])

        # given
        license_v2 = license_v1.copy()
        license_v2.update({
            'licenses': ['CECILL-2.0']
        })

        self.storage.content_fossology_license_add([license_v2],
                                                   conflict_update=True)

        actual_licenses = list(self.storage.content_fossology_license_get(
            [cont['sha1']]))

        # license did change as the v2 was used to overwrite v1
        expected_license.update({
            'licenses': ['CECILL-2.0']
        })
        self.assertEqual(actual_licenses, [expected_license])

    @istest
    def content_metadata_missing(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-translator']['id']

        cont2 = self.cont2
        self.storage.content_add([cont2])

        metadatas = [
            {
                'id': self.cont2['sha1'],
                'indexer_configuration_id': tool_id,
            },
            {
                'id': self.missing_cont['sha1'],
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = list(self.storage.content_metadata_missing(metadatas))

        # then
        self.assertEqual(list(actual_missing), [
            self.cont2['sha1'],
            self.missing_cont['sha1'],
        ])

        # given
        self.storage.content_metadata_add([{
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/moranegg/metadata_test'
                },
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'indexer_configuration_id': tool_id
        }])

        # when
        actual_missing = list(self.storage.content_metadata_missing(metadatas))

        # then
        self.assertEqual(actual_missing, [self.missing_cont['sha1']])

    @istest
    def content_metadata_get(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-translator']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        metadata1 = {
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/moranegg/metadata_test'
                },
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'indexer_configuration_id': tool_id,
        }

        # when
        self.storage.content_metadata_add([metadata1])

        # then
        actual_metadatas = list(self.storage.content_metadata_get(
            [self.cont2['sha1'], self.missing_cont['sha1']]))

        expected_metadatas = [{
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'codeRepository': {
                    'type': 'git',
                    'url': 'https://github.com/moranegg/metadata_test'
                },
                'description': 'Simple package.json test for indexer',
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'tool': tools['swh-metadata-translator']
        }]

        self.assertEqual(actual_metadatas, expected_metadatas)

    @istest
    def content_metadata_add_drop_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-translator']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        metadata_v1 = {
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_metadata_add([metadata_v1])

        # when
        actual_metadatas = list(self.storage.content_metadata_get(
            [self.cont2['sha1']]))

        expected_metadatas_v1 = [{
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'tool': tools['swh-metadata-translator']
        }]

        self.assertEqual(actual_metadatas, expected_metadatas_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata': {
                'other': {},
                'name': 'test_drop_duplicated_metadata',
                'version': '0.0.1'
            },
        })

        self.storage.content_metadata_add([metadata_v2])

        # then
        actual_metadatas = list(self.storage.content_metadata_get(
            [self.cont2['sha1']]))

        # metadata did not change as the v2 was dropped.
        self.assertEqual(actual_metadatas, expected_metadatas_v1)

    @istest
    def content_metadata_add_update_in_place_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-translator']['id']
        cont2 = self.cont2
        self.storage.content_add([cont2])

        metadata_v1 = {
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.content_metadata_add([metadata_v1])

        # when
        actual_metadatas = list(self.storage.content_metadata_get(
            [self.cont2['sha1']]))

        # then
        expected_metadatas_v1 = [{
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'name': 'test_metadata',
                'version': '0.0.1'
            },
            'tool': tools['swh-metadata-translator']
        }]
        self.assertEqual(actual_metadatas, expected_metadatas_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata': {
                'other': {},
                'name': 'test_update_duplicated_metadata',
                'version': '0.0.1'
            },
        })
        self.storage.content_metadata_add([metadata_v2], conflict_update=True)

        actual_metadatas = list(self.storage.content_metadata_get(
            [self.cont2['sha1']]))

        # language did not change as the v2 was dropped.
        expected_metadatas_v2 = [{
            'id': self.cont2['sha1'],
            'translated_metadata': {
                'other': {},
                'name': 'test_update_duplicated_metadata',
                'version': '0.0.1'
            },
            'tool': tools['swh-metadata-translator']
        }]

        # metadata did change as the v2 was used to overwrite v1
        self.assertEqual(actual_metadatas, expected_metadatas_v2)

    @istest
    def revision_metadata_missing(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-detector']['id']

        rev = self.revision
        missing_rev = self.revision2
        self.storage.revision_add([rev])

        metadatas = [
            {
                'id': rev['id'],
                'indexer_configuration_id': tool_id,
            },
            {
                'id': missing_rev['id'],
                'indexer_configuration_id': tool_id,
            }
        ]

        # when
        actual_missing = list(self.storage.revision_metadata_missing(
                              metadatas))

        # then
        self.assertEqual(list(actual_missing), [
            rev['id'],
            missing_rev['id'],
        ])

        # given
        self.storage.revision_metadata_add([{
            'id': rev['id'],
            'translated_metadata': {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'type': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id
        }])

        # when
        actual_missing = list(self.storage.revision_metadata_missing(
                              metadatas))

        # then
        self.assertEqual(actual_missing, [missing_rev['id']])

    @istest
    def revision_metadata_get(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-detector']['id']
        rev = self.revision2
        self.storage.revision_add([rev])

        metadata_rev = {
            'id': rev['id'],
            'translated_metadata': {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'type': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id
        }

        # when
        self.storage.revision_metadata_add([metadata_rev])

        # then
        actual_metadatas = list(self.storage.revision_metadata_get(
            [self.revision2['id'], self.revision['id']]))

        expected_metadatas = [{
            'id': rev['id'],
            'translated_metadata': metadata_rev['translated_metadata'],
            'tool': tools['swh-metadata-detector']
        }]

        self.assertEqual(actual_metadatas, expected_metadatas)

    @istest
    def revision_metadata_add_drop_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-detector']['id']
        revision = self.revision
        self.storage.revision_add([revision])

        metadata_v1 = {
            'id': self.revision['id'],
            'translated_metadata':  {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'type': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.revision_metadata_add([metadata_v1])

        # when
        actual_metadatas = list(self.storage.revision_metadata_get(
            [self.revision['id']]))

        expected_metadatas_v1 = [{
            'id': self.revision['id'],
            'translated_metadata':  metadata_v1['translated_metadata'],
            'tool': tools['swh-metadata-detector']
        }]

        self.assertEqual(actual_metadatas, expected_metadatas_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata':  {
                'name': 'test_metadata',
                'author': 'MG',
            },
        })

        self.storage.revision_metadata_add([metadata_v2])

        # then
        actual_metadatas = list(self.storage.revision_metadata_get(
            [self.revision['id']]))

        # metadata did not change as the v2 was dropped.
        self.assertEqual(actual_metadatas, expected_metadatas_v1)

    @istest
    def revision_metadata_add_update_in_place_duplicate(self):
        # given
        tools = self.fetch_tools()
        tool_id = tools['swh-metadata-detector']['id']
        revision = self.revision2
        self.storage.revision_add([revision])

        metadata_v1 = {
            'id': self.revision2['id'],
            'translated_metadata': {
                'developmentStatus': None,
                'version': None,
                'operatingSystem': None,
                'description': None,
                'keywords': None,
                'issueTracker': None,
                'name': None,
                'author': None,
                'relatedLink': None,
                'url': None,
                'type': None,
                'license': None,
                'maintainer': None,
                'email': None,
                'softwareRequirements': None,
                'identifier': None
            },
            'indexer_configuration_id': tool_id,
        }

        # given
        self.storage.revision_metadata_add([metadata_v1])

        # when
        actual_metadatas = list(self.storage.revision_metadata_get(
            [self.revision2['id']]))

        # then
        expected_metadatas_v1 = [{
            'id': self.revision2['id'],
            'translated_metadata':  metadata_v1['translated_metadata'],
            'tool': tools['swh-metadata-detector']
        }]
        self.assertEqual(actual_metadatas, expected_metadatas_v1)

        # given
        metadata_v2 = metadata_v1.copy()
        metadata_v2.update({
            'translated_metadata':  {
                'name': 'test_update_duplicated_metadata',
                'author': 'MG'
            },
        })
        self.storage.revision_metadata_add([metadata_v2], conflict_update=True)

        actual_metadatas = list(self.storage.revision_metadata_get(
            [self.revision2['id']]))

        # language did not change as the v2 was dropped.
        expected_metadatas_v2 = [{
            'id': self.revision2['id'],
            'translated_metadata': metadata_v2['translated_metadata'],
            'tool': tools['swh-metadata-detector']
        }]

        # metadata did change as the v2 was used to overwrite v1
        self.assertEqual(actual_metadatas, expected_metadatas_v2)

    @istest
    def indexer_configuration_get_missing(self):
        tool = {
            'tool_name': 'unknown-tool',
            'tool_version': '3.1.0rc2-31-ga2cbb8c',
            'tool_configuration': {"command_line": "nomossa <filepath>"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)

        self.assertIsNone(actual_tool)

    @istest
    def indexer_configuration_get(self):
        tool = {
            'tool_name': 'nomos',
            'tool_version': '3.1.0rc2-31-ga2cbb8c',
            'tool_configuration': {"command_line": "nomossa <filepath>"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)

        expected_tool = tool.copy()
        expected_tool['id'] = 1

        self.assertEqual(expected_tool, actual_tool)

    @istest
    def indexer_configuration_metadata_get_missing_context(self):
        tool = {
            'tool_name': 'swh-metadata-translator',
            'tool_version': '0.0.1',
            'tool_configuration': {"context": "unknown-context"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)

        self.assertIsNone(actual_tool)

    @istest
    def indexer_configuration_metadata_get(self):
        tool = {
            'tool_name': 'swh-metadata-translator',
            'tool_version': '0.0.1',
            'tool_configuration': {"type": "local", "context": "npm"},
        }

        actual_tool = self.storage.indexer_configuration_get(tool)

        expected_tool = tool.copy()
        expected_tool['id'] = actual_tool['id']

        self.assertEqual(expected_tool, actual_tool)

    @istest
    def origin_metadata_add(self):
        # given
        origin_metadata0 = self.storage.origin_metadata_get(self.origin)
        self.assertIsNone(origin_metadata0)

        origin_id = self.storage.origin_add([self.origin])

        # when adding for the same origin 2 metadatas
        o_m_id1 = self.storage.origin_metadata_add(self.origin_metadata)
        o_m_id2 = self.storage.origin_metadata_add(self.origin_metadata2)
        actual_origin_metadata1 = self.storage.origin_metadata_get(o_m_id1)
        actual_origin_metadata2 = self.storage.origin_metadata_get(o_m_id2)

        # then
        self.assertEqual(actual_origin_metadata1['id'], o_m_id1)
        self.assertEqual(actual_origin_metadata2['id'], o_m_id2)
        self.assertEqual(actual_origin_metadata1['origin_id'], origin_id)
        self.assertEqual(actual_origin_metadata2['origin_id'], origin_id)

    @istest
    def origin_metadata_get(self):
        # given
        origin_id = self.storage.origin_add([self.origin])

        # when adding for the same origin 2 metadatas
        o_m_id1 = self.storage.origin_metadata_add(self.origin_metadata)
        o_m_id2 = self.storage.origin_metadata_add(self.origin_metadata2)
        all_metadatas = self.storage.origin_metadata_get_all({
            'origin_id': origin_id,
        })
        m_by_provenance = self.storage.origin_metadata_get_by_provenance({
            'origin_id': origin_id,
            'provenance': self.origin_metadata['provenance']
        })
        expected_results = [{
            'id': o_m_id1,
            'origin_id': origin_id,
            'date': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'provenance': 'deposit-hal',
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            }
        }, {
            'id': o_m_id2,
            'origin_id': origin_id,
            'date': datetime.datetime(2017, 1, 1, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'provenance': 'lister-hal',
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            }
        }]

        # then
        self.assertEqual(len(all_metadatas), 2)
        self.assertEqual(len(m_by_provenance), 1)
        self.assertEqual(all_metadatas, expected_results)


class TestLocalStorage(CommonTestStorage, unittest.TestCase):
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

    # The remote API doesn't expose _person_add
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


class AlteringSchemaTest(BaseTestStorage, unittest.TestCase):
    """This class is dedicated for the rare case where the schema needs to
       be altered dynamically.

       Otherwise, the tests could be blocking when ran altogether.

    """
    @istest
    def content_update(self):
        cont = copy.deepcopy(self.cont)

        self.storage.content_add([cont])
        # alter the sha1_git for example
        cont['sha1_git'] = hash_to_bytes(
            '3a60a5275d0333bf13468e8b3dcab90f4046e654')

        self.storage.content_update([cont], keys=['sha1_git'])

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
    def content_update_with_new_cols(self):
        self.cursor.execute("""alter table content
                               add column test text default null,
                               add column test2 text default null""")

        cont = copy.deepcopy(self.cont2)
        self.storage.content_add([cont])
        cont['test'] = 'value-1'
        cont['test2'] = 'value-2'

        self.storage.content_update([cont], keys=['test', 'test2'])

        self.cursor.execute(
            'SELECT sha1, sha1_git, sha256, length, status, test, test2'
            ' FROM content WHERE sha1 = %s',
            (cont['sha1'],))

        datum = self.cursor.fetchone()
        self.assertEqual(
            (datum[0].tobytes(), datum[1].tobytes(), datum[2].tobytes(),
             datum[3], datum[4], datum[5], datum[6]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible', cont['test'], cont['test2']))

        self.cursor.execute("""alter table content drop column test,
                                                   drop column test2""")
