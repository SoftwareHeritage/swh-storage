# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import unittest
import itertools
from collections import defaultdict
from unittest.mock import Mock, patch

import pytest

from hypothesis import given, strategies

from swh.model import from_disk, identifiers
from swh.model.hashutil import hash_to_bytes
from swh.storage.tests.storage_testing import StorageTestFixture
from swh.storage import HashCollision

from .generate_data_test import gen_contents


@pytest.mark.db
class StorageTestDbFixture(StorageTestFixture):
    def setUp(self):
        super().setUp()

        db = self.test_db[self.TEST_DB_NAME]
        self.conn = db.conn
        self.cursor = db.cursor

        self.maxDiff = None

    def tearDown(self):
        self.reset_storage_tables()
        super().tearDown()


class TestStorageData:
    def setUp(self):
        super().setUp()

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
                    'name': b'subdir',
                    'type': 'dir',
                    'target': self.dir['id'],
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

        self.provider = {
            'name': 'hal',
            'type': 'deposit-client',
            'url': 'http:///hal/inria',
            'metadata': {
                'location': 'France'
            }
        }

        self.metadata_tool = {
            'name': 'swh-deposit',
            'version': '0.0.1',
            'configuration': {
                'sword_version': '2'
            }
        }

        self.origin_metadata = {
            'origin': self.origin,
            'discovery_date': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                                tzinfo=datetime.timezone.utc),
            'provider': self.provider,
            'tool': 'swh-deposit',
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
             }
        }

        self.origin_metadata2 = {
            'origin': self.origin,
            'discovery_date': datetime.datetime(2017, 1, 1, 23, 0, 0,
                                                tzinfo=datetime.timezone.utc),
            'provider': self.provider,
            'tool': 'swh-deposit',
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
             }
        }

        self.date_visit1 = datetime.datetime(2015, 1, 1, 23, 0, 0,
                                             tzinfo=datetime.timezone.utc)

        self.date_visit2 = datetime.datetime(2017, 1, 1, 23, 0, 0,
                                             tzinfo=datetime.timezone.utc)

        self.date_visit3 = datetime.datetime(2018, 1, 1, 23, 0, 0,
                                             tzinfo=datetime.timezone.utc)

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
            'message': b'v0.0.2\nMisc performance improvements + bug fixes',
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

        self.snapshot = {
            'id': hash_to_bytes('2498dbf535f882bc7f9a18fb16c9ad27fda7bab7'),
            'branches': {
                b'master': {
                    'target': self.revision['id'],
                    'target_type': 'revision',
                },
            },
            'next_branch': None
        }

        self.empty_snapshot = {
            'id': hash_to_bytes('1a8893e6a86f444e8be8e7bda6cb34fb1735a00e'),
            'branches': {},
            'next_branch': None
        }

        self.complete_snapshot = {
            'id': hash_to_bytes('6e65b86363953b780d92b0a928f3e8fcdd10db36'),
            'branches': {
                b'directory': {
                    'target': hash_to_bytes(
                        '1bd0e65f7d2ff14ae994de17a1e7fe65111dcad8'),
                    'target_type': 'directory',
                },
                b'content': {
                    'target': hash_to_bytes(
                        'fe95a46679d128ff167b7c55df5d02356c5a1ae1'),
                    'target_type': 'content',
                },
                b'alias': {
                    'target': b'revision',
                    'target_type': 'alias',
                },
                b'revision': {
                    'target': hash_to_bytes(
                        'aafb16d69fd30ff58afdd69036a26047f3aebdc6'),
                    'target_type': 'revision',
                },
                b'release': {
                    'target': hash_to_bytes(
                        '7045404f3d1c54e6473c71bbb716529fbad4be24'),
                    'target_type': 'release',
                },
                b'snapshot': {
                    'target': hash_to_bytes(
                        '1a8893e6a86f444e8be8e7bda6cb34fb1735a00e'),
                    'target_type': 'snapshot',
                },
                b'dangling': None,
            },
            'next_branch': None
        }


class CommonTestStorage(TestStorageData):
    """Base class for Storage testing.

    This class is used as-is to test local storage (see TestLocalStorage
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

    def test_check_config(self):
        self.assertTrue(self.storage.check_config(check_write=True))
        self.assertTrue(self.storage.check_config(check_write=False))

    def test_content_add(self):
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

    def test_content_add_collision(self):
        cont1 = self.cont

        # create (corrupted) content with same sha1{,_git} but != sha256
        cont1b = cont1.copy()
        sha256_array = bytearray(cont1b['sha256'])
        sha256_array[0] += 1
        cont1b['sha256'] = bytes(sha256_array)

        with self.assertRaises(HashCollision) as cm:
            self.storage.content_add([cont1, cont1b])

        self.assertIn(cm.exception.args[0], ['sha1', 'sha1_git', 'blake2s256'])

    def test_skipped_content_add(self):
        cont = self.skipped_cont.copy()
        cont2 = self.skipped_cont2.copy()
        cont2['blake2s256'] = None

        self.storage.content_add([cont, cont, cont2])

        self.cursor.execute('SELECT sha1, sha1_git, sha256, blake2s256, '
                            'length, status, reason '
                            'FROM skipped_content ORDER BY sha1_git')

        datums = self.cursor.fetchall()

        self.assertEqual(2, len(datums))
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

    @pytest.mark.property_based
    @given(strategies.sets(
        elements=strategies.sampled_from(
            ['sha256', 'sha1_git', 'blake2s256']),
        min_size=0))
    def test_content_missing(self, algos):
        algos |= {'sha1'}
        cont2 = self.cont2
        missing_cont = self.missing_cont
        self.storage.content_add([cont2])
        test_contents = [cont2]
        missing_per_hash = defaultdict(list)
        for i in range(256):
            test_content = missing_cont.copy()
            for hash in algos:
                test_content[hash] = bytes([i]) + test_content[hash][1:]
                missing_per_hash[hash].append(test_content[hash])
            test_contents.append(test_content)

        self.assertCountEqual(
            self.storage.content_missing(test_contents),
            missing_per_hash['sha1']
        )

        for hash in algos:
            self.assertCountEqual(
                self.storage.content_missing(test_contents, key_hash=hash),
                missing_per_hash[hash]
            )

    def test_content_missing_per_sha1(self):
        # given
        cont2 = self.cont2
        missing_cont = self.missing_cont
        self.storage.content_add([cont2])
        # when
        gen = self.storage.content_missing_per_sha1([cont2['sha1'],
                                                     missing_cont['sha1']])

        # then
        self.assertEqual(list(gen), [missing_cont['sha1']])

    def test_content_get_metadata(self):
        cont1 = self.cont.copy()
        cont2 = self.cont2.copy()

        self.storage.content_add([cont1, cont2])

        gen = self.storage.content_get_metadata([cont1['sha1'], cont2['sha1']])

        # we only retrieve the metadata
        cont1.pop('data')
        cont2.pop('data')

        self.assertCountEqual(list(gen), [cont1, cont2])

    def test_content_get_metadata_missing_sha1(self):
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

    @staticmethod
    def _transform_entries(dir_, *, prefix=b''):
        for ent in dir_['entries']:
            yield {
                'dir_id': dir_['id'],
                'type': ent['type'],
                'target': ent['target'],
                'name': prefix + ent['name'],
                'perms': ent['perms'],
                'status': None,
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'length': None,
            }

    def test_directory_add(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        self.storage.directory_add([self.dir])

        actual_data = list(self.storage.directory_ls(self.dir['id']))
        expected_data = list(self._transform_entries(self.dir))
        self.assertCountEqual(expected_data, actual_data)

        after_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([], after_missing)

    def test_directory_get_recursive(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        self.storage.directory_add([self.dir, self.dir2, self.dir3])

        actual_data = list(self.storage.directory_ls(
            self.dir['id'], recursive=True))
        expected_data = list(self._transform_entries(self.dir))
        self.assertCountEqual(expected_data, actual_data)

        actual_data = list(self.storage.directory_ls(
            self.dir2['id'], recursive=True))
        expected_data = list(self._transform_entries(self.dir2))
        self.assertCountEqual(expected_data, actual_data)

        actual_data = list(self.storage.directory_ls(
            self.dir3['id'], recursive=True))
        expected_data = list(itertools.chain(
            self._transform_entries(self.dir3),
            self._transform_entries(self.dir, prefix=b'subdir/')))
        self.assertCountEqual(expected_data, actual_data)

    def test_directory_entry_get_by_path(self):
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
                'name': b'subdir',
                'type': 'dir',
                'target': self.dir['id'],
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

    def test_revision_add(self):
        init_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([self.revision['id']], list(init_missing))

        self.storage.revision_add([self.revision])

        end_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([], list(end_missing))

    def test_revision_log(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])

        # when
        actual_results = list(self.storage.revision_log(
            [self.revision4['id']]))

        # hack: ids generated
        for actual_result in actual_results:
            if 'id' in actual_result['author']:
                del actual_result['author']['id']
            if 'id' in actual_result['committer']:
                del actual_result['committer']['id']

        self.assertEqual(len(actual_results), 2)  # rev4 -child-> rev3
        self.assertEqual(actual_results[0],
                         self.normalize_entity(self.revision4))
        self.assertEqual(actual_results[1],
                         self.normalize_entity(self.revision3))

    def test_revision_log_with_limit(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])
        actual_results = list(self.storage.revision_log(
            [self.revision4['id']], 1))

        # hack: ids generated
        for actual_result in actual_results:
            if 'id' in actual_result['author']:
                del actual_result['author']['id']
            if 'id' in actual_result['committer']:
                del actual_result['committer']['id']

        self.assertEqual(len(actual_results), 1)
        self.assertEqual(actual_results[0], self.revision4)

    @staticmethod
    def _short_revision(revision):
        return [revision['id'], revision['parents']]

    def test_revision_shortlog(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])

        # when
        actual_results = list(self.storage.revision_shortlog(
            [self.revision4['id']]))

        self.assertEqual(len(actual_results), 2)  # rev4 -child-> rev3
        self.assertEqual(list(actual_results[0]),
                         self._short_revision(self.revision4))
        self.assertEqual(list(actual_results[1]),
                         self._short_revision(self.revision3))

    def test_revision_shortlog_with_limit(self):
        # given
        # self.revision4 -is-child-of-> self.revision3
        self.storage.revision_add([self.revision3,
                                   self.revision4])
        actual_results = list(self.storage.revision_shortlog(
            [self.revision4['id']], 1))

        self.assertEqual(len(actual_results), 1)
        self.assertEqual(list(actual_results[0]),
                         self._short_revision(self.revision4))

    def test_revision_get(self):
        self.storage.revision_add([self.revision])

        actual_revisions = list(self.storage.revision_get(
            [self.revision['id'], self.revision2['id']]))

        # when
        if 'id' in actual_revisions[0]['author']:
            del actual_revisions[0]['author']['id']  # hack: ids are generated
        if 'id' in actual_revisions[0]['committer']:
            del actual_revisions[0]['committer']['id']

        self.assertEqual(len(actual_revisions), 2)
        self.assertEqual(actual_revisions[0],
                         self.normalize_entity(self.revision))
        self.assertIsNone(actual_revisions[1])

    def test_revision_get_no_parents(self):
        self.storage.revision_add([self.revision3])

        get = list(self.storage.revision_get([self.revision3['id']]))

        self.assertEqual(len(get), 1)
        self.assertEqual(get[0]['parents'], [])  # no parents on this one

    def test_release_add(self):
        init_missing = self.storage.release_missing([self.release['id'],
                                                     self.release2['id']])
        self.assertEqual([self.release['id'], self.release2['id']],
                         list(init_missing))

        self.storage.release_add([self.release, self.release2])

        end_missing = self.storage.release_missing([self.release['id'],
                                                    self.release2['id']])
        self.assertEqual([], list(end_missing))

    def test_release_get(self):
        # given
        self.storage.release_add([self.release, self.release2])

        # when
        actual_releases = list(self.storage.release_get([self.release['id'],
                                                         self.release2['id']]))

        # then
        for actual_release in actual_releases:
            if 'id' in actual_release['author']:
                del actual_release['author']['id']  # hack: ids are generated

        self.assertEqual([self.normalize_entity(self.release),
                          self.normalize_entity(self.release2)],
                         [actual_releases[0], actual_releases[1]])

        unknown_releases = \
            list(self.storage.release_get([self.release3['id']]))

        self.assertIsNone(unknown_releases[0])

    def test_origin_add_one(self):
        origin0 = self.storage.origin_get(self.origin)
        self.assertIsNone(origin0)

        id = self.storage.origin_add_one(self.origin)

        actual_origin = self.storage.origin_get({'url': self.origin['url'],
                                                 'type': self.origin['type']})
        self.assertEqual(actual_origin['id'], id)

        id2 = self.storage.origin_add_one(self.origin)

        self.assertEqual(id, id2)

    def test_origin_add(self):
        origin0 = self.storage.origin_get(self.origin)
        self.assertIsNone(origin0)

        origin1, origin2 = self.storage.origin_add([self.origin, self.origin2])

        actual_origin = self.storage.origin_get({
            'url': self.origin['url'],
            'type': self.origin['type'],
        })
        self.assertEqual(actual_origin['id'], origin1['id'])

        actual_origin2 = self.storage.origin_get({
            'url': self.origin2['url'],
            'type': self.origin2['type'],
        })
        self.assertEqual(actual_origin2['id'], origin2['id'])

    def test_origin_add_twice(self):
        add1 = self.storage.origin_add([self.origin, self.origin2])
        add2 = self.storage.origin_add([self.origin, self.origin2])

        self.assertEqual(add1, add2)

    def test_origin_get(self):
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
                                          'url': self.origin['url']})

    def test_origin_search(self):
        found_origins = list(self.storage.origin_search(self.origin['url']))
        self.assertEqual(len(found_origins), 0)

        found_origins = list(self.storage.origin_search(self.origin['url'],
                                                        regexp=True))
        self.assertEqual(len(found_origins), 0)

        id = self.storage.origin_add_one(self.origin)
        origin_data = {'id': id,
                       'type': self.origin['type'],
                       'url': self.origin['url']}
        found_origins = list(self.storage.origin_search(self.origin['url']))
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin_data)

        found_origins = list(self.storage.origin_search(
            '.' + self.origin['url'][1:-1] + '.', regexp=True))
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin_data)

        id2 = self.storage.origin_add_one(self.origin2)
        origin2_data = {'id': id2,
                        'type': self.origin2['type'],
                        'url': self.origin2['url']}
        found_origins = list(self.storage.origin_search(self.origin2['url']))
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin2_data)

        found_origins = list(self.storage.origin_search(
            '.' + self.origin2['url'][1:-1] + '.', regexp=True))
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin2_data)

        found_origins = list(self.storage.origin_search('/'))
        self.assertEqual(len(found_origins), 2)

        found_origins = list(self.storage.origin_search('.*/.*', regexp=True))
        self.assertEqual(len(found_origins), 2)

        found_origins = list(self.storage.origin_search('/', offset=0, limit=1)) # noqa
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin_data)

        found_origins = list(self.storage.origin_search('.*/.*', offset=0, limit=1, regexp=True)) # noqa
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin_data)

        found_origins = list(self.storage.origin_search('/', offset=1, limit=1)) # noqa
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin2_data)

        found_origins = list(self.storage.origin_search('.*/.*', offset=1, limit=1, regexp=True)) # noqa
        self.assertEqual(len(found_origins), 1)
        self.assertEqual(found_origins[0], origin2_data)

    def test_origin_visit_add(self):
        # given
        self.assertIsNone(self.storage.origin_get(self.origin2))

        origin_id = self.storage.origin_add_one(self.origin2)
        self.assertIsNotNone(origin_id)

        # when
        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            date=self.date_visit2)

        # then
        self.assertEqual(origin_visit1['origin'], origin_id)
        self.assertIsNotNone(origin_visit1['visit'])

        actual_origin_visits = list(self.storage.origin_visit_get(origin_id))
        self.assertEqual(actual_origin_visits,
                         [{
                             'origin': origin_id,
                             'date': self.date_visit2,
                             'visit': origin_visit1['visit'],
                             'status': 'ongoing',
                             'metadata': None,
                             'snapshot': None,
                         }])

    def test_origin_visit_update(self):
        # given
        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id2 = self.storage.origin_add_one(self.origin)

        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            date=self.date_visit2)

        origin_visit2 = self.storage.origin_visit_add(
            origin_id,
            date=self.date_visit3)

        origin_visit3 = self.storage.origin_visit_add(
            origin_id2,
            date=self.date_visit3)

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
        self.assertEqual(actual_origin_visits, [{
            'origin': origin_visit2['origin'],
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'status': 'full',
            'metadata': visit1_metadata,
            'snapshot': None,
        }, {
            'origin': origin_visit2['origin'],
            'date': self.date_visit3,
            'visit': origin_visit2['visit'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }])

        actual_origin_visits_bis = list(self.storage.origin_visit_get(
            origin_id, limit=1))
        self.assertEqual(actual_origin_visits_bis,
                         [{
                             'origin': origin_visit2['origin'],
                             'date': self.date_visit2,
                             'visit': origin_visit1['visit'],
                             'status': 'full',
                             'metadata': visit1_metadata,
                             'snapshot': None,
                         }])

        actual_origin_visits_ter = list(self.storage.origin_visit_get(
            origin_id, last_visit=origin_visit1['visit']))
        self.assertEqual(actual_origin_visits_ter,
                         [{
                             'origin': origin_visit2['origin'],
                             'date': self.date_visit3,
                             'visit': origin_visit2['visit'],
                             'status': 'ongoing',
                             'metadata': None,
                             'snapshot': None,
                         }])

        actual_origin_visits2 = list(self.storage.origin_visit_get(origin_id2))
        self.assertEqual(actual_origin_visits2,
                         [{
                             'origin': origin_visit3['origin'],
                             'date': self.date_visit3,
                             'visit': origin_visit3['visit'],
                             'status': 'partial',
                             'metadata': None,
                             'snapshot': None,
                         }])

    def test_origin_visit_get_by(self):
        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id2 = self.storage.origin_add_one(self.origin)

        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            date=self.date_visit2)

        self.storage.snapshot_add(origin_id, origin_visit1['visit'],
                                  self.snapshot)

        # Add some other {origin, visit} entries
        self.storage.origin_visit_add(origin_id, date=self.date_visit3)
        self.storage.origin_visit_add(origin_id2, date=self.date_visit3)

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
            'snapshot': self.snapshot['id'],
        })

        # when
        actual_origin_visit1 = self.storage.origin_visit_get_by(
            origin_visit1['origin'], origin_visit1['visit'])

        # then
        self.assertEqual(actual_origin_visit1, expected_origin_visit)

    def test_origin_visit_get_by_no_result(self):
        # No result
        actual_origin_visit = self.storage.origin_visit_get_by(
            10, 999)

        self.assertIsNone(actual_origin_visit)

    def test_snapshot_add_get_empty(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add(origin_id, visit_id, self.empty_snapshot)

        by_id = self.storage.snapshot_get(self.empty_snapshot['id'])
        self.assertEqual(by_id, self.empty_snapshot)

        by_ov = self.storage.snapshot_get_by_origin_visit(origin_id, visit_id)
        self.assertEqual(by_ov, self.empty_snapshot)

    def test_snapshot_add_get_complete(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add(origin_id, visit_id, self.complete_snapshot)

        by_id = self.storage.snapshot_get(self.complete_snapshot['id'])
        self.assertEqual(by_id, self.complete_snapshot)

        by_ov = self.storage.snapshot_get_by_origin_visit(origin_id, visit_id)
        self.assertEqual(by_ov, self.complete_snapshot)

    def test_snapshot_add_count_branches(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add(origin_id, visit_id, self.complete_snapshot)

        snp_id = self.complete_snapshot['id']
        snp_size = self.storage.snapshot_count_branches(snp_id)

        expected_snp_size = {
            'alias': 1,
            'content': 1,
            'directory': 1,
            'release': 1,
            'revision': 1,
            'snapshot': 1,
            None: 1
        }

        self.assertEqual(snp_size, expected_snp_size)

    def test_snapshot_add_get_paginated(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add(origin_id, visit_id, self.complete_snapshot)

        snp_id = self.complete_snapshot['id']
        branches = self.complete_snapshot['branches']
        branch_names = list(sorted(branches))

        snapshot = self.storage.snapshot_get_branches(snp_id,
                                                      branches_from=b'release')

        rel_idx = branch_names.index(b'release')
        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in branch_names[rel_idx:]
            },
            'next_branch': None,
        }

        self.assertEqual(snapshot, expected_snapshot)

        snapshot = self.storage.snapshot_get_branches(snp_id,
                                                      branches_count=1)

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                 branch_names[0]: branches[branch_names[0]],
            },
            'next_branch': b'content',
        }
        self.assertEqual(snapshot, expected_snapshot)

        snapshot = self.storage.snapshot_get_branches(
            snp_id, branches_from=b'directory', branches_count=3)

        dir_idx = branch_names.index(b'directory')
        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in branch_names[dir_idx:dir_idx + 3]
            },
            'next_branch': branch_names[dir_idx + 3],
        }

        self.assertEqual(snapshot, expected_snapshot)

    def test_snapshot_add_get_filtered(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add(origin_id, visit_id, self.complete_snapshot)

        snp_id = self.complete_snapshot['id']
        branches = self.complete_snapshot['branches']

        snapshot = self.storage.snapshot_get_branches(
            snp_id, target_types=['release', 'revision'])

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: tgt
                for name, tgt in branches.items()
                if tgt and tgt['target_type'] in ['release', 'revision']
            },
            'next_branch': None,
        }

        self.assertEqual(snapshot, expected_snapshot)

        snapshot = self.storage.snapshot_get_branches(snp_id,
                                                      target_types=['alias'])

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: tgt
                for name, tgt in branches.items()
                if tgt and tgt['target_type'] == 'alias'
            },
            'next_branch': None,
        }

        self.assertEqual(snapshot, expected_snapshot)

    def test_snapshot_add_get(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add(origin_id, visit_id, self.snapshot)

        by_id = self.storage.snapshot_get(self.snapshot['id'])
        self.assertEqual(by_id, self.snapshot)

        by_ov = self.storage.snapshot_get_by_origin_visit(origin_id, visit_id)
        self.assertEqual(by_ov, self.snapshot)

        origin_visit_info = self.storage.origin_visit_get_by(origin_id,
                                                             visit_id)
        self.assertEqual(origin_visit_info['snapshot'], self.snapshot['id'])

    def test_snapshot_add_nonexistent_visit(self):
        origin_id = self.storage.origin_add_one(self.origin)
        visit_id = 54164461156

        with self.assertRaises(ValueError):
            self.storage.snapshot_add(origin_id, visit_id, self.snapshot)

    def test_snapshot_add_twice(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit1_id = origin_visit1['visit']
        self.storage.snapshot_add(origin_id, visit1_id, self.snapshot)

        by_ov1 = self.storage.snapshot_get_by_origin_visit(origin_id,
                                                           visit1_id)
        self.assertEqual(by_ov1, self.snapshot)

        origin_visit2 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit2)
        visit2_id = origin_visit2['visit']

        self.storage.snapshot_add(origin_id, visit2_id, self.snapshot)

        by_ov2 = self.storage.snapshot_get_by_origin_visit(origin_id,
                                                           visit2_id)
        self.assertEqual(by_ov2, self.snapshot)

    def test_snapshot_get_nonexistent(self):
        bogus_snapshot_id = b'bogus snapshot id 00'
        bogus_origin_id = 1
        bogus_visit_id = 1

        by_id = self.storage.snapshot_get(bogus_snapshot_id)
        self.assertIsNone(by_id)

        by_ov = self.storage.snapshot_get_by_origin_visit(bogus_origin_id,
                                                          bogus_visit_id)
        self.assertIsNone(by_ov)

    def test_snapshot_get_latest(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit1_id = origin_visit1['visit']
        origin_visit2 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit2)
        visit2_id = origin_visit2['visit']

        # Add a visit with the same date as the previous one
        origin_visit3 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit2)
        visit3_id = origin_visit3['visit']

        # Two visits, both with no snapshot: latest snapshot is None
        self.assertIsNone(self.storage.snapshot_get_latest(origin_id))

        # Add snapshot to visit1, latest snapshot = visit 1 snapshot
        self.storage.snapshot_add(origin_id, visit1_id, self.complete_snapshot)
        self.assertEqual(self.complete_snapshot,
                         self.storage.snapshot_get_latest(origin_id))

        # Status filter: both visits are status=ongoing, so no snapshot
        # returned
        self.assertIsNone(
            self.storage.snapshot_get_latest(origin_id,
                                             allowed_statuses=['full'])
        )

        # Mark the first visit as completed and check status filter again
        self.storage.origin_visit_update(origin_id, visit1_id, status='full')
        self.assertEqual(
            self.complete_snapshot,
            self.storage.snapshot_get_latest(origin_id,
                                             allowed_statuses=['full']),
        )

        # Add snapshot to visit2 and check that the new snapshot is returned
        self.storage.snapshot_add(origin_id, visit2_id, self.empty_snapshot)
        self.assertEqual(self.empty_snapshot,
                         self.storage.snapshot_get_latest(origin_id))

        # Check that the status filter is still working
        self.assertEqual(
            self.complete_snapshot,
            self.storage.snapshot_get_latest(origin_id,
                                             allowed_statuses=['full']),
        )

        # Add snapshot to visit3 (same date as visit2) and check that
        # the new snapshot is returned
        self.storage.snapshot_add(origin_id, visit3_id, self.complete_snapshot)
        self.assertEqual(self.complete_snapshot,
                         self.storage.snapshot_get_latest(origin_id))

    def test_stat_counters(self):
        expected_keys = ['content', 'directory',
                         'origin', 'person', 'revision']

        # Initially, all counters are 0

        self.storage.refresh_stat_counters()
        counters = self.storage.stat_counters()
        self.assertTrue(set(expected_keys) <= set(counters))
        for key in expected_keys:
            self.assertEqual(counters[key], 0)

        # Add a content. Only the content counter should increase.

        self.storage.content_add([self.cont])

        self.storage.refresh_stat_counters()
        counters = self.storage.stat_counters()

        self.assertTrue(set(expected_keys) <= set(counters))
        for key in expected_keys:
            if key != 'content':
                self.assertEqual(counters[key], 0)
        self.assertEqual(counters['content'], 1)

        # Add other objects. Check their counter increased as well.

        origin_id = self.storage.origin_add_one(self.origin2)
        origin_visit1 = self.storage.origin_visit_add(
            origin_id,
            date=self.date_visit2)
        self.storage.snapshot_add(origin_id, origin_visit1['visit'],
                                  self.snapshot)
        self.storage.directory_add([self.dir])
        self.storage.revision_add([self.revision])

        self.storage.refresh_stat_counters()
        counters = self.storage.stat_counters()
        self.assertEqual(counters['content'], 1)
        self.assertEqual(counters['directory'], 1)
        self.assertEqual(counters['snapshot'], 1)
        self.assertEqual(counters['origin'], 1)
        self.assertEqual(counters['revision'], 1)

    def test_content_find_with_present_content(self):
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

    def test_content_find_with_non_present_content(self):
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

    def test_content_find_bad_input(self):
        # 1. with bad input
        with self.assertRaises(ValueError):
            self.storage.content_find({})  # empty is bad

        # 2. with bad input
        with self.assertRaises(ValueError):
            self.storage.content_find(
                {'unknown-sha1': 'something'})  # not the right key

    def test_object_find_by_sha1_git(self):
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

    def test_tool_add(self):
        tool = {
            'name': 'some-unknown-tool',
            'version': 'some-version',
            'configuration': {"debian-package": "some-package"},
        }

        actual_tool = self.storage.tool_get(tool)
        self.assertIsNone(actual_tool)  # does not exist

        # add it
        actual_tools = list(self.storage.tool_add([tool]))

        self.assertEqual(len(actual_tools), 1)
        actual_tool = actual_tools[0]
        self.assertIsNotNone(actual_tool)  # now it exists
        new_id = actual_tool.pop('id')
        self.assertEqual(actual_tool, tool)

        actual_tools2 = list(self.storage.tool_add([tool]))
        actual_tool2 = actual_tools2[0]
        self.assertIsNotNone(actual_tool2)  # now it exists
        new_id2 = actual_tool2.pop('id')

        self.assertEqual(new_id, new_id2)
        self.assertEqual(actual_tool, actual_tool2)

    def test_tool_add_multiple(self):
        tool = {
            'name': 'some-unknown-tool',
            'version': 'some-version',
            'configuration': {"debian-package": "some-package"},
        }

        actual_tools = list(self.storage.tool_add([tool]))
        self.assertEqual(len(actual_tools), 1)

        new_tools = [tool, {
            'name': 'yet-another-tool',
            'version': 'version',
            'configuration': {},
        }]

        actual_tools = list(self.storage.tool_add(new_tools))
        self.assertEqual(len(actual_tools), 2)

        # order not guaranteed, so we iterate over results to check
        for tool in actual_tools:
            _id = tool.pop('id')
            self.assertIsNotNone(_id)
            self.assertIn(tool, new_tools)

    def test_tool_get_missing(self):
        tool = {
            'name': 'unknown-tool',
            'version': '3.1.0rc2-31-ga2cbb8c',
            'configuration': {"command_line": "nomossa <filepath>"},
        }

        actual_tool = self.storage.tool_get(tool)

        self.assertIsNone(actual_tool)

    def test_tool_metadata_get_missing_context(self):
        tool = {
            'name': 'swh-metadata-translator',
            'version': '0.0.1',
            'configuration': {"context": "unknown-context"},
        }

        actual_tool = self.storage.tool_get(tool)

        self.assertIsNone(actual_tool)

    def test_tool_metadata_get(self):
        tool = {
            'name': 'swh-metadata-translator',
            'version': '0.0.1',
            'configuration': {"type": "local", "context": "npm"},
        }

        tools = list(self.storage.tool_add([tool]))
        expected_tool = tools[0]

        # when
        actual_tool = self.storage.tool_get(tool)

        # then
        self.assertEqual(expected_tool, actual_tool)

    def test_metadata_provider_get(self):
        # given
        no_provider = self.storage.metadata_provider_get(6459456445615)
        self.assertIsNone(no_provider)
        # when
        provider_id = self.storage.metadata_provider_add(
            self.provider['name'],
            self.provider['type'],
            self.provider['url'],
            self.provider['metadata'])

        actual_provider = self.storage.metadata_provider_get(provider_id)
        expected_provider = {
            'provider_name': self.provider['name'],
            'provider_url': self.provider['url']
        }
        # then
        del actual_provider['id']
        self.assertTrue(actual_provider, expected_provider)

    def test_metadata_provider_get_by(self):
        # given
        no_provider = self.storage.metadata_provider_get_by({
            'provider_name': self.provider['name'],
            'provider_url': self.provider['url']
        })
        self.assertIsNone(no_provider)
        # when
        provider_id = self.storage.metadata_provider_add(
            self.provider['name'],
            self.provider['type'],
            self.provider['url'],
            self.provider['metadata'])

        actual_provider = self.storage.metadata_provider_get_by({
            'provider_name': self.provider['name'],
            'provider_url': self.provider['url']
        })
        # then
        self.assertTrue(provider_id, actual_provider['id'])

    def test_origin_metadata_add(self):
        # given
        origin_id = self.storage.origin_add([self.origin])[0]['id']
        origin_metadata0 = list(self.storage.origin_metadata_get_by(origin_id))
        self.assertTrue(len(origin_metadata0) == 0)

        tools = list(self.storage.tool_add([self.metadata_tool]))
        tool = tools[0]

        self.storage.metadata_provider_add(
                           self.provider['name'],
                           self.provider['type'],
                           self.provider['url'],
                           self.provider['metadata'])
        provider = self.storage.metadata_provider_get_by({
                            'provider_name': self.provider['name'],
                            'provider_url': self.provider['url']
                      })

        # when adding for the same origin 2 metadatas
        self.storage.origin_metadata_add(
                    origin_id,
                    self.origin_metadata['discovery_date'],
                    provider['id'],
                    tool['id'],
                    self.origin_metadata['metadata'])
        actual_om1 = list(self.storage.origin_metadata_get_by(origin_id))
        # then
        self.assertEqual(len(actual_om1), 1)
        self.assertEqual(actual_om1[0]['origin_id'], origin_id)

    def test_origin_metadata_get(self):
        # given
        origin_id = self.storage.origin_add([self.origin])[0]['id']
        origin_id2 = self.storage.origin_add([self.origin2])[0]['id']

        self.storage.metadata_provider_add(self.provider['name'],
                                           self.provider['type'],
                                           self.provider['url'],
                                           self.provider['metadata'])
        provider = self.storage.metadata_provider_get_by({
                            'provider_name': self.provider['name'],
                            'provider_url': self.provider['url']
                   })
        tool = list(self.storage.tool_add([self.metadata_tool]))[0]
        # when adding for the same origin 2 metadatas
        self.storage.origin_metadata_add(
                    origin_id,
                    self.origin_metadata['discovery_date'],
                    provider['id'],
                    tool['id'],
                    self.origin_metadata['metadata'])
        self.storage.origin_metadata_add(
                    origin_id2,
                    self.origin_metadata2['discovery_date'],
                    provider['id'],
                    tool['id'],
                    self.origin_metadata2['metadata'])
        self.storage.origin_metadata_add(
                    origin_id,
                    self.origin_metadata2['discovery_date'],
                    provider['id'],
                    tool['id'],
                    self.origin_metadata2['metadata'])
        all_metadatas = list(self.storage.origin_metadata_get_by(origin_id))
        metadatas_for_origin2 = list(self.storage.origin_metadata_get_by(
                                          origin_id2))
        expected_results = [{
            'origin_id': origin_id,
            'discovery_date': datetime.datetime(
                                2017, 1, 1, 23, 0,
                                tzinfo=datetime.timezone.utc),
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            },
            'provider_id': provider['id'],
            'provider_name': 'hal',
            'provider_type': 'deposit-client',
            'provider_url': 'http:///hal/inria',
            'tool_id': tool['id']
        }, {
            'origin_id': origin_id,
            'discovery_date': datetime.datetime(
                                2015, 1, 1, 23, 0,
                                tzinfo=datetime.timezone.utc),
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            },
            'provider_id': provider['id'],
            'provider_name': 'hal',
            'provider_type': 'deposit-client',
            'provider_url': 'http:///hal/inria',
            'tool_id': tool['id']
        }]

        # then
        self.assertEqual(len(all_metadatas), 2)
        self.assertEqual(len(metadatas_for_origin2), 1)
        self.assertCountEqual(all_metadatas, expected_results)

    def test_origin_metadata_get_by_provider_type(self):
        # given
        origin_id = self.storage.origin_add([self.origin])[0]['id']
        origin_id2 = self.storage.origin_add([self.origin2])[0]['id']
        provider1_id = self.storage.metadata_provider_add(
                           self.provider['name'],
                           self.provider['type'],
                           self.provider['url'],
                           self.provider['metadata'])
        provider1 = self.storage.metadata_provider_get_by({
                            'provider_name': self.provider['name'],
                            'provider_url': self.provider['url']
                   })
        self.assertEqual(provider1,
                         self.storage.metadata_provider_get(provider1_id))

        provider2_id = self.storage.metadata_provider_add(
                            'swMATH',
                            'registry',
                            'http://www.swmath.org/',
                            {'email': 'contact@swmath.org',
                             'license': 'All rights reserved'})
        provider2 = self.storage.metadata_provider_get_by({
                            'provider_name': 'swMATH',
                            'provider_url': 'http://www.swmath.org/'
                   })
        self.assertEqual(provider2,
                         self.storage.metadata_provider_get(provider2_id))

        # using the only tool now inserted in the data.sql, but for this
        # provider should be a crawler tool (not yet implemented)
        tool = list(self.storage.tool_add([self.metadata_tool]))[0]

        # when adding for the same origin 2 metadatas
        self.storage.origin_metadata_add(
                    origin_id,
                    self.origin_metadata['discovery_date'],
                    provider1['id'],
                    tool['id'],
                    self.origin_metadata['metadata'])
        self.storage.origin_metadata_add(
                    origin_id2,
                    self.origin_metadata2['discovery_date'],
                    provider2['id'],
                    tool['id'],
                    self.origin_metadata2['metadata'])
        provider_type = 'registry'
        m_by_provider = list(self.storage.
                             origin_metadata_get_by(
                                origin_id2,
                                provider_type))
        for item in m_by_provider:
            if 'id' in item:
                del item['id']
        expected_results = [{
            'origin_id': origin_id2,
            'discovery_date': datetime.datetime(
                                2017, 1, 1, 23, 0,
                                tzinfo=datetime.timezone.utc),
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            },
            'provider_id': provider2['id'],
            'provider_name': 'swMATH',
            'provider_type': provider_type,
            'provider_url': 'http://www.swmath.org/',
            'tool_id': tool['id']
        }]
        # then

        self.assertEqual(len(m_by_provider), 1)
        self.assertEqual(m_by_provider, expected_results)


class CommonPropTestStorage:
    def assert_contents_ok(self, expected_contents, actual_contents,
                           keys_to_check={'sha1', 'data'}):
        """Assert that a given list of contents matches on a given set of keys.

        """
        for k in keys_to_check:
            expected_list = sorted([c[k] for c in expected_contents])
            actual_list = sorted([c[k] for c in actual_contents])
            self.assertEqual(actual_list, expected_list)

    @given(gen_contents(min_size=1, max_size=4))
    def test_generate_content_get(self, contents):
        # add contents to storage
        self.storage.content_add(contents)

        # input the list of sha1s we want from storage
        get_sha1s = [c['sha1'] for c in contents]

        # retrieve contents
        actual_contents = list(self.storage.content_get(get_sha1s))

        self.assert_contents_ok(contents, actual_contents)

    @given(gen_contents(min_size=1, max_size=4))
    def test_generate_content_get_metadata(self, contents):
        # add contents to storage
        self.storage.content_add(contents)

        # input the list of sha1s we want from storage
        get_sha1s = [c['sha1'] for c in contents]

        # retrieve contents
        actual_contents = list(self.storage.content_get_metadata(get_sha1s))

        self.assertEqual(len(actual_contents), len(contents))

        # will check that all contents are retrieved correctly
        one_content = contents[0]
        # content_get_metadata does not return data
        keys_to_check = set(one_content.keys()) - {'data'}
        self.assert_contents_ok(contents, actual_contents,
                                keys_to_check=keys_to_check)

    def test_generate_content_get_range_limit_none(self):
        """content_get_range call with wrong limit input should fail"""
        with self.assertRaises(ValueError) as e:
            self.storage.content_get_range(start=None, end=None, limit=None)

        self.assertEqual(e.exception.args, (
            'Development error: limit should not be None',))

    @given(gen_contents(min_size=1, max_size=4))
    def test_generate_content_get_range_no_limit(self, contents):
        """content_get_range returns contents within range provided"""
        self.reset_storage_tables()
        # add contents to storage
        self.storage.content_add(contents)

        # input the list of sha1s we want from storage
        get_sha1s = sorted([c['sha1'] for c in contents])
        start = get_sha1s[0]
        end = get_sha1s[-1]

        # retrieve contents
        actual_result = self.storage.content_get_range(start, end)

        actual_contents = actual_result['contents']
        actual_next = actual_result['next']

        self.assertEqual(len(contents), len(actual_contents))
        self.assertIsNone(actual_next)

        one_content = contents[0]
        keys_to_check = set(one_content.keys()) - {'data'}
        self.assert_contents_ok(contents, actual_contents, keys_to_check)

    @given(gen_contents(min_size=4, max_size=4))
    def test_generate_content_get_range_limit(self, contents):
        """content_get_range paginates results if limit exceeded"""
        self.reset_storage_tables()
        contents_map = {c['sha1']: c for c in contents}

        # add contents to storage
        self.storage.content_add(contents)

        # input the list of sha1s we want from storage
        get_sha1s = sorted([c['sha1'] for c in contents])
        start = get_sha1s[0]
        end = get_sha1s[-1]

        # retrieve contents limited to 3 results
        limited_results = len(contents) - 1
        actual_result = self.storage.content_get_range(start, end,
                                                       limit=limited_results)

        actual_contents = actual_result['contents']
        actual_next = actual_result['next']

        self.assertEqual(limited_results, len(actual_contents))
        self.assertIsNotNone(actual_next)
        self.assertEqual(actual_next, get_sha1s[-1])

        expected_contents = [contents_map[sha1] for sha1 in get_sha1s[:-1]]
        keys_to_check = set(contents[0].keys()) - {'data'}
        self.assert_contents_ok(expected_contents, actual_contents,
                                keys_to_check)

        # retrieve next part
        actual_results2 = self.storage.content_get_range(start=end, end=end)
        actual_contents2 = actual_results2['contents']
        actual_next2 = actual_results2['next']

        self.assertEqual(1, len(actual_contents2))
        self.assertIsNone(actual_next2)

        self.assert_contents_ok([contents_map[actual_next]], actual_contents2,
                                keys_to_check)


@pytest.mark.db
class TestLocalStorage(CommonTestStorage, StorageTestDbFixture,
                       unittest.TestCase):
    """Test the local storage"""

    # Can only be tested with local storage as you can't mock
    # datetimes for the remote server
    def test_fetch_history(self):
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
    def test_person_get(self):
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

    # This test is only relevant on the local storage, with an actual
    # objstorage raising an exception
    def test_content_add_objstorage_exception(self):
        self.storage.objstorage.add = Mock(
            side_effect=Exception('mocked broken objstorage')
        )

        with self.assertRaises(Exception) as e:
            self.storage.content_add([self.cont])

        self.assertEqual(e.exception.args, ('mocked broken objstorage',))
        missing = list(self.storage.content_missing([self.cont]))
        self.assertEqual(missing, [self.cont['sha1']])


@pytest.mark.db
@pytest.mark.property_based
class PropTestLocalStorage(CommonPropTestStorage, StorageTestDbFixture,
                           unittest.TestCase):
    pass


class AlteringSchemaTest(TestStorageData, StorageTestDbFixture,
                         unittest.TestCase):
    """This class is dedicated for the rare case where the schema needs to
       be altered dynamically.

       Otherwise, the tests could be blocking when ran altogether.

    """
    def test_content_update(self):
        cont = copy.deepcopy(self.cont)

        self.storage.content_add([cont])
        # alter the sha1_git for example
        cont['sha1_git'] = hash_to_bytes(
            '3a60a5275d0333bf13468e8b3dcab90f4046e654')

        self.storage.content_update([cont], keys=['sha1_git'])

        with self.storage.get_db().transaction() as cur:
            cur.execute('SELECT sha1, sha1_git, sha256, length, status'
                        ' FROM content WHERE sha1 = %s',
                        (cont['sha1'],))
            datum = cur.fetchone()

        self.assertEqual(
            (datum[0].tobytes(), datum[1].tobytes(), datum[2].tobytes(),
             datum[3], datum[4]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible'))

    def test_content_update_with_new_cols(self):
        with self.storage.get_db().transaction() as cur:
            cur.execute("""alter table content
                           add column test text default null,
                           add column test2 text default null""")

        cont = copy.deepcopy(self.cont2)
        self.storage.content_add([cont])
        cont['test'] = 'value-1'
        cont['test2'] = 'value-2'

        self.storage.content_update([cont], keys=['test', 'test2'])
        with self.storage.get_db().transaction() as cur:
            cur.execute(
                'SELECT sha1, sha1_git, sha256, length, status, test, test2'
                ' FROM content WHERE sha1 = %s',
                (cont['sha1'],))

            datum = cur.fetchone()

        self.assertEqual(
            (datum[0].tobytes(), datum[1].tobytes(), datum[2].tobytes(),
             datum[3], datum[4], datum[5], datum[6]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible', cont['test'], cont['test2']))

        with self.storage.get_db().transaction() as cur:
            cur.execute("""alter table content drop column test,
                           drop column test2""")
