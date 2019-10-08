# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from contextlib import contextmanager
import datetime
import itertools
import queue
import random
import threading
import unittest
from collections import defaultdict
from unittest.mock import Mock, patch

import psycopg2
import pytest

from hypothesis import given, strategies, settings, HealthCheck

from typing import ClassVar, Optional

from swh.model import from_disk, identifiers
from swh.model.hashutil import hash_to_bytes
from swh.model.hypothesis_strategies import origins, objects
from swh.storage.tests.storage_testing import StorageTestFixture
from swh.storage import HashCollision

from .generate_data_test import gen_contents


@pytest.mark.db
class StorageTestDbFixture(StorageTestFixture):
    def setUp(self):
        super().setUp()
        self.maxDiff = None

    def tearDown(self):
        self.reset_storage()
        if hasattr(self.storage, '_pool') and self.storage._pool:
            self.storage._pool.closeall()
        super().tearDown()

    def get_db(self):
        return self.storage.db()

    @contextmanager
    def db_transaction(self):
        with self.get_db() as db:
            with db.transaction() as cur:
                yield db, cur


class TestStorageData:
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)

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
            'origin': 'file:///dev/zero',
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

        self.dir4 = {
            'id': hash_to_bytes('33e45d56f88993aae6a0198013efa80716fd8922'),
            'entries': [
                {
                    'name': b'subdir1',
                    'type': 'dir',
                    'target': self.dir3['id'],
                    'perms': from_disk.DentryPerms.directory,
                },
            ]
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
            'type': 'hg',
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
        }

        self.empty_snapshot = {
            'id': hash_to_bytes('1a8893e6a86f444e8be8e7bda6cb34fb1735a00e'),
            'branches': {},
        }

        self.complete_snapshot = {
            'id': hash_to_bytes('6e65b86363953b780d92b0a928f3e8fcdd10db36'),
            'branches': {
                b'directory': {
                    'target': hash_to_bytes(
                        '1bd0e65f7d2ff14ae994de17a1e7fe65111dcad8'),
                    'target_type': 'directory',
                },
                b'directory2': {
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
    maxDiff = None  # type: ClassVar[Optional[int]]
    _test_origin_ids = True

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

        insertion_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        actual_result = self.storage.content_add([cont])
        insertion_end_time = datetime.datetime.now(tz=datetime.timezone.utc)

        self.assertEqual(actual_result, {
            'content:add': 1,
            'content:add:bytes': cont['length'],
            'skipped_content:add': 0
        })

        self.assertEqual(list(self.storage.content_get([cont['sha1']])),
                         [{'sha1': cont['sha1'], 'data': cont['data']}])

        expected_cont = cont.copy()
        del expected_cont['data']
        journal_objects = list(self.journal_writer.objects)
        for (obj_type, obj) in journal_objects:
            self.assertLessEqual(insertion_start_time, obj['ctime'])
            self.assertLessEqual(obj['ctime'], insertion_end_time)
            del obj['ctime']
        self.assertEqual(journal_objects,
                         [('content', expected_cont)])

    def test_content_add_validation(self):
        cont = self.cont

        with self.assertRaisesRegex(ValueError, 'status'):
            self.storage.content_add([{**cont, 'status': 'foobar'}])

        with self.assertRaisesRegex(ValueError, "(?i)length"):
            self.storage.content_add([{**cont, 'length': -2}])

        with self.assertRaisesRegex(
                (ValueError, psycopg2.IntegrityError), 'reason') as cm:
            self.storage.content_add([{**cont, 'status': 'absent'}])

        if type(cm.exception) == psycopg2.IntegrityError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.NOT_NULL_VIOLATION)

        with self.assertRaisesRegex(
                ValueError,
                "^Must not provide a reason if content is not absent.$"):
            self.storage.content_add([{**cont, 'reason': 'foobar'}])

    def test_content_get_missing(self):
        cont = self.cont

        self.storage.content_add([cont])

        # Query a single missing content
        results = list(self.storage.content_get(
            [self.cont2['sha1']]))
        self.assertEqual(results,
                         [None])

        # Check content_get does not abort after finding a missing content
        results = list(self.storage.content_get(
            [self.cont['sha1'], self.cont2['sha1']]))
        self.assertEqual(results,
                         [{'sha1': cont['sha1'], 'data': cont['data']}, None])

        # Check content_get does not discard found countent when it finds
        # a missing content.
        results = list(self.storage.content_get(
            [self.cont2['sha1'], self.cont['sha1']]))
        self.assertEqual(results,
                         [None, {'sha1': cont['sha1'], 'data': cont['data']}])

    def test_content_add_same_input(self):
        cont = self.cont

        actual_result = self.storage.content_add([cont, cont])
        self.assertEqual(actual_result, {
            'content:add': 1,
            'content:add:bytes': cont['length'],
            'skipped_content:add': 0
        })

    def test_content_add_different_input(self):
        cont = self.cont
        cont2 = self.cont2

        actual_result = self.storage.content_add([cont, cont2])
        self.assertEqual(actual_result, {
            'content:add': 2,
            'content:add:bytes': cont['length'] + cont2['length'],
            'skipped_content:add': 0
        })

    def test_content_add_twice(self):
        actual_result = self.storage.content_add([self.cont])
        self.assertEqual(actual_result, {
            'content:add': 1,
            'content:add:bytes': self.cont['length'],
            'skipped_content:add': 0
        })
        self.assertEqual(len(self.journal_writer.objects), 1)

        actual_result = self.storage.content_add([self.cont, self.cont2])
        self.assertEqual(actual_result, {
            'content:add': 1,
            'content:add:bytes': self.cont2['length'],
            'skipped_content:add': 0
        })
        self.assertEqual(len(self.journal_writer.objects), 2)

        self.assertEqual(len(self.storage.content_find(self.cont)), 1)
        self.assertEqual(len(self.storage.content_find(self.cont2)), 1)

    def test_content_add_db(self):
        cont = self.cont

        actual_result = self.storage.content_add([cont])

        self.assertEqual(actual_result, {
            'content:add': 1,
            'content:add:bytes': cont['length'],
            'skipped_content:add': 0
        })

        if hasattr(self.storage, 'objstorage'):
            self.assertIn(cont['sha1'], self.storage.objstorage)

        with self.db_transaction() as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, length, status'
                        ' FROM content WHERE sha1 = %s',
                        (cont['sha1'],))
            datum = cur.fetchone()

        self.assertEqual(
            datum,
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible'))

        expected_cont = cont.copy()
        del expected_cont['data']
        journal_objects = list(self.journal_writer.objects)
        for (obj_type, obj) in journal_objects:
            del obj['ctime']
        self.assertEqual(journal_objects,
                         [('content', expected_cont)])

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

    def test_content_add_metadata(self):
        cont = self.cont.copy()
        del cont['data']
        cont['ctime'] = datetime.datetime.now()

        actual_result = self.storage.content_add_metadata([cont])
        self.assertEqual(actual_result, {
            'content:add': 1,
            'skipped_content:add': 0
        })

        expected_cont = cont.copy()
        del expected_cont['ctime']
        self.assertEqual(
            list(self.storage.content_get_metadata([cont['sha1']])),
            [expected_cont])

        self.assertEqual(list(self.journal_writer.objects),
                         [('content', cont)])

    def test_content_add_metadata_same_input(self):
        cont = self.cont.copy()
        del cont['data']
        cont['ctime'] = datetime.datetime.now()

        actual_result = self.storage.content_add_metadata([cont, cont])
        self.assertEqual(actual_result, {
            'content:add': 1,
            'skipped_content:add': 0
        })

    def test_content_add_metadata_different_input(self):
        cont = self.cont.copy()
        del cont['data']
        cont['ctime'] = datetime.datetime.now()
        cont2 = self.cont2.copy()
        del cont2['data']
        cont2['ctime'] = datetime.datetime.now()

        actual_result = self.storage.content_add_metadata([cont, cont2])
        self.assertEqual(actual_result, {
            'content:add': 2,
            'skipped_content:add': 0
        })

    def test_content_add_metadata_db(self):
        cont = self.cont.copy()
        del cont['data']
        cont['ctime'] = datetime.datetime.now()

        actual_result = self.storage.content_add_metadata([cont])

        self.assertEqual(actual_result, {
            'content:add': 1,
            'skipped_content:add': 0
        })

        if hasattr(self.storage, 'objstorage'):
            self.assertNotIn(cont['sha1'], self.storage.objstorage)

        with self.db_transaction() as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, length, status'
                        ' FROM content WHERE sha1 = %s',
                        (cont['sha1'],))
            datum = cur.fetchone()

        self.assertEqual(
            datum,
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible'))

        self.assertEqual(list(self.journal_writer.objects),
                         [('content', cont)])

    def test_content_add_metadata_collision(self):
        cont1 = self.cont.copy()
        del cont1['data']
        cont1['ctime'] = datetime.datetime.now()

        # create (corrupted) content with same sha1{,_git} but != sha256
        cont1b = cont1.copy()
        sha256_array = bytearray(cont1b['sha256'])
        sha256_array[0] += 1
        cont1b['sha256'] = bytes(sha256_array)

        with self.assertRaises(HashCollision) as cm:
            self.storage.content_add_metadata([cont1, cont1b])

        self.assertIn(cm.exception.args[0], ['sha1', 'sha1_git', 'blake2s256'])

    def test_skipped_content_add_db(self):
        cont = self.skipped_cont.copy()
        cont2 = self.skipped_cont2.copy()
        cont2['blake2s256'] = None

        actual_result = self.storage.content_add([cont, cont, cont2])

        self.assertEqual(actual_result, {
            'content:add': 0,
            'content:add:bytes': 0,
            'skipped_content:add': 2,
        })

        with self.db_transaction() as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, blake2s256, '
                        'length, status, reason '
                        'FROM skipped_content ORDER BY sha1_git')

            data = cur.fetchall()

        self.assertEqual(2, len(data))
        self.assertEqual(
            data[0],
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['blake2s256'], cont['length'], 'absent',
             'Content too long')
        )

        self.assertEqual(
            data[1],
            (cont2['sha1'], cont2['sha1_git'], cont2['sha256'],
             cont2['blake2s256'], cont2['length'], 'absent',
             'Content too long')
        )

    def test_skipped_content_add(self):
        cont = self.skipped_cont.copy()
        cont2 = self.skipped_cont2.copy()
        cont2['blake2s256'] = None

        missing = list(self.storage.skipped_content_missing([cont, cont2]))

        self.assertEqual(len(missing), 2, missing)

        actual_result = self.storage.content_add([cont, cont, cont2])

        self.assertEqual(actual_result, {
            'content:add': 0,
            'content:add:bytes': 0,
            'skipped_content:add': 2,
        })

        missing = list(self.storage.skipped_content_missing([cont, cont2]))

        self.assertEqual(missing, [])

    @pytest.mark.property_based
    @settings(deadline=None)  # this test is very slow
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

    @pytest.mark.property_based
    @given(strategies.sets(
        elements=strategies.sampled_from(
            ['sha256', 'sha1_git', 'blake2s256']),
        min_size=0))
    def test_content_missing_unknown_algo(self, algos):
        algos |= {'sha1'}
        cont2 = self.cont2
        missing_cont = self.missing_cont
        self.storage.content_add([cont2])
        test_contents = [cont2]
        missing_per_hash = defaultdict(list)
        for i in range(16):
            test_content = missing_cont.copy()
            for hash in algos:
                test_content[hash] = bytes([i]) + test_content[hash][1:]
                missing_per_hash[hash].append(test_content[hash])
            test_content['nonexisting_algo'] = b'\x00'
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

        actual_result = self.storage.directory_add([self.dir])
        self.assertEqual(actual_result, {'directory:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('directory', self.dir)])

        actual_data = list(self.storage.directory_ls(self.dir['id']))
        expected_data = list(self._transform_entries(self.dir))
        self.assertCountEqual(expected_data, actual_data)

        after_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([], after_missing)

    def test_directory_add_validation(self):
        dir_ = copy.deepcopy(self.dir)
        dir_['entries'][0]['type'] = 'foobar'

        with self.assertRaisesRegex(ValueError, 'type.*foobar'):
            self.storage.directory_add([dir_])

        dir_ = copy.deepcopy(self.dir)
        del dir_['entries'][0]['target']

        with self.assertRaisesRegex(
                (TypeError, psycopg2.IntegrityError), 'target') as cm:
            self.storage.directory_add([dir_])

        if type(cm.exception) == psycopg2.IntegrityError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.NOT_NULL_VIOLATION)

    def test_directory_add_twice(self):
        actual_result = self.storage.directory_add([self.dir])
        self.assertEqual(actual_result, {'directory:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('directory', self.dir)])

        actual_result = self.storage.directory_add([self.dir])
        self.assertEqual(actual_result, {'directory:add': 0})

        self.assertEqual(list(self.journal_writer.objects),
                         [('directory', self.dir)])

    def test_directory_get_recursive(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        actual_result = self.storage.directory_add(
            [self.dir, self.dir2, self.dir3])
        self.assertEqual(actual_result, {'directory:add': 3})

        self.assertEqual(list(self.journal_writer.objects),
                         [('directory', self.dir),
                          ('directory', self.dir2),
                          ('directory', self.dir3)])

        # List directory containing a file and an unknown subdirectory
        actual_data = list(self.storage.directory_ls(
            self.dir['id'], recursive=True))
        expected_data = list(self._transform_entries(self.dir))
        self.assertCountEqual(expected_data, actual_data)

        # List directory containing a file and an unknown subdirectory
        actual_data = list(self.storage.directory_ls(
            self.dir2['id'], recursive=True))
        expected_data = list(self._transform_entries(self.dir2))
        self.assertCountEqual(expected_data, actual_data)

        # List directory containing a known subdirectory, entries should
        # be both those of the directory and of the subdir
        actual_data = list(self.storage.directory_ls(
            self.dir3['id'], recursive=True))
        expected_data = list(itertools.chain(
            self._transform_entries(self.dir3),
            self._transform_entries(self.dir, prefix=b'subdir/')))
        self.assertCountEqual(expected_data, actual_data)

    def test_directory_get_non_recursive(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        actual_result = self.storage.directory_add(
            [self.dir, self.dir2, self.dir3])
        self.assertEqual(actual_result, {'directory:add': 3})

        self.assertEqual(list(self.journal_writer.objects),
                         [('directory', self.dir),
                          ('directory', self.dir2),
                          ('directory', self.dir3)])

        # List directory containing a file and an unknown subdirectory
        actual_data = list(self.storage.directory_ls(self.dir['id']))
        expected_data = list(self._transform_entries(self.dir))
        self.assertCountEqual(expected_data, actual_data)

        # List directory contaiining a single file
        actual_data = list(self.storage.directory_ls(self.dir2['id']))
        expected_data = list(self._transform_entries(self.dir2))
        self.assertCountEqual(expected_data, actual_data)

        # List directory containing a known subdirectory, entries should
        # only be those of the parent directory, not of the subdir
        actual_data = list(self.storage.directory_ls(self.dir3['id']))
        expected_data = list(self._transform_entries(self.dir3))
        self.assertCountEqual(expected_data, actual_data)

    def test_directory_entry_get_by_path(self):
        # given
        init_missing = list(self.storage.directory_missing([self.dir3['id']]))
        self.assertEqual([self.dir3['id']], init_missing)

        actual_result = self.storage.directory_add([self.dir3, self.dir4])
        self.assertEqual(actual_result, {'directory:add': 2})

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

        # same, but deeper
        for entry, expected_entry in zip(self.dir3['entries'],
                                         expected_entries):
            actual_entry = self.storage.directory_entry_get_by_path(
                self.dir4['id'],
                [b'subdir1', entry['name']])
            expected_entry = expected_entry.copy()
            expected_entry['name'] = b'subdir1/' + expected_entry['name']
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

        actual_result = self.storage.revision_add([self.revision])
        self.assertEqual(actual_result, {'revision:add': 1})

        end_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([], list(end_missing))

        self.assertEqual(list(self.journal_writer.objects),
                         [('revision', self.revision)])

    def test_revision_add_validation(self):
        rev = copy.deepcopy(self.revision)
        rev['date']['offset'] = 2**16

        with self.assertRaisesRegex(
                (ValueError, psycopg2.DataError), 'offset') as cm:
            self.storage.revision_add([rev])

        if type(cm.exception) == psycopg2.DataError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE)

        rev = copy.deepcopy(self.revision)
        rev['committer_date']['offset'] = 2**16

        with self.assertRaisesRegex(
                (ValueError, psycopg2.DataError), 'offset') as cm:
            self.storage.revision_add([rev])

        if type(cm.exception) == psycopg2.DataError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE)

        rev = copy.deepcopy(self.revision)
        rev['type'] = 'foobar'

        with self.assertRaisesRegex(
                (ValueError, psycopg2.DataError), '(?i)type') as cm:
            self.storage.revision_add([rev])

        if type(cm.exception) == psycopg2.DataError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION)

    def test_revision_add_twice(self):
        actual_result = self.storage.revision_add([self.revision])
        self.assertEqual(actual_result, {'revision:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('revision', self.revision)])

        actual_result = self.storage.revision_add(
            [self.revision, self.revision2])
        self.assertEqual(actual_result, {'revision:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('revision', self.revision),
                          ('revision', self.revision2)])

    def test_revision_add_name_clash(self):
        revision1 = self.revision.copy()
        revision2 = self.revision2.copy()
        revision1['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe',
            'email': b'john.doe@example.com'
        }
        revision2['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe ',
            'email': b'john.doe@example.com '
        }
        actual_result = self.storage.revision_add([revision1, revision2])
        self.assertEqual(actual_result, {'revision:add': 2})

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

        self.assertEqual(list(self.journal_writer.objects),
                         [('revision', self.revision3),
                          ('revision', self.revision4)])

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

    def test_revision_log_unknown_revision(self):
        rev_log = list(self.storage.revision_log([self.revision['id']]))
        self.assertEqual(rev_log, [])

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

        actual_result = self.storage.release_add([self.release, self.release2])
        self.assertEqual(actual_result, {'release:add': 2})

        end_missing = self.storage.release_missing([self.release['id'],
                                                    self.release2['id']])
        self.assertEqual([], list(end_missing))

        self.assertEqual(list(self.journal_writer.objects),
                         [('release', self.release),
                          ('release', self.release2)])

    def test_release_add_no_author_date(self):
        release = self.release.copy()
        release['author'] = None
        release['date'] = None

        actual_result = self.storage.release_add([release])
        self.assertEqual(actual_result, {'release:add': 1})

        end_missing = self.storage.release_missing([self.release['id']])
        self.assertEqual([], list(end_missing))

        self.assertEqual(list(self.journal_writer.objects),
                         [('release', release)])

    def test_release_add_validation(self):
        rel = copy.deepcopy(self.release)
        rel['date']['offset'] = 2**16

        with self.assertRaisesRegex(
                (ValueError, psycopg2.DataError), 'offset') as cm:
            self.storage.release_add([rel])

        if type(cm.exception) == psycopg2.DataError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE)

        rel = copy.deepcopy(self.release)
        rel['author'] = None

        with self.assertRaisesRegex(
                (ValueError, psycopg2.IntegrityError), 'date') as cm:
            self.storage.release_add([rel])

        if type(cm.exception) == psycopg2.IntegrityError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.CHECK_VIOLATION)

    def test_release_add_twice(self):
        actual_result = self.storage.release_add([self.release])
        self.assertEqual(actual_result, {'release:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('release', self.release)])

        actual_result = self.storage.release_add([self.release, self.release2])
        self.assertEqual(actual_result, {'release:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('release', self.release),
                          ('release', self.release2)])

    def test_release_add_name_clash(self):
        release1 = self.release.copy()
        release2 = self.release2.copy()
        release1['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe',
            'email': b'john.doe@example.com'
        }
        release2['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe ',
            'email': b'john.doe@example.com '
        }
        actual_result = self.storage.release_add([release1, release2])
        self.assertEqual(actual_result, {'release:add': 2})

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

        actual_origin = self.storage.origin_get({'url': self.origin['url']})
        if self._test_origin_ids:
            self.assertEqual(actual_origin['id'], id)
        self.assertEqual(actual_origin['url'], self.origin['url'])

        id2 = self.storage.origin_add_one(self.origin)

        self.assertEqual(id, id2)

    def test_origin_add(self):
        origin0 = self.storage.origin_get([self.origin])[0]
        self.assertIsNone(origin0)

        origin1, origin2 = self.storage.origin_add([self.origin, self.origin2])

        actual_origin = self.storage.origin_get([{
            'url': self.origin['url'],
        }])[0]
        if self._test_origin_ids:
            self.assertEqual(actual_origin['id'], origin1['id'])
        self.assertEqual(actual_origin['url'], origin1['url'])

        actual_origin2 = self.storage.origin_get([{
            'url': self.origin2['url'],
        }])[0]
        if self._test_origin_ids:
            self.assertEqual(actual_origin2['id'], origin2['id'])
        self.assertEqual(actual_origin2['url'], origin2['url'])

        if 'id' in actual_origin:
            del actual_origin['id']
            del actual_origin2['id']

        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', actual_origin),
                          ('origin', actual_origin2)])

    def test_origin_add_twice(self):
        add1 = self.storage.origin_add([self.origin, self.origin2])

        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', self.origin),
                          ('origin', self.origin2)])

        add2 = self.storage.origin_add([self.origin, self.origin2])

        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', self.origin),
                          ('origin', self.origin2)])

        self.assertEqual(add1, add2)

    def test_origin_add_validation(self):
        with self.assertRaisesRegex((TypeError, KeyError), 'url'):
            self.storage.origin_add([{'type': 'git'}])

    def test_origin_get_legacy(self):
        self.assertIsNone(self.storage.origin_get(self.origin))
        id = self.storage.origin_add_one(self.origin)

        # lookup per url (returns id)
        actual_origin0 = self.storage.origin_get(
            {'url': self.origin['url']})
        if self._test_origin_ids:
            self.assertEqual(actual_origin0['id'], id)
        self.assertEqual(actual_origin0['url'], self.origin['url'])

        # lookup per id (returns dict)
        if self._test_origin_ids:
            actual_origin1 = self.storage.origin_get({'id': id})

            self.assertEqual(actual_origin1, {'id': id,
                                              'type': self.origin['type'],
                                              'url': self.origin['url']})

    def test_origin_get(self):
        self.assertIsNone(self.storage.origin_get(self.origin))
        origin_id = self.storage.origin_add_one(self.origin)

        # lookup per url (returns id)
        actual_origin0 = self.storage.origin_get(
            [{'url': self.origin['url']}])
        self.assertEqual(len(actual_origin0), 1, actual_origin0)
        if self._test_origin_ids:
            self.assertEqual(actual_origin0[0]['id'], origin_id)
        self.assertEqual(actual_origin0[0]['url'], self.origin['url'])

        if self._test_origin_ids:
            # lookup per id (returns dict)
            actual_origin1 = self.storage.origin_get([{'id': origin_id}])

            self.assertEqual(len(actual_origin1), 1, actual_origin1)
            self.assertEqual(actual_origin1[0], {'id': origin_id,
                                                 'type': self.origin['type'],
                                                 'url': self.origin['url']})

    def test_origin_get_consistency(self):
        self.assertIsNone(self.storage.origin_get(self.origin))
        id = self.storage.origin_add_one(self.origin)

        with self.assertRaises(ValueError):
            self.storage.origin_get([
                {'url': self.origin['url']},
                {'id': id}])

    def test_origin_search_single_result(self):
        found_origins = list(self.storage.origin_search(self.origin['url']))
        self.assertEqual(len(found_origins), 0)

        found_origins = list(self.storage.origin_search(self.origin['url'],
                                                        regexp=True))
        self.assertEqual(len(found_origins), 0)

        self.storage.origin_add_one(self.origin)
        origin_data = {
            'type': self.origin['type'],
            'url': self.origin['url']}
        found_origins = list(self.storage.origin_search(self.origin['url']))
        self.assertEqual(len(found_origins), 1)
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        self.assertEqual(found_origins[0], origin_data)

        found_origins = list(self.storage.origin_search(
            '.' + self.origin['url'][1:-1] + '.', regexp=True))
        self.assertEqual(len(found_origins), 1)
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        self.assertEqual(found_origins[0], origin_data)

        self.storage.origin_add_one(self.origin2)
        origin2_data = {
            'type': self.origin2['type'],
            'url': self.origin2['url']}
        found_origins = list(self.storage.origin_search(self.origin2['url']))
        self.assertEqual(len(found_origins), 1)
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        self.assertEqual(found_origins[0], origin2_data)

        found_origins = list(self.storage.origin_search(
            '.' + self.origin2['url'][1:-1] + '.', regexp=True))
        self.assertEqual(len(found_origins), 1)
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        self.assertEqual(found_origins[0], origin2_data)

    def test_origin_search_no_regexp(self):
        self.storage.origin_add_one(self.origin)
        self.storage.origin_add_one(self.origin2)

        origin = self.storage.origin_get({'url': self.origin['url']})
        origin2 = self.storage.origin_get({'url': self.origin2['url']})

        # no pagination

        found_origins = list(self.storage.origin_search('/'))
        self.assertEqual(len(found_origins), 2)

        # offset=0

        found_origins0 = list(self.storage.origin_search('/', offset=0, limit=1)) # noqa
        self.assertEqual(len(found_origins0), 1)
        self.assertIn(found_origins0[0], [origin, origin2])

        # offset=1

        found_origins1 = list(self.storage.origin_search('/', offset=1, limit=1)) # noqa
        self.assertEqual(len(found_origins1), 1)
        self.assertIn(found_origins1[0], [origin, origin2])

        # check both origins were returned

        self.assertCountEqual(found_origins0 + found_origins1,
                              [origin, origin2])

    def test_origin_search_regexp_substring(self):
        self.storage.origin_add_one(self.origin)
        self.storage.origin_add_one(self.origin2)

        origin = self.storage.origin_get({'url': self.origin['url']})
        origin2 = self.storage.origin_get({'url': self.origin2['url']})

        # no pagination

        found_origins = list(self.storage.origin_search('/', regexp=True))
        self.assertEqual(len(found_origins), 2)

        # offset=0

        found_origins0 = list(self.storage.origin_search('/', offset=0, limit=1, regexp=True)) # noqa
        self.assertEqual(len(found_origins0), 1)
        self.assertIn(found_origins0[0], [origin, origin2])

        # offset=1

        found_origins1 = list(self.storage.origin_search('/', offset=1, limit=1, regexp=True)) # noqa
        self.assertEqual(len(found_origins1), 1)
        self.assertIn(found_origins1[0], [origin, origin2])

        # check both origins were returned

        self.assertCountEqual(found_origins0 + found_origins1,
                              [origin, origin2])

    def test_origin_search_regexp_fullstring(self):
        self.storage.origin_add_one(self.origin)
        self.storage.origin_add_one(self.origin2)

        origin = self.storage.origin_get({'url': self.origin['url']})
        origin2 = self.storage.origin_get({'url': self.origin2['url']})

        # no pagination

        found_origins = list(self.storage.origin_search('.*/.*', regexp=True))
        self.assertEqual(len(found_origins), 2)

        # offset=0

        found_origins0 = list(self.storage.origin_search('.*/.*', offset=0, limit=1, regexp=True)) # noqa
        self.assertEqual(len(found_origins0), 1)
        self.assertIn(found_origins0[0], [origin, origin2])

        # offset=1

        found_origins1 = list(self.storage.origin_search('.*/.*', offset=1, limit=1, regexp=True)) # noqa
        self.assertEqual(len(found_origins1), 1)
        self.assertIn(found_origins1[0], [origin, origin2])

        # check both origins were returned

        self.assertCountEqual(
            found_origins0 + found_origins1,
            [origin, origin2])

    @given(strategies.booleans())
    def test_origin_visit_add(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # given
        self.assertIsNone(self.storage.origin_get([self.origin2])[0])

        origin_id = self.storage.origin_add_one(self.origin2)
        self.assertIsNotNone(origin_id)

        origin_id_or_url = self.origin2['url'] if use_url else origin_id

        # when
        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            type='git',
            date=self.date_visit2)

        actual_origin_visits = list(self.storage.origin_visit_get(
            origin_id_or_url))
        self.assertEqual(actual_origin_visits,
                         [{
                             'origin': origin_id,
                             'date': self.date_visit2,
                             'visit': origin_visit1['visit'],
                             'type': 'git',
                             'status': 'ongoing',
                             'metadata': None,
                             'snapshot': None,
                         }])

        expected_origin = self.origin2.copy()
        data = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': 'git',
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin_visit', data)])

    def test_origin_visit_get__unknown_origin(self):
        self.assertEqual([], list(self.storage.origin_visit_get('foo')))
        if self._test_origin_ids:
            self.assertEqual([], list(self.storage.origin_visit_get(10)))

    @given(strategies.booleans())
    def test_origin_visit_add_default_type(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # given
        self.assertIsNone(self.storage.origin_get([self.origin2])[0])

        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id_or_url = self.origin2['url'] if use_url else origin_id
        self.assertIsNotNone(origin_id)

        # when
        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit2)
        origin_visit2 = self.storage.origin_visit_add(
            origin_id_or_url,
            date='2018-01-01 23:00:00+00')

        # then
        self.assertEqual(origin_visit1['origin'], origin_id)
        self.assertIsNotNone(origin_visit1['visit'])

        actual_origin_visits = list(self.storage.origin_visit_get(
            origin_id_or_url))
        self.assertEqual(actual_origin_visits, [
            {
                'origin': origin_id,
                'date': self.date_visit2,
                'visit': origin_visit1['visit'],
                'type': 'hg',
                'status': 'ongoing',
                'metadata': None,
                'snapshot': None,
            },
            {
                'origin': origin_id,
                'date': self.date_visit3,
                'visit': origin_visit2['visit'],
                'type': 'hg',
                'status': 'ongoing',
                'metadata': None,
                'snapshot': None,
            },
        ])

        expected_origin = self.origin2.copy()
        data1 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': 'hg',
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': expected_origin,
            'date': self.date_visit3,
            'visit': origin_visit2['visit'],
            'type': 'hg',
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin_visit', data1),
                          ('origin_visit', data2)])

    def test_origin_visit_add_validation(self):
        origin_id_or_url = self.storage.origin_add_one(self.origin2)

        with self.assertRaises((TypeError, psycopg2.ProgrammingError)) as cm:
            self.storage.origin_visit_add(origin_id_or_url, date=[b'foo'])

        if type(cm.exception) == psycopg2.ProgrammingError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.UNDEFINED_FUNCTION)

    @given(strategies.booleans())
    def test_origin_visit_update(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # given
        origin_id = self.storage.origin_add_one(self.origin)
        origin_id2 = self.storage.origin_add_one(self.origin2)
        origin2_id_or_url = self.origin2['url'] if use_url else origin_id2

        origin_id_or_url = self.origin['url'] if use_url else origin_id

        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit2)

        origin_visit2 = self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit3)

        origin_visit3 = self.storage.origin_visit_add(
            origin2_id_or_url,
            date=self.date_visit3)

        # when
        visit1_metadata = {
            'contents': 42,
            'directories': 22,
        }
        self.storage.origin_visit_update(
            origin_id_or_url,
            origin_visit1['visit'], status='full',
            metadata=visit1_metadata)
        self.storage.origin_visit_update(
            origin2_id_or_url,
            origin_visit3['visit'], status='partial')

        # then
        actual_origin_visits = list(self.storage.origin_visit_get(
            origin_id_or_url))
        self.assertEqual(actual_origin_visits, [{
            'origin': origin_visit2['origin'],
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'status': 'full',
            'metadata': visit1_metadata,
            'snapshot': None,
        }, {
            'origin': origin_visit2['origin'],
            'date': self.date_visit3,
            'visit': origin_visit2['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }])

        actual_origin_visits_bis = list(self.storage.origin_visit_get(
            origin_id_or_url,
            limit=1))
        self.assertEqual(actual_origin_visits_bis,
                         [{
                             'origin': origin_visit2['origin'],
                             'date': self.date_visit2,
                             'visit': origin_visit1['visit'],
                             'type': self.origin['type'],
                             'status': 'full',
                             'metadata': visit1_metadata,
                             'snapshot': None,
                         }])

        actual_origin_visits_ter = list(self.storage.origin_visit_get(
            origin_id_or_url,
            last_visit=origin_visit1['visit']))
        self.assertEqual(actual_origin_visits_ter,
                         [{
                             'origin': origin_visit2['origin'],
                             'date': self.date_visit3,
                             'visit': origin_visit2['visit'],
                             'type': self.origin['type'],
                             'status': 'ongoing',
                             'metadata': None,
                             'snapshot': None,
                         }])

        actual_origin_visits2 = list(self.storage.origin_visit_get(
            origin2_id_or_url))
        self.assertEqual(actual_origin_visits2,
                         [{
                             'origin': origin_visit3['origin'],
                             'date': self.date_visit3,
                             'visit': origin_visit3['visit'],
                             'type': self.origin2['type'],
                             'status': 'partial',
                             'metadata': None,
                             'snapshot': None,
                         }])

        expected_origin = self.origin.copy()
        expected_origin2 = self.origin2.copy()
        data1 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': expected_origin,
            'date': self.date_visit3,
            'visit': origin_visit2['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data3 = {
            'origin': expected_origin2,
            'date': self.date_visit3,
            'visit': origin_visit3['visit'],
            'type': self.origin2['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data4 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'metadata': visit1_metadata,
            'status': 'full',
            'snapshot': None,
        }
        data5 = {
            'origin': expected_origin2,
            'date': self.date_visit3,
            'visit': origin_visit3['visit'],
            'type': self.origin2['type'],
            'status': 'partial',
            'metadata': None,
            'snapshot': None,
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin', expected_origin2),
                          ('origin_visit', data1),
                          ('origin_visit', data2),
                          ('origin_visit', data3),
                          ('origin_visit', data4),
                          ('origin_visit', data5)])

    def test_origin_visit_update_validation(self):
        origin_id = self.storage.origin_add_one(self.origin)
        visit = self.storage.origin_visit_add(
            origin_id,
            date=self.date_visit2)

        with self.assertRaisesRegex(
                (ValueError, psycopg2.DataError), 'status') as cm:
            self.storage.origin_visit_update(
                origin_id, visit['visit'], status='foobar')

        if type(cm.exception) == psycopg2.DataError:
            self.assertEqual(cm.exception.pgcode,
                             psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION)

    def test_origin_visit_find_by_date(self):
        # given
        self.storage.origin_add_one(self.origin)

        self.storage.origin_visit_add(
            self.origin['url'],
            date=self.date_visit2)

        origin_visit2 = self.storage.origin_visit_add(
            self.origin['url'],
            date=self.date_visit3)

        origin_visit3 = self.storage.origin_visit_add(
            self.origin['url'],
            date=self.date_visit2)

        # Simple case
        visit = self.storage.origin_visit_find_by_date(
            self.origin['url'], self.date_visit3)
        self.assertEqual(visit['visit'], origin_visit2['visit'])

        # There are two visits at the same date, the latest must be returned
        visit = self.storage.origin_visit_find_by_date(
            self.origin['url'], self.date_visit2)
        self.assertEqual(visit['visit'], origin_visit3['visit'])

    def test_origin_visit_find_by_date__unknown_origin(self):
        self.storage.origin_visit_find_by_date('foo', self.date_visit2)

    @settings(deadline=None)
    @given(strategies.booleans())
    def test_origin_visit_update_missing_snapshot(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # given
        origin_id = self.storage.origin_add_one(self.origin)
        origin_id_or_url = self.origin['url'] if use_url else origin_id

        origin_visit = self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit1)

        # when
        self.storage.origin_visit_update(
            origin_id_or_url,
            origin_visit['visit'],
            snapshot=self.snapshot['id'])

        # then
        actual_origin_visit = self.storage.origin_visit_get_by(
            origin_id_or_url,
            origin_visit['visit'])
        self.assertEqual(actual_origin_visit['snapshot'], self.snapshot['id'])

        # when
        self.storage.snapshot_add([self.snapshot])
        self.assertEqual(actual_origin_visit['snapshot'], self.snapshot['id'])

    @settings(deadline=None)
    @given(strategies.booleans())
    def test_origin_visit_get_by(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        origin_id = self.storage.origin_add_one(self.origin)
        origin_id2 = self.storage.origin_add_one(self.origin2)

        origin_id_or_url = self.origin['url'] if use_url else origin_id
        origin2_id_or_url = self.origin2['url'] if use_url else origin_id2

        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit2)

        self.storage.snapshot_add([self.snapshot])
        self.storage.origin_visit_update(
            origin_id_or_url,
            origin_visit1['visit'],
            snapshot=self.snapshot['id'])

        # Add some other {origin, visit} entries
        self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit3)
        self.storage.origin_visit_add(
            origin2_id_or_url,
            date=self.date_visit3)

        # when
        visit1_metadata = {
            'contents': 42,
            'directories': 22,
        }

        self.storage.origin_visit_update(
            origin_id_or_url,
            origin_visit1['visit'], status='full',
            metadata=visit1_metadata)

        expected_origin_visit = origin_visit1.copy()
        expected_origin_visit.update({
            'origin': origin_id,
            'visit': origin_visit1['visit'],
            'date': self.date_visit2,
            'type': self.origin['type'],
            'metadata': visit1_metadata,
            'status': 'full',
            'snapshot': self.snapshot['id'],
        })

        # when
        actual_origin_visit1 = self.storage.origin_visit_get_by(
            origin_id_or_url,
            origin_visit1['visit'])

        # then
        self.assertEqual(actual_origin_visit1, expected_origin_visit)

    def test_origin_visit_get_by__unknown_origin(self):
        if self._test_origin_ids:
            self.assertIsNone(self.storage.origin_visit_get_by(2, 10))
        self.assertIsNone(self.storage.origin_visit_get_by('foo', 10))

    @given(strategies.booleans())
    def test_origin_visit_upsert_new(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # given
        self.assertIsNone(self.storage.origin_get([self.origin2])[0])

        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id_or_url = self.origin2['url'] if use_url else origin_id
        self.assertIsNotNone(origin_id)

        # when
        self.storage.origin_visit_upsert([
            {
                 'origin': self.origin2,
                 'date': self.date_visit2,
                 'visit': 123,
                 'type': self.origin2['type'],
                 'status': 'full',
                 'metadata': None,
                 'snapshot': None,
             },
            {
                 'origin': self.origin2,
                 'date': '2018-01-01 23:00:00+00',
                 'visit': 1234,
                 'type': self.origin2['type'],
                 'status': 'full',
                 'metadata': None,
                 'snapshot': None,
             },
        ])

        # then
        actual_origin_visits = list(self.storage.origin_visit_get(
            origin_id_or_url))
        self.assertEqual(actual_origin_visits, [
            {
                'origin': origin_id,
                'date': self.date_visit2,
                'visit': 123,
                'type': self.origin2['type'],
                'status': 'full',
                'metadata': None,
                'snapshot': None,
            },
            {
                'origin': origin_id,
                'date': self.date_visit3,
                'visit': 1234,
                'type': self.origin2['type'],
                'status': 'full',
                'metadata': None,
                'snapshot': None,
            },
        ])

        expected_origin = self.origin2.copy()
        data1 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': 123,
            'type': self.origin2['type'],
            'status': 'full',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': expected_origin,
            'date': self.date_visit3,
            'visit': 1234,
            'type': self.origin2['type'],
            'status': 'full',
            'metadata': None,
            'snapshot': None,
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin_visit', data1),
                          ('origin_visit', data2)])

    @settings(deadline=None)
    @given(strategies.booleans())
    def test_origin_visit_upsert_existing(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # given
        self.assertIsNone(self.storage.origin_get([self.origin2])[0])

        origin_id = self.storage.origin_add_one(self.origin2)
        origin_id_or_url = self.origin2['url'] if use_url else origin_id
        self.assertIsNotNone(origin_id)

        # when
        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            date=self.date_visit2)
        self.storage.origin_visit_upsert([{
             'origin': self.origin2,
             'date': self.date_visit2,
             'visit': origin_visit1['visit'],
             'type': self.origin2['type'],
             'status': 'full',
             'metadata': None,
             'snapshot': None,
         }])

        # then
        self.assertEqual(origin_visit1['origin'], origin_id)
        self.assertIsNotNone(origin_visit1['visit'])

        actual_origin_visits = list(self.storage.origin_visit_get(
            origin_id_or_url))
        self.assertEqual(actual_origin_visits,
                         [{
                             'origin': origin_id,
                             'date': self.date_visit2,
                             'visit': origin_visit1['visit'],
                             'type': self.origin2['type'],
                             'status': 'full',
                             'metadata': None,
                             'snapshot': None,
                         }])

        expected_origin = self.origin2.copy()
        data1 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': self.origin2['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit1['visit'],
            'type': self.origin2['type'],
            'status': 'full',
            'metadata': None,
            'snapshot': None,
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin_visit', data1),
                          ('origin_visit', data2)])

    def test_origin_visit_get_by_no_result(self):
        if self._test_origin_ids:
            actual_origin_visit = self.storage.origin_visit_get_by(
                10, 999)
            self.assertIsNone(actual_origin_visit)

        self.storage.origin_add([self.origin])
        actual_origin_visit = self.storage.origin_visit_get_by(
            self.origin['url'], 999)
        self.assertIsNone(actual_origin_visit)

    @settings(deadline=None)  # this test is very slow
    @given(strategies.booleans())
    def test_origin_visit_get_latest(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        origin_id = self.storage.origin_add_one(self.origin)
        origin_id_or_url = self.origin['url'] if use_url else origin_id
        origin_url = self.origin['url']
        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            self.date_visit1)
        visit1_id = origin_visit1['visit']
        origin_visit2 = self.storage.origin_visit_add(
            origin_id_or_url,
            self.date_visit2)
        visit2_id = origin_visit2['visit']

        # Add a visit with the same date as the previous one
        origin_visit3 = self.storage.origin_visit_add(
            origin_id_or_url,
            self.date_visit2)
        visit3_id = origin_visit3['visit']

        origin_visit1 = self.storage.origin_visit_get_by(origin_url, visit1_id)
        origin_visit2 = self.storage.origin_visit_get_by(origin_url, visit2_id)
        origin_visit3 = self.storage.origin_visit_get_by(origin_url, visit3_id)

        # Two visits, both with no snapshot
        self.assertEqual(
            origin_visit3,
            self.storage.origin_visit_get_latest(origin_url))
        self.assertIsNone(
            self.storage.origin_visit_get_latest(origin_url,
                                                 require_snapshot=True))

        # Add snapshot to visit1; require_snapshot=True makes it return
        # visit1 and require_snapshot=False still returns visit2
        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id_or_url, visit1_id,
            snapshot=self.complete_snapshot['id'])
        self.assertEqual(
            {**origin_visit1, 'snapshot': self.complete_snapshot['id']},
            self.storage.origin_visit_get_latest(
                origin_url, require_snapshot=True)
        )
        self.assertEqual(
            origin_visit3,
            self.storage.origin_visit_get_latest(origin_url)
        )

        # Status filter: all three visits are status=ongoing, so no visit
        # returned
        self.assertIsNone(
            self.storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full'])
        )

        # Mark the first visit as completed and check status filter again
        self.storage.origin_visit_update(
            origin_id_or_url,
            visit1_id, status='full')
        self.assertEqual(
            {
                **origin_visit1,
                'snapshot': self.complete_snapshot['id'],
                'status': 'full'},
            self.storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full']),
        )
        self.assertEqual(
            origin_visit3,
            self.storage.origin_visit_get_latest(origin_url),
        )

        # Add snapshot to visit2 and check that the new snapshot is returned
        self.storage.snapshot_add([self.empty_snapshot])
        self.storage.origin_visit_update(
            origin_id_or_url, visit2_id,
            snapshot=self.empty_snapshot['id'])
        self.assertEqual(
            {**origin_visit2, 'snapshot': self.empty_snapshot['id']},
            self.storage.origin_visit_get_latest(
                origin_url, require_snapshot=True),
        )
        self.assertEqual(
            origin_visit3,
            self.storage.origin_visit_get_latest(origin_url),
        )

        # Check that the status filter is still working
        self.assertEqual(
            {
                **origin_visit1,
                'snapshot': self.complete_snapshot['id'],
                'status': 'full'},
            self.storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full']),
        )

        # Add snapshot to visit3 (same date as visit2)
        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id_or_url, visit3_id, snapshot=self.complete_snapshot['id'])
        self.assertEqual(
            {
                **origin_visit1,
                'snapshot': self.complete_snapshot['id'],
                'status': 'full'},
            self.storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full']),
        )
        self.assertEqual(
            {
                **origin_visit1,
                'snapshot': self.complete_snapshot['id'],
                'status': 'full'},
            self.storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full'], require_snapshot=True),
        )
        self.assertEqual(
            {**origin_visit3, 'snapshot': self.complete_snapshot['id']},
            self.storage.origin_visit_get_latest(
                origin_url),
        )
        self.assertEqual(
            {**origin_visit3, 'snapshot': self.complete_snapshot['id']},
            self.storage.origin_visit_get_latest(
                origin_url, require_snapshot=True),
        )

    def test_person_fullname_unicity(self):
        # given (person injection through revisions for example)
        revision = self.revision

        # create a revision with same committer fullname but wo name and email
        revision2 = copy.deepcopy(self.revision2)
        revision2['committer'] = dict(revision['committer'])
        revision2['committer']['email'] = None
        revision2['committer']['name'] = None

        self.storage.revision_add([revision])
        self.storage.revision_add([revision2])

        # when getting added revisions
        revisions = list(
            self.storage.revision_get([revision['id'], revision2['id']]))

        # then
        # check committers are the same
        self.assertEqual(revisions[0]['committer'],
                         revisions[1]['committer'])

    def test_snapshot_add_get_empty(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        actual_result = self.storage.snapshot_add([self.empty_snapshot])
        self.assertEqual(actual_result, {'snapshot:add': 1})

        self.storage.origin_visit_update(
            origin_id, visit_id, snapshot=self.empty_snapshot['id'])

        by_id = self.storage.snapshot_get(self.empty_snapshot['id'])
        self.assertEqual(by_id, {**self.empty_snapshot, 'next_branch': None})

        by_ov = self.storage.snapshot_get_by_origin_visit(origin_id, visit_id)
        self.assertEqual(by_ov, {**self.empty_snapshot, 'next_branch': None})

        expected_origin = self.origin.copy()
        data1 = {
            'origin': expected_origin,
            'date': self.date_visit1,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': expected_origin,
            'date': self.date_visit1,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': self.empty_snapshot['id'],
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin_visit', data1),
                          ('snapshot', self.empty_snapshot),
                          ('origin_visit', data2)])

    def test_snapshot_add_get_complete(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        actual_result = self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit_id, snapshot=self.complete_snapshot['id'])
        self.assertEqual(actual_result, {'snapshot:add': 1})

        by_id = self.storage.snapshot_get(self.complete_snapshot['id'])
        self.assertEqual(by_id,
                         {**self.complete_snapshot, 'next_branch': None})

        by_ov = self.storage.snapshot_get_by_origin_visit(origin_id, visit_id)
        self.assertEqual(by_ov,
                         {**self.complete_snapshot, 'next_branch': None})

    def test_snapshot_add_many(self):
        actual_result = self.storage.snapshot_add(
            [self.snapshot, self.complete_snapshot])
        self.assertEqual(actual_result, {'snapshot:add': 2})

        self.assertEqual(
            {**self.complete_snapshot, 'next_branch': None},
            self.storage.snapshot_get(self.complete_snapshot['id']))

        self.assertEqual(
            {**self.snapshot, 'next_branch': None},
            self.storage.snapshot_get(self.snapshot['id']))

    def test_snapshot_add_many_incremental(self):
        actual_result = self.storage.snapshot_add([self.complete_snapshot])
        self.assertEqual(actual_result, {'snapshot:add': 1})

        actual_result2 = self.storage.snapshot_add(
            [self.snapshot, self.complete_snapshot])
        self.assertEqual(actual_result2, {'snapshot:add': 1})

        self.assertEqual(
            {**self.complete_snapshot, 'next_branch': None},
            self.storage.snapshot_get(self.complete_snapshot['id']))

        self.assertEqual(
            {**self.snapshot, 'next_branch': None},
            self.storage.snapshot_get(self.snapshot['id']))

    def test_snapshot_add_twice(self):
        actual_result = self.storage.snapshot_add([self.empty_snapshot])
        self.assertEqual(actual_result, {'snapshot:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('snapshot', self.empty_snapshot)])

        actual_result = self.storage.snapshot_add([self.snapshot])
        self.assertEqual(actual_result, {'snapshot:add': 1})

        self.assertEqual(list(self.journal_writer.objects),
                         [('snapshot', self.empty_snapshot),
                          ('snapshot', self.snapshot)])

    def test_snapshot_add_validation(self):
        snap = copy.deepcopy(self.snapshot)
        snap['branches'][b'foo'] = {'target_type': 'revision'}

        with self.assertRaisesRegex(KeyError, 'target'):
            self.storage.snapshot_add([snap])

        snap = copy.deepcopy(self.snapshot)
        snap['branches'][b'foo'] = {'target': b'\x42'*20}

        with self.assertRaisesRegex(KeyError, 'target_type'):
            self.storage.snapshot_add([snap])

    def test_snapshot_add_count_branches(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        actual_result = self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit_id, snapshot=self.complete_snapshot['id'])
        self.assertEqual(actual_result, {'snapshot:add': 1})

        snp_id = self.complete_snapshot['id']
        snp_size = self.storage.snapshot_count_branches(snp_id)

        expected_snp_size = {
            'alias': 1,
            'content': 1,
            'directory': 2,
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

        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit_id,
            snapshot=self.complete_snapshot['id'])

        snp_id = self.complete_snapshot['id']
        branches = self.complete_snapshot['branches']
        branch_names = list(sorted(branches))

        # Test branch_from

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

        # Test branches_count

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

        # test branch_from + branches_count

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

        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit_id, snapshot=self.complete_snapshot['id'])

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

    def test_snapshot_add_get_filtered_and_paginated(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit_id, snapshot=self.complete_snapshot['id'])

        snp_id = self.complete_snapshot['id']
        branches = self.complete_snapshot['branches']
        branch_names = list(sorted(branches))

        # Test branch_from

        snapshot = self.storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_from=b'directory2')

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in (b'directory2', b'release')
            },
            'next_branch': None,
        }

        self.assertEqual(snapshot, expected_snapshot)

        # Test branches_count

        snapshot = self.storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_count=1)

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                b'directory': branches[b'directory']
            },
            'next_branch': b'directory2',
        }
        self.assertEqual(snapshot, expected_snapshot)

        # Test branches_count

        snapshot = self.storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_count=2)

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in (b'directory', b'directory2')
            },
            'next_branch': b'release',
        }
        self.assertEqual(snapshot, expected_snapshot)

        # test branch_from + branches_count

        snapshot = self.storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_from=b'directory2', branches_count=1)

        dir_idx = branch_names.index(b'directory2')
        expected_snapshot = {
            'id': snp_id,
            'branches': {
                branch_names[dir_idx]: branches[branch_names[dir_idx]],
            },
            'next_branch': b'release',
        }

        self.assertEqual(snapshot, expected_snapshot)

    def test_snapshot_add_get(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit_id = origin_visit1['visit']

        self.storage.snapshot_add([self.snapshot])
        self.storage.origin_visit_update(
            origin_id, visit_id, snapshot=self.snapshot['id'])

        by_id = self.storage.snapshot_get(self.snapshot['id'])
        self.assertEqual(by_id, {**self.snapshot, 'next_branch': None})

        by_ov = self.storage.snapshot_get_by_origin_visit(origin_id, visit_id)
        self.assertEqual(by_ov, {**self.snapshot, 'next_branch': None})

        origin_visit_info = self.storage.origin_visit_get_by(origin_id,
                                                             visit_id)
        self.assertEqual(origin_visit_info['snapshot'], self.snapshot['id'])

    def test_snapshot_add_nonexistent_visit(self):
        origin_id = self.storage.origin_add_one(self.origin)
        visit_id = 54164461156

        self.journal_writer.objects[:] = []

        self.storage.snapshot_add([self.snapshot])

        with self.assertRaises(ValueError):
            self.storage.origin_visit_update(
                origin_id, visit_id, snapshot=self.snapshot['id'])

        self.assertEqual(list(self.journal_writer.objects), [
            ('snapshot', self.snapshot)])

    def test_snapshot_add_twice__by_origin_visit(self):
        origin_id = self.storage.origin_add_one(self.origin)
        origin_visit1 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit1)
        visit1_id = origin_visit1['visit']
        self.storage.snapshot_add([self.snapshot])
        self.storage.origin_visit_update(
            origin_id, visit1_id, snapshot=self.snapshot['id'])

        by_ov1 = self.storage.snapshot_get_by_origin_visit(origin_id,
                                                           visit1_id)
        self.assertEqual(by_ov1, {**self.snapshot, 'next_branch': None})

        origin_visit2 = self.storage.origin_visit_add(origin_id,
                                                      self.date_visit2)
        visit2_id = origin_visit2['visit']

        self.storage.snapshot_add([self.snapshot])
        self.storage.origin_visit_update(
            origin_id, visit2_id, snapshot=self.snapshot['id'])

        by_ov2 = self.storage.snapshot_get_by_origin_visit(origin_id,
                                                           visit2_id)
        self.assertEqual(by_ov2, {**self.snapshot, 'next_branch': None})

        expected_origin = self.origin.copy()
        data1 = {
            'origin': expected_origin,
            'date': self.date_visit1,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': expected_origin,
            'date': self.date_visit1,
            'visit': origin_visit1['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': self.snapshot['id'],
        }
        data3 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit2['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data4 = {
            'origin': expected_origin,
            'date': self.date_visit2,
            'visit': origin_visit2['visit'],
            'type': self.origin['type'],
            'status': 'ongoing',
            'metadata': None,
            'snapshot': self.snapshot['id'],
        }
        self.assertEqual(list(self.journal_writer.objects),
                         [('origin', expected_origin),
                          ('origin_visit', data1),
                          ('snapshot', self.snapshot),
                          ('origin_visit', data2),
                          ('origin_visit', data3),
                          ('origin_visit', data4)])

    @settings(deadline=None)  # this test is very slow
    @given(strategies.booleans())
    def test_snapshot_get_latest(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        origin_id = self.storage.origin_add_one(self.origin)
        origin_id_or_url = self.origin['url'] if use_url else origin_id
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
        self.assertIsNone(self.storage.snapshot_get_latest(
            origin_id_or_url))

        # Add snapshot to visit1, latest snapshot = visit 1 snapshot
        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit1_id, snapshot=self.complete_snapshot['id'])
        self.assertEqual({**self.complete_snapshot, 'next_branch': None},
                         self.storage.snapshot_get_latest(
                             origin_id_or_url))

        # Status filter: all three visits are status=ongoing, so no snapshot
        # returned
        self.assertIsNone(
            self.storage.snapshot_get_latest(
                origin_id_or_url,
                allowed_statuses=['full'])
        )

        # Mark the first visit as completed and check status filter again
        self.storage.origin_visit_update(origin_id, visit1_id, status='full')
        self.assertEqual(
            {**self.complete_snapshot, 'next_branch': None},
            self.storage.snapshot_get_latest(
                origin_id_or_url,
                allowed_statuses=['full']),
        )

        # Add snapshot to visit2 and check that the new snapshot is returned
        self.storage.snapshot_add([self.empty_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit2_id, snapshot=self.empty_snapshot['id'])
        self.assertEqual({**self.empty_snapshot, 'next_branch': None},
                         self.storage.snapshot_get_latest(origin_id))

        # Check that the status filter is still working
        self.assertEqual(
            {**self.complete_snapshot, 'next_branch': None},
            self.storage.snapshot_get_latest(
                origin_id_or_url,
                allowed_statuses=['full']),
        )

        # Add snapshot to visit3 (same date as visit2) and check that
        # the new snapshot is returned
        self.storage.snapshot_add([self.complete_snapshot])
        self.storage.origin_visit_update(
            origin_id, visit3_id, snapshot=self.complete_snapshot['id'])
        self.assertEqual({**self.complete_snapshot, 'next_branch': None},
                         self.storage.snapshot_get_latest(
                             origin_id_or_url))

    @given(strategies.booleans())
    def test_snapshot_get_latest__missing_snapshot(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        # Origin does not exist
        self.assertIsNone(self.storage.snapshot_get_latest(
            self.origin['url'] if use_url else 999))

        origin_id = self.storage.origin_add_one(self.origin)
        origin_id_or_url = self.origin['url'] if use_url else origin_id
        origin_visit1 = self.storage.origin_visit_add(
            origin_id_or_url,
            self.date_visit1)
        visit1_id = origin_visit1['visit']
        origin_visit2 = self.storage.origin_visit_add(
            origin_id_or_url,
            self.date_visit2)
        visit2_id = origin_visit2['visit']

        # Two visits, both with no snapshot: latest snapshot is None
        self.assertIsNone(self.storage.snapshot_get_latest(
            origin_id_or_url))

        # Add unknown snapshot to visit1, check that the inconsistency is
        # detected
        self.storage.origin_visit_update(
            origin_id_or_url,
            visit1_id, snapshot=self.complete_snapshot['id'])
        with self.assertRaises(ValueError):
            self.storage.snapshot_get_latest(
                origin_id_or_url)

        # Status filter: both visits are status=ongoing, so no snapshot
        # returned
        self.assertIsNone(
            self.storage.snapshot_get_latest(
                origin_id_or_url,
                allowed_statuses=['full'])
        )

        # Mark the first visit as completed and check status filter again
        self.storage.origin_visit_update(
            origin_id_or_url,
            visit1_id, status='full')
        with self.assertRaises(ValueError):
            self.storage.snapshot_get_latest(
                origin_id_or_url,
                allowed_statuses=['full']),

        # Actually add the snapshot and check status filter again
        self.storage.snapshot_add([self.complete_snapshot])
        self.assertEqual(
            {**self.complete_snapshot, 'next_branch': None},
            self.storage.snapshot_get_latest(
                origin_id_or_url)
        )

        # Add unknown snapshot to visit2 and check that the inconsistency
        # is detected
        self.storage.origin_visit_update(
            origin_id_or_url,
            visit2_id, snapshot=self.snapshot['id'])
        with self.assertRaises(ValueError):
            self.storage.snapshot_get_latest(
                origin_id_or_url)

        # Actually add that snapshot and check that the new one is returned
        self.storage.snapshot_add([self.snapshot])
        self.assertEqual(
            {**self.snapshot, 'next_branch': None},
            self.storage.snapshot_get_latest(
                origin_id_or_url)
        )

    def test_stat_counters(self):
        expected_keys = ['content', 'directory',
                         'origin', 'revision']

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

        self.storage.origin_add_one(self.origin2)
        origin_visit1 = self.storage.origin_visit_add(
            self.origin2['url'], date=self.date_visit2)
        self.storage.snapshot_add([self.snapshot])
        self.storage.origin_visit_update(
            self.origin2['url'], origin_visit1['visit'],
            snapshot=self.snapshot['id'])
        self.storage.directory_add([self.dir])
        self.storage.revision_add([self.revision])
        self.storage.release_add([self.release])

        self.storage.refresh_stat_counters()
        counters = self.storage.stat_counters()
        self.assertEqual(counters['content'], 1)
        self.assertEqual(counters['directory'], 1)
        self.assertEqual(counters['snapshot'], 1)
        self.assertEqual(counters['origin'], 1)
        self.assertEqual(counters['origin_visit'], 1)
        self.assertEqual(counters['revision'], 1)
        self.assertEqual(counters['release'], 1)
        self.assertEqual(counters['snapshot'], 1)
        if 'person' in counters:
            self.assertEqual(counters['person'], 3)

    def test_content_find_ctime(self):
        cont = self.cont.copy()
        del cont['data']
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        cont['ctime'] = now
        self.storage.content_add_metadata([cont])

        actually_present = self.storage.content_find({'sha1': cont['sha1']})

        # check ctime up to one second
        dt = actually_present[0]['ctime'] - now
        self.assertLessEqual(abs(dt.total_seconds()), 1, dt)
        del actually_present[0]['ctime']

        self.assertEqual(actually_present[0], {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'blake2s256': cont['blake2s256'],
            'length': cont['length'],
            'status': 'visible'
        })

    def test_content_find_with_present_content(self):
        # 1. with something to find
        cont = self.cont
        self.storage.content_add([cont, self.cont2])

        actually_present = self.storage.content_find(
            {'sha1': cont['sha1']}
            )
        self.assertEqual(1, len(actually_present))
        actually_present[0].pop('ctime')

        self.assertEqual(actually_present[0], {
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
        self.assertEqual(1, len(actually_present))

        actually_present[0].pop('ctime')
        self.assertEqual(actually_present[0], {
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
        self.assertEqual(1, len(actually_present))

        actually_present[0].pop('ctime')
        self.assertEqual(actually_present[0], {
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
        self.assertEqual(1, len(actually_present))

        actually_present[0].pop('ctime')
        self.assertEqual(actually_present[0], {
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

        self.assertEqual(actually_present, [])

        # 2. with something that does not exist
        actually_present = self.storage.content_find(
            {'sha1_git': missing_cont['sha1_git']})

        self.assertEqual(actually_present, [])

        # 3. with something that does not exist
        actually_present = self.storage.content_find(
            {'sha256': missing_cont['sha256']})

        self.assertEqual(actually_present, [])

    def test_content_find_with_duplicate_input(self):
        cont1 = self.cont
        duplicate_cont = cont1.copy()

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(duplicate_cont['sha1'])
        sha1_array[0] += 1
        duplicate_cont['sha1'] = bytes(sha1_array)
        sha1git_array = bytearray(duplicate_cont['sha1_git'])
        sha1git_array[0] += 1
        duplicate_cont['sha1_git'] = bytes(sha1git_array)
        # Inject the data
        self.storage.content_add([cont1, duplicate_cont])
        finder = {'blake2s256': duplicate_cont['blake2s256'],
                  'sha256': duplicate_cont['sha256']}
        actual_result = list(self.storage.content_find(finder))

        cont1.pop('data')
        duplicate_cont.pop('data')
        actual_result[0].pop('ctime')
        actual_result[1].pop('ctime')

        expected_result = [
           cont1, duplicate_cont
        ]
        self.assertCountEqual(expected_result, actual_result)

    def test_content_find_with_duplicate_sha256(self):
        cont1 = self.cont
        duplicate_cont = cont1.copy()

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(duplicate_cont['sha1'])
        sha1_array[0] += 1
        duplicate_cont['sha1'] = bytes(sha1_array)
        sha1git_array = bytearray(duplicate_cont['sha1_git'])
        sha1git_array[0] += 1
        duplicate_cont['sha1_git'] = bytes(sha1git_array)
        blake2s256_array = bytearray(duplicate_cont['blake2s256'])
        blake2s256_array[0] += 1
        duplicate_cont['blake2s256'] = bytes(blake2s256_array)
        self.storage.content_add([cont1, duplicate_cont])
        finder = {
            'sha256': duplicate_cont['sha256']
            }
        actual_result = list(self.storage.content_find(finder))

        cont1.pop('data')
        duplicate_cont.pop('data')
        actual_result[0].pop('ctime')
        actual_result[1].pop('ctime')
        expected_result = [
            cont1, duplicate_cont
        ]
        self.assertCountEqual(expected_result, actual_result)
        # Find with both sha256 and blake2s256
        finder = {
            'sha256': duplicate_cont['sha256'],
            'blake2s256': duplicate_cont['blake2s256']
        }
        actual_result = list(self.storage.content_find(finder))

        actual_result[0].pop('ctime')

        expected_result = [
            duplicate_cont
        ]
        self.assertCountEqual(expected_result, actual_result)

    def test_content_find_with_duplicate_blake2s256(self):
        cont1 = self.cont
        duplicate_cont = cont1.copy()

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(duplicate_cont['sha1'])
        sha1_array[0] += 1
        duplicate_cont['sha1'] = bytes(sha1_array)
        sha1git_array = bytearray(duplicate_cont['sha1_git'])
        sha1git_array[0] += 1
        duplicate_cont['sha1_git'] = bytes(sha1git_array)
        sha256_array = bytearray(duplicate_cont['sha256'])
        sha256_array[0] += 1
        duplicate_cont['sha256'] = bytes(sha256_array)
        self.storage.content_add([cont1, duplicate_cont])
        finder = {
            'blake2s256': duplicate_cont['blake2s256']
            }
        actual_result = list(self.storage.content_find(finder))

        cont1.pop('data')
        duplicate_cont.pop('data')
        actual_result[0].pop('ctime')
        actual_result[1].pop('ctime')
        expected_result = [
            cont1, duplicate_cont
        ]
        self.assertCountEqual(expected_result, actual_result)
        # Find with both sha256 and blake2s256
        finder = {
            'sha256': duplicate_cont['sha256'],
            'blake2s256': duplicate_cont['blake2s256']
        }
        actual_result = list(self.storage.content_find(finder))

        actual_result[0].pop('ctime')

        expected_result = [
            duplicate_cont
        ]
        self.assertCountEqual(expected_result, actual_result)

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
                if 'object_id' in obj:
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
        actual_tools = self.storage.tool_add([tool])

        self.assertEqual(len(actual_tools), 1)
        actual_tool = actual_tools[0]
        self.assertIsNotNone(actual_tool)  # now it exists
        new_id = actual_tool.pop('id')
        self.assertEqual(actual_tool, tool)

        actual_tools2 = self.storage.tool_add([tool])
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

        actual_tools = self.storage.tool_add(new_tools)
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

        tools = self.storage.tool_add([tool])
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

    @given(strategies.booleans())
    def test_origin_metadata_add(self, use_url):
        self.reset_storage()
        # given
        origin = self.storage.origin_add([self.origin])[0]
        origin_id = origin['id']
        if use_url:
            origin = origin['url']
        else:
            origin = origin['id']
        origin_metadata0 = list(self.storage.origin_metadata_get_by(
            origin))
        self.assertEqual(len(origin_metadata0), 0, origin_metadata0)

        tools = self.storage.tool_add([self.metadata_tool])
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
                    origin,
                    self.origin_metadata['discovery_date'],
                    provider['id'],
                    tool['id'],
                    self.origin_metadata['metadata'])
        self.storage.origin_metadata_add(
                    origin,
                    '2015-01-01 23:00:00+00',
                    provider['id'],
                    tool['id'],
                    self.origin_metadata2['metadata'])
        actual_om = list(self.storage.origin_metadata_get_by(
            origin))
        # then
        self.assertCountEqual(
            [item['origin_id'] for item in actual_om],
            [origin_id, origin_id])

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
        tool = self.storage.tool_add([self.metadata_tool])[0]
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
        all_metadatas = list(self.storage.origin_metadata_get_by(
            origin_id))
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

    def test_metadata_provider_add(self):
        provider = {
            'provider_name': 'swMATH',
            'provider_type': 'registry',
            'provider_url': 'http://www.swmath.org/',
            'metadata': {
                'email': 'contact@swmath.org',
                'license': 'All rights reserved'
            }
        }
        provider['id'] = provider_id = self.storage.metadata_provider_add(
            **provider)
        self.assertEqual(
            provider,
            self.storage.metadata_provider_get_by({
               'provider_name': 'swMATH',
               'provider_url': 'http://www.swmath.org/'
            }))
        self.assertEqual(
            provider,
            self.storage.metadata_provider_get(provider_id))

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
        tool = self.storage.tool_add([self.metadata_tool])[0]

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
        m_by_provider = list(self.storage.origin_metadata_get_by(
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
    _test_origin_ids = True

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
        self.reset_storage()
        # add contents to storage
        self.storage.content_add(contents)

        # input the list of sha1s we want from storage
        get_sha1s = [c['sha1'] for c in contents]

        # retrieve contents
        actual_contents = list(self.storage.content_get(get_sha1s))

        self.assert_contents_ok(contents, actual_contents)

    @given(gen_contents(min_size=1, max_size=4))
    def test_generate_content_get_metadata(self, contents):
        self.reset_storage()
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

    @given(gen_contents(),
           strategies.binary(min_size=20, max_size=20),
           strategies.binary(min_size=20, max_size=20))
    def test_generate_content_get_range(self, contents, start, end):
        """content_get_range paginates results if limit exceeded"""
        self.reset_storage()
        # add contents to storage
        self.storage.content_add(contents)

        actual_result = self.storage.content_get_range(start, end)

        actual_contents = actual_result['contents']
        actual_next = actual_result['next']

        self.assertEqual(actual_next, None)

        expected_contents = [c for c in contents
                             if start <= c['sha1'] <= end]
        if expected_contents:
            keys_to_check = set(contents[0].keys()) - {'data'}
            self.assert_contents_ok(expected_contents, actual_contents,
                                    keys_to_check)
        else:
            self.assertEqual(actual_contents, [])

    def test_generate_content_get_range_limit_none(self):
        """content_get_range call with wrong limit input should fail"""
        with self.assertRaises(ValueError) as e:
            self.storage.content_get_range(start=None, end=None, limit=None)

        self.assertEqual(e.exception.args, (
            'Development error: limit should not be None',))

    @given(gen_contents(min_size=1, max_size=4))
    def test_generate_content_get_range_no_limit(self, contents):
        """content_get_range returns contents within range provided"""
        self.reset_storage()
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
        self.reset_storage()
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

    def test_origin_get_invalid_id_legacy(self):
        if self._test_origin_ids:
            invalid_origin_id = 1
            origin_info = self.storage.origin_get({'id': invalid_origin_id})
            self.assertIsNone(origin_info)

            origin_visits = list(self.storage.origin_visit_get(
                invalid_origin_id))
            self.assertEqual(origin_visits, [])

    def test_origin_get_invalid_id(self):
        if self._test_origin_ids:
            origin_info = self.storage.origin_get([{'id': 1}, {'id': 2}])
            self.assertEqual(origin_info, [None, None])

            origin_visits = list(self.storage.origin_visit_get(1))
            self.assertEqual(origin_visits, [])

    @given(strategies.lists(origins().map(lambda x: x.to_dict()),
                            unique_by=lambda x: x['url'],
                            min_size=6, max_size=15))
    def test_origin_get_range(self, new_origins):
        self.reset_storage()
        nb_origins = len(new_origins)

        self.storage.origin_add(new_origins)

        origin_from = random.randint(1, nb_origins-1)
        origin_count = random.randint(1, nb_origins - origin_from)

        actual_origins = list(
            self.storage.origin_get_range(origin_from=origin_from,
                                          origin_count=origin_count))

        for origin in actual_origins:
            del origin['id']

        for origin in actual_origins:
            self.assertIn(origin, new_origins)

        origin_from = -1
        origin_count = 5
        origins = list(
            self.storage.origin_get_range(origin_from=origin_from,
                                          origin_count=origin_count))
        self.assertEqual(len(origins), origin_count)

        origin_from = 10000
        origins = list(
            self.storage.origin_get_range(origin_from=origin_from,
                                          origin_count=origin_count))
        self.assertEqual(len(origins), 0)

    def test_origin_count(self):

        new_origins = [
            {
                'type': 'git',
                'url': 'https://github.com/user1/repo1'
            },
            {
                'type': 'git',
                'url': 'https://github.com/user2/repo1'
            },
            {
                'type': 'git',
                'url': 'https://github.com/user3/repo1'
            },
            {
                'type': 'git',
                'url': 'https://gitlab.com/user1/repo1'
            },
            {
                'type': 'git',
                'url': 'https://gitlab.com/user2/repo1'
            }
        ]

        self.storage.origin_add(new_origins)

        self.assertEqual(self.storage.origin_count('github'), 3)
        self.assertEqual(self.storage.origin_count('gitlab'), 2)
        self.assertEqual(
            self.storage.origin_count('.*user.*', regexp=True), 5)
        self.assertEqual(
            self.storage.origin_count('.*user.*', regexp=False), 0)
        self.assertEqual(
            self.storage.origin_count('.*user1.*', regexp=True), 2)
        self.assertEqual(
            self.storage.origin_count('.*user1.*', regexp=False), 0)

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(strategies.lists(objects(), max_size=2))
    def test_add_arbitrary(self, objects):
        self.reset_storage()
        for (obj_type, obj) in objects:
            obj = obj.to_dict()
            if obj_type == 'origin_visit':
                origin_id = self.storage.origin_add_one(obj.pop('origin'))
                if 'visit' in obj:
                    del obj['visit']
                self.storage.origin_visit_add(
                    origin_id, obj['date'], obj['type'])
            else:
                method = getattr(self.storage, obj_type + '_add')
                try:
                    method([obj])
                except HashCollision:
                    pass


@pytest.mark.db
class TestLocalStorage(CommonTestStorage, StorageTestDbFixture,
                       unittest.TestCase):
    """Test the local storage"""

    # Can only be tested with local storage as you can't mock
    # datetimes for the remote server
    @given(strategies.booleans())
    def test_fetch_history(self, use_url):
        if not self._test_origin_ids and not use_url:
            return
        self.reset_storage()

        origin_id = self.storage.origin_add_one(self.origin)
        origin_id_or_url = self.origin['url'] if use_url else origin_id
        with patch('datetime.datetime'):
            datetime.datetime.now.return_value = self.fetch_history_date
            fetch_history_id = self.storage.fetch_history_start(
                origin_id_or_url)
            datetime.datetime.now.assert_called_with(tz=datetime.timezone.utc)

        with patch('datetime.datetime'):
            datetime.datetime.now.return_value = self.fetch_history_end
            self.storage.fetch_history_end(fetch_history_id,
                                           self.fetch_history_data)

        fetch_history = self.storage.fetch_history_get(fetch_history_id)
        expected_fetch_history = self.fetch_history_data.copy()

        expected_fetch_history['id'] = fetch_history_id
        expected_fetch_history['origin'] = origin_id
        expected_fetch_history['date'] = self.fetch_history_date
        expected_fetch_history['duration'] = self.fetch_history_duration

        self.assertEqual(expected_fetch_history, fetch_history)

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
class TestStorageRaceConditions(TestStorageData, StorageTestDbFixture,
                                unittest.TestCase):
    @pytest.mark.xfail
    def test_content_add_race(self):

        results = queue.Queue()

        def thread():
            try:
                with self.db_transaction() as (db, cur):
                    ret = self.storage.content_add([self.cont], db=db,
                                                   cur=cur)
                results.put((threading.get_ident(), 'data', ret))
            except Exception as e:
                results.put((threading.get_ident(), 'exc', e))

        t1 = threading.Thread(target=thread)
        t2 = threading.Thread(target=thread)
        t1.start()
        # this avoids the race condition
        # import time
        # time.sleep(1)
        t2.start()
        t1.join()
        t2.join()

        r1 = results.get(block=False)
        r2 = results.get(block=False)

        with pytest.raises(queue.Empty):
            results.get(block=False)
        assert r1[0] != r2[0]
        assert r1[1] == 'data', 'Got exception %r in Thread%s' % (r1[2], r1[0])
        assert r2[1] == 'data', 'Got exception %r in Thread%s' % (r2[2], r2[0])


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
        self.storage.journal_writer = None  # TODO, not supported

        cont = copy.deepcopy(self.cont)

        self.storage.content_add([cont])
        # alter the sha1_git for example
        cont['sha1_git'] = hash_to_bytes(
            '3a60a5275d0333bf13468e8b3dcab90f4046e654')

        self.storage.content_update([cont], keys=['sha1_git'])

        with self.db_transaction() as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, length, status'
                        ' FROM content WHERE sha1 = %s',
                        (cont['sha1'],))
            datum = cur.fetchone()

        self.assertEqual(
            (datum[0], datum[1], datum[2],
             datum[3], datum[4]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible'))

    def test_content_update_with_new_cols(self):
        self.storage.journal_writer = None  # TODO, not supported

        with self.db_transaction() as (db, cur):
            cur.execute("""alter table content
                           add column test text default null,
                           add column test2 text default null""")

        cont = copy.deepcopy(self.cont2)
        self.storage.content_add([cont])
        cont['test'] = 'value-1'
        cont['test2'] = 'value-2'

        self.storage.content_update([cont], keys=['test', 'test2'])
        with self.db_transaction() as (_, cur):
            cur.execute(
                '''SELECT sha1, sha1_git, sha256, length, status,
                   test, test2
                   FROM content WHERE sha1 = %s''',
                (cont['sha1'],))

            datum = cur.fetchone()

        self.assertEqual(
            (datum[0], datum[1], datum[2],
             datum[3], datum[4], datum[5], datum[6]),
            (cont['sha1'], cont['sha1_git'], cont['sha256'],
             cont['length'], 'visible', cont['test'], cont['test2']))

        with self.db_transaction() as (_, cur):
            cur.execute("""alter table content drop column test,
                                               drop column test2""")
