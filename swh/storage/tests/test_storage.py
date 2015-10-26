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

        self.revision = {
            'id': b'56789012345678901234',
            'message': b'hello',
            'author_name': b'Nicolas Dandrimont',
            'author_email': b'nicolas@example.com',
            'committer_name': b'St\xc3fano Zacchiroli',
            'committer_email': b'stefano@example.com',
            'parents': [b'01234567890123456789', b'23434512345123456789'],
            'date': datetime.datetime(2015, 1, 1, 22, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'date_offset': 120,
            'committer_date': datetime.datetime(2015, 1, 2, 22, 0, 0,
                                                tzinfo=datetime.timezone.utc),
            'committer_date_offset': -120,
            'type': 'git',
            'directory': self.dir['id'],
            'synthetic': False
        }

        self.revision2 = {
            'id': b'87659012345678904321',
            'message': b'hello again',
            'author_name': 'Roberto Dicosmo',
            'author_email': 'roberto@example.com',
            'committer_name': 'tony',
            'committer_email': 'ar@dumont.fr',
            'parents': [b'01234567890123456789'],
            'date': datetime.datetime(2015, 1, 1, 22, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'date_offset': 120,
            'committer_date': datetime.datetime(2015, 1, 2, 22, 0, 0,
                                                tzinfo=datetime.timezone.utc),
            'committer_date_offset': -120,
            'type': 'git',
            'directory': self.dir2['id'],
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
            'branch': 'master',
            'revision': b'67890123456789012345',
            'authority': '5f4d4c51-498a-4e28-88b3-b3e4e8396cba',
            'validity': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                          tzinfo=datetime.timezone.utc),
        }

        self.occurrence2 = {
            'branch': 'master',
            'revision': self.revision2['id'],
            'authority': '5f4d4c51-498a-4e28-88b3-b3e4e8396cba',
            'validity': datetime.datetime(2015, 1, 1, 23, 0, 0,
                                          tzinfo=datetime.timezone.utc),
        }

        self.release = {
            'id': b'87659012345678901234',
            'name': 'v0.0.1',
            'date': datetime.datetime(2015, 1, 1, 22, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'offset': 120,
            'author_name': 'olasd',
            'author_email': 'nic@olasd.fr',
            'comment': 'synthetic release',
            'synthetic': True
        }

        self.release2 = {
            'id': b'56789012348765901234',
            'name': 'v0.0.2',
            'date': datetime.datetime(2015, 1, 2, 23, 0, 0,
                                      tzinfo=datetime.timezone.utc),
            'offset': 120,
            'author_name': 'tony',
            'author_email': 'ar@dumont.fr',
            'comment': 'v0.0.2\nMisc performance improvments + bug fixes',
            'synthetic': False
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

    def tearDown(self):
        shutil.rmtree(self.objroot)

        self.cursor.execute("""SELECT table_name FROM information_schema.tables
                               WHERE table_schema = %s""", ('public',))

        tables = set(table for (table,) in self.cursor.fetchall())
        tables -= {'dbversion', 'entity', 'entity_history', 'listable_entity'}

        for table in tables:
            self.cursor.execute('truncate table %s cascade' % table)
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
    def content_exist_with_present_content(self):
        # 1. with something to find
        cont = self.cont
        self.storage.content_add([cont])

        actually_present = self.storage.content_exist({'sha1': cont['sha1']})

        self.assertEquals(actually_present, True, "Should be present")

        # 2. with something to find
        actually_present = self.storage.content_exist(
            {'sha1_git': cont['sha1_git']})

        self.assertEquals(actually_present, True, "Should be present")

        # 3. with something to find
        actually_present = self.storage.content_exist(
            {'sha256': cont['sha256']})

        self.assertEquals(actually_present, True, "Should be present")

        # 4. with something to find
        actually_present = self.storage.content_exist(
            {'sha1': cont['sha1'],
             'sha1_git': cont['sha1_git'],
             'sha256': cont['sha256']})

        self.assertEquals(actually_present, True, "Should be present")

    @istest
    def content_exist_with_non_present_content(self):
        # 1. with something that does not exist
        missing_cont = self.missing_cont

        actually_present = self.storage.content_exist(
            {'sha1': missing_cont['sha1']})

        self.assertEquals(actually_present, False, "Should be missing")

        # 2. with something that does not exist
        actually_present = self.storage.content_exist(
            {'sha1_git': missing_cont['sha1_git']})

        self.assertEquals(actually_present, False, "Should be missing")

        # 3. with something that does not exist
        actually_present = self.storage.content_exist(
            {'sha256': missing_cont['sha256']})

        self.assertEquals(actually_present, False, "Should be missing")

    @istest
    def content_exist_bad_input(self):
        # 1. with bad input
        with self.assertRaises(ValueError):
            self.storage.content_exist({})  # empty is bad

        # 2. with bad input
        with self.assertRaises(ValueError):
            self.storage.content_exist(
                {'unknown-sha1': 'something'})  # not the right key

    @istest
    def directory_add(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        self.storage.directory_add([self.dir])

        stored_data = list(self.storage.directory_get(self.dir['id']))

        data_to_store = [
            (self.dir['id'], ent['type'], ent['target'], ent['name'],
             ent['perms'])
            for ent in sorted(self.dir['entries'], key=lambda ent: ent['name'])
        ]

        self.assertEqual(data_to_store, stored_data)

        after_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([], after_missing)

    @istest
    def revision_add(self):
        init_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([self.revision['id']], list(init_missing))

        self.storage.revision_add([self.revision])

        end_missing = self.storage.revision_missing([self.revision['id']])
        self.assertEqual([], list(end_missing))

    @istest
    def revision_get(self):
        self.storage.revision_add([self.revision])

        get = list(self.storage.revision_get([self.revision['id'],
                                              self.revision2['id']]))

        self.assertEqual(len(get), 2)
        self.assertEqual(self.revision, get[0])
        self.assertEqual(None, get[1])

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
    def origin_add(self):
        self.assertIsNone(self.storage.origin_get(self.origin))
        id = self.storage.origin_add_one(self.origin)
        self.assertEqual(self.storage.origin_get(self.origin), id)

    @istest
    def occurrence_add(self):
        origin_id = self.storage.origin_add_one(self.origin2)

        revision = self.revision.copy()
        revision['id'] = self.occurrence['revision']
        self.storage.revision_add([revision])

        occur = self.occurrence
        occur['origin'] = origin_id
        self.storage.occurrence_add([occur])
        self.storage.occurrence_add([occur])

        test_query = '''select origin, branch, revision, authority, validity
                        from occurrence_history
                        order by origin, validity'''

        self.cursor.execute(test_query)
        ret = self.cursor.fetchall()
        self.assertEqual(len(ret), 1)
        self.assertEqual((ret[0][0], ret[0][1], ret[0][2].tobytes(),
                          ret[0][3]), (occur['origin'],
                                       occur['branch'], occur['revision'],
                                       occur['authority']))

        self.assertEqual(ret[0][4].lower, occur['validity'])
        self.assertEqual(ret[0][4].lower_inc, True)
        self.assertEqual(ret[0][4].upper, datetime.datetime.max)

        orig_validity = occur['validity']
        occur['validity'] += datetime.timedelta(hours=10)
        self.storage.occurrence_add([occur])

        self.cursor.execute(test_query)
        ret = self.cursor.fetchall()
        self.assertEqual(len(ret), 2)
        self.assertEqual(ret[0][4].lower, orig_validity)
        self.assertEqual(ret[0][4].lower_inc, True)
        self.assertEqual(ret[0][4].upper, occur['validity'])
        self.assertEqual(ret[0][4].upper_inc, False)
        self.assertEqual(ret[1][4].lower, occur['validity'])
        self.assertEqual(ret[1][4].lower_inc, True)
        self.assertEqual(ret[1][4].upper, datetime.datetime.max)

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
        self.assertEquals(occ['revision'], self.revision2['id'])
        self.assertEquals(occ['path'], self.dir2['entries'][0]['name'])

        occ2 = self.storage.content_find_occurrence(
            {'sha1_git': self.cont2['sha1_git']})

        self.assertEquals(occ2['origin_type'], self.origin2['type'])
        self.assertEquals(occ2['origin_url'], self.origin2['url'])
        self.assertEquals(occ2['branch'], self.occurrence2['branch'])
        self.assertEquals(occ2['revision'], self.revision2['id'])
        self.assertEquals(occ2['path'], self.dir2['entries'][0]['name'])

        occ3 = self.storage.content_find_occurrence(
            {'sha256': self.cont2['sha256']})

        self.assertEquals(occ3['origin_type'], self.origin2['type'])
        self.assertEquals(occ3['origin_url'], self.origin2['url'])
        self.assertEquals(occ3['branch'], self.occurrence2['branch'])
        self.assertEquals(occ3['revision'], self.revision2['id'])
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
    def stat_counters(self):
        expected_keys = ['content', 'directory', 'directory_entry_dir',
                         'occurrence', 'origin', 'person', 'revision']
        counters = self.storage.stat_counters()

        self.assertTrue(set(expected_keys) <= set(counters))
        self.assertIsInstance(counters[expected_keys[0]], int)


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
