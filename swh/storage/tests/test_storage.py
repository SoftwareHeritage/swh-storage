# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil
import tempfile
import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from .db_testing import DbTestFixture
from swh.core.hashutil import hex_to_hash
from swh.storage import Storage


@attr('db')
class TestStorage(DbTestFixture, unittest.TestCase):

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

        self.dir = {
            'id': b'12345678901234567890',
            'entries': [
                {
                    'name': 'foo',
                    'type': 'file',
                    'target': self.cont['sha1_git'],
                    'perms': 0o644,
                    'atime': None,
                    'ctime': None,
                    'mtime': None,
                },
                {
                    'name': 'bar',
                    'type': 'dir',
                    'target': b'12345678901234567890',
                    'perms': 0o2000,
                    'atime': None,
                    'ctime': None,
                    'mtime': None,
                },
            ],
        }

    def tearDown(self):
        shutil.rmtree(self.objroot)
        super().tearDown()

    @istest
    def content_add(self):
        cont = self.cont

        self.storage.content_add([cont])
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
    def content_missing(self):
        cont2 = self.cont2
        missing_cont = self.missing_cont
        self.storage.content_add([cont2])
        gen = self.storage.content_missing([cont2, missing_cont])

        self.assertEqual(list(gen), [missing_cont['sha1']])

    @istest
    def directory_add(self):
        init_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([self.dir['id']], init_missing)

        self.storage.directory_add([self.dir])

        stored_data = list(self.storage.directory_get(self.dir['id']))

        data_to_store = [
            (self.dir['id'], ent['type'], ent['target'], ent['name'],
             ent['perms'], ent['atime'], ent['ctime'], ent['mtime'])
            for ent in sorted(self.dir['entries'], key=lambda ent: ent['name'])
        ]

        self.assertEqual(data_to_store, stored_data)

        after_missing = list(self.storage.directory_missing([self.dir['id']]))
        self.assertEqual([], after_missing)
