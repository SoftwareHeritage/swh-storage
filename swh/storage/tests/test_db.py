# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core.tests.db_testing import DbTestFixture
from swh.model.hashutil import hash_to_bytes
from swh.storage.db import Db


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_DIR, '../../../../swh-storage-testdata')


@attr('db')
class TestDb(DbTestFixture, unittest.TestCase):

    TEST_DB_DUMP = os.path.join(TEST_DATA_DIR, 'dumps/swh.dump')

    def setUp(self):
        super().setUp()
        self.db = Db(self.conn)

    def tearDown(self):
        self.db.conn.close()
        super().tearDown()

    @istest
    def add_content(self):
        cur = self.cursor
        sha1 = hash_to_bytes('34973274ccef6ab4dfaaf86599792fa9c3fe4689')
        self.db.mktemp('content', cur)
        self.db.copy_to([{
            'sha1': sha1,
            'sha1_git': hash_to_bytes(
                'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd'),
            'sha256': hash_to_bytes(
                '673650f936cb3b0a2f93ce09d81be107'
                '48b1b203c19e8176b4eefc1964a0cf3a'),
            'length': 3}],
                        'tmp_content',
                        ['sha1', 'sha1_git', 'sha256', 'length'],
                        cur)
        self.db.content_add_from_temp(cur)
        self.cursor.execute('SELECT sha1 FROM content WHERE sha1 = %s',
                            (sha1,))
        self.assertEqual(self.cursor.fetchone()[0].tobytes(), sha1)
