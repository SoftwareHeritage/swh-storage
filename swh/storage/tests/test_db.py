# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from .db_testing import DbTestFixture
from swh.storage.db import Db


@attr('db')
class TestDb(DbTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.db = Db(self.conn)

    def tearDown(self):
        self.db.conn.close()
        super().tearDown()

    @istest
    def add_content(self):
        cur = self.cursor
        sha1 = '34973274ccef6ab4dfaaf86599792fa9c3fe4689'
        self.db.mktemp('content', cur)
        self.db.copy_to([{
            'sha1': sha1,
            'sha1_git': 'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd',
            'sha256': '673650f936cb3b0a2f93ce09d81be10748b1b203'
            'c19e8176b4eefc1964a0cf3a',
            'length': 3}],
                        'tmp_content',
                        ['sha1', 'sha1_git', 'sha256', 'length'],
                        cur)
        self.db.content_add_from_temp(cur)
        self.cursor.execute('SELECT sha1 FROM content WHERE sha1 = %s',
                            (sha1,))
        self.assertEqual(self.cursor.fetchone(), (sha1,))
