# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from .db_testing import DbTestFixture


@attr('db')
class TestDb(DbTestFixture, unittest.TestCase):

    @istest
    def connect(self):
        self.assertFalse(self.conn.closed)

    @istest
    def insert_content(self):
        self.cursor.execute(
            'INSERT INTO content (sha1, sha1_git, sha256, length, status) ' +
            'VALUES (%s, %s, %s, %s, %s)',
            ('34973274ccef6ab4dfaaf86599792fa9c3fe4689',
             'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd',
             '673650f936cb3b0a2f93ce09d81be10748b1b203'
             'c19e8176b4eefc1964a0cf3a',
             3, 'visible'))
