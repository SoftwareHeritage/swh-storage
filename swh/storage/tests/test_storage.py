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
from swh.storage import Storage


@attr('db')
class TestStorage(DbTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.objroot = tempfile.mkdtemp()
        self.storage = Storage(self.conn, self.objroot)

    def tearDown(self):
        shutil.rmtree(self.objroot)
        super().tearDown()

    @istest
    def add_content(self):
        cont = {
            'data': b'42\n',
            'length': 3,
            'sha1': '34973274ccef6ab4dfaaf86599792fa9c3fe4689',
            'sha1_git': 'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd',
            'sha256': '673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a'  # NOQA
        }
        self.storage.add_content([cont])
        self.assertIn(cont['sha1'], self.storage.objstorage)
        self.cursor.execute('SELECT sha1, sha1_git, sha256, length, status'
                            ' FROM content WHERE sha1 = %s',
                            (cont['sha1'],))
        self.assertEqual(self.cursor.fetchone(),
                         (cont['sha1'], cont['sha1_git'], cont['sha256'],
                          cont['length'], 'visible'))
