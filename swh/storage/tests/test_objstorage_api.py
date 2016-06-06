# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile
import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core import hashutil
from swh.storage.exc import ObjNotFoundError, Error
from swh.storage.tests.server_testing import ServerTestFixture
from swh.storage.objstorage.api.client import RemoteObjStorage
from swh.storage.objstorage.api.server import app


@attr('db')
class TestRemoteObjStorage(ServerTestFixture, unittest.TestCase):
    """ Test the remote archive API.
    """

    def setUp(self):
        self.config = {'storage_base': tempfile.mkdtemp(),
                       'storage_depth': 3}
        self.app = app
        super().setUp()
        self.objstorage = RemoteObjStorage(self.url())

    def tearDown(self):
        super().tearDown()

    @istest
    def content_add(self):
        content = bytes('Test content', 'utf8')
        id = self.objstorage.content_add(content)
        self.assertEquals(self.objstorage.content_get(id), content)

    @istest
    def content_get_present(self):
        content = bytes('content_get_present', 'utf8')
        content_hash = hashutil.hashdata(content)
        id = self.objstorage.content_add(content)
        self.assertEquals(content_hash['sha1'], id)

    @istest
    def content_get_missing(self):
        content = bytes('content_get_missing', 'utf8')
        content_hash = hashutil.hashdata(content)
        with self.assertRaises(ObjNotFoundError):
            self.objstorage.content_get(content_hash['sha1'])

    @istest
    def content_get_random(self):
        ids = []
        for i in range(100):
            content = bytes('content_get_present', 'utf8')
            id = self.objstorage.content_add(content)
            ids.append(id)
        for id in self.objstorage.content_get_random(50):
            self.assertIn(id, ids)

    @istest
    def content_check_invalid(self):
        content = bytes('content_check_invalid', 'utf8')
        id = self.objstorage.content_add(content)
        hex_obj_id = hashutil.hash_to_hex(id)
        dir_path = os.path.join(
            self.config['storage_base'],
            *[hex_obj_id[i*2:i*2+2]
              for i in range(int(self.config['storage_depth']))]
        )
        path = os.path.join(dir_path, hex_obj_id)
        content = list(content)
        with open(path, 'bw') as f:
            content[0] = (content[0] + 1) % 128
            f.write(bytes(content))
        with self.assertRaises(Error):
            self.objstorage.content_check(id)

    @istest
    def content_check_valid(self):
        content = bytes('content_check_valid', 'utf8')
        id = self.objstorage.content_add(content)
        try:
            self.objstorage.content_check(id)
        except:
            self.fail('Integrity check failed')

    @istest
    def content_check_missing(self):
        content = bytes('content_check_valid', 'utf8')
        content_hash = hashutil.hashdata(content)
        with self.assertRaises(ObjNotFoundError):
            self.objstorage.content_check(content_hash['sha1'])
