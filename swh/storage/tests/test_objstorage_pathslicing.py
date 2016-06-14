# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest

from nose.tools import istest

from swh.core import hashutil
from swh.storage import exc
from swh.storage.objstorage import PathSlicingObjStorage

from objstorage_testing import ObjStorageTestFixture


class TestpathSlicingObjStorage(ObjStorageTestFixture, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.depth = 3
        self.slicing = 2
        self.tmpdir = tempfile.mkdtemp()
        self.storage = PathSlicingObjStorage(self.tmpdir, self.depth,
                                             self.slicing)

    def content_path(self, obj_id):
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        return self.storage._obj_path(hex_obj_id)

    @istest
    def contains(self):
        content_p, obj_id_p = self.hash_content(b'contains_present')
        content_m, obj_id_m = self.hash_content(b'contains_missing')
        self.storage.add(content_p, obj_id=obj_id_p)
        self.assertIn(obj_id_p, self.storage)
        self.assertNotIn(obj_id_m, self.storage)

    @istest
    def iter(self):
        content, obj_id = self.hash_content(b'iter')
        self.assertEqual(list(iter(self.storage)), [])
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(list(iter(self.storage)), [obj_id])

    @istest
    def len(self):
        content, obj_id = self.hash_content(b'check_not_gzip')
        self.assertEqual(len(self.storage), 0)
        self.storage.add(content, obj_id=obj_id)
        self.assertEqual(len(self.storage), 1)

    @istest
    def check_not_gzip(self):
        content, obj_id = self.hash_content(b'check_not_gzip')
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), 'ab') as f:  # Add garbage.
            f.write(b'garbage')
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    @istest
    def check_id_mismatch(self):
        content, obj_id = self.hash_content(b'check_id_mismatch')
        self.storage.add(content, obj_id=obj_id)
        with open(self.content_path(obj_id), 'wb') as f:
            f.write(b'unexpected content')
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    @istest
    def get_random_contents(self):
        content, obj_id = self.hash_content(b'get_random_content')
        self.storage.add(content, obj_id=obj_id)
        random_contents = list(self.storage.get_random(1))
        self.assertEqual(1, len(random_contents))
        self.assertIn(obj_id, random_contents)
