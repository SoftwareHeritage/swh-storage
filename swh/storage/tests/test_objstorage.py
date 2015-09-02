# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile
import unittest

from io import BytesIO
from nose.tools import istest

from swh.storage.objstorage import ObjStorage


class Hashlib(unittest.TestCase):

    def setUp(self):
        self.content = b'42\n'
        self.obj_id = 'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd'
        self.obj_steps = ['d8', '1c', 'c0']
        self.obj_relpath = os.path.join(*(self.obj_steps + [self.obj_id]))
        self.tmpdir = tempfile.mkdtemp()
        self.storage = ObjStorage(root=self.tmpdir, depth=3)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    @istest
    def add_bytes_w_id(self):
        obj_path = os.path.join(self.tmpdir, self.obj_relpath)
        self.storage.add_bytes(self.content, obj_id=self.obj_id)
        self.assertTrue(os.path.isfile(obj_path))
        self.assertEqual(open(obj_path, 'rb').read(), self.content)

    @istest
    def add_bytes_wo_id(self):
        obj_path = os.path.join(self.tmpdir, self.obj_relpath)
        self.storage.add_bytes(self.content)
        self.assertTrue(os.path.isfile(obj_path))
        self.assertEqual(open(obj_path, 'rb').read(), self.content)

    @istest
    def add_file_w_id(self):
        obj_path = os.path.join(self.tmpdir, self.obj_relpath)
        self.storage.add_file(BytesIO(self.content),
                              len(self.content),
                              obj_id=self.obj_id)
        self.assertTrue(os.path.isfile(obj_path))
        self.assertEqual(open(obj_path, 'rb').read(), self.content)

    @istest
    def add_file_wo_id(self):
        obj_path = os.path.join(self.tmpdir, self.obj_relpath)
        self.storage.add_file(BytesIO(self.content),
                              len(self.content))
        self.assertTrue(os.path.isfile(obj_path))
        self.assertEqual(open(obj_path, 'rb').read(), self.content)
