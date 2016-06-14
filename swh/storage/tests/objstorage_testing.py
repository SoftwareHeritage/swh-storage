# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from nose.tools import istest

from swh.core import hashutil
from swh.storage import exc


class ObjStorageTestFixture():

    def setUp(self):
        super().setUp()

    def hash_content(self, content):
        obj_id = hashutil.hashdata(content)['sha1']
        return content, obj_id

    def assertContentMatch(self, obj_id, expected_content):
        content = self.storage.get(obj_id)
        self.assertEqual(content, expected_content)

    @istest
    def add_get_w_id(self):
        content, obj_id = self.hash_content(b'add_get_w_id')
        r = self.storage.add(content, obj_id=obj_id)
        self.assertEqual(obj_id, r)
        self.assertContentMatch(obj_id, content)

    @istest
    def add_get_wo_id(self):
        content, obj_id = self.hash_content(b'add_get_wo_id')
        r = self.storage.add(content)
        self.assertEqual(obj_id, r)
        self.assertContentMatch(obj_id, content)

    @istest
    def restore_content(self):
        valid_content, valid_obj_id = self.hash_content(b'restore_content')
        invalid_content = b'unexpected content'
        id_adding = self.storage.add(invalid_content, valid_obj_id)
        id_restore = self.storage.restore(valid_content)
        # Adding a false content then restore it to the right one and
        # then perform a verification should result in a successful check.
        self.assertEqual(id_adding, valid_obj_id)
        self.assertEqual(id_restore, valid_obj_id)
        self.assertContentMatch(valid_obj_id, valid_content)

    @istest
    def get_missing(self):
        content, obj_id = self.hash_content(b'get_missing')
        with self.assertRaises(exc.Error):
            self.storage.get(obj_id)

    @istest
    def check_missing(self):
        content, obj_id = self.hash_content(b'check_missing')
        with self.assertRaises(exc.Error):
            self.storage.check(obj_id)

    @istest
    def check_present(self):
        content, obj_id = self.hash_content(b'check_missing')
        self.storage.add(content)
        try:
            self.storage.check(obj_id)
        except:
            self.fail('Integrity check failed')
