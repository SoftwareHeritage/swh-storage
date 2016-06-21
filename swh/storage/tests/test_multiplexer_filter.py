# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import unittest

from string import ascii_lowercase

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core import hashutil
from swh.storage.exc import ObjNotFoundError, Error
from swh.storage.objstorage import ObjStorage
from swh.storage.objstorage.multiplexer.filter import (add_filter, read_only,
                                                       id_prefix, id_regex)


def get_random_content():
    return bytes(''.join(random.sample(ascii_lowercase, 10)), 'utf8')


class MockObjStorage(ObjStorage):
    """ Mock an object storage for testing the filters.
    """
    def __init__(self):
        self.objects = {}

    def __contains__(self, obj_id):
        return obj_id in self.objects

    def __len__(self):
        return len(self.objects)

    def __iter__(self):
        return iter(self.objects)

    def id(self, content):
        # Id is the content itself for easily choose the id of
        # a content for filtering.
        return hashutil.hashdata(content)['sha1']

    def add(self, content, obj_id=None, check_presence=True):
        if obj_id is None:
            obj_id = self.id(content)

        if check_presence and obj_id in self.objects:
            return obj_id

        self.objects[obj_id] = content
        return obj_id

    def restore(self, content, obj_id=None):
        return self.add(content, obj_id, check_presence=False)

    def get(self, obj_id):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)
        return self.objects[obj_id]

    def check(self, obj_id):
        if obj_id not in self:
            raise ObjNotFoundError(obj_id)
        if obj_id != self.id(self.objects[obj_id]):
            raise Error(obj_id)

    def get_random(self, batch_size):
        batch_size = min(len(self), batch_size)
        return random.sample(list(self.objects), batch_size)


@attr('!db')
class MixinTestReadFilter(unittest.TestCase):
    # Read only filter should not allow writing

    def setUp(self):
        super().setUp()
        storage = MockObjStorage()

        self.valid_content = b'pre-existing content'
        self.invalid_content = b'invalid_content'
        self.true_invalid_content = b'Anything that is not correct'
        self.absent_content = b'non-existent content'
        # Create a valid content.
        self.valid_id = storage.add(self.valid_content)
        # Create an invalid id and add a content with it.
        self.invalid_id = storage.id(self.true_invalid_content)
        storage.add(self.invalid_content, obj_id=self.invalid_id)
        # Compute an id for a non-existing content.
        self.absent_id = storage.id(self.absent_content)

        self.storage = add_filter(storage, read_only())

    @istest
    def can_contains(self):
        self.assertTrue(self.valid_id in self.storage)
        self.assertTrue(self.invalid_id in self.storage)
        self.assertFalse(self.absent_id in self.storage)

    @istest
    def can_iter(self):
        self.assertIn(self.valid_id, iter(self.storage))
        self.assertIn(self.invalid_id, iter(self.storage))

    @istest
    def can_len(self):
        self.assertEqual(2, len(self.storage))

    @istest
    def can_get(self):
        self.assertEqual(self.valid_content, self.storage.get(self.valid_id))
        self.assertEqual(self.invalid_content,
                         self.storage.get(self.invalid_id))

    @istest
    def can_check(self):
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.absent_id)
        with self.assertRaises(Error):
            self.storage.check(self.invalid_id)
        self.storage.check(self.valid_id)

    @istest
    def can_get_random(self):
        self.assertEqual(1, len(self.storage.get_random(1)))
        self.assertEqual(len(self.storage), len(self.storage.get_random(1000)))

    @istest
    def cannot_add(self):
        new_id = self.storage.add(b'New content')
        result = self.storage.add(self.valid_content, self.valid_id)
        self.assertNotIn(new_id, self.storage)
        self.assertIsNone(result)

    @istest
    def cannot_restore(self):
        new_id = self.storage.restore(b'New content')
        result = self.storage.restore(self.valid_content, self.valid_id)
        self.assertNotIn(new_id, self.storage)
        self.assertIsNone(result)


class MixinTestIdFilter():
    """ Mixin class that tests the filters based on filter.IdFilter

    Methods "make_valid", "make_invalid" and "filter_storage" must be
    implemented by subclasses.
    """

    def setUp(self):
        super().setUp()
        # Use a hack here : as the mock uses the content as id, it is easy to
        # create contents that are filtered or not.
        self.prefix = '71'
        storage = MockObjStorage()

        # Make the storage filtered
        self.base_storage = storage
        self.storage = self.filter_storage(storage)

        # Present content with valid id
        self.present_valid_content = self.ensure_valid(b'yroqdtotji')
        self.present_valid_id = storage.id(self.present_valid_content)

        # Present content with invalid id
        self.present_invalid_content = self.ensure_invalid(b'glxddlmmzb')
        self.present_invalid_id = storage.id(self.present_invalid_content)

        # Missing content with valid id
        self.missing_valid_content = self.ensure_valid(b'rmzkdclkez')
        self.missing_valid_id = storage.id(self.missing_valid_content)

        # Missing content with invalid id
        self.missing_invalid_content = self.ensure_invalid(b'hlejfuginh')
        self.missing_invalid_id = storage.id(self.missing_invalid_content)

        # Present corrupted content with valid id
        self.present_corrupted_valid_content = self.ensure_valid(b'cdsjwnpaij')
        self.true_present_corrupted_valid_content = self.ensure_valid(
            b'mgsdpawcrr')
        self.present_corrupted_valid_id = storage.id(
            self.true_present_corrupted_valid_content)

        # Present corrupted content with invalid id
        self.present_corrupted_invalid_content = self.ensure_invalid(
            b'pspjljnrco')
        self.true_present_corrupted_invalid_content = self.ensure_invalid(
            b'rjocbnnbso')
        self.present_corrupted_invalid_id = storage.id(
            self.true_present_corrupted_invalid_content)

        # Missing (potentially) corrupted content with valid id
        self.missing_corrupted_valid_content = self.ensure_valid(
            b'zxkokfgtou')
        self.true_missing_corrupted_valid_content = self.ensure_valid(
            b'royoncooqa')
        self.missing_corrupted_valid_id = storage.id(
            self.true_missing_corrupted_valid_content)

        # Missing (potentially) corrupted content with invalid id
        self.missing_corrupted_invalid_content = self.ensure_invalid(
            b'hxaxnrmnyk')
        self.true_missing_corrupted_invalid_content = self.ensure_invalid(
            b'qhbolyuifr')
        self.missing_corrupted_invalid_id = storage.id(
            self.true_missing_corrupted_invalid_content)

        # Add the content that are supposed to be present
        storage.add(self.present_valid_content)
        storage.add(self.present_invalid_content)
        storage.add(self.present_corrupted_valid_content,
                    obj_id=self.present_corrupted_valid_id)
        storage.add(self.present_corrupted_invalid_content,
                    obj_id=self.present_corrupted_invalid_id)

    def filter_storage(self, storage):
        raise NotImplementedError(
            'Id_filter test class must have a filter_storage method')

    def ensure_valid(self, content=None):
        if content is None:
            content = get_random_content()
        while not self.storage.is_valid(self.base_storage.id(content)):
            content = get_random_content()
        return content

    def ensure_invalid(self, content=None):
        if content is None:
            content = get_random_content()
        while self.storage.is_valid(self.base_storage.id(content)):
            content = get_random_content()
        return content

    @istest
    def contains(self):
        # Both contents are present, but the invalid one should be ignored.
        self.assertTrue(self.present_valid_id in self.storage)
        self.assertFalse(self.present_invalid_id in self.storage)
        self.assertFalse(self.missing_valid_id in self.storage)
        self.assertFalse(self.missing_invalid_id in self.storage)
        self.assertTrue(self.present_corrupted_valid_id in self.storage)
        self.assertFalse(self.present_corrupted_invalid_id in self.storage)
        self.assertFalse(self.missing_corrupted_valid_id in self.storage)
        self.assertFalse(self.missing_corrupted_invalid_id in self.storage)

    @istest
    def iter(self):
        self.assertIn(self.present_valid_id, iter(self.storage))
        self.assertNotIn(self.present_invalid_id, iter(self.storage))
        self.assertNotIn(self.missing_valid_id, iter(self.storage))
        self.assertNotIn(self.missing_invalid_id, iter(self.storage))
        self.assertIn(self.present_corrupted_valid_id, iter(self.storage))
        self.assertNotIn(self.present_corrupted_invalid_id, iter(self.storage))
        self.assertNotIn(self.missing_corrupted_valid_id, iter(self.storage))
        self.assertNotIn(self.missing_corrupted_invalid_id, iter(self.storage))

    @istest
    def len(self):
        # Four contents are present, but only two should be valid.
        self.assertEqual(2, len(self.storage))

    @istest
    def get(self):
        self.assertEqual(self.present_valid_content,
                         self.storage.get(self.present_valid_id))
        with self.assertRaises(ObjNotFoundError):
            self.storage.get(self.present_invalid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.get(self.missing_valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.get(self.missing_invalid_id)
        self.assertEqual(self.present_corrupted_valid_content,
                         self.storage.get(self.present_corrupted_valid_id))
        with self.assertRaises(ObjNotFoundError):
            self.storage.get(self.present_corrupted_invalid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.get(self.missing_corrupted_valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.get(self.missing_corrupted_invalid_id)

    @istest
    def check(self):
        self.storage.check(self.present_valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.present_invalid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.missing_valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.missing_invalid_id)
        with self.assertRaises(Error):
            self.storage.check(self.present_corrupted_valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.present_corrupted_invalid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.missing_corrupted_valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(self.missing_corrupted_invalid_id)

    @istest
    def get_random(self):
        self.assertEqual(0, len(list(self.storage.get_random(0))))

        random_content = list(self.storage.get_random(1000))
        self.assertIn(self.present_valid_id, random_content)
        self.assertNotIn(self.present_invalid_id, random_content)
        self.assertNotIn(self.missing_valid_id, random_content)
        self.assertNotIn(self.missing_invalid_id, random_content)
        self.assertIn(self.present_corrupted_valid_id, random_content)
        self.assertNotIn(self.present_corrupted_invalid_id, random_content)
        self.assertNotIn(self.missing_corrupted_valid_id, random_content)
        self.assertNotIn(self.missing_corrupted_invalid_id, random_content)

    @istest
    def add(self):
        # Add valid and invalid contents to the storage and check their
        # presence with the unfiltered storage.
        valid_content = self.ensure_valid(b'ulepsrjbgt')
        valid_id = self.base_storage.id(valid_content)
        invalid_content = self.ensure_invalid(b'znvghkjked')
        invalid_id = self.base_storage.id(invalid_content)
        self.storage.add(valid_content)
        self.storage.add(invalid_content)
        self.assertTrue(valid_id in self.base_storage)
        self.assertFalse(invalid_id in self.base_storage)

    @istest
    def restore(self):
        # Add corrupted content to the storage and the try to restore it
        valid_content = self.ensure_valid(b'ulepsrjbgt')
        valid_id = self.base_storage.id(valid_content)
        corrupted_content = self.ensure_valid(b'ltjkjsloyb')
        corrupted_id = self.base_storage.id(corrupted_content)
        self.storage.add(corrupted_content, obj_id=valid_id)
        with self.assertRaises(ObjNotFoundError):
            self.storage.check(corrupted_id)
        with self.assertRaises(Error):
            self.storage.check(valid_id)
        self.storage.restore(valid_content)
        self.storage.check(valid_id)


@attr('!db')
class TestPrefixFilter(MixinTestIdFilter, unittest.TestCase):
    def setUp(self):
        self.prefix = b'71'
        super().setUp()

    def ensure_valid(self, content):
        obj_id = hashutil.hashdata(content)['sha1']
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        self.assertTrue(hex_obj_id.startswith(self.prefix))
        return content

    def ensure_invalid(self, content):
        obj_id = hashutil.hashdata(content)['sha1']
        hex_obj_id = hashutil.hash_to_hex(obj_id)
        self.assertFalse(hex_obj_id.startswith(self.prefix))
        return content

    def filter_storage(self, storage):
        return add_filter(storage, id_prefix(self.prefix))


@attr('!db')
class TestRegexFilter(MixinTestIdFilter, unittest.TestCase):
    def setUp(self):
        self.regex = r'[a-f][0-9].*'
        super().setUp()

    def filter_storage(self, storage):
        return add_filter(storage, id_regex(self.regex))
