# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import gzip
import tempfile
import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.core import hashutil
from swh.storage.checker.checker import ContentChecker


class MockBackupStorage():

    def __init__(self):
        self.values = {}

    def content_add(self, id, value):
        self.values[id] = value

    def content_get(self, ids):
        for id in ids:
            try:
                data = self.values[id]
            except KeyError:
                yield None
                continue

            yield {'sha1': id, 'data': data}


@attr('fs')
class TestChecker(unittest.TestCase):
    """ Test the content integrity checker
    """

    def setUp(self):
        super().setUp()
        # Connect to an objstorage
        config = {'batch_size': 10}
        path = tempfile.mkdtemp()
        depth = 3
        self.checker = ContentChecker(config, path, depth, 'http://None')
        self.checker.backup_storages = [MockBackupStorage(),
                                        MockBackupStorage()]

    def corrupt_content(self, id):
        """ Make the given content invalid.
        """
        hex_id = hashutil.hash_to_hex(id)
        file_path = self.checker.objstorage._obj_path(hex_id)
        with gzip.open(file_path, 'wb') as f:
            f.write(b'Unexpected content')

    @istest
    def check_valid_content(self):
        # Check that a valid content is valid.
        content = b'check_valid_content'
        id = self.checker.objstorage.add(content)
        self.assertTrue(self.checker.check_content(id))

    @istest
    def check_invalid_content(self):
        # Check that an invalid content is noticed.
        content = b'check_invalid_content'
        id = self.checker.objstorage.add(content)
        self.corrupt_content(id)
        self.assertFalse(self.checker.check_content(id))

    @istest
    def repair_content_present_first(self):
        # Try to repair a content that is in the backup storage.
        content = b'repair_content_present_first'
        id = self.checker.objstorage.add(content)
        # Add a content to the mock
        self.checker.backup_storages[0].content_add(id, content)
        # Corrupt and repair it.
        self.corrupt_content(id)
        self.assertFalse(self.checker.check_content(id))
        self.checker.repair_contents([id])
        self.assertTrue(self.checker.check_content(id))

    @istest
    def repair_content_present_second(self):
        # Try to repair a content that is not in the first backup storage.
        content = b'repair_content_present_second'
        id = self.checker.objstorage.add(content)
        # Add a content to the mock
        self.checker.backup_storages[1].content_add(id, content)
        # Corrupt and repair it.
        self.corrupt_content(id)
        self.assertFalse(self.checker.check_content(id))
        self.checker.repair_contents([id])
        self.assertTrue(self.checker.check_content(id))

    @istest
    def repair_content_present_distributed(self):
        # Try to repair two contents that are in separate backup storages.
        content1 = b'repair_content_present_distributed_2'
        content2 = b'repair_content_present_distributed_1'
        id1 = self.checker.objstorage.add(content1)
        id2 = self.checker.objstorage.add(content2)
        # Add content to the mock.
        self.checker.backup_storages[0].content_add(id1, content1)
        self.checker.backup_storages[0].content_add(id2, content2)
        # Corrupt and repair it
        self.corrupt_content(id1)
        self.corrupt_content(id2)
        self.assertFalse(self.checker.check_content(id1))
        self.assertFalse(self.checker.check_content(id2))
        self.checker.repair_contents([id1, id2])
        self.assertTrue(self.checker.check_content(id1))
        self.assertTrue(self.checker.check_content(id2))

    @istest
    def repair_content_missing(self):
        # Try to repair a content that is NOT in the backup storage.
        content = b'repair_content_present'
        id = self.checker.objstorage.add(content)
        # Corrupt and repair it.
        self.corrupt_content(id)
        self.assertFalse(self.checker.check_content(id))
        self.checker.repair_contents([id])
        self.assertFalse(self.checker.check_content(id))
