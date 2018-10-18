# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile

from swh.storage import get_storage

from swh.core.tests.db_testing import SingleDbTestFixture
from swh.storage.tests import SQL_DIR


class StorageTestFixture(SingleDbTestFixture):
    """Mix this in a test subject class to get Storage testing support.

    This fixture requires to come before SingleDbTestFixture in the
    inheritance list as it uses its methods to setup its own
    internal database.

    Usage example:

        class MyTestStorage(StorageTestFixture, unittest.TestCase):
            ...

    """
    TEST_DB_NAME = 'softwareheritage-test-storage'
    TEST_DB_DUMP = os.path.join(SQL_DIR, '*.sql')

    def setUp(self):
        super().setUp()
        self.objtmp = tempfile.TemporaryDirectory()

        self.storage_config = {
            'cls': 'local',
            'args': {
                'db': 'dbname=%s' % self.TEST_DB_NAME,
                'objstorage': {
                    'cls': 'pathslicing',
                    'args': {
                        'root': self.objtmp.name,
                        'slicing': '0:1/1:5',
                    },
                },
            },
        }
        self.storage = get_storage(**self.storage_config)

    def tearDown(self):
        self.objtmp.cleanup()
        self.storage = None
        super().tearDown()

    def reset_storage_tables(self):
        excluded = {'dbversion', 'tool'}
        self.reset_db_tables(self.TEST_DB_NAME, excluded=excluded)
