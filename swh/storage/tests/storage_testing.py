# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import pathlib

from swh.storage import get_storage


class StorageTestFixture:
    """Mix this in a test subject class to get Storage testing support.

    This fixture requires to come before DbTestFixture in the inheritance list
    as it uses its methods to setup its own internal database.

    Usage example:

        class TestStorage(StorageTestFixture, DbTestFixture):
            ...
    """
    TEST_STORAGE_DB_NAME = 'softwareheritage-test-storage'

    @classmethod
    def setUpClass(cls):
        if not hasattr(cls, 'DB_TEST_FIXTURE_IMPORTED'):
            raise RuntimeError("StorageTestFixture needs to be followed by "
                               "DbTestFixture in the inheritance list.")

        test_dir = pathlib.Path(__file__).absolute().parent
        test_data_dir = test_dir / '../../../../swh-storage-testdata'
        test_db_dump = (test_data_dir / 'dumps/swh.dump').absolute()
        cls.add_db(cls.TEST_STORAGE_DB_NAME, str(test_db_dump), 'pg_dump')
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.objtmp = tempfile.TemporaryDirectory()

        storage_conf = {
            'cls': 'local',
            'args': {
                'db': self.test_db[self.TEST_STORAGE_DB_NAME].conn,
                'objstorage': {
                    'cls': 'pathslicing',
                    'args': {
                        'root': self.objtmp.name,
                        'slicing': '0:1/1:5',
                    },
                },
            },
        }
        self.storage = get_storage(**storage_conf)

    def tearDown(self):
        self.objtmp.cleanup()
        super().tearDown()
