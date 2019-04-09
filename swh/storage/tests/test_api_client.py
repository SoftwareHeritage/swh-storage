# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import shutil
import tempfile
import unittest

from swh.core.tests.server_testing import ServerTestFixture
import swh.storage.storage as storage
from swh.storage.journal_writer import \
    get_journal_writer, InMemoryJournalWriter
from swh.storage.api.client import RemoteStorage
import swh.storage.api.server as server
from swh.storage.api.server import app
from swh.storage.tests.test_storage import \
    CommonTestStorage, CommonPropTestStorage, StorageTestDbFixture


class RemoteStorageFixture(ServerTestFixture, StorageTestDbFixture,
                           unittest.TestCase):
    """Test the remote storage API.

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonTestStorage.
    """

    def setUp(self):
        def mock_get_journal_writer(cls, args=None):
            assert cls == 'inmemory'
            return journal_writer
        server.storage = None
        storage.get_journal_writer = mock_get_journal_writer
        journal_writer = InMemoryJournalWriter()
        self.journal_writer = journal_writer

        # ServerTestFixture needs to have self.objroot for
        # setUp() method, but this field is defined in
        # AbstractTestStorage's setUp()
        # To avoid confusion, override the self.objroot to a
        # one chosen in this class.
        self.storage_base = tempfile.mkdtemp()
        self.config = {
            'storage': {
                'cls': 'local',
                'args': {
                    'db': 'dbname=%s' % self.TEST_DB_NAME,
                    'objstorage': {
                        'cls': 'pathslicing',
                        'args': {
                            'root': self.storage_base,
                            'slicing': '0:2',
                        },
                    },
                    'journal_writer': {
                        'cls': 'inmemory',
                    }
                }
            }
        }
        self.app = app
        super().setUp()
        self.storage = RemoteStorage(self.url())
        self.objroot = self.storage_base

    def tearDown(self):
        storage.get_journal_writer = get_journal_writer
        super().tearDown()
        shutil.rmtree(self.storage_base)


@pytest.mark.db
class TestRemoteStorage(CommonTestStorage, RemoteStorageFixture):
    @pytest.mark.skip('refresh_stat_counters not available in the remote api.')
    def test_stat_counters(self):
        pass


@pytest.mark.db
@pytest.mark.property_based
class PropTestRemoteStorage(CommonPropTestStorage, RemoteStorageFixture):
    @pytest.mark.skip('too slow')
    def test_add_arbitrary(self):
        pass
