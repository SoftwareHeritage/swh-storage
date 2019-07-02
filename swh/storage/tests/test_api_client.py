# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import shutil
import tempfile
import unittest

import pytest

from swh.core.api.tests.server_testing import ServerTestFixture
import swh.storage.storage as storage
from swh.storage.journal_writer import \
    get_journal_writer, InMemoryJournalWriter
from swh.storage.in_memory import Storage as InMemoryStorage
from swh.storage.api.client import RemoteStorage
import swh.storage.api.server as server
from swh.storage.api.server import app
from swh.storage.tests.test_storage import \
    CommonTestStorage, CommonPropTestStorage, StorageTestDbFixture


class RemotePgStorageFixture(StorageTestDbFixture, ServerTestFixture,
                             unittest.TestCase):
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
        self.objroot = self.storage_base
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

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.storage_base)
        storage.get_journal_writer = get_journal_writer

    def reset_storage(self):
        excluded = {'dbversion', 'tool'}
        self.reset_db_tables(self.TEST_DB_NAME, excluded=excluded)
        self.journal_writer.objects[:] = []


class RemoteMemStorageFixture(ServerTestFixture, unittest.TestCase):
    def setUp(self):
        self.config = {
            'storage': {
                'cls': 'memory',
                'args': {
                    'journal_writer': {
                        'cls': 'inmemory',
                    }
                }
            }
        }
        self.__storage = InMemoryStorage(
            journal_writer={'cls': 'inmemory'})

        self._get_storage_patcher = unittest.mock.patch(
            'swh.storage.api.server.get_storage', return_value=self.__storage)
        self._get_storage_patcher.start()
        self.app = app
        super().setUp()
        self.storage = RemoteStorage(self.url())
        self.journal_writer = self.__storage.journal_writer

    def tearDown(self):
        super().tearDown()
        self._get_storage_patcher.stop()

    def reset_storage(self):
        self.storage.reset()
        self.journal_writer.objects[:] = []


@pytest.mark.network
class TestRemoteMemStorage(CommonTestStorage, RemoteMemStorageFixture):
    @pytest.mark.skip('refresh_stat_counters not available in the remote api.')
    def test_stat_counters(self):
        pass

    @pytest.mark.skip('postgresql-specific test')
    def test_content_add_db(self):
        pass

    @pytest.mark.skip('postgresql-specific test')
    def test_skipped_content_add_db(self):
        pass

    @pytest.mark.skip('postgresql-specific test')
    def test_content_add_metadata_db(self):
        pass

    @pytest.mark.skip(
        'not implemented, see https://forge.softwareheritage.org/T1633')
    def test_skipped_content_add(self):
        pass


@pytest.mark.db
@pytest.mark.network
class TestRemotePgStorage(CommonTestStorage, RemotePgStorageFixture):
    @pytest.mark.skip('refresh_stat_counters not available in the remote api.')
    def test_stat_counters(self):
        pass


@pytest.mark.db
@pytest.mark.property_based
class PropTestRemotePgStorage(CommonPropTestStorage, RemotePgStorageFixture):
    @pytest.mark.skip('too slow')
    def test_add_arbitrary(self):
        pass
