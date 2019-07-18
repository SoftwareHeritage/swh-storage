# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import unittest

import pytest

from swh.storage.in_memory import Storage, ENABLE_ORIGIN_IDS

from swh.storage.tests.test_storage import \
    CommonTestStorage, CommonPropTestStorage


class TestInMemoryStorage(CommonTestStorage, unittest.TestCase):
    """Test the in-memory storage API

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonTestStorage.
    """
    _test_origin_ids = ENABLE_ORIGIN_IDS

    def setUp(self):
        super().setUp()
        self.reset_storage()

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

    if not _test_origin_ids:
        @pytest.mark.skip('requires origin ids')
        def test_origin_metadata_add(self):
            pass

        @pytest.mark.skip('requires origin ids')
        def test_origin_metadata_get(self):
            pass

        @pytest.mark.skip('requires origin ids')
        def test_origin_metadata_get_by_provider_type(self):
            pass

    def reset_storage(self):
        self.storage = Storage(journal_writer={'cls': 'inmemory'})
        self.journal_writer = self.storage.journal_writer


@pytest.mark.property_based
class PropTestInMemoryStorage(CommonPropTestStorage, unittest.TestCase):
    """Test the in-memory storage API

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonPropTestStorage.
    """
    _test_origin_ids = ENABLE_ORIGIN_IDS

    def setUp(self):
        super().setUp()
        self.storage = Storage()

    def reset_storage(self):
        self.storage = Storage()

    if not _test_origin_ids:
        @pytest.mark.skip('requires origin ids')
        def test_origin_get_range(self, new_origins):
            pass
