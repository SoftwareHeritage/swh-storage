# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import unittest

import pytest

from swh.storage.in_memory import Storage

from swh.storage.tests.test_storage import \
    CommonTestStorage, CommonPropTestStorage


class TestInMemoryStorage(CommonTestStorage, unittest.TestCase):
    """Test the in-memory storage API

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonTestStorage.
    """
    def setUp(self):
        super().setUp()
        self.storage = Storage()

    @pytest.mark.skip('postgresql-specific test')
    def test_content_add(self):
        pass

    @pytest.mark.skip('postgresql-specific test')
    def test_skipped_content_add(self):
        pass


@pytest.mark.db
@pytest.mark.property_based
class PropTestInMemoryStorage(CommonPropTestStorage, unittest.TestCase):
    """Test the in-memory storage API

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in CommonPropTestStorage.
    """
    def setUp(self):
        super().setUp()
        self.storage = Storage()

    def reset_storage_tables(self):
        self.storage = Storage()
