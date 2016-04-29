# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import tempfile

from swh.storage.tests.test_storage import AbstractTestStorage
from swh.storage.tests.server_testing import ServerTestFixture
from swh.storage.api.client import RemoteStorage
from swh.storage.api.server import app


class TestRemoteStorage(AbstractTestStorage, ServerTestFixture,
                        unittest.TestCase):
    """Test the remote storage API.

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in AbstractTestStorage.
    """

    def setUp(self):
        # ServerTestFixture needs to have self.objroot for
        # setUp() method, but this field is defined in
        # AbstractTestStorage's setUp()
        # To avoid confusion, override the self.objroot to a
        # one choosen in this class.
        storage_base = tempfile.mkdtemp()
        self.config = {'db': 'dbname=%s' % self.dbname,
                       'storage_base': storage_base}
        self.app = app
        super().setUp()
        self.storage = RemoteStorage(self.url())
        self.objroot = storage_base
