# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .test_storage import TestStorage

from swh.storage.remote_storage import RemoteStorage


class TestRemoteStorage(TestStorage):
    def setUp(self):
        super().setUp()

        self.storage = RemoteStorage('http://127.0.0.1:5000/')

    def tearDown(self):
        super().tearDown()
