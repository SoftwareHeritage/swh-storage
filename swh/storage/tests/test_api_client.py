# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import multiprocessing
import socket
import time
import unittest
from urllib.request import urlopen

from swh.storage.tests.test_storage import AbstractTestStorage
from swh.storage.api.client import RemoteStorage
from swh.storage.api.server import app


class TestRemoteStorage(AbstractTestStorage, unittest.TestCase):
    """Test the remote storage API.

    This class doesn't define any tests as we want identical
    functionality between local and remote storage. All the tests are
    therefore defined in AbstractTestStorage.
    """

    def setUp(self):
        super().setUp()

        self.start_server()
        self.storage = RemoteStorage(self.url())

    def tearDown(self):
        self.stop_server()

        super().tearDown()

    def url(self):
        return 'http://127.0.0.1:%d/' % self.port

    def start_server(self):
        """Spawn the API server using multiprocessing"""
        self.process = None

        # WSGI app configuration
        self.app = app
        self.app.config['db'] = 'dbname=%s' % self.dbname
        self.app.config['storage_base'] = self.objroot

        # Get an available port number
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', 0))
        self.port = sock.getsockname()[1]
        sock.close()

        # We need a worker function for multiprocessing
        def worker(app, port):
            return app.run(port=port, use_reloader=False)

        self.process = multiprocessing.Process(
            target=worker, args=(self.app, self.port)
        )
        self.process.start()

        # Wait max. 5 seconds for server to spawn
        i = 0
        while i < 20:
            try:
                urlopen(self.url())
            except Exception:
                i += 1
                time.sleep(0.25)
            else:
                break

    def stop_server(self):
        """Terminate the API server"""
        if self.process:
            self.process.terminate()
