# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import multiprocessing
import socket
import time

from urllib.request import urlopen


class ServerTestFixture():
    """ Base class for http client/server testing.

    Mix this in a test class in order to have access to an http flask
    server running in background.

    Note that the subclass should define a dictionary in self.config
    that contains the flask server config.
    And a flask application in self.app that corresponds to the type of
    server the tested client needs.

    To ensure test isolation, each test will run in a different server
    and a different repertory.

    In order to correctly work, the subclass must call the parents class's
    setUp() and tearDown() methods.
    """

    def setUp(self):
        super().setUp()
        self.start_server()

    def tearDown(self):
        self.stop_server()
        super().tearDown()

    def url(self):
        return 'http://127.0.0.1:%d/' % self.port

    def start_server(self):
        """ Spawn the API server using multiprocessing.
        """
        self.process = None

        # WSGI app configuration
        for key, value in self.config.items():
            self.app.config[key] = value
        # Get an available port number
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', 0))
        self.port = sock.getsockname()[1]
        sock.close()

        # Worker function for multiprocessing
        def worker(app, port):
            return app.run(port=port, use_reloader=False)

        self.process = multiprocessing.Process(
            target=worker, args=(self.app, self.port)
        )
        self.process.start()

        # Wait max 5 seconds for server to spawn
        i = 0
        while i < 20:
            try:
                urlopen(self.url())
            except Exception:
                i += 1
                time.sleep(0.25)
            else:
                return

    def stop_server(self):
        """ Terminate the API server's process.
        """
        if self.process:
            self.process.terminate()
