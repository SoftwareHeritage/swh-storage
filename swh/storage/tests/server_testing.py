# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import aiohttp
import multiprocessing
import socket
import time

from urllib.request import urlopen


class ServerTestFixtureBaseClass(metaclass=abc.ABCMeta):
    """Base class for http client/server testing implementations.

    Override this class to implement the following methods:
    - process_config: to do something needed for the server
      configuration (e.g propagate the configuration to other part)
    - define_worker_function: define the function that will actually
      run the server.

    To ensure test isolation, each test will run in a different server
    and a different folder.

    In order to correctly work, the subclass must call the parents
    class's setUp() and tearDown() methods.

    """
    def setUp(self):
        super().setUp()
        self.start_server()

    def tearDown(self):
        self.stop_server()
        super().tearDown()

    def url(self):
        return 'http://127.0.0.1:%d/' % self.port

    def process_config(self):
        """Process the server's configuration.  Do something useful for
        example, pass along the self.config dictionary inside the
        self.app.

        By default, do nothing.

        """
        pass

    @abc.abstractmethod
    def define_worker_function(self, app, port):
        """Define how the actual implementation server will run.

        """
        pass

    def start_server(self):
        """ Spawn the API server using multiprocessing.
        """
        self.process = None

        self.process_config()

        # Get an available port number
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', 0))
        self.port = sock.getsockname()[1]
        sock.close()

        worker_fn = self.define_worker_function()

        self.process = multiprocessing.Process(
            target=worker_fn, args=(self.app, self.port)
        )
        self.process.start()

        # Wait max 5 seconds for server to spawn
        i = 0
        while i < 500:
            try:
                urlopen(self.url())
            except Exception:
                i += 1
                time.sleep(0.01)
            else:
                return

    def stop_server(self):
        """ Terminate the API server's process.
        """
        if self.process:
            self.process.terminate()


class ServerTestFixture(ServerTestFixtureBaseClass):
    """Base class for http client/server testing (e.g flask).

    Mix this in a test class in order to have access to an http server
    running in background.

    Note that the subclass should define a dictionary in self.config
    that contains the server config.  And an application in self.app
    that corresponds to the type of server the tested client needs.

    To ensure test isolation, each test will run in a different server
    and a different folder.

    In order to correctly work, the subclass must call the parents
    class's setUp() and tearDown() methods.
    """
    def process_config(self):
        # WSGI app configuration
        for key, value in self.config.items():
            self.app.config[key] = value

    def define_worker_function(self):
        def worker(app, port):
            return app.run(port=port, use_reloader=False)

        return worker


class ServerTestFixtureAsync(ServerTestFixtureBaseClass):
    """Base class for http client/server async testing (e.g aiohttp).

    Mix this in a test class in order to have access to an http server
    running in background.

    Note that the subclass should define an application in self.app
    that corresponds to the type of server the tested client needs.

    To ensure test isolation, each test will run in a different server
    and a different folder.

    In order to correctly work, the subclass must call the parents
    class's setUp() and tearDown() methods.

    """
    def define_worker_function(self):
        def worker(app, port):
            return aiohttp.web.run_app(app, port=int(port))

        return worker
