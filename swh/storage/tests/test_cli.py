# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import logging
import tempfile
import requests
import threading
import time

from contextlib import contextmanager
from aiohttp.test_utils import unused_port

import yaml

from click.testing import CliRunner

from swh.storage.cli import storage as cli


logger = logging.getLogger(__name__)


CLI_CONFIG = {
    "storage": {"cls": "memory",},
}


def invoke(*args, env=None):
    config = copy.deepcopy(CLI_CONFIG)
    runner = CliRunner()
    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        return runner.invoke(cli, args, obj={"log_level": logging.DEBUG}, env=env,)


@contextmanager
def check_rpc_serve():
    # this context manager adds, if needed, a /quit route to the flask application that
    # uses the werzeuk tric to exit the test server started by app.run()
    #
    # The /testing/ gathering code is executed in a thread while the main thread runs
    # the test server. Results of the tests consist in a list of requests Response
    # objects stored in the `response` shared list.
    #
    # This convoluted execution code is needed because the flask app needs to run in the
    # main thread.
    #
    # The context manager will yield the port on which tests (GET queries) will be done,
    # so the test function should start the RPC server on this port in the body of the
    # context manager.

    from swh.storage.api.server import app
    from flask import request

    if "/quit" not in [r.rule for r in app.url_map.iter_rules()]:

        @app.route("/quit")
        def quit_app():
            request.environ["werkzeug.server.shutdown"]()
            return "Bye"

    port = unused_port()
    responses = []

    def run_tests():
        # we do run the "test" part in the thread because flask does not like the
        # app.run() to be executed in a (non-main) thread
        def get(path):
            for i in range(5):
                try:
                    resp = requests.get(f"http://127.0.0.1:{port}{path}")
                    break
                except requests.exceptions.ConnectionError:
                    time.sleep(0.2)
            responses.append(resp)

        get("/")  # ensure the server starts and can reply the '/' path

        get("/quit")  # ask the test server to quit gracefully

    t = threading.Thread(target=run_tests)
    t.start()
    yield port  # this is where the caller should start the server listening on "port"

    # we expect to reach this point because the /quit endpoint should have been called,
    # thus the server, executed in the caller's context manager's body, should now
    # return
    t.join()

    # check the GET requests we made in the thread have expected results
    assert len(responses) == 2
    assert responses[0].status_code == 200
    assert "Software Heritage storage server" in responses[0].text
    assert responses[1].status_code == 200
    assert responses[1].text == "Bye"


def test_rpc_serve():
    with check_rpc_serve() as port:
        invoke("rpc-serve", "--host", "127.0.0.1", "--port", port)


def test_rpc_serve_bwcompat():
    def invoke(*args, env=None):
        config = copy.deepcopy(CLI_CONFIG)
        runner = CliRunner()
        with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
            yaml.dump(config, config_fd)
            config_fd.seek(0)
            args = list(args) + [config_fd.name]
            return runner.invoke(cli, args, obj={"log_level": logging.DEBUG}, env=env,)

    with check_rpc_serve() as port:
        invoke("rpc-serve", "--host", "127.0.0.1", "--port", port)
