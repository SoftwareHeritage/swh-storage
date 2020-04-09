# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import logging

from swh.core import config
from swh.storage import get_storage as get_swhstorage
from swh.core.api import RPCServerApp, error_handler, encode_data_server as encode_data

from ..interface import StorageInterface
from ..metrics import timed
from ..exc import StorageArgumentException

from .serializers import ENCODERS, DECODERS


def get_storage():
    global storage
    if not storage:
        storage = get_swhstorage(**app.config["storage"])

    return storage


class StorageServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS


app = StorageServerApp(
    __name__, backend_class=StorageInterface, backend_factory=get_storage
)
storage = None


@app.errorhandler(StorageArgumentException)
def argument_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.route("/")
@timed
def index():
    return """<html>
<head><title>Software Heritage storage server</title></head>
<body>
<p>You have reached the
<a href="https://www.softwareheritage.org/">Software Heritage</a>
storage server.<br />
See its
<a href="https://docs.softwareheritage.org/devel/swh-storage/">documentation
and API</a> for more information</p>
</body>
</html>"""


@app.route("/stat/counters", methods=["GET"])
@timed
def stat_counters():
    return encode_data(get_storage().stat_counters())


@app.route("/stat/refresh", methods=["GET"])
@timed
def refresh_stat_counters():
    return encode_data(get_storage().refresh_stat_counters())


api_cfg = None


def load_and_check_config(config_file, type="local"):
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_file (str): Path to the configuration file to load
        type (str): configuration type. For 'local' type, more
                    checks are done.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_file:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_file):
        raise FileNotFoundError("Configuration file %s does not exist" % (config_file,))

    cfg = config.read(config_file)
    if "storage" not in cfg:
        raise KeyError("Missing '%storage' configuration")

    return cfg


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
       a configuration file.

       SWH_CONFIG_FILENAME environment variable defines the
       configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_file)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app


if __name__ == "__main__":
    print("Deprecated. Use swh-storage")
