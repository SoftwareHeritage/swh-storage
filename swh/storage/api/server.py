# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from typing import Any, Dict, Optional

from psycopg2.errors import OperationalError, QueryCanceled

from swh.core import config
from swh.core.api import RPCServerApp
from swh.core.api import encode_data_server as encode_data
from swh.core.api import error_handler, serializers
from swh.storage import get_storage as get_swhstorage

from ..exc import StorageArgumentException
from ..interface import StorageInterface
from ..metrics import send_metric, timed
from .serializers import DECODERS, ENCODERS


def get_storage():
    global storage
    if not storage:
        storage = get_swhstorage(**app.config["storage"])

    return storage


class StorageServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS

    method_decorators = [timed]

    def _process_metrics(self, metrics, endpoint):
        for metric, count in metrics.items():
            send_metric(metric=metric, count=count, method_name=endpoint)

    def post_content_add(self, ret, kw):
        self._process_metrics(ret, "content_add")

    def post_content_add_metadata(self, ret, kw):
        self._process_metrics(ret, "content_add_metadata")

    def post_skipped_content_add(self, ret, kw):
        self._process_metrics(ret, "skipped_content_add")

    def post_directory_add(self, ret, kw):
        self._process_metrics(ret, "directory_add")

    def post_revision_add(self, ret, kw):
        self._process_metrics(ret, "revision_add")

    def post_release_add(self, ret, kw):
        self._process_metrics(ret, "release_add")

    def post_snapshot_add(self, ret, kw):
        self._process_metrics(ret, "snapshot_add")

    def post_origin_visit_status_add(self, ret, kw):
        self._process_metrics(ret, "origin_visit_status_add")

    def post_origin_add(self, ret, kw):
        self._process_metrics(ret, "origin_add")

    def post_raw_extrinsic_metadata_add(self, ret, kw):
        self._process_metrics(ret, "raw_extrinsic_metadata_add")

    def post_metadata_fetcher_add(self, ret, kw):
        self._process_metrics(ret, "metadata_fetcher_add")

    def post_metadata_authority_add(self, ret, kw):
        self._process_metrics(ret, "metadata_authority_add")

    def post_extid_add(self, ret, kw):
        self._process_metrics(ret, "extid_add")

    def post_origin_visit_add(self, ret, kw):
        nb_visits = len(ret)
        send_metric(
            "origin_visit:add",
            count=nb_visits,
            # method_name should be "origin_visit_add", but changing it now would break
            # existing metrics
            method_name="origin_visit",
        )


app = StorageServerApp(
    __name__, backend_class=StorageInterface, backend_factory=get_storage
)
storage = None


@app.errorhandler(StorageArgumentException)
def argument_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.errorhandler(OperationalError)
def operationalerror_exception_handler(exception):
    # Same as error_handler(exception, encode_data); but does not log or send to Sentry.
    # These errors are noisy, and are better logged on the caller's side after it
    # retried a few times.
    # Additionally, we return 503 instead of 500, telling clients they should retry.
    response = encode_data(serializers.exception_to_dict(exception))
    response.status_code = 503
    return response


@app.errorhandler(QueryCanceled)
def querycancelled_exception_handler(exception):
    # Ditto, but 500 instead of 503, because this is usually caused by the query
    # size instead of a transient failure
    response = encode_data(serializers.exception_to_dict(exception))
    response.status_code = 500
    return response


@app.errorhandler(Exception)
def default_error_handler(exception):
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


def load_and_check_config(config_path: Optional[str]) -> Dict[str, Any]:
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_path: Path to the configuration file to load

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_path:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} does not exist")

    cfg = config.read(config_path)
    if "storage" not in cfg:
        raise KeyError("Missing 'storage' configuration")

    return cfg


def make_app_from_configfile() -> StorageServerApp:
    """Run the WSGI app from the webserver, loading the configuration from
    a configuration file.

    SWH_CONFIG_FILENAME environment variable defines the
    configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_path = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_path)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app


if __name__ == "__main__":
    print("Deprecated. Use swh-storage")
