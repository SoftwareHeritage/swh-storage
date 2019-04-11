# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import logging

from flask import request
from functools import wraps

from swh.core import config
from swh.storage import get_storage as get_swhstorage
from swh.core.api import (SWHServerAPIApp, decode_request,
                          error_handler,
                          encode_data_server as encode_data)
from swh.core.statsd import statsd


app = SWHServerAPIApp(__name__)
storage = None


OPERATIONS_METRIC = 'swh_storage_operations_total'
OPERATIONS_UNIT_METRIC = "swh_storage_operations_{unit}_total"
DURATION_METRIC = "swh_storage_request_duration_seconds"


def timed(f):
    """Time that function!

    """
    @wraps(f)
    def d(*a, **kw):
        with statsd.timed(DURATION_METRIC, tags={'endpoint': f.__name__}):
            return f(*a, **kw)

    return d


def encode(f):
    @wraps(f)
    def d(*a, **kw):
        r = f(*a, **kw)
        return encode_data(r)

    return d


def send_metric(metric, count, method_name):
    """Send statsd metric with count for method `method_name`

    If count is 0, the metric is discarded.  If the metric is not
    parseable, the metric is discarded with a log message.

    Args:
        metric (str): Metric's name (e.g content:add, content:add:bytes)
        count (int): Associated value for the metric
        method_name (str): Method's name

    Returns:
        Bool to explicit if metric has been set or not
    """
    if count == 0:
        return False

    metric_type = metric.split(':')
    _length = len(metric_type)
    if _length == 2:
        object_type, operation = metric_type
        metric_name = OPERATIONS_METRIC
    elif _length == 3:
        object_type, operation, unit = metric_type
        metric_name = OPERATIONS_UNIT_METRIC.format(unit=unit)
    else:
        logging.warning('Skipping unknown metric {%s: %s}' % (
            metric, count))
        return False

    statsd.increment(
        metric_name, count, tags={
            'endpoint': method_name,
            'object_type': object_type,
            'operation': operation,
        })
    return True


def process_metrics(f):
    """Increment object counters for the decorated function.

    """
    @wraps(f)
    def d(*a, **kw):
        r = f(*a, **kw)
        for metric, count in r.items():
            send_metric(metric=metric, count=count, method_name=f.__name__)

        return r

    return d


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


def get_storage():
    global storage
    if not storage:
        storage = get_swhstorage(**app.config['storage'])

    return storage


@app.route('/')
@timed
def index():
    return '''<html>
<head><title>Software Heritage storage server</title></head>
<body>
<p>You have reached the
<a href="https://www.softwareheritage.org/">Software Heritage</a>
storage server.<br />
See its
<a href="https://docs.softwareheritage.org/devel/swh-storage/">documentation
and API</a> for more information</p>
</html>'''


@app.route('/check_config', methods=['POST'])
@timed
def check_config():
    return encode_data(get_storage().check_config(**decode_request(request)))


@app.route('/content/missing', methods=['POST'])
@timed
def content_missing():
    return encode_data(get_storage().content_missing(
        **decode_request(request)))


@app.route('/content/missing/sha1', methods=['POST'])
@timed
def content_missing_per_sha1():
    return encode_data(get_storage().content_missing_per_sha1(
        **decode_request(request)))


@app.route('/content/skipped/missing', methods=['POST'])
@timed
def skipped_content_missing():
    return encode_data(get_storage().skipped_content_missing(
        **decode_request(request)))


@app.route('/content/present', methods=['POST'])
@timed
def content_find():
    return encode_data(get_storage().content_find(**decode_request(request)))


@app.route('/content/add', methods=['POST'])
@timed
@encode
@process_metrics
def content_add():
    return get_storage().content_add(**decode_request(request))


@app.route('/content/add_metadata', methods=['POST'])
@timed
@encode
@process_metrics
def content_add_metadata():
    return get_storage().content_add_metadata(**decode_request(request))


@app.route('/content/update', methods=['POST'])
@timed
def content_update():
    return encode_data(get_storage().content_update(**decode_request(request)))


@app.route('/content/data', methods=['POST'])
@timed
def content_get():
    return encode_data(get_storage().content_get(**decode_request(request)))


@app.route('/content/metadata', methods=['POST'])
@timed
def content_get_metadata():
    return encode_data(get_storage().content_get_metadata(
        **decode_request(request)))


@app.route('/content/range', methods=['POST'])
@timed
def content_get_range():
    return encode_data(get_storage().content_get_range(
        **decode_request(request)))


@app.route('/directory/missing', methods=['POST'])
@timed
def directory_missing():
    return encode_data(get_storage().directory_missing(
        **decode_request(request)))


@app.route('/directory/add', methods=['POST'])
@timed
@encode
@process_metrics
def directory_add():
    return get_storage().directory_add(**decode_request(request))


@app.route('/directory/path', methods=['POST'])
@timed
def directory_entry_get_by_path():
    return encode_data(get_storage().directory_entry_get_by_path(
        **decode_request(request)))


@app.route('/directory/ls', methods=['POST'])
@timed
def directory_ls():
    return encode_data(get_storage().directory_ls(
        **decode_request(request)))


@app.route('/revision/add', methods=['POST'])
@timed
@encode
@process_metrics
def revision_add():
    return get_storage().revision_add(**decode_request(request))


@app.route('/revision', methods=['POST'])
@timed
def revision_get():
    return encode_data(get_storage().revision_get(**decode_request(request)))


@app.route('/revision/log', methods=['POST'])
@timed
def revision_log():
    return encode_data(get_storage().revision_log(**decode_request(request)))


@app.route('/revision/shortlog', methods=['POST'])
@timed
def revision_shortlog():
    return encode_data(get_storage().revision_shortlog(
        **decode_request(request)))


@app.route('/revision/missing', methods=['POST'])
@timed
def revision_missing():
    return encode_data(get_storage().revision_missing(
        **decode_request(request)))


@app.route('/release/add', methods=['POST'])
@timed
@encode
@process_metrics
def release_add():
    return get_storage().release_add(**decode_request(request))


@app.route('/release', methods=['POST'])
@timed
def release_get():
    return encode_data(get_storage().release_get(**decode_request(request)))


@app.route('/release/missing', methods=['POST'])
@timed
def release_missing():
    return encode_data(get_storage().release_missing(
        **decode_request(request)))


@app.route('/object/find_by_sha1_git', methods=['POST'])
@timed
def object_find_by_sha1_git():
    return encode_data(get_storage().object_find_by_sha1_git(
        **decode_request(request)))


@app.route('/snapshot/add', methods=['POST'])
@timed
@encode
@process_metrics
def snapshot_add():
    req_data = decode_request(request)
    if 'snapshot' in req_data:
        req_data['snapshots'] = req_data.pop('snapshot')
    return get_storage().snapshot_add(**req_data)


@app.route('/snapshot', methods=['POST'])
@timed
def snapshot_get():
    return encode_data(get_storage().snapshot_get(**decode_request(request)))


@app.route('/snapshot/by_origin_visit', methods=['POST'])
@timed
def snapshot_get_by_origin_visit():
    return encode_data(get_storage().snapshot_get_by_origin_visit(
        **decode_request(request)))


@app.route('/snapshot/latest', methods=['POST'])
@timed
def snapshot_get_latest():
    return encode_data(get_storage().snapshot_get_latest(
        **decode_request(request)))


@app.route('/snapshot/count_branches', methods=['POST'])
@timed
def snapshot_count_branches():
    return encode_data(get_storage().snapshot_count_branches(
        **decode_request(request)))


@app.route('/snapshot/get_branches', methods=['POST'])
@timed
def snapshot_get_branches():
    return encode_data(get_storage().snapshot_get_branches(
        **decode_request(request)))


@app.route('/origin/get', methods=['POST'])
@timed
def origin_get():
    return encode_data(get_storage().origin_get(**decode_request(request)))


@app.route('/origin/get_range', methods=['POST'])
@timed
def origin_get_range():
    return encode_data(get_storage().origin_get_range(
        **decode_request(request)))


@app.route('/origin/search', methods=['POST'])
@timed
def origin_search():
    return encode_data(get_storage().origin_search(**decode_request(request)))


@app.route('/origin/count', methods=['POST'])
@timed
def origin_count():
    return encode_data(get_storage().origin_count(**decode_request(request)))


@app.route('/origin/add_multi', methods=['POST'])
@timed
@encode
def origin_add():
    origins = get_storage().origin_add(**decode_request(request))
    send_metric('origin:add', count=len(origins), method_name='origin_add')
    return origins


@app.route('/origin/add', methods=['POST'])
@timed
@encode
def origin_add_one():
    origin = get_storage().origin_add_one(**decode_request(request))
    send_metric('origin:add', count=1, method_name='origin_add_one')
    return origin


@app.route('/origin/visit/get', methods=['POST'])
@timed
def origin_visit_get():
    return encode_data(get_storage().origin_visit_get(
        **decode_request(request)))


@app.route('/origin/visit/getby', methods=['POST'])
@timed
def origin_visit_get_by():
    return encode_data(
        get_storage().origin_visit_get_by(**decode_request(request)))


@app.route('/origin/visit/add', methods=['POST'])
@timed
@encode
def origin_visit_add():
    origin_visit = get_storage().origin_visit_add(
        **decode_request(request))
    send_metric('origin_visit:add', count=1, method_name='origin_visit')
    return origin_visit


@app.route('/origin/visit/update', methods=['POST'])
@timed
def origin_visit_update():
    return encode_data(get_storage().origin_visit_update(
        **decode_request(request)))


@app.route('/origin/visit/upsert', methods=['POST'])
@timed
def origin_visit_upsert():
    return encode_data(get_storage().origin_visit_upsert(
        **decode_request(request)))


@app.route('/person', methods=['POST'])
@timed
def person_get():
    return encode_data(get_storage().person_get(**decode_request(request)))


@app.route('/fetch_history', methods=['GET'])
@timed
def fetch_history_get():
    return encode_data(get_storage().fetch_history_get(request.args['id']))


@app.route('/fetch_history/start', methods=['POST'])
@timed
def fetch_history_start():
    return encode_data(
        get_storage().fetch_history_start(**decode_request(request)))


@app.route('/fetch_history/end', methods=['POST'])
@timed
def fetch_history_end():
    return encode_data(
        get_storage().fetch_history_end(**decode_request(request)))


@app.route('/tool/data', methods=['POST'])
@timed
def tool_get():
    return encode_data(get_storage().tool_get(
        **decode_request(request)))


@app.route('/tool/add', methods=['POST'])
@timed
@encode
def tool_add():
    tools = get_storage().tool_add(**decode_request(request))
    send_metric('tool:add', count=len(tools), method_name='tool_add')
    return tools


@app.route('/origin/metadata/add', methods=['POST'])
@timed
@encode
def origin_metadata_add():
    origin_metadata = get_storage().origin_metadata_add(
        **decode_request(request))
    send_metric(
        'origin_metadata:add', count=1, method_name='origin_metadata_add')
    return origin_metadata


@app.route('/origin/metadata/get', methods=['POST'])
@timed
def origin_metadata_get_by():
    return encode_data(get_storage().origin_metadata_get_by(**decode_request(
                                                       request)))


@app.route('/provider/add', methods=['POST'])
@timed
@encode
def metadata_provider_add():
    metadata_provider = get_storage().metadata_provider_add(**decode_request(
        request))
    send_metric(
        'metadata_provider:add', count=1, method_name='metadata_provider')
    return metadata_provider


@app.route('/provider/get', methods=['POST'])
@timed
def metadata_provider_get():
    return encode_data(get_storage().metadata_provider_get(**decode_request(
                                                       request)))


@app.route('/provider/getby', methods=['POST'])
@timed
def metadata_provider_get_by():
    return encode_data(get_storage().metadata_provider_get_by(**decode_request(
                                                       request)))


@app.route('/stat/counters', methods=['GET'])
@timed
def stat_counters():
    return encode_data(get_storage().stat_counters())


@app.route('/algos/diff_directories', methods=['POST'])
@timed
def diff_directories():
    return encode_data(get_storage().diff_directories(
        **decode_request(request)))


@app.route('/algos/diff_revisions', methods=['POST'])
@timed
def diff_revisions():
    return encode_data(get_storage().diff_revisions(**decode_request(request)))


@app.route('/algos/diff_revision', methods=['POST'])
@timed
def diff_revision():
    return encode_data(get_storage().diff_revision(**decode_request(request)))


api_cfg = None


def load_and_check_config(config_file, type='local'):
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
        raise EnvironmentError('Configuration file must be defined')

    if not os.path.exists(config_file):
        raise FileNotFoundError('Configuration file %s does not exist' % (
            config_file, ))

    cfg = config.read(config_file)
    if 'storage' not in cfg:
        raise KeyError("Missing '%storage' configuration")

    if type == 'local':
        vcfg = cfg['storage']
        cls = vcfg.get('cls')
        if cls != 'local':
            raise ValueError(
                "The storage backend can only be started with a 'local' "
                "configuration")

        args = vcfg['args']
        for key in ('db', 'objstorage'):
            if not args.get(key):
                raise ValueError(
                    "Invalid configuration; missing '%s' config entry" % key)

    return cfg


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
       a configuration file.

       SWH_CONFIG_FILENAME environment variable defines the
       configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_file = os.environ.get('SWH_CONFIG_FILENAME')
        api_cfg = load_and_check_config(config_file)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app


if __name__ == '__main__':
    print('Deprecated. Use swh-storage')
