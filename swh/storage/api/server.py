# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import click

from flask import request

from swh.core import config
from swh.storage import get_storage as get_swhstorage
from swh.core.api import (SWHServerAPIApp, decode_request,
                          error_handler,
                          encode_data_server as encode_data)

DEFAULT_CONFIG_PATH = 'storage/storage'
DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-dev',
            'objstorage': {
                'cls': 'pathslicing',
                'args': {
                    'root': '/srv/softwareheritage/objects',
                    'slicing': '0:2/2:4/4:6',
                },
            },
        },
    })
}


app = SWHServerAPIApp(__name__)
storage = None


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


def get_storage():
    global storage
    if not storage:
        storage = get_swhstorage(**app.config['storage'])

    return storage


@app.route('/')
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
def check_config():
    return encode_data(get_storage().check_config(**decode_request(request)))


@app.route('/content/missing', methods=['POST'])
def content_missing():
    return encode_data(get_storage().content_missing(
        **decode_request(request)))


@app.route('/content/missing/sha1', methods=['POST'])
def content_missing_per_sha1():
    return encode_data(get_storage().content_missing_per_sha1(
        **decode_request(request)))


@app.route('/content/present', methods=['POST'])
def content_find():
    return encode_data(get_storage().content_find(**decode_request(request)))


@app.route('/content/add', methods=['POST'])
def content_add():
    return encode_data(get_storage().content_add(**decode_request(request)))


@app.route('/content/update', methods=['POST'])
def content_update():
    return encode_data(get_storage().content_update(**decode_request(request)))


@app.route('/content/data', methods=['POST'])
def content_get():
    return encode_data(get_storage().content_get(**decode_request(request)))


@app.route('/content/metadata', methods=['POST'])
def content_get_metadata():
    return encode_data(get_storage().content_get_metadata(
        **decode_request(request)))


@app.route('/content/range', methods=['POST'])
def content_get_range():
    return encode_data(get_storage().content_get_range(
        **decode_request(request)))


@app.route('/directory/missing', methods=['POST'])
def directory_missing():
    return encode_data(get_storage().directory_missing(
        **decode_request(request)))


@app.route('/directory/add', methods=['POST'])
def directory_add():
    return encode_data(get_storage().directory_add(**decode_request(request)))


@app.route('/directory/path', methods=['POST'])
def directory_entry_get_by_path():
    return encode_data(get_storage().directory_entry_get_by_path(
        **decode_request(request)))


@app.route('/directory/ls', methods=['GET'])
def directory_ls():
    dir = request.args['directory'].encode('utf-8', 'surrogateescape')
    rec = json.loads(request.args.get('recursive', 'False').lower())
    return encode_data(get_storage().directory_ls(dir, recursive=rec))


@app.route('/revision/add', methods=['POST'])
def revision_add():
    return encode_data(get_storage().revision_add(**decode_request(request)))


@app.route('/revision', methods=['POST'])
def revision_get():
    return encode_data(get_storage().revision_get(**decode_request(request)))


@app.route('/revision/log', methods=['POST'])
def revision_log():
    return encode_data(get_storage().revision_log(**decode_request(request)))


@app.route('/revision/shortlog', methods=['POST'])
def revision_shortlog():
    return encode_data(get_storage().revision_shortlog(
        **decode_request(request)))


@app.route('/revision/missing', methods=['POST'])
def revision_missing():
    return encode_data(get_storage().revision_missing(
        **decode_request(request)))


@app.route('/release/add', methods=['POST'])
def release_add():
    return encode_data(get_storage().release_add(**decode_request(request)))


@app.route('/release', methods=['POST'])
def release_get():
    return encode_data(get_storage().release_get(**decode_request(request)))


@app.route('/release/missing', methods=['POST'])
def release_missing():
    return encode_data(get_storage().release_missing(
        **decode_request(request)))


@app.route('/object/find_by_sha1_git', methods=['POST'])
def object_find_by_sha1_git():
    return encode_data(get_storage().object_find_by_sha1_git(
        **decode_request(request)))


@app.route('/snapshot/add', methods=['POST'])
def snapshot_add():
    return encode_data(get_storage().snapshot_add(**decode_request(request)))


@app.route('/snapshot', methods=['POST'])
def snapshot_get():
    return encode_data(get_storage().snapshot_get(**decode_request(request)))


@app.route('/snapshot/by_origin_visit', methods=['POST'])
def snapshot_get_by_origin_visit():
    return encode_data(get_storage().snapshot_get_by_origin_visit(
        **decode_request(request)))


@app.route('/snapshot/latest', methods=['POST'])
def snapshot_get_latest():
    return encode_data(get_storage().snapshot_get_latest(
        **decode_request(request)))


@app.route('/snapshot/count_branches', methods=['POST'])
def snapshot_count_branches():
    return encode_data(get_storage().snapshot_count_branches(
        **decode_request(request)))


@app.route('/snapshot/get_branches', methods=['POST'])
def snapshot_get_branches():
    return encode_data(get_storage().snapshot_get_branches(
        **decode_request(request)))


@app.route('/origin/get', methods=['POST'])
def origin_get():
    return encode_data(get_storage().origin_get(**decode_request(request)))


@app.route('/origin/search', methods=['POST'])
def origin_search():
    return encode_data(get_storage().origin_search(**decode_request(request)))


@app.route('/origin/add_multi', methods=['POST'])
def origin_add():
    return encode_data(get_storage().origin_add(**decode_request(request)))


@app.route('/origin/add', methods=['POST'])
def origin_add_one():
    return encode_data(get_storage().origin_add_one(**decode_request(request)))


@app.route('/origin/visit/get', methods=['POST'])
def origin_visit_get():
    return encode_data(get_storage().origin_visit_get(
        **decode_request(request)))


@app.route('/origin/visit/getby', methods=['POST'])
def origin_visit_get_by():
    return encode_data(
        get_storage().origin_visit_get_by(**decode_request(request)))


@app.route('/origin/visit/add', methods=['POST'])
def origin_visit_add():
    return encode_data(get_storage().origin_visit_add(
        **decode_request(request)))


@app.route('/origin/visit/update', methods=['POST'])
def origin_visit_update():
    return encode_data(get_storage().origin_visit_update(
        **decode_request(request)))


@app.route('/person', methods=['POST'])
def person_get():
    return encode_data(get_storage().person_get(**decode_request(request)))


@app.route('/fetch_history', methods=['GET'])
def fetch_history_get():
    return encode_data(get_storage().fetch_history_get(request.args['id']))


@app.route('/fetch_history/start', methods=['POST'])
def fetch_history_start():
    return encode_data(
        get_storage().fetch_history_start(**decode_request(request)))


@app.route('/fetch_history/end', methods=['POST'])
def fetch_history_end():
    return encode_data(
        get_storage().fetch_history_end(**decode_request(request)))


@app.route('/entity/add', methods=['POST'])
def entity_add():
    return encode_data(
        get_storage().entity_add(**decode_request(request)))


@app.route('/entity/get', methods=['POST'])
def entity_get():
    return encode_data(
        get_storage().entity_get(**decode_request(request)))


@app.route('/entity', methods=['GET'])
def entity_get_one():
    return encode_data(get_storage().entity_get_one(request.args['uuid']))


@app.route('/entity/from_lister_metadata', methods=['POST'])
def entity_from_lister_metadata():
    return encode_data(get_storage().entity_get_from_lister_metadata(
        **decode_request(request)))


@app.route('/tool/data', methods=['POST'])
def tool_get():
    return encode_data(get_storage().tool_get(
        **decode_request(request)))


@app.route('/tool/add', methods=['POST'])
def tool_add():
    return encode_data(get_storage().tool_add(
        **decode_request(request)))


@app.route('/origin/metadata/add', methods=['POST'])
def origin_metadata_add():
    return encode_data(get_storage().origin_metadata_add(**decode_request(
                                                       request)))


@app.route('/origin/metadata/get', methods=['POST'])
def origin_metadata_get_by():
    return encode_data(get_storage().origin_metadata_get_by(**decode_request(
                                                       request)))


@app.route('/provider/add', methods=['POST'])
def metadata_provider_add():
    return encode_data(get_storage().metadata_provider_add(**decode_request(
                                                       request)))


@app.route('/provider/get', methods=['POST'])
def metadata_provider_get():
    return encode_data(get_storage().metadata_provider_get(**decode_request(
                                                       request)))


@app.route('/provider/getby', methods=['POST'])
def metadata_provider_get_by():
    return encode_data(get_storage().metadata_provider_get_by(**decode_request(
                                                       request)))


@app.route('/stat/counters', methods=['GET'])
def stat_counters():
    return encode_data(get_storage().stat_counters())


@app.route('/algos/diff_directories', methods=['POST'])
def diff_directories():
    return encode_data(get_storage().diff_directories(
        **decode_request(request)))


@app.route('/algos/diff_revisions', methods=['POST'])
def diff_revisions():
    return encode_data(get_storage().diff_revisions(**decode_request(request)))


@app.route('/algos/diff_revision', methods=['POST'])
def diff_revision():
    return encode_data(get_storage().diff_revision(**decode_request(request)))


def run_from_webserver(environ, start_response,
                       config_path=DEFAULT_CONFIG_PATH):
    """Run the WSGI app from the webserver, loading the configuration."""
    cfg = config.load_named_config(config_path, DEFAULT_CONFIG)
    app.config.update(cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app(environ, start_response)


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5002, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app.config.update(config.read(config_path, DEFAULT_CONFIG))
    app.run(host, port=int(port), debug=bool(debug))


if __name__ == '__main__':
    launch()
