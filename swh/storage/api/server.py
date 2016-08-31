# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import click

from flask import Flask, g, request

from swh.core import config
from swh.storage import Storage
from swh.objstorage.api.common import (BytesRequest, decode_request,
                                       error_handler,
                                       encode_data_server as encode_data)

DEFAULT_CONFIG = {
    'db': ('str', 'dbname=softwareheritage-dev'),
    'storage_base': ('str', '/tmp/swh-storage/test'),
}


app = Flask(__name__)
app.request_class = BytesRequest


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.storage = Storage(app.config['db'], app.config['storage_base'])


@app.route('/')
def index():
    return 'SWH Storage API server'


@app.route('/content/missing', methods=['POST'])
def content_missing():
    return encode_data(g.storage.content_missing(**decode_request(request)))


@app.route('/content/missing/sha1', methods=['POST'])
def content_missing_per_sha1():
    return encode_data(g.storage.content_missing_per_sha1(
        **decode_request(request)))


@app.route('/content/present', methods=['POST'])
def content_find():
    return encode_data(g.storage.content_find(**decode_request(request)))


@app.route('/content/provenance', methods=['POST'])
def content_find_provenance():
    res = g.storage.content_find_provenance(**decode_request(request))
    return encode_data(res)


@app.route('/content/add', methods=['POST'])
def content_add():
    return encode_data(g.storage.content_add(**decode_request(request)))


@app.route('/content/data', methods=['POST'])
def content_get():
    return encode_data(g.storage.content_get(**decode_request(request)))


@app.route('/content/metadata', methods=['POST'])
def content_get_metadata():
    return encode_data(g.storage.content_get_metadata(
        **decode_request(request)))


@app.route('/directory', methods=['POST'])
def directory_get():
    return encode_data(g.storage.directory_get(**decode_request(request)))


@app.route('/directory/missing', methods=['POST'])
def directory_missing():
    return encode_data(g.storage.directory_missing(**decode_request(request)))


@app.route('/directory/add', methods=['POST'])
def directory_add():
    return encode_data(g.storage.directory_add(**decode_request(request)))


@app.route('/directory/path', methods=['POST'])
def directory_entry_get_by_path():
    return encode_data(g.storage.directory_entry_get_by_path(
        **decode_request(request)))


@app.route('/directory/ls', methods=['GET'])
def directory_ls():
    dir = request.args['directory'].encode('utf-8', 'surrogateescape')
    rec = json.loads(request.args.get('recursive', 'False').lower())
    return encode_data(g.storage.directory_ls(dir, recursive=rec))


@app.route('/revision/add', methods=['POST'])
def revision_add():
    return encode_data(g.storage.revision_add(**decode_request(request)))


@app.route('/revision', methods=['POST'])
def revision_get():
    return encode_data(g.storage.revision_get(**decode_request(request)))


@app.route('/revision/by', methods=['POST'])
def revision_get_by():
    return encode_data(g.storage.revision_get_by(**decode_request(request)))


@app.route('/revision/log', methods=['POST'])
def revision_log():
    return encode_data(g.storage.revision_log(**decode_request(request)))


@app.route('/revision/logby', methods=['POST'])
def revision_log_by():
    return encode_data(g.storage.revision_log_by(**decode_request(request)))


@app.route('/revision/shortlog', methods=['POST'])
def revision_shortlog():
    return encode_data(g.storage.revision_shortlog(**decode_request(request)))


@app.route('/revision/missing', methods=['POST'])
def revision_missing():
    return encode_data(g.storage.revision_missing(**decode_request(request)))


@app.route('/cache/content_revision', methods=['POST'])
def cache_content_revision_add():
    return encode_data(g.storage.cache_content_revision_add(
        **decode_request(request)))


@app.route('/cache/revision_origin', methods=['POST'])
def cache_revision_origin_add():
    return encode_data(g.storage.cache_revision_origin_add(
        **decode_request(request)))


@app.route('/release/add', methods=['POST'])
def release_add():
    return encode_data(g.storage.release_add(**decode_request(request)))


@app.route('/release', methods=['POST'])
def release_get():
    return encode_data(g.storage.release_get(**decode_request(request)))


@app.route('/release/by', methods=['POST'])
def release_get_by():
    return encode_data(g.storage.release_get_by(**decode_request(request)))


@app.route('/release/missing', methods=['POST'])
def release_missing():
    return encode_data(g.storage.release_missing(**decode_request(request)))


@app.route('/object/find_by_sha1_git', methods=['POST'])
def object_find_by_sha1_git():
    return encode_data(g.storage.object_find_by_sha1_git(
        **decode_request(request)))


@app.route('/occurrence', methods=['POST'])
def occurrence_get():
    return encode_data(g.storage.occurrence_get(**decode_request(request)))


@app.route('/occurrence/add', methods=['POST'])
def occurrence_add():
    return encode_data(g.storage.occurrence_add(**decode_request(request)))


@app.route('/origin/get', methods=['POST'])
def origin_get():
    return encode_data(g.storage.origin_get(**decode_request(request)))


@app.route('/origin/add', methods=['POST'])
def origin_add_one():
    return encode_data(g.storage.origin_add_one(**decode_request(request)))


@app.route('/origin/visit/get', methods=['POST'])
def origin_visit_get():
    return encode_data(g.storage.origin_visit_get(**decode_request(request)))


@app.route('/origin/visit/add', methods=['POST'])
def origin_visit_add():
    return encode_data(g.storage.origin_visit_add(**decode_request(request)))


@app.route('/origin/visit/update', methods=['POST'])
def origin_visit_update():
    return encode_data(g.storage.origin_visit_update(
        **decode_request(request)))


@app.route('/person', methods=['POST'])
def person_get():
    return encode_data(g.storage.person_get(**decode_request(request)))


@app.route('/fetch_history', methods=['GET'])
def fetch_history_get():
    return encode_data(g.storage.fetch_history_get(request.args['id']))


@app.route('/fetch_history/start', methods=['POST'])
def fetch_history_start():
    return encode_data(
        g.storage.fetch_history_start(**decode_request(request)))


@app.route('/fetch_history/end', methods=['POST'])
def fetch_history_end():
    return encode_data(
        g.storage.fetch_history_end(**decode_request(request)))


@app.route('/entity/add', methods=['POST'])
def entity_add():
    return encode_data(
        g.storage.entity_add(**decode_request(request)))


@app.route('/entity/get', methods=['POST'])
def entity_get():
    return encode_data(
        g.storage.entity_get(**decode_request(request)))


@app.route('/entity', methods=['GET'])
def entity_get_one():
    return encode_data(g.storage.entity_get_one(request.args['uuid']))


@app.route('/entity/from_lister_metadata', methods=['POST'])
def entity_from_lister_metadata():
    return encode_data(
        g.storage.entity_get_from_lister_metadata(**decode_request(request)))


@app.route('/stat/counters', methods=['GET'])
def stat_counters():
    return encode_data(g.storage.stat_counters())


def run_from_webserver(environ, start_response):
    """Run the WSGI app from the webserver, loading the configuration."""

    config_path = '/etc/softwareheritage/storage/storage.ini'

    app.config.update(config.read(config_path, DEFAULT_CONFIG))

    handler = logging.StreamHandler()
    app.logger.addHandler(handler)

    return app(environ, start_response)


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5000, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app.config.update(config.read(config_path, DEFAULT_CONFIG))
    app.run(host, port=int(port), debug=bool(debug))


if __name__ == '__main__':
    launch()
