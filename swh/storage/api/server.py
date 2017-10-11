# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import click

from flask import g, request

from swh.core import config
from swh.storage import get_storage
from swh.core.api import (SWHServerAPIApp, decode_request,
                          error_handler,
                          encode_data_server as encode_data)

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


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.storage = get_storage(**app.config['storage'])


@app.route('/')
def index():
    return 'SWH Storage API server'


@app.route('/check_config', methods=['POST'])
def check_config():
    return encode_data(g.storage.check_config(**decode_request(request)))


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


@app.route('/content/update', methods=['POST'])
def content_update():
    return encode_data(g.storage.content_update(**decode_request(request)))


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


@app.route('/cache/contents', methods=['GET'])
def cache_content_get_all():
    return encode_data(g.storage.cache_content_get_all())


@app.route('/cache/content', methods=['POST'])
def cache_content_get():
    return encode_data(g.storage.cache_content_get(
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


@app.route('/origin/add_multi', methods=['POST'])
def origin_add():
    return encode_data(g.storage.origin_add(**decode_request(request)))


@app.route('/origin/add', methods=['POST'])
def origin_add_one():
    return encode_data(g.storage.origin_add_one(**decode_request(request)))


@app.route('/origin/visit/get', methods=['POST'])
def origin_visit_get():
    return encode_data(g.storage.origin_visit_get(**decode_request(request)))


@app.route('/origin/visit/getby', methods=['POST'])
def origin_visit_get_by():
    return encode_data(
        g.storage.origin_visit_get_by(**decode_request(request)))


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


@app.route('/content_mimetype/add', methods=['POST'])
def content_mimetype_add():
    return encode_data(
        g.storage.content_mimetype_add(**decode_request(request)))


@app.route('/content_mimetype/missing', methods=['POST'])
def content_mimetype_missing():
    return encode_data(
        g.storage.content_mimetype_missing(**decode_request(request)))


@app.route('/content_mimetype', methods=['POST'])
def content_mimetype_get():
    return encode_data(
        g.storage.content_mimetype_get(**decode_request(request)))


@app.route('/content_language/add', methods=['POST'])
def content_language_add():
    return encode_data(
        g.storage.content_language_add(**decode_request(request)))


@app.route('/content_language/missing', methods=['POST'])
def content_language_missing():
    return encode_data(
        g.storage.content_language_missing(**decode_request(request)))


@app.route('/content_language', methods=['POST'])
def content_language_get():
    return encode_data(
        g.storage.content_language_get(**decode_request(request)))


@app.route('/content/ctags/add', methods=['POST'])
def content_ctags_add():
    return encode_data(
        g.storage.content_ctags_add(**decode_request(request)))


@app.route('/content/ctags/search', methods=['POST'])
def content_ctags_search():
    return encode_data(
        g.storage.content_ctags_search(**decode_request(request)))


@app.route('/content/ctags/missing', methods=['POST'])
def content_ctags_missing():
    return encode_data(
        g.storage.content_ctags_missing(**decode_request(request)))


@app.route('/content/ctags', methods=['POST'])
def content_ctags_get():
    return encode_data(
        g.storage.content_ctags_get(**decode_request(request)))


@app.route('/content/fossology_license/add', methods=['POST'])
def content_fossology_license_add():
    return encode_data(
        g.storage.content_fossology_license_add(**decode_request(request)))


@app.route('/content/fossology_license', methods=['POST'])
def content_fossology_license_get():
    return encode_data(
        g.storage.content_fossology_license_get(**decode_request(request)))


@app.route('/indexer_configuration/data', methods=['POST'])
def indexer_configuration_get():
    return encode_data(g.storage.indexer_configuration_get(
        **decode_request(request)))


@app.route('/content_metadata/add', methods=['POST'])
def content_metadata_add():
    return encode_data(
        g.storage.content_metadata_add(**decode_request(request)))


@app.route('/content_metadata/missing', methods=['POST'])
def content_metadata_missing():
    return encode_data(
        g.storage.content_metadata_missing(**decode_request(request)))


@app.route('/content_metadata', methods=['POST'])
def content_metadata_get():
    return encode_data(
        g.storage.content_metadata_get(**decode_request(request)))


@app.route('/revision_metadata/add', methods=['POST'])
def revision_metadata_add():
    return encode_data(
        g.storage.revision_metadata_add(**decode_request(request)))


@app.route('/revision_metadata/missing', methods=['POST'])
def revision_metadata_missing():
    return encode_data(
        g.storage.revision_metadata_missing(**decode_request(request)))


@app.route('/revision_metadata', methods=['POST'])
def revision_metadata_get():
    return encode_data(
        g.storage.revision_metadata_get(**decode_request(request)))


@app.route('/stat/counters', methods=['GET'])
def stat_counters():
    return encode_data(g.storage.stat_counters())


def run_from_webserver(environ, start_response):
    """Run the WSGI app from the webserver, loading the configuration."""

    config_path = '/etc/softwareheritage/storage/storage.yml'

    app.config.update(config.read(config_path, DEFAULT_CONFIG))

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
