# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

from flask import Flask, Request, Response, abort, g, request

from swh.core import config
from swh.core.json import SWHJSONDecoder, SWHJSONEncoder
from swh.core.hashutil import hash_to_hex
from swh.storage import Storage

DEFAULT_CONFIG = {
    'db': ('str', 'dbname=softwareheritage-dev'),
    'storage_base': ('str', '/tmp/swh-storage/test'),
}


class BytesRequest(Request):
    """Request with proper escaping of arbitrary byte sequences."""
    encoding = 'utf-8'
    encoding_errors = 'surrogateescape'


app = Flask(__name__)
app.json_encoder = SWHJSONEncoder
app.json_decoder = SWHJSONDecoder
app.request_class = BytesRequest


def jsonify(data):
    return Response(
        json.dumps(data, cls=SWHJSONEncoder),
        mimetype='application/json',
    )


@app.before_request
def before_request():
    g.storage = Storage(app.config['db'], app.config['storage_base'])


@app.route('/')
def index():
    return 'Hello'


@app.route('/content/missing', methods=['POST'])
def content_missing():
    return jsonify(g.storage.content_missing(**request.json))


@app.route('/content/present', methods=['POST'])
def content_present():
    return jsonify(g.storage.content_present(**request.json))


@app.route('/content/add', methods=['POST'])
def content_add():
    json_data = request.files['metadata'].read().decode('utf-8')
    metadata = json.loads(json_data, cls=SWHJSONDecoder)['content']
    for file_data in metadata:
        file_id = hash_to_hex(file_data['sha1'])
        file = request.files[file_id]
        file_data['data'] = file.read()
        file.close()
    return jsonify(g.storage.content_add(content=metadata))


@app.route('/directory/missing', methods=['POST'])
def directory_missing():
    return jsonify(g.storage.directory_missing(**request.json))


@app.route('/directory/add', methods=['POST'])
def directory_add():
    return jsonify(g.storage.directory_add(**request.json))


@app.route('/directory', methods=['GET'])
def directory_get():
    dir = request.args['directory'].encode('utf-8', 'surrogateescape')
    return jsonify(g.storage.directory_get(dir))


@app.route('/revision/add', methods=['POST'])
def revision_add():
    return jsonify(g.storage.revision_add(**request.json))


@app.route('/revision/missing', methods=['POST'])
def revision_missing():
    return jsonify(g.storage.revision_missing(**request.json))


@app.route('/release/add', methods=['POST'])
def release_add():
    return jsonify(g.storage.release_add(**request.json))


@app.route('/release/missing', methods=['POST'])
def release_missing():
    return jsonify(g.storage.release_missing(**request.json))


@app.route('/occurrence/add', methods=['POST'])
def occurrence_add():
    return jsonify(g.storage.occurrence_add(**request.json))


@app.route('/origin', methods=['GET'])
def origin_get():
    origin = {
        'type': request.args['type'],
        'url': request.args['url'],
    }

    id = g.storage.origin_get(origin)

    if not id:
        abort(404)
    else:
        origin['id'] = id
        return jsonify(origin)


@app.route('/origin', methods=['POST'])
def origin_add_one():
    return jsonify(g.storage.origin_add_one(**request.json))


def run_from_webserver(environ, start_response):
    """Run the WSGI app from the webserver, loading the configuration."""

    config_path = '/etc/softwareheritage/storage/storage.ini'

    app.config.update(config.read(config_path, DEFAULT_CONFIG))

    handler = logging.StreamHandler()
    app.logger.addHandler(handler)

    return app(environ, start_response)


if __name__ == '__main__':
    import sys

    app.config.update(config.read(sys.argv[1], DEFAULT_CONFIG))
    app.run(debug=True)
