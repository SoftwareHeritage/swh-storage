# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click

from flask import Flask, g, request

from swh.core import config
from swh.storage.objstorage import ObjStorage
from swh.storage.api.common import (BytesRequest, decode_request,
                                    error_handler,
                                    encode_data_server as encode_data)

DEFAULT_CONFIG = {
    'storage_base': ('str', '/tmp/swh-storage/objects/'),
    'storage_depth': ('int', 3)
}

app = Flask(__name__)
app.request_class = BytesRequest


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.objstorage = ObjStorage(app.config['storage_base'],
                              app.config['storage_depth'])


@app.route('/')
def index():
    return "Helloworld!"


@app.route('/content')
def content():
    return str(list(g.storage))


@app.route('/content/add', methods=['POST'])
def add_bytes():
    return encode_data(g.objstorage.add_bytes(**decode_request(request)))


@app.route('/content/get', methods=['POST'])
def get_bytes():
    return encode_data(g.objstorage.get_bytes(**decode_request(request)))


@app.route('/content/check', methods=['POST'])
def check():
    return encode_data(g.objstorage.check(**decode_request(request)))


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
