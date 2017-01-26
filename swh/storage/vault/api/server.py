# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
from flask import abort, g
from werkzeug.routing import BaseConverter
from swh.core import config
from swh.core.api import (SWHServerAPIApp, error_handler,
                          encode_data_server as encode_data)
from swh.storage import get_storage
from swh.storage.vault.api import cooking_tasks  # NOQA
from swh.storage.vault.cache import VaultCache
from swh.storage.vault.cooker import DirectoryVaultCooker
from swh.scheduler.celery_backend.config import app as celery_app


cooking_task_name = 'swh.storage.vault.api.cooking_tasks.SWHCookingTask'


DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-dev',
            'objstorage': {
                'root': '/tmp/objects',
                'slicing': '0:2/2:4/4:6',
            },
        },
    }),
    'cache': ('dict', {'root': '/tmp/vaultcache'})
}


class RegexConverter(BaseConverter):
    def __init__(self, url_map, *items):
        super().__init__(url_map)
        self.regex = items[0]


app = SWHServerAPIApp(__name__)
app.url_map.converters['regex'] = RegexConverter


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.cache = VaultCache(**app.config['cache'])
    g.cooker = DirectoryVaultCooker(
        get_storage(**app.config['storage']),
        g.cache
    )


@app.route('/')
def index():
    return 'SWH vault API server'


@app.route('/vault/<regex("directory|revision|snapshot"):type>/',
           methods=['GET'])
def ls_directory(type):
    return encode_data(list(
        g.cache.ls(type)
    ))


@app.route('/vault/<regex("directory|revision|snapshot"):type>/<id>/',
           methods=['GET'])
def get_cooked_directory(type, id):
    if not g.cache.is_cached(type, id):
        abort(404)
    return encode_data(g.cache.get(type, id).decode())


@app.route('/vault/<regex("directory|revision|snapshot"):type>/<id>/',
           methods=['POST'])
def cook_request_directory(type, id):
    task = celery_app.tasks[cooking_task_name]
    task.delay(type, id, app.config['storage'], app.config['cache'])
    # Return url to get the content and 201 CREATED
    return encode_data('/vault/%s/%s/' % (type, id)), 201


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
