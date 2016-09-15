# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
from flask import Flask, abort, g
from swh.core import config
from swh.objstorage.api.common import encode_data_server as encode_data
from swh.objstorage.api.common import BytesRequest, error_handler
from swh.storage import get_storage
from swh.storage.vault.api import cooking_tasks  # NOQA
from swh.storage.vault.cache import VaultCache
from swh.storage.vault.cooker import DirectoryVaultCooker
from swh.scheduler.celery_backend.config import app as celery_app
from flask_profile import Profiler


cooking_task_name = 'swh.storage.vault.api.cooking_tasks.SWHCookingTask'


DEFAULT_CONFIG = {
    'storage': ('dict', {'storage_class': 'local_storage',
                         'storage_args': [
                             'dbname=softwareheritage-dev',
                             '/tmp/objects'
                         ]
                         }),
    'cache': ('dict', {'root': '/tmp/vaultcache'})
}


app = Flask(__name__)
Profiler(app)
app.request_class = BytesRequest


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


@app.route('/vault/directory/', methods=['GET'])
def ls_directory():
    return encode_data(list(
        g.cache.ls('directory')
    ))


@app.route('/vault/directory/<dir_id>/', methods=['GET'])
def get_cooked_directory(dir_id):
    if not g.cache.is_cached('directory', dir_id):
        abort(404)
    return encode_data(g.cache.get('directory', dir_id).decode())


@app.route('/vault/directory/<dir_id>/', methods=['POST'])
def cook_request_directory(dir_id):
    task = celery_app.tasks[cooking_task_name]
    task.delay(dir_id, app.config['storage'], app.config['cache'])
    # Return url to get the content and 201 CREATED
    return encode_data('/vault/directory/dir_id/'), 201


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
