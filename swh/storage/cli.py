# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.storage.api.server import load_and_check_config, app


@click.group(name='storage', context_settings=CONTEXT_SETTINGS)
@click.pass_context
def storage(ctx):
    '''Software Heritage Storage tools.'''
    pass


@storage.command(name='rpc-serve')
@click.argument('config-path', required=True)
@click.option('--host', default='0.0.0.0',
              metavar='IP', show_default=True,
              help="Host ip address to bind the server on")
@click.option('--port', default=5002, type=click.INT,
              metavar='PORT', show_default=True,
              help="Binding port of the server")
@click.option('--debug/--no-debug', default=True,
              help="Indicates if the server should run in debug mode")
@click.pass_context
def serve(ctx, config_path, host, port, debug):
    '''Software Heritage Storage RPC server.

    Do NOT use this in a production environment.
    '''
    if 'log_level' in ctx.obj:
        logging.getLogger('werkzeug').setLevel(ctx.obj['log_level'])
    api_cfg = load_and_check_config(config_path, type='any')
    app.config.update(api_cfg)
    app.run(host, port=int(port), debug=bool(debug))


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix='SWH_STORAGE')


if __name__ == '__main__':
    main()
