# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.storage.api.server import load_and_check_config, app


@click.group(name="storage", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def storage(ctx):
    """Software Heritage Storage tools."""
    pass


@storage.command(name="rpc-serve")
@click.argument("config-path", required=True)
@click.option(
    "--host",
    default="0.0.0.0",
    metavar="IP",
    show_default=True,
    help="Host ip address to bind the server on",
)
@click.option(
    "--port",
    default=5002,
    type=click.INT,
    metavar="PORT",
    show_default=True,
    help="Binding port of the server",
)
@click.option(
    "--debug/--no-debug",
    default=True,
    help="Indicates if the server should run in debug mode",
)
@click.pass_context
def serve(ctx, config_path, host, port, debug):
    """Software Heritage Storage RPC server.

    Do NOT use this in a production environment.
    """
    if "log_level" in ctx.obj:
        logging.getLogger("werkzeug").setLevel(ctx.obj["log_level"])
    api_cfg = load_and_check_config(config_path, type="any")
    app.config.update(api_cfg)
    app.run(host, port=int(port), debug=bool(debug))


@storage.command()
@click.argument("object_type")
@click.option("--start-object", default=None)
@click.option("--end-object", default=None)
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_context
def backfill(ctx, object_type, start_object, end_object, dry_run):
    """Run the backfiller

    The backfiller list objects from a Storage and produce journal entries from
    there.

    Typically used to rebuild a journal or compensate for missing objects in a
    journal (eg. due to a downtime of this later).

    The configuration file requires the following entries:
    - brokers: a list of kafka endpoints (the journal) in which entries will be
               added.
    - storage_dbconn: URL to connect to the storage DB.
    - prefix: the prefix of the topics (topics will be <prefix>.<object_type>).
    - client_id: the kafka client ID.

    """
    # for "lazy" loading
    from swh.storage.backfill import JournalBackfiller

    try:
        from systemd.daemon import notify
    except ImportError:
        notify = None

    conf = ctx.obj["config"]
    backfiller = JournalBackfiller(conf)

    if notify:
        notify("READY=1")

    try:
        backfiller.run(
            object_type=object_type,
            start_object=start_object,
            end_object=end_object,
            dry_run=dry_run,
        )
    except KeyboardInterrupt:
        if notify:
            notify("STOPPING=1")
        ctx.exit(0)


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix="SWH_STORAGE")


if __name__ == "__main__":
    main()
