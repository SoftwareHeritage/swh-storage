# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
import os
import warnings

import click

from swh.core import config
from swh.core.cli import CONTEXT_SETTINGS
from swh.journal.client import get_journal_client
from swh.storage import get_storage
from swh.storage.api.server import load_and_check_config, app

try:
    from systemd.daemon import notify
except ImportError:
    notify = None


@click.group(name="storage", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.pass_context
def storage(ctx, config_file):
    """Software Heritage Storage tools."""
    if not config_file:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")

    if config_file:
        if not os.path.exists(config_file):
            raise ValueError("%s does not exist" % config_file)
        conf = config.read(config_file)
    else:
        conf = {}

    ctx.ensure_object(dict)

    ctx.obj["config"] = conf


@storage.command(name="rpc-serve")
@click.argument("config-path", default=None, required=False)
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
    if config_path:
        # for bw compat
        warnings.warn(
            "The `config_path` argument of the `swh storage rpc-server` is now "
            "deprecated. Please use the --config option of `swh storage` instead.",
            DeprecationWarning,
        )
        api_cfg = load_and_check_config(config_path, type="any")
        app.config.update(api_cfg)
    else:
        app.config.update(ctx.obj["config"])

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


@storage.command()
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many objects. Default is to " "run forever.",
)
@click.pass_context
def replay(ctx, stop_after_objects):
    """Fill a Storage by reading a Journal.

    There can be several 'replayers' filling a Storage as long as they use
    the same `group-id`.
    """
    from swh.storage.replay import process_replay_objects

    conf = ctx.obj["config"]
    try:
        storage = get_storage(**conf.pop("storage"))
    except KeyError:
        ctx.fail("You must have a storage configured in your config file.")
    client_cfg = conf.pop("journal_client")
    if stop_after_objects:
        client_cfg["stop_after_objects"] = stop_after_objects
    try:
        client = get_journal_client(**client_cfg)
    except ValueError as exc:
        ctx.fail(exc)

    worker_fn = functools.partial(process_replay_objects, storage=storage)

    if notify:
        notify("READY=1")

    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix="SWH_STORAGE")


if __name__ == "__main__":
    main()
