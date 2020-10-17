# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
import os
from typing import Dict, Optional

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group

try:
    from systemd.daemon import notify
except ImportError:
    notify = None


@swh_cli_group.group(name="storage", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.option(
    "--check-config",
    default=None,
    type=click.Choice(["no", "read", "write"]),
    help=(
        "Check the configuration of the storage at startup for read or write access; "
        "if set, override the value present in the configuration file if any. "
        "Defaults to 'read' for the 'backfill' command, and 'write' for 'rpc-server' "
        "and 'replay' commands."
    ),
)
@click.pass_context
def storage(ctx, config_file, check_config):
    """Software Heritage Storage tools."""
    from swh.core import config

    if not config_file:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")

    if config_file:
        if not os.path.exists(config_file):
            raise ValueError("%s does not exist" % config_file)
        conf = config.read(config_file)
    else:
        conf = {}

    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")

    ctx.ensure_object(dict)
    ctx.obj["config"] = conf
    ctx.obj["check_config"] = check_config


@storage.command(name="rpc-serve")
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
def serve(ctx, host, port, debug):
    """Software Heritage Storage RPC server.

    Do NOT use this in a production environment.
    """
    from swh.storage.api.server import app

    if "log_level" in ctx.obj:
        logging.getLogger("werkzeug").setLevel(ctx.obj["log_level"])
    ensure_check_config(ctx.obj["config"], ctx.obj["check_config"], "write")
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
    ensure_check_config(ctx.obj["config"], ctx.obj["check_config"], "read")

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
    import functools

    from swh.journal.client import get_journal_client
    from swh.storage import get_storage
    from swh.storage.replay import process_replay_objects

    ensure_check_config(ctx.obj["config"], ctx.obj["check_config"], "write")

    conf = ctx.obj["config"]
    storage = get_storage(**conf.pop("storage"))

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


def ensure_check_config(storage_cfg: Dict, check_config: Optional[str], default: str):
    """Helper function to inject the setting of check_config option in the storage config
    dict according to the expected default value (default value depends on the command,
    eg. backfill can be read-only).

    """
    if check_config is not None:
        if check_config == "no":
            storage_cfg.pop("check_config", None)
        else:
            storage_cfg["check_config"] = {"check_write": check_config == "write"}
    else:
        if "check_config" not in storage_cfg:
            storage_cfg["check_config"] = {"check_write": default == "write"}


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix="SWH_STORAGE")


if __name__ == "__main__":
    main()
