# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
import os
from typing import IO, Callable, Dict, Optional, Tuple, Union, cast

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
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
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


@storage.group(name="cassandra", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def cassandra(ctx) -> None:
    from swh.storage.cassandra.cql import CqlRunner

    config = ctx.obj["config"]["storage"]

    # Tentatively extract the nested cls 'cassandra' configuration
    if config["cls"] == "pipeline":
        config = config["steps"][-1]

    # Check immediately whether the configuration is a 'cassandra' one (this is the
    # configuration which has the necessary configuration keys)
    if config["cls"] != "cassandra":
        ctx.fail(f"cls must be 'cassandra', not '{config['cls']}'")

    # Check whether the mandatory keys are present and raise if not.
    for key in ("cls", "hosts", "keyspace", "auth_provider"):
        if key not in config:
            ctx.fail(f"Missing {key} key in config file.")

    ctx.obj["cql_runner"] = CqlRunner(
        hosts=config["hosts"],
        port=config.get("port", 9042),
        keyspace=config["keyspace"],
        auth_provider=config["auth_provider"],
        consistency_level="ONE",
        table_options=config.get("table_options", {}),
        register_user_types=False,  # requires the keyspace to exist first
    )


@cassandra.command(name="init")
@click.pass_context
def cassandra_init(ctx) -> None:
    """Creates a Cassandra keyspace with table definitions suitable for use
    by swh-storage's Cassandra backend"""
    from swh.storage.cassandra.cql import create_keyspace, mark_all_migrations_completed

    create_keyspace(ctx.obj["cql_runner"])
    mark_all_migrations_completed(ctx.obj["cql_runner"])

    click.echo("Done.")


@cassandra.command(name="list-migrations")
@click.pass_context
def cassandra_list_migrations(ctx) -> None:
    """Creates a Cassandra keyspace with table definitions suitable for use
    by swh-storage's Cassandra backend"""
    import textwrap

    from swh.storage.cassandra.migrations import list_migrations

    for migration, status in list_migrations(ctx.obj["cql_runner"]):
        click.echo(
            f"{migration.id}{' (required)' if migration.required else ''}: {status.value}"
        )
        if migration.help:
            click.echo(textwrap.indent(migration.help, prefix="    "))
        click.echo("")


@cassandra.command(name="upgrade")
@click.option("--migration", "migration_ids", multiple=True)
@click.pass_context
def cassandra_upgrade(ctx, migration_ids: tuple[str, ...]) -> None:
    """Applies all pending migrations that can run automatically

    Exit codes:

    * 0: migrations applied
    * 1: unexpected crash
    * 2: (unassigned)
    * 3: no migrations to run
    * 4: some required migrations need to be manually applied
    * 5: some optional migrations need to be manually applied
    * 6: some required (and optional) migrations could not be applied because a dependency
      is missing (only if --migration was passed)
    * 7: some optional migrations could not be applied because a dependency is missing
      (only if --migration was passed)
    """
    from swh.storage.cassandra.migrations import (
        MIGRATIONS,
        apply_migrations,
        create_migrations_table_if_needed,
    )

    cql_runner = ctx.obj["cql_runner"]

    create_migrations_table_if_needed(cql_runner)

    (
        applied_any,
        remaining_manual_migrations,
        remaining_migrations_missing_dependencies,
    ) = apply_migrations(
        cql_runner,
        (migration_ids or {migration.id for migration in MIGRATIONS}),
    )

    if remaining_manual_migrations:
        remaining_required_manual_migrations = [
            m for m in remaining_manual_migrations if m.required
        ]
        click.echo(
            "Some migrations need to be manually applied: "
            + ", ".join(migration.id for migration in remaining_manual_migrations)
        )
        if remaining_required_manual_migrations:
            click.echo(
                "Including these required migrations: "
                + ", ".join(
                    migration.id for migration in remaining_required_manual_migrations
                )
            )
            ctx.exit(4)
        else:
            ctx.exit(5)
    elif remaining_migrations_missing_dependencies:
        click.echo(
            "Some migrations could not be applied because a dependency is missing: "
            + ", ".join(
                migration.id for migration in remaining_migrations_missing_dependencies
            )
        )
        remaining_required_migrations_missing_dependencies = [
            m for m in remaining_migrations_missing_dependencies if m.required
        ]
        if remaining_required_migrations_missing_dependencies:
            click.echo(
                "Including these required migrations: "
                + ", ".join(
                    migration.id
                    for migration in remaining_required_migrations_missing_dependencies
                )
            )
            ctx.exit(6)
        else:
            ctx.exit(7)
    elif applied_any:
        click.echo("Done.")
        ctx.exit(0)
    else:
        click.echo("No migration to run")
        ctx.exit(3)


@cassandra.command(name="mark-upgraded")
@click.option("--migration", "migration_ids", multiple=True)
@click.pass_context
def cassandra_mark_upgraded(ctx, migration_ids: tuple[str, ...]) -> None:
    """Marks a migration as run

    Exit codes:

    * 0: ok
    * 1: unexpected crash
    * 2: (unassigned)
    * 3: nothing to do"""
    from swh.storage.cassandra.migrations import MIGRATIONS, MigrationStatus
    from swh.storage.cassandra.model import MigrationRow

    logger = logging.getLogger(__name__)

    cql_runner = ctx.obj["cql_runner"]

    if migration_ids:
        migrations = {
            migration.id: migration
            for migration in MIGRATIONS
            if migration.id in migration_ids
        }
    else:
        migrations = {migration.id: migration for migration in MIGRATIONS}

    unknown_migrations = set(migrations) - {migration.id for migration in MIGRATIONS}
    if unknown_migrations:
        raise click.ClickException(
            f"Unknown migrations: {', '.join(unknown_migrations)}"
        )

    rows = cql_runner.migration_get(list(migrations))
    for row in rows:
        if row.status == MigrationStatus.COMPLETED:
            logger.warning("Migration %s was already completed", row.id)

    new_rows = []
    for migration in migrations.values():
        new_rows.append(
            MigrationRow(
                id=migration.id,
                dependencies=migration.dependencies,
                min_read_version=migration.min_read_version,
                status=MigrationStatus.COMPLETED.value,
            )
        )

    if new_rows:
        cql_runner.migration_add_concurrent(new_rows)
        click.echo(
            f"Migrations {', '.join(row.id for row in new_rows)} marked as complete."
        )
    else:
        click.echo("Nothing to do.")
        ctx.exit(3)


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


@storage.command(name="backfill")
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


@storage.command(name="replay")
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many objects. Default is to " "run forever.",
)
@click.option(
    "--type",
    "-t",
    "object_types",
    default=[],
    type=click.Choice(
        # use a hardcoded list to prevent having to load the
        # replay module at cli loading time
        [
            "origin",
            "origin_visit",
            "origin_visit_status",
            "snapshot",
            "revision",
            "release",
            "directory",
            "content",
            "skipped_content",
            "metadata_authority",
            "metadata_fetcher",
            "raw_extrinsic_metadata",
            "extid",
        ]
    ),
    help="Object types to replay",
    multiple=True,
)
@click.option(
    "--known-mismatched-hashes",
    "-X",
    "invalid_hashes_file",
    default=None,
    type=click.File("r"),
    help=(
        "File of SWHIDs of objects that are known to have invalid hashes "
        "but still need to be replayed."
    ),
)
@click.pass_context
def replay(
    ctx: click.Context,
    stop_after_objects: int,
    object_types: str,
    invalid_hashes_file: IO,
):
    """Fill a Storage by reading a Journal.

    This is typically used for a mirror configuration, reading the Software
    Heritage kafka journal to retrieve objects of the Software Heritage main
    storage to feed a replication storage. There can be several 'replayers'
    filling a Storage as long as they use the same `group-id`.

    The expected configuration file should have 2 sections:

    - storage: the configuration of the storage in which to add objects
      received from the kafka journal,

    - journal_client: the configuration of access to the kafka journal. See the
      documentation of `swh.journal` for more details on the possible
      configuration entries in this section.

      https://docs.softwareheritage.org/devel/apidoc/swh.journal.client.html

    In addition to these 2 mandatory config sections, a third 'replayer' may be
    specified with a 'error_reporter' config entry allowing to specify redis
    connection parameters that will be used to report non-recoverable mirroring,
    eg.::

      storage:
        [...]
      journal_client:
        [...]
      replayer:
        error_reporter:
          host: redis.local
          port: 6379
          db: 1

    """
    import functools

    from swh.journal.client import get_journal_client
    from swh.model.swhids import CoreSWHID
    from swh.storage import get_storage
    from swh.storage.replay import ModelObjectDeserializer, process_replay_objects

    ensure_check_config(ctx.obj["config"], ctx.obj["check_config"], "write")

    conf = ctx.obj["config"]
    storage = get_storage(**conf.pop("storage"))

    client_cfg = conf.pop("journal_client")
    replayer_cfg = conf.pop("replayer", {})

    if "error_reporter" in replayer_cfg:
        from redis import Redis

        reporter = Redis(**replayer_cfg.get("error_reporter")).set
    else:
        reporter = None
    validate = client_cfg.get("privileged", False)

    if not validate and reporter:
        ctx.fail(
            "Invalid configuration: you cannot have 'error_reporter' set if "
            "'privileged' is False; we cannot validate anonymized objects."
        )

    KMH = Optional[Tuple[Tuple[str, bytes, bytes]]]

    known_mismatched_hashes: KMH = None
    if validate and invalid_hashes_file:
        inv_obj_ids = (
            row.strip().split(",") for row in invalid_hashes_file if row.strip()
        )
        swhids = (
            (CoreSWHID.from_string(swhid.strip()), bytes.fromhex(computed_id.strip()))
            for swhid, computed_id in inv_obj_ids
        )

        known_mismatched_hashes = cast(
            KMH,
            tuple(
                (swhid.object_type.name.lower(), swhid.object_id, computed_id)
                for swhid, computed_id in swhids
            ),
        )

    deserializer = ModelObjectDeserializer(
        reporter=cast(Union[Callable[[str, bytes], None], None], reporter),
        validate=validate,
        known_mismatched_hashes=known_mismatched_hashes,
    )

    client_cfg["value_deserializer"] = deserializer.convert
    if object_types:
        client_cfg["object_types"] = object_types
    if stop_after_objects:
        client_cfg["stop_after_objects"] = stop_after_objects

    try:
        client = get_journal_client(**client_cfg)
    except ValueError as exc:
        ctx.fail(str(exc))

    worker_fn = functools.partial(process_replay_objects, storage=storage)

    if notify:
        notify("READY=1")

    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        click.echo("Done.")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()


@storage.command(name="create-object-reference-partitions")
@click.argument("start")
@click.argument("end")
@click.pass_context
def create_object_reference_partitions(ctx: click.Context, start: str, end: str):
    """Create object_reference partitions from START_DATE to END_DATE"""

    import datetime

    from swh.storage import get_storage
    from swh.storage.interface import PartitionsManagementInterface

    storage = get_storage(**ctx.obj["config"]["storage"])

    # This function uses the storage backend directly, so we
    # need unwrap all the layers of proxies (e.g. record_references, buffer,
    # filter, etc.) to get access to the "concrete" underlying storage.
    while hasattr(storage, "storage"):
        storage = storage.storage

    if not isinstance(storage, PartitionsManagementInterface):
        ctx.fail(
            "Storage instance needs to be a direct PostgreSQL or Cassandra storage"
        )

    start_date = datetime.date.fromisoformat(start)
    end_date = datetime.date.fromisoformat(end)

    if start_date > end_date:
        ctx.fail("Start date must be before end date.")

    cur_date = start_date
    end_year, end_week = end_date.isocalendar()[0:2]
    while True:
        year, week = cur_date.isocalendar()[0:2]
        if (year, week) > (end_year, end_week):
            break

        monday, sunday = storage.object_references_create_partition(year, week)

        click.echo(
            "Created object references table for dates between %s and %s"
            % (monday, sunday)
        )

        cur_date += datetime.timedelta(days=7)


@storage.command(name="remove-old-object-reference-partitions")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="do not ask for confirmation before removing tables",
)
@click.argument("before")
@click.pass_context
def remove_old_object_reference_partitions(
    ctx: click.Context, force: bool, before: str
):
    """Remove object_reference partitions for values older than BEFORE"""
    import datetime

    from swh.storage import get_storage
    from swh.storage.interface import PartitionsManagementInterface

    storage = get_storage(**ctx.obj["config"]["storage"])

    # This function uses the storage backend directly, so we
    # need unwrap all the layers of proxies (e.g. record_references, buffer,
    # filter, etc.) to get access to the "concrete" underlying storage.
    while hasattr(storage, "storage"):
        storage = storage.storage

    if not isinstance(storage, PartitionsManagementInterface):
        ctx.fail(
            "Storage instance needs to be a direct PostgreSQL or Cassandra storage"
        )

    before_date = datetime.date.fromisoformat(before)

    existing_partitions = storage.object_references_list_partitions()
    to_remove = []
    for partition in existing_partitions:
        # If it ends after the specified date, we should keep this partition
        # and the ones after.
        if partition.end > before_date:
            break
        to_remove.append(partition)

    if len(to_remove) == 0:
        click.echo("Nothing needs to be removed.")
        ctx.exit(0)

    if len(existing_partitions) == len(to_remove):
        ctx.fail(
            "Trying to remove all existing partitions. This can’t be right, sorry."
        )

    if not force:
        click.echo(
            "We will remove the following partitions before "
            f"{before_date.strftime('%Y-%m-%d')}:"
        )
        for partition in to_remove:
            click.echo(f"- {partition.year:04d} week {partition.week:02d}")
        click.confirm("Do you want to proceed?", default=False, abort=True)

    for partition in to_remove:
        storage.object_references_drop_partition(partition)


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
