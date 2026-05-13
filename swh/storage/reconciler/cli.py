# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""``swh storage reconciler run`` — CLI subcommand for the content reconciler.

Wires :class:`swh.storage.reconciler.ContentReconciler` to a
:class:`~swh.journal.client.JournalClient` and runs the consumer loop
against a configured Cassandra storage and Kafka cluster.
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
from typing import Optional

import click

from swh.core.cli import CONTEXT_SETTINGS

# Import the parent group so the entry-point machinery (see
# pyproject.toml's [project.entry-points."swh.cli.subcommands"] section)
# finds the subcommand attached to "swh storage" once this module is
# imported.
from swh.storage.cli import storage

try:
    from systemd.daemon import notify
except ImportError:
    notify = None


logger = logging.getLogger(__name__)


@storage.group(name="reconciler", context_settings=CONTEXT_SETTINGS)
def reconciler():
    """Journal-driven content reconciler.

    See the architectural issue ``swh/devel/swh-storage#4727`` for the
    consistency model: Kafka as durable intent log, Cassandra as derived
    index, this reconciler as repair path.
    """
    pass


@reconciler.command(name="run")
@click.option(
    "--repair-enabled/--observe-only",
    default=False,
    help=(
        "If --repair-enabled is set, missing rows are re-inserted "
        "idempotently. Default --observe-only emits the repairs_total "
        "metric but does not write."
    ),
)
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many Content events. Default is run forever.",
)
@click.pass_context
def run(
    ctx: click.Context,
    repair_enabled: bool,
    stop_after_objects: Optional[int],
):
    """Consume `swh.journal.objects.content` and reconcile Cassandra state.

    The expected configuration file has two sections:

    \b
      - storage: configuration of the CassandraStorage whose state will
        be verified (and, in --repair-enabled mode, repaired).
      - journal_client: configuration of access to the Kafka journal.
        See the documentation of `swh.journal` for details:
        https://docs.softwareheritage.org/devel/apidoc/swh.journal.client.html

    Example::

    \b
        storage:
          cls: cassandra
          hosts: [cassandra-1]
          keyspace: swh
        journal_client:
          cls: kafka
          brokers: [kafka-1]
          group_id: swh-storage-reconciler
    """
    from swh.journal.client import get_journal_client
    from swh.storage import get_storage
    from swh.storage.reconciler import ContentReconciler
    from swh.storage.replay import ModelObjectDeserializer

    conf = ctx.obj["config"]

    storage_obj = get_storage(**conf.pop("storage"))

    deserializer = ModelObjectDeserializer(validate=True)

    client_cfg = conf.pop("journal_client")
    client_cfg["value_deserializer"] = deserializer.convert
    # Reconciler always subscribes to a single topic.
    client_cfg["object_types"] = ("content",)
    if stop_after_objects:
        client_cfg["stop_after_objects"] = stop_after_objects

    try:
        client = get_journal_client(**client_cfg)
    except ValueError as exc:
        ctx.fail(str(exc))

    reconciler_obj = ContentReconciler(
        storage_obj,
        repair_enabled=repair_enabled,
    )

    if notify:
        notify("READY=1")

    logger.info(
        "Reconciler starting: %s mode",
        "repair-enabled" if repair_enabled else "observe-only",
    )

    try:
        client.process(reconciler_obj.process_batch)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        click.echo("Done.")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()
