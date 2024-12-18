# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dataclasses
import enum
import graphlib
import logging
import textwrap
from typing import Callable, Iterable, Optional, Sequence

from .cql import CqlRunner
from .model import MigrationRow

logger = logging.getLogger(__name__)


class MigrationStatus(enum.Enum):
    PENDING = "pending"
    """The migration was not applied yet"""
    RUNNING = "running"
    COMPLETED = "completed"


@dataclasses.dataclass
class Migration:
    id: str
    """Unique identifier of this migration.

    Should have the format: ``YYYY-MM-DD_developer_readable_name``"""

    dependencies: set[str]
    """Set of identifiers of migrations this migration depends on"""

    min_read_version: str
    """Lowest version of the Python code that should be allowed to read the database
    if this migration is applied"""

    script: Optional[Callable[[CqlRunner], None]]
    """If provided, this is a function that runs the migration.

    If not provided, the migration must be run manually, using steps described
    in the documentation"""

    help: Optional[str]
    """Documentation of the migration

    Typically describes what to do if ``script`` is :const:`None`."""

    required: bool
    """Whether this migration must be applied for the current version of the Python code
    to allow instantiating :class:`swh.storage.cassandra.CassandraStorage`."""


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        id="2024-12-12_init",
        dependencies=set(),
        min_read_version="2.9.0",
        script=lambda _cql_runner: None,
        help="Dummy migration that represents the database schema as of v2.9.0" "",
        required=True,
    ),
)


def list_migrations(
    cql_runner: CqlRunner, rows: Optional[Sequence[MigrationRow]] = None
) -> list[tuple[Migration, MigrationStatus]]:
    """Returns all known migrations, in topological order

    ``rows``, should be the value returned by ``cql_runner.migration_list``.

    This includes migrations that are not required to instantiate
    :class:`swh.storage.cassandra.CassandraStorage`."""
    dependency_graph = {m.id: m.dependencies for m in MIGRATIONS}
    if rows is None:
        rows = list(cql_runner.migration_list())
    statuses = {row.id: row.status for row in rows}
    migrations = {migration.id: migration for migration in MIGRATIONS}
    return [
        (
            migrations[migration_id],
            MigrationStatus(statuses.get(migration_id, "pending")),
        )
        for migration_id in graphlib.TopologicalSorter(dependency_graph).static_order()
    ]


def apply_migrations(
    cql_runner: CqlRunner, ids_to_apply: Iterable[str]
) -> tuple[bool, Sequence[Migration], Sequence[Migration]]:
    """Applies migrations with the given ids (unless they already are).

    Returns:
        * whether any was run, and
        * which migrations cannot run because they are missing dependencies
        * which migrations still need to be run manually.
    """
    applied_any = False
    remaining_manual_migrations = []
    remaining_migrations_missing_dependencies = []

    statuses = {
        migration.id: status for (migration, status) in list_migrations(cql_runner)
    }
    migrations_to_apply = [
        migration for migration in MIGRATIONS if migration.id in ids_to_apply
    ]
    for migration in migrations_to_apply:
        status = statuses[migration.id]
        if status == MigrationStatus.PENDING:
            missing_dependencies = {
                dependency
                for dependency in migration.dependencies
                if statuses[dependency] != MigrationStatus.COMPLETED
            }
            if missing_dependencies:
                logger.warning(
                    "Cannot apply %s: depends on %s",
                    migration.id,
                    ", ".join(missing_dependencies),
                )
                remaining_migrations_missing_dependencies.append(migration)
                continue
            cql_runner.migration_add_one(
                MigrationRow(
                    id=migration.id,
                    dependencies=migration.dependencies,
                    min_read_version=migration.min_read_version,
                    status=MigrationStatus.RUNNING.value,
                )
            )
            if migration.script is None:
                logger.info("Skipping %s", migration.id)
                if migration.help:
                    logger.info("%s", textwrap.indent(migration.help, "    "))
                remaining_manual_migrations.append(migration)
            else:
                logger.info("Running %s...", migration.id)
                migration.script(cql_runner)
                cql_runner.migration_add_one(
                    MigrationRow(
                        id=migration.id,
                        dependencies=migration.dependencies,
                        min_read_version=migration.min_read_version,
                        status=MigrationStatus.COMPLETED.value,
                    )
                )
                logger.info("Done.")
                statuses[migration.id] = MigrationStatus.COMPLETED
                applied_any = True

    return (
        applied_any,
        remaining_manual_migrations,
        remaining_migrations_missing_dependencies,
    )
