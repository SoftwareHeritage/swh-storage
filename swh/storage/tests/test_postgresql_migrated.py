# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests postgresql migrations by initializing with an old schema, applying migrations,
then running all the tests."""

import glob
from os import path

import pytest
import pytest_postgresql

from swh.core.db.pytest_plugin import postgresql_fact
from swh.core.utils import numfile_sortkey as sortkey
import swh.storage
from swh.storage.tests.storage_tests import TestStorage  # noqa

BASE_DIR = path.dirname(swh.storage.__file__)
SQL_UPGRADES_DIR = path.join(BASE_DIR, "../../sql/upgrades")


PRE_MIGRATION_SCHEMA_DIR = "sql-v0.18.0"
"""swh/storage/tests/data/{PRE_MIGRATION_SCHEMA_DIR}/ should be a copy of
swh/storage/sql/ from a previous release."""

BASE_DBVERSION = 164
"""dbversion in swh/storage/tests/data/{PRE_MIGRATION_SCHEMA_DIR}/30_schema.sql"""

pre_migration_schema_files = sorted(
    glob.glob(path.join(BASE_DIR, "tests/data", PRE_MIGRATION_SCHEMA_DIR, "*.sql"))
)

migration_files = sorted(glob.glob(path.join(SQL_UPGRADES_DIR, "*.sql")), key=sortkey,)
"""All migration files."""

use_migration_files = [
    filename
    for filename in migration_files
    if int(path.splitext(path.basename(filename))[0]) > BASE_DBVERSION
]
"""Migration files used to go from BASE_DBVERSION to the current dbversion."""

migrated_swh_storage_postgresql = postgresql_fact(
    "postgresql_proc_migrated",
    db_name="storage",
    dump_files=pre_migration_schema_files + use_migration_files,
)

postgresql_proc_migrated = pytest_postgresql.factories.postgresql_proc()
"""Like postgresql_proc, but initialized with the old schema + migration files,
instead of directly with the current schema."""


@pytest.fixture
def swh_storage_backend_config(
    swh_storage_backend_config, migrated_swh_storage_postgresql
):
    yield {
        **swh_storage_backend_config,
        "db": migrated_swh_storage_postgresql.dsn,
    }
