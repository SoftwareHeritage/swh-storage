# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import environ, path

import pytest

from swh.core.db.pytest_plugin import postgresql_fact
import swh.storage
from swh.storage import get_storage
from swh.storage.tests.storage_data import StorageData

SQL_DIR = path.join(path.dirname(swh.storage.__file__), "sql")

environ["LC_ALL"] = "C.UTF-8"


swh_storage_postgresql = postgresql_fact(
    "postgresql_proc", db_name="storage", dump_files=path.join(SQL_DIR, "*.sql")
)


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    yield {
        "cls": "local",
        "db": swh_storage_postgresql.dsn,
        "objstorage": {"cls": "memory"},
        "check_config": {"check_write": True},
    }


@pytest.fixture
def swh_storage(swh_storage_backend_config):
    return get_storage(**swh_storage_backend_config)


@pytest.fixture
def sample_data() -> StorageData:
    """Pre-defined sample storage object data to manipulate

    Returns:
        StorageData whose attribute keys are data model objects. Either multiple
        objects: contents, directories, revisions, releases, ... or simple ones:
        content, directory, revision, release, ...

    """
    return StorageData()
