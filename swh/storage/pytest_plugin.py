# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
from os import environ

import pytest
from pytest_postgresql import factories

from swh.core.db.pytest_plugin import initialize_database_for_module, postgresql_fact
from swh.storage import get_storage
from swh.storage.postgresql.db import Db as StorageDb
from swh.storage.tests.storage_data import StorageData

environ["LC_ALL"] = "C.UTF-8"


swh_storage_postgresql_proc = factories.postgresql_proc(
    dbname="storage",
    load=[
        partial(
            initialize_database_for_module,
            modname="storage",
            version=StorageDb.current_version,
        )
    ],
)


swh_storage_postgresql = postgresql_fact("swh_storage_postgresql_proc")


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    yield {
        "cls": "postgresql",
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
