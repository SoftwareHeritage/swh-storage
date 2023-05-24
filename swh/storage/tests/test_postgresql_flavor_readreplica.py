# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.storage.postgresql.storage import Storage as StorageDatastore
from swh.storage.pytest_plugin import create_object_references_partition
from swh.storage.tests.test_postgresql import TestPgStorage  # noqa: F401
from swh.storage.tests.test_postgresql import TestStorage  # noqa: F401
from swh.storage.tests.test_postgresql import TestStorageRaceConditions  # noqa: F401

swh_storage_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="storage",
            flavor="read_replica",
            version=StorageDatastore.current_version,
        ),
        create_object_references_partition,
    ],
)


@pytest.mark.db
def test_pgstorage_flavor(swh_storage):
    # get_flavor retrieve directly from the db
    assert swh_storage.get_flavor() == "read_replica"

    # flavor property (value is cached)
    assert swh_storage._flavor is None
    assert swh_storage.flavor == "read_replica"
    assert swh_storage._flavor == "read_replica"
