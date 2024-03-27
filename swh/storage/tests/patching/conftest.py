# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.storage.postgresql.storage import Storage as StorageDatastore
from swh.storage.proxies.patching.db import PatchingAdmin, PatchingQuery

patching_db_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="storage",
            flavor="only_masking",  # TODO: add new flavor instead of reusing?
            version=StorageDatastore.current_version,
        ),
    ],
)


patching_db_postgresql = factories.postgresql(
    "patching_db_postgresql_proc",
)


@pytest.fixture
def patching_admin(patching_db_postgresql) -> PatchingAdmin:
    return PatchingAdmin.connect(patching_db_postgresql.info.dsn)


@pytest.fixture
def patching_query(patching_db_postgresql) -> PatchingQuery:
    return PatchingQuery.connect(patching_db_postgresql.info.dsn)
