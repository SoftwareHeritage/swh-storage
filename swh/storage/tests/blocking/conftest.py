# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.storage.proxies.blocking.db import BlockingAdmin, BlockingQuery

blocking_db_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="storage.proxies.blocking",
            version=BlockingAdmin.current_version,
        ),
    ],
)


blocking_db_postgresql = factories.postgresql(
    "blocking_db_postgresql_proc",
)


@pytest.fixture
def blocking_admin(blocking_db_postgresql) -> BlockingAdmin:
    return BlockingAdmin.connect(blocking_db_postgresql.info.dsn)


@pytest.fixture
def blocking_query(blocking_db_postgresql) -> BlockingQuery:
    return BlockingQuery.connect(blocking_db_postgresql.info.dsn)
