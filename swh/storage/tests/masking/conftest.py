# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.storage.proxies.masking import MaskingProxyStorage
from swh.storage.proxies.masking.db import MaskingAdmin, MaskingQuery

masking_db_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="storage.proxies.masking",
            version=MaskingAdmin.current_version,
        ),
    ],
)


masking_db_postgresql = factories.postgresql(
    "masking_db_postgresql_proc",
)


@pytest.fixture
def masking_admin(masking_db_postgresql) -> MaskingAdmin:
    return MaskingAdmin.connect(masking_db_postgresql.info.dsn)


@pytest.fixture
def masking_query(masking_db_postgresql) -> MaskingQuery:
    return MaskingQuery.connect(masking_db_postgresql.info.dsn)


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": {
            "cls": "memory",
        },
    }


@pytest.fixture
def swh_storage(masking_db_postgresql, swh_storage_backend):
    return MaskingProxyStorage(
        db=masking_db_postgresql.info.dsn, storage=swh_storage_backend
    )
