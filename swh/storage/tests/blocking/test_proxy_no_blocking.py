# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest

from swh.storage.proxies.blocking import BlockingProxyStorage
from swh.storage.tests.test_in_memory import TestInMemoryStorage as _TestStorage

# This simply test that without any blocked URL defined in the blocking proxy,
# all standard tests are OK


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": {
            "cls": "memory",
        },
    }


@pytest.fixture
def swh_storage(blocking_db_postgresql, swh_storage_backend):
    return BlockingProxyStorage(
        blocking_db=blocking_db_postgresql.info.dsn, storage=swh_storage_backend
    )


class TestStorage(_TestStorage):
    @pytest.mark.xfail(reason="typing.Protocol instance check is annoying")
    def test_types(self, *args, **kwargs):
        super().test_types(*args, **kwargs)
