# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools

import pytest

from swh.storage.exc import BlockedOriginException
from swh.storage.proxies.blocking import BlockingProxyStorage
from swh.storage.proxies.blocking.db import BlockingState
from swh.storage.tests.storage_data import StorageData
from swh.storage.tests.test_in_memory import TestInMemoryStorage as _TestStorage


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": {
            "cls": "memory",
        },
    }


# This simply test that with the last 2 origins found in StorageData.origins
# marked as blocked, most standard tests or OK but a few of them (the ones
# actually inserting these 2 now blocked origins). These few tests need to be
# overloaded accordingly.

BLOCKED_ORIGINS = set(origin.url for origin in StorageData.origins[-2:])


@pytest.fixture
def swh_storage(blocking_db_postgresql, blocking_admin, swh_storage_backend):
    # Create a request
    request = blocking_admin.create_request(slug="foo", reason="bar")

    blocking_admin.set_origins_state(
        request_id=request.id,
        new_state=BlockingState.DECISION_PENDING,
        urls=list(BLOCKED_ORIGINS),
    )

    return BlockingProxyStorage(
        blocking_db=blocking_db_postgresql.info.dsn, storage=swh_storage_backend
    )


class TestStorage(_TestStorage):
    @pytest.mark.xfail(reason="typing.Protocol instance check is annoying")
    def test_types(self, *args, **kwargs):
        super().test_types(*args, **kwargs)


EXPECTED_BLOCKED_EXCEPTIONS = {
    "test_origin_add",
    "test_origin_visit_status_get_random_nothing_found",
}

for method_name in dir(TestStorage):
    if not method_name.startswith("test_"):
        continue

    method = getattr(TestStorage, method_name)

    @functools.wraps(method)
    def wrapped_test(*args, method=method, **kwargs):
        try:
            method(*args, **kwargs)
        except BlockedOriginException as exc:
            assert (
                method.__name__ in EXPECTED_BLOCKED_EXCEPTIONS
            ), f"{method.__name__} shouldn't raise a BlockedOriginException"

            assert exc.blocked, "The exception was raised with no blocked origin"

            assert (
                set(exc.blocked.keys()) <= BLOCKED_ORIGINS
            ), "Found an unexpectedly blocked origin"
        else:
            assert (
                method.__name__ not in EXPECTED_BLOCKED_EXCEPTIONS
            ), f"{method.__name__} should raise a BlockedOriginException"

    setattr(TestStorage, method_name, wrapped_test)
