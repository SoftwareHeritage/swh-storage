# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

import pytest

from swh.storage.proxies.counting import CountingProxyStorage, Stats
from swh.storage.tests.test_in_memory import (  # noqa
    TestInMemoryStorage,
    TestStorageDeletion,
    swh_storage_backend_config,
)

# tests are executed using imported classes using overloaded swh_storage fixture
# below


@pytest.fixture
def swh_storage(swh_storage):
    return CountingProxyStorage(swh_storage)


@patch("time.monotonic", side_effect=(10.0, 15))
def test_directory_ls(mock_monotonic, swh_storage, sample_data):
    swh_storage.directory_ls(sample_data.directory.id)

    assert swh_storage.stats == Stats(
        calls={
            "directory_ls": 1,
        },
        total_time={
            "directory_ls": 5.0,
        },
        arg_lengths={},
    )


@patch("time.monotonic", side_effect=(10.0, 15) * 3)
def test_revision_add(
    mock_monotonic,
    swh_storage,
    sample_data,
    subtests,
):
    for num_revisions in (0, 1, 5):
        with subtests.test(num_revisions=num_revisions):
            swh_storage.revision_add([sample_data.revision] * num_revisions)

            assert swh_storage.stats == Stats(
                calls={
                    "revision_add": 1,
                },
                total_time={
                    "revision_add": 5.0,
                },
                arg_lengths={
                    "revision_add": num_revisions,
                },
            )
            swh_storage.stats = Stats()
