# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

import pytest

from swh.storage.metrics import COUNTED_ARG_METRIC, DURATION_METRIC
from swh.storage.proxies.statsd import StatsdProxyStorage
from swh.storage.tests.test_in_memory import (  # noqa
    TestInMemoryStorage,
    TestStorageDeletion,
    swh_storage_backend_config,
)

# tests are executed using imported classes using overloaded swh_storage fixture
# below


@pytest.fixture
def swh_storage(swh_storage):
    return StatsdProxyStorage(swh_storage)


@patch("swh.core.statsd.statsd.increment")
@patch("swh.core.statsd.statsd.timing")
@patch("swh.core.statsd.monotonic", side_effect=(10.0, 15))
def test_directory_ls(
    mock_monotonic, mock_statsd_timing, mock_statsd_increment, swh_storage, sample_data
):
    swh_storage.directory_ls(sample_data.directory.id)

    mock_statsd_timing.assert_called_with(
        DURATION_METRIC,
        5000.0,
        tags={
            "endpoint": "directory_ls",
        },
        sample_rate=1,
    )
    mock_statsd_increment.assert_not_called()


@patch("swh.core.statsd.statsd.increment")
@patch("swh.core.statsd.statsd.timing")
@patch("swh.core.statsd.monotonic", side_effect=(10.0, 15.0) * 3)
def test_revision_add(
    mock_monotonic,
    mock_statsd_timing,
    mock_statsd_increment,
    swh_storage,
    sample_data,
    subtests,
):
    for num_revisions in (0, 1, 5):
        with subtests.test(num_revisions=num_revisions):
            swh_storage.revision_add([sample_data.revision] * num_revisions)

            mock_statsd_timing.assert_called_with(
                DURATION_METRIC,
                5000.0,
                tags={
                    "endpoint": "revision_add",
                },
                sample_rate=1,
            )
            mock_statsd_increment.assert_called_with(
                COUNTED_ARG_METRIC,
                num_revisions,
                tags={
                    "endpoint": "revision_add",
                },
            )
