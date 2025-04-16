# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from .test_cli_object_references import (
    swh_storage_with_partitions,
    test_create_object_reference_partitions,
    test_remove_old_object_reference_partitions,
    test_remove_old_object_reference_partitions_using_force_argument,
)

__all__ = (
    "swh_storage_with_partitions",
    "test_create_object_reference_partitions",
    "test_remove_old_object_reference_partitions",
    "test_remove_old_object_reference_partitions_using_force_argument",
)


@pytest.fixture
def swh_storage_backend_config(request, swh_storage_cassandra_backend_config):
    """An swh-storage object that gets injected into the CLI functions."""
    return swh_storage_cassandra_backend_config
