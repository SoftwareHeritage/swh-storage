# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage.interface import StorageInterface
from swh.storage.tests.storage_data import StorageData


def test_sample_data(sample_data):
    assert isinstance(sample_data, StorageData)


def test_swh_storage(swh_storage: StorageInterface):
    assert isinstance(swh_storage, StorageInterface)


def test_swh_storage_backend_config(swh_storage_backend_config):
    assert isinstance(swh_storage_backend_config, dict)
