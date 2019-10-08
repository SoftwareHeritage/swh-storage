# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.storage import get_storage
from swh.storage.tests.test_storage import (  # noqa
    TestStorage, TestStorageGeneratedData)
from swh.storage.in_memory import ENABLE_ORIGIN_IDS


TestStorage._test_origin_ids = ENABLE_ORIGIN_IDS
TestStorageGeneratedData._test_origin_ids = ENABLE_ORIGIN_IDS


# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below

@pytest.fixture
def swh_storage():
    storage_config = {
        'cls': 'memory',
        'args': {
            'journal_writer': {
                'cls': 'memory',
            },
        },
    }
    storage = get_storage(**storage_config)
    return storage
