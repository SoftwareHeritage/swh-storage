# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.storage.tests.test_storage import (  # noqa
    TestStorage, TestStorageGeneratedData)


# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below

@pytest.fixture
def swh_storage_backend_config():
    yield {
        'cls': 'memory',
        'journal_writer': {
            'cls': 'memory',
        },
    }
