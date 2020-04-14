# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

import pytest

from swh.storage.api.client import RemoteStorage
import swh.storage.api.server as server
import swh.storage.storage
from swh.storage.tests.test_storage import TestStorageGeneratedData  # noqa
from swh.storage.tests.test_storage import TestStorage as _TestStorage
from swh.storage.tests.test_api_client import swh_storage  # noqa

# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below


@pytest.fixture
def app_server():
    storage_config = {
        "cls": "validate",
        "storage": {"cls": "memory", "journal_writer": {"cls": "memory",},},
    }
    server.storage = swh.storage.get_storage(**storage_config)
    yield server


@pytest.fixture
def app(app_server):
    return app_server.app


@pytest.fixture
def swh_rpc_client_class():
    return RemoteStorage


class TestStorage(_TestStorage):
    def test_content_update(self, swh_storage, app_server):  # noqa
        # TODO, journal_writer not supported
        swh_storage.journal_writer.journal = None
        with patch.object(server.storage.journal_writer, "journal", None):
            super().test_content_update(swh_storage)

    @pytest.mark.skip("non-applicable test")
    def test_content_add_from_lazy_content(self):
        pass
