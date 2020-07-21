# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

import pytest

import swh.storage.api.server as server
import swh.storage.storage
from swh.storage import get_storage
from swh.storage.tests.test_storage import TestStorageGeneratedData  # noqa
from swh.storage.tests.test_storage import TestStorage as _TestStorage

# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below


@pytest.fixture
def app_server():
    server.storage = swh.storage.get_storage(
        cls="memory", journal_writer={"cls": "memory"}
    )
    yield server


@pytest.fixture
def app(app_server):
    return app_server.app


@pytest.fixture
def swh_rpc_client_class():
    def storage_factory(**kwargs):
        storage_config = {
            "cls": "remote",
            **kwargs,
        }
        return get_storage(**storage_config)

    return storage_factory


@pytest.fixture
def swh_storage(swh_rpc_client, app_server):
    # This version of the swh_storage fixture uses the swh_rpc_client fixture
    # to instantiate a RemoteStorage (see swh_rpc_client_class above) that
    # proxies, via the swh.core RPC mechanism, the local (in memory) storage
    # configured in the app_server fixture above.
    #
    # Also note that, for the sake of
    # making it easier to write tests, the in-memory journal writer of the
    # in-memory backend storage is attached to the RemoteStorage as its
    # journal_writer attribute.
    storage = swh_rpc_client

    journal_writer = getattr(storage, "journal_writer", None)
    storage.journal_writer = app_server.storage.journal_writer
    yield storage
    storage.journal_writer = journal_writer


class TestStorage(_TestStorage):
    def test_content_update(self, swh_storage, app_server, sample_data):
        # TODO, journal_writer not supported
        swh_storage.journal_writer.journal = None
        with patch.object(server.storage.journal_writer, "journal", None):
            super().test_content_update(swh_storage, sample_data)
