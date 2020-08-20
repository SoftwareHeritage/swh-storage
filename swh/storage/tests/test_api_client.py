# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

import swh.storage.api.server as server
import swh.storage.storage
from swh.storage import get_storage
from swh.storage.tests.test_storage import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)
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


class TestStorageApi(_TestStorage):
    @pytest.mark.skip(
        'The "person" table of the pgsql is a legacy thing, and not '
        "supported by the cassandra backend."
    )
    def test_person_fullname_unicity(self):
        pass

    @pytest.mark.skip("content_update is not yet implemented for Cassandra")
    def test_content_update(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count(self):
        pass


class TestStorageApiGeneratedData(_TestStorageGeneratedData):
    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_no_visits(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_with_visits_and_snapshot(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_with_visits_no_snapshot(self):
        pass
