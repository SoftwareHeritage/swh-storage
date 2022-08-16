# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2.errors
import pytest

from swh.core.api import RemoteException, TransientRemoteException
import swh.storage
from swh.storage import get_storage
import swh.storage.api.server as server
from swh.storage.tests.storage_tests import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)
from swh.storage.tests.storage_tests import TestStorage as _TestStorage

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
    storage.objstorage = app_server.storage.objstorage
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

    def test_exception(self, app_server, swh_storage, mocker):
        """Checks the client re-raises unknown exceptions as a :exc:`RemoteException`"""
        assert swh_storage.revision_get(["\x01" * 20]) == [None]
        mocker.patch.object(
            app_server.storage._cql_runner,
            "revision_get",
            side_effect=ValueError("crash"),
        )
        with pytest.raises(RemoteException) as e:
            swh_storage.revision_get(["\x01" * 20])
        assert not isinstance(e, TransientRemoteException)

    def test_operationalerror_exception(self, app_server, swh_storage, mocker):
        """Checks the client re-raises as a :exc:`TransientRemoteException`
        rather than the base :exc:`RemoteException`; so the retrying proxy
        retries for longer."""
        assert swh_storage.revision_get(["\x01" * 20]) == [None]
        mocker.patch.object(
            app_server.storage._cql_runner,
            "revision_get",
            side_effect=psycopg2.errors.AdminShutdown("cluster is shutting down"),
        )
        with pytest.raises(RemoteException) as excinfo:
            swh_storage.revision_get(["\x01" * 20])
        assert isinstance(excinfo.value, TransientRemoteException)

    def test_querycancelled_exception(self, app_server, swh_storage, mocker):
        """Checks the client re-raises as a :exc:`TransientRemoteException`
        rather than the base :exc:`RemoteException`; so the retrying proxy
        retries for longer."""
        assert swh_storage.revision_get(["\x01" * 20]) == [None]
        mocker.patch.object(
            app_server.storage._cql_runner,
            "revision_get",
            side_effect=psycopg2.errors.QueryCanceled("too big!"),
        )
        with pytest.raises(RemoteException) as excinfo:
            swh_storage.revision_get(["\x01" * 20])
        assert not isinstance(excinfo.value, TransientRemoteException)


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
