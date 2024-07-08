# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from uuid import uuid4

import psycopg2.errors
import pytest

from swh.core.api import RemoteException, TransientRemoteException
from swh.model.model import Origin
from swh.model.swhids import ExtendedSWHID
import swh.storage
from swh.storage import get_storage
import swh.storage.api.server as server
from swh.storage.exc import BlockedOriginException, MaskedObjectException
from swh.storage.proxies.blocking.db import BlockingState, BlockingStatus
from swh.storage.proxies.masking.db import MaskedState, MaskedStatus
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


@pytest.fixture
def swh_storage_backend(app_server, swh_storage):
    return app_server.storage


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

    @pytest.mark.skip("in-memory backend has no timeout")
    def test_querytimeout(self):
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

    def test_masked_object_exception(self, app_server, swh_storage, mocker):
        """Checks the client re-raises masking proxy exceptions"""
        assert swh_storage.revision_get(["\x01" * 20]) == [None]
        masked = {
            ExtendedSWHID.from_string("swh:1:rev:" + ("01" * 20)): [
                MaskedStatus(MaskedState.DECISION_PENDING, request=uuid4())
            ]
        }
        mocker.patch.object(
            app_server.storage._cql_runner,
            "revision_get",
            side_effect=MaskedObjectException(masked),
        )
        with pytest.raises(MaskedObjectException) as e:
            swh_storage.revision_get(["\x01" * 20])
        assert e.value.masked == masked

    def test_blocked_origin_exception(self, app_server, swh_storage, mocker):
        """Checks the client re-raises masking proxy exceptions"""
        assert swh_storage.origin_get(["https://example.com"]) == [None]
        blocked = {
            "https://example.com": BlockingStatus(
                BlockingState.DECISION_PENDING, request=uuid4()
            )
        }
        mocker.patch.object(
            app_server.storage._cql_runner,
            "origin_add_one",
            side_effect=BlockedOriginException(blocked),
        )
        with pytest.raises(BlockedOriginException) as e:
            swh_storage.origin_add([Origin(url="https://example.com")])
        assert e.value.blocked == blocked


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
