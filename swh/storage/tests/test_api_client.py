# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.storage.api.client import RemoteStorage
import swh.storage.api.server as server
import swh.storage.storage
from swh.storage.tests.test_storage import (  # noqa
    TestStorage, TestStorageGeneratedData)

# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below


@pytest.fixture
def app():
    storage_config = {
        'cls': 'memory',
        'journal_writer': {
            'cls': 'memory',
        },
    }
    server.storage = swh.storage.get_storage(**storage_config)
    # hack hack hack!
    # We attach the journal storage to the app here to make it accessible to
    # the test (as swh_storage.journal_writer); see swh_storage below.
    server.app.journal_writer = server.storage.journal_writer
    yield server.app
    del server.app.journal_writer


@pytest.fixture
def swh_rpc_client_class():
    return RemoteStorage


@pytest.fixture
def swh_storage(swh_rpc_client, app):
    # This version of the swh_storage fixture uses the swh_rpc_client fixture
    # to instantiate a RemoteStorage (see swh_rpc_client_class above) that
    # proxies, via the swh.core RPC mechanism, the local (in memory) storage
    # configured in the app fixture above.
    #
    # Also note that, for the sake of
    # making it easier to write tests, the in-memory journal writer of the
    # in-memory backend storage is attached to the RemoteStorage as its
    # journal_writer attribute.
    storage = swh_rpc_client
    journal_writer = getattr(storage, 'journal_writer', None)
    storage.journal_writer = app.journal_writer
    yield storage
    storage.journal_writer = journal_writer
