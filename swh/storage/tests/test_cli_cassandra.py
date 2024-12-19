# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest

from swh.model.model import Origin
from swh.storage import get_storage

from .test_cli import invoke

logger = logging.getLogger(__name__)


@pytest.mark.cassandra
def test_init(
    swh_storage_cassandra_cluster,
    cassandra_auth_provider_config,
):
    (hosts, port) = swh_storage_cassandra_cluster
    keyspace = "test" + os.urandom(10).hex()

    storage_config = dict(
        cls="cassandra",
        hosts=hosts,
        port=port,
        keyspace=keyspace,
        journal_writer={"cls": "memory"},
        objstorage={"cls": "memory"},
        auth_provider=cassandra_auth_provider_config,
    )

    result = invoke("cassandra", "init", local_config={"storage": storage_config})
    assert result.exit_code == 0, result.output
    assert result.output == "Done.\n"

    # Check we can write and read to it
    storage = get_storage(**storage_config)
    origin = Origin(url="http://example.org")
    storage.origin_add([origin])
    assert storage.origin_get([origin.url]) == [origin]
