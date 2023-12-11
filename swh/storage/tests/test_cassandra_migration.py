# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This module tests the migration capabilities of the Cassandra backend,
by sending CQL commands (eg. 'ALTER TABLE'), and
by monkey-patching large parts of the implementations to simulate code updates,."""

import dataclasses
import functools
import operator
from typing import Dict, Iterable, Optional

import attr
import pytest

from swh.model.model import Content
from swh.storage import get_storage
from swh.storage.cassandra.cql import (
    CqlRunner,
    _prepared_insert_statement,
    _prepared_select_statement,
)
from swh.storage.cassandra.model import ContentRow
from swh.storage.cassandra.schema import CONTENT_INDEX_TEMPLATE, HASH_ALGORITHMS
from swh.storage.cassandra.storage import CassandraStorage
from swh.storage.exc import StorageArgumentException

from .storage_data import StorageData


@pytest.fixture
def swh_storage_backend_config(swh_storage_cassandra_backend_config):
    return swh_storage_cassandra_backend_config


##############################
# Common structures


def byte_xor_hash(data):
    # Behold, a one-line hash function:
    return bytes([functools.reduce(operator.xor, data)])


@attr.s
class ContentWithXor(Content):
    """An hypothetical upgrade of Content with an extra "hash"."""

    byte_xor = attr.ib(type=bytes, default=None)


##############################
# Test simple migrations


@dataclasses.dataclass
class ContentRowWithXor(ContentRow):
    """An hypothetical upgrade of ContentRow with an extra "hash",
    but not in the primary key."""

    byte_xor: bytes


class CqlRunnerWithXor(CqlRunner):
    """An hypothetical upgrade of ContentRow with an extra "hash",
    but not in the primary key."""

    @_prepared_select_statement(
        ContentRowWithXor,
        f"WHERE {' AND '.join(map('%s = ?'.__mod__, HASH_ALGORITHMS))}",
    )
    def content_get_from_pk(
        self, content_hashes: Dict[str, bytes], *, statement
    ) -> Optional[ContentRow]:
        rows = list(
            self._execute_with_retries(
                statement, [content_hashes[algo] for algo in HASH_ALGORITHMS]
            )
        )
        assert len(rows) <= 1
        if rows:
            return ContentRowWithXor(**rows[0])
        else:
            return None

    @_prepared_select_statement(
        ContentRowWithXor,
        f"WHERE token({', '.join(ContentRowWithXor.PARTITION_KEY)}) = ?",
    )
    def content_get_from_tokens(
        self, tokens, *, statement
    ) -> Iterable[ContentRowWithXor]:
        return map(
            ContentRowWithXor.from_dict,
            self._execute_many_with_retries(statement, [(token,) for token in tokens]),
        )

    # Redecorate content_add_prepare with the new ContentRow class
    content_add_prepare = _prepared_insert_statement(ContentRowWithXor)(  # type: ignore
        CqlRunner.content_add_prepare.__wrapped__  # type: ignore
    )


@pytest.mark.cassandra
def test_add_content_column(
    swh_storage: CassandraStorage, swh_storage_backend_config, mocker  # noqa
) -> None:
    """Adds a column to the 'content' table and a new matching index.
    This is a simple migration, as it does not require an update to the primary key.
    """
    content_xor_hash = byte_xor_hash(StorageData.content.data)

    # First insert some existing data
    swh_storage.content_add([StorageData.content, StorageData.content2])

    # Then update the schema
    session = swh_storage._cql_runner._cluster.connect(swh_storage._cql_runner.keyspace)
    session.execute("ALTER TABLE content ADD byte_xor blob")
    for statement in CONTENT_INDEX_TEMPLATE.split("\n\n"):
        session.execute(statement.format(main_algo="byte_xor"))

    # Should not affect the running code at all:
    assert swh_storage.content_get([StorageData.content.sha1]) == [
        attr.evolve(StorageData.content, data=None)
    ]
    with pytest.raises(StorageArgumentException):
        swh_storage.content_find({"byte_xor": content_xor_hash})  # type: ignore

    # Then update the running code:
    new_hash_algos = HASH_ALGORITHMS + ["byte_xor"]
    mocker.patch("swh.storage.cassandra.storage.HASH_ALGORITHMS", new_hash_algos)
    mocker.patch("swh.storage.cassandra.cql.HASH_ALGORITHMS", new_hash_algos)
    mocker.patch("swh.model.model.DEFAULT_ALGORITHMS", new_hash_algos)
    mocker.patch("swh.storage.cassandra.storage.Content", ContentWithXor)
    mocker.patch("swh.storage.cassandra.storage.ContentRow", ContentRowWithXor)
    mocker.patch("swh.storage.cassandra.model.ContentRow", ContentRowWithXor)
    mocker.patch("swh.storage.cassandra.storage.CqlRunner", CqlRunnerWithXor)

    # Forge new objects with this extra hash:
    new_content = ContentWithXor.from_dict(
        {
            "byte_xor": byte_xor_hash(StorageData.content.data),
            **StorageData.content.to_dict(),
        }
    )
    new_content2 = ContentWithXor.from_dict(
        {
            "byte_xor": byte_xor_hash(StorageData.content2.data),
            **StorageData.content2.to_dict(),
        }
    )

    # Simulates a restart:
    swh_storage._set_cql_runner()

    # Old algos still works, and return the new object type:
    assert swh_storage.content_get([StorageData.content.sha1]) == [
        attr.evolve(new_content, data=None, byte_xor=None)
    ]

    # The new algo does not work, we did not backfill it yet:
    assert swh_storage.content_find({"byte_xor": content_xor_hash}) == []  # type: ignore

    # A normal storage would not overwrite, because the object already exists,
    # as it is not aware it is missing a field:
    swh_storage.content_add([new_content, new_content2])
    assert swh_storage.content_find({"byte_xor": content_xor_hash}) == []  # type: ignore

    # Backfill (in production this would be done with a replayer reading from
    # the journal):
    overwriting_swh_storage = get_storage(
        allow_overwrite=True, **swh_storage_backend_config
    )
    overwriting_swh_storage.content_add([new_content, new_content2])

    # Now, the object can be found:
    assert swh_storage.content_find({"byte_xor": content_xor_hash}) == [  # type: ignore
        attr.evolve(new_content, data=None)
    ]


##############################
# Test complex  migrations


@dataclasses.dataclass
class ContentRowWithXorPK(ContentRow):
    """An hypothetical upgrade of ContentRow with an extra "hash",
    in the primary key."""

    TABLE = "content_v2"
    PARTITION_KEY = ("sha1", "sha1_git", "sha256", "blake2s256", "byte_xor")

    byte_xor: bytes


class CqlRunnerWithXorPK(CqlRunner):
    """An hypothetical upgrade of ContentRow with an extra "hash",
    but not in the primary key."""

    @_prepared_select_statement(
        ContentRowWithXorPK,
        f"WHERE {' AND '.join(map('%s = ?'.__mod__, HASH_ALGORITHMS))} AND byte_xor=?",
    )
    def content_get_from_pk(
        self, content_hashes: Dict[str, bytes], *, statement
    ) -> Optional[ContentRow]:
        rows = list(
            self._execute_with_retries(
                statement,
                [content_hashes[algo] for algo in HASH_ALGORITHMS + ["byte_xor"]],
            )
        )
        assert len(rows) <= 1
        if rows:
            return ContentRowWithXorPK(**rows[0])
        else:
            return None

    @_prepared_select_statement(
        ContentRowWithXorPK,
        f"WHERE token({', '.join(ContentRowWithXorPK.PARTITION_KEY)}) = ?",
    )
    def content_get_from_tokens(
        self, tokens, *, statement
    ) -> Iterable[ContentRowWithXorPK]:
        return map(
            ContentRowWithXorPK.from_dict,
            self._execute_many_with_retries(statement, [(token,) for token in tokens]),
        )

    # Redecorate content_add_prepare with the new ContentRow class
    content_add_prepare = _prepared_insert_statement(ContentRowWithXorPK)(  # type: ignore  # noqa
        CqlRunner.content_add_prepare.__wrapped__  # type: ignore
    )


@pytest.mark.cassandra
def test_change_content_pk(
    swh_storage: CassandraStorage, swh_storage_backend_config, mocker  # noqa
) -> None:
    """Adds a column to the 'content' table and a new matching index;
    and make this new column part of the primary key
    This is a complex migration, as it requires copying the whole table
    """
    content_xor_hash = byte_xor_hash(StorageData.content.data)
    session = swh_storage._cql_runner._cluster.connect(swh_storage._cql_runner.keyspace)

    # First insert some existing data
    swh_storage.content_add([StorageData.content, StorageData.content2])

    # Then add a new table and a new index
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS content_v2 (
            sha1          blob,
            sha1_git      blob,
            sha256        blob,
            blake2s256    blob,
            byte_xor      blob,
            length        bigint,
            ctime         timestamp,
                -- creation time, i.e. time of (first) injection into the storage
            status        ascii,
            PRIMARY KEY ((sha1, sha1_git, sha256, blake2s256, byte_xor))
        );"""
    )
    for statement in CONTENT_INDEX_TEMPLATE.split("\n\n"):
        session.execute(statement.format(main_algo="byte_xor"))

    # Should not affect the running code at all:
    assert swh_storage.content_get([StorageData.content.sha1]) == [
        attr.evolve(StorageData.content, data=None)
    ]
    with pytest.raises(StorageArgumentException):
        swh_storage.content_find({"byte_xor": content_xor_hash})  # type: ignore

    # Then update the running code:
    new_hash_algos = HASH_ALGORITHMS + ["byte_xor"]
    mocker.patch("swh.storage.cassandra.storage.HASH_ALGORITHMS", new_hash_algos)
    mocker.patch("swh.storage.cassandra.cql.HASH_ALGORITHMS", new_hash_algos)
    mocker.patch("swh.model.model.DEFAULT_ALGORITHMS", new_hash_algos)
    mocker.patch("swh.storage.cassandra.storage.Content", ContentWithXor)
    mocker.patch("swh.storage.cassandra.storage.ContentRow", ContentRowWithXorPK)
    mocker.patch("swh.storage.cassandra.model.ContentRow", ContentRowWithXorPK)
    mocker.patch("swh.storage.cassandra.storage.CqlRunner", CqlRunnerWithXorPK)

    # Forge new objects with this extra hash:
    new_content = ContentWithXor.from_dict(
        {
            "byte_xor": byte_xor_hash(StorageData.content.data),
            **StorageData.content.to_dict(),
        }
    )
    new_content2 = ContentWithXor.from_dict(
        {
            "byte_xor": byte_xor_hash(StorageData.content2.data),
            **StorageData.content2.to_dict(),
        }
    )

    # Replay to the new table.
    # In production this would be done with a replayer reading from the journal,
    # while loaders would still write to the DB.
    overwriting_swh_storage = get_storage(
        allow_overwrite=True, **swh_storage_backend_config
    )
    overwriting_swh_storage.content_add([new_content, new_content2])

    # Old algos still works, and return the new object type;
    # but the byte_xor value is None because it is only available in the new
    # table, which this storage is not yet configured to use
    assert swh_storage.content_get([StorageData.content.sha1]) == [
        attr.evolve(new_content, data=None, byte_xor=None)
    ]

    # When the replayer gets close to the end of the logs, loaders are stopped
    # to allow the replayer to catch up with the end of the log.
    # When it does, we can switch over to the new swh-storage's code.

    # Simulates a restart:
    swh_storage._set_cql_runner()

    # Now, the object can be found with the new hash:
    assert swh_storage.content_find({"byte_xor": content_xor_hash}) == [  # type: ignore
        attr.evolve(new_content, data=None)
    ]

    # Remove the old table:
    session.execute("DROP TABLE content")

    # Object is still available, because we don't use it anymore
    assert swh_storage.content_find({"byte_xor": content_xor_hash}) == [  # type: ignore
        attr.evolve(new_content, data=None)
    ]

    # THE END.

    # Test teardown expects a table with this name to exist:
    session.execute("CREATE TABLE content (foo blob PRIMARY KEY);")

    # Clean up this table, test teardown does not know about it:
    session.execute("DROP TABLE content_v2;")
