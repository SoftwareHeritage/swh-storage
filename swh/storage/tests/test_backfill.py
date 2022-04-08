# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
from unittest.mock import patch

import pytest

from swh.journal.client import JournalClient
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.storage import get_storage
from swh.storage.backfill import (
    PARTITION_KEY,
    JournalBackfiller,
    byte_ranges,
    compute_query,
    raw_extrinsic_metadata_target_ranges,
)
from swh.storage.in_memory import InMemoryStorage
from swh.storage.replay import ModelObjectDeserializer, process_replay_objects
from swh.storage.tests.test_replay import check_replayed

TEST_CONFIG = {
    "journal_writer": {
        "brokers": ["localhost"],
        "prefix": "swh.tmp_journal.new",
        "client_id": "swh.journal.client.test",
    },
    "storage": {"cls": "postgresql", "db": "service=swh-dev"},
}


def test_config_ko_missing_mandatory_key():
    """Missing configuration key will make the initialization fail"""
    for key in TEST_CONFIG.keys():
        config = TEST_CONFIG.copy()
        config.pop(key)

        with pytest.raises(ValueError) as e:
            JournalBackfiller(config)

        error = "Configuration error: The following keys must be provided: %s" % (
            ",".join([key]),
        )
        assert e.value.args[0] == error


def test_config_ko_unknown_object_type():
    """Parse arguments will fail if the object type is unknown"""
    backfiller = JournalBackfiller(TEST_CONFIG)
    with pytest.raises(ValueError) as e:
        backfiller.parse_arguments("unknown-object-type", 1, 2)

    error = (
        "Object type unknown-object-type is not supported. "
        "The only possible values are %s" % (", ".join(sorted(PARTITION_KEY)))
    )
    assert e.value.args[0] == error


def test_compute_query_content():
    query, where_args, column_aliases = compute_query("content", "\x000000", "\x000001")

    assert where_args == ["\x000000", "\x000001"]

    assert column_aliases == [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "status",
        "ctime",
    ]

    assert (
        query
        == """
select sha1,sha1_git,sha256,blake2s256,length,status,ctime
from content

where (sha1) >= %s and (sha1) < %s
    """
    )


def test_compute_query_skipped_content():
    query, where_args, column_aliases = compute_query("skipped_content", None, None)

    assert where_args == []

    assert column_aliases == [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "ctime",
        "status",
        "reason",
    ]

    assert (
        query
        == """
select sha1,sha1_git,sha256,blake2s256,length,ctime,status,reason
from skipped_content


    """
    )


def test_compute_query_origin_visit():
    query, where_args, column_aliases = compute_query("origin_visit", 1, 10)

    assert where_args == [1, 10]

    assert column_aliases == [
        "visit",
        "type",
        "origin",
        "date",
    ]

    assert (
        query
        == """
select visit,type,origin.url as origin,date
from origin_visit
left join origin on origin_visit.origin=origin.id
where (origin_visit.origin) >= %s and (origin_visit.origin) < %s
    """
    )


def test_compute_query_release():
    query, where_args, column_aliases = compute_query("release", "\x000002", "\x000003")

    assert where_args == ["\x000002", "\x000003"]

    assert column_aliases == [
        "id",
        "date",
        "date_offset_bytes",
        "comment",
        "name",
        "synthetic",
        "target",
        "target_type",
        "author_id",
        "author_name",
        "author_email",
        "author_fullname",
        "raw_manifest",
    ]

    assert (
        query
        == """
select release.id as id,date,date_offset_bytes,comment,release.name as name,synthetic,target,target_type,a.id as author_id,a.name as author_name,a.email as author_email,a.fullname as author_fullname,raw_manifest
from release
left join person a on release.author=a.id
where (release.id) >= %s and (release.id) < %s
    """  # noqa
    )


@pytest.mark.parametrize("numbits", [2, 3, 8, 16])
def test_byte_ranges(numbits):
    ranges = list(byte_ranges(numbits))

    assert len(ranges) == 2**numbits
    assert ranges[0][0] is None
    assert ranges[-1][1] is None

    bounds = []
    for i, (left, right) in enumerate(zip(ranges[:-1], ranges[1:])):
        assert left[1] == right[0], f"Mismatched bounds in {i}th range"
        bounds.append(left[1])

    assert bounds == sorted(bounds)


def test_raw_extrinsic_metadata_target_ranges():
    ranges = list(raw_extrinsic_metadata_target_ranges())

    assert ranges[0][0] == ""
    assert ranges[-1][1] is None

    bounds = []
    for i, (left, right) in enumerate(zip(ranges[:-1], ranges[1:])):
        assert left[1] == right[0], f"Mismatched bounds in {i}th range"
        bounds.append(left[1])

    assert bounds == sorted(bounds)


RANGE_GENERATORS = {
    "content": lambda start, end: [(None, None)],
    "skipped_content": lambda start, end: [(None, None)],
    "directory": lambda start, end: [(None, None)],
    "extid": lambda start, end: [(None, None)],
    "metadata_authority": lambda start, end: [(None, None)],
    "metadata_fetcher": lambda start, end: [(None, None)],
    "revision": lambda start, end: [(None, None)],
    "release": lambda start, end: [(None, None)],
    "snapshot": lambda start, end: [(None, None)],
    "origin": lambda start, end: [(None, 10000)],
    "origin_visit": lambda start, end: [(None, 10000)],
    "origin_visit_status": lambda start, end: [(None, 10000)],
    "raw_extrinsic_metadata": lambda start, end: [(None, None)],
}


@patch("swh.storage.backfill.RANGE_GENERATORS", RANGE_GENERATORS)
def test_backfiller(
    swh_storage_backend_config,
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
    caplog,
):
    prefix1 = f"{kafka_prefix}-1"
    prefix2 = f"{kafka_prefix}-2"

    journal1 = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer-1",
        "prefix": prefix1,
    }
    swh_storage_backend_config["journal_writer"] = journal1
    storage = get_storage(**swh_storage_backend_config)
    # fill the storage and the journal (under prefix1)
    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(storage, object_type + "_add")
        method(objects)

    # now apply the backfiller on the storage to fill the journal under prefix2
    backfiller_config = {
        "journal_writer": {
            "brokers": [kafka_server],
            "client_id": "kafka_writer-2",
            "prefix": prefix2,
        },
        "storage": swh_storage_backend_config,
    }

    # Backfilling
    backfiller = JournalBackfiller(backfiller_config)
    for object_type in TEST_OBJECTS:
        backfiller.run(object_type, None, None)

    # Trace log messages for unhandled object types in the replayer
    caplog.set_level(logging.DEBUG, "swh.storage.replay")

    # now check journal content are the same under both topics
    # use the replayer scaffolding to fill storages to make is a bit easier
    # Replaying #1
    deserializer = ModelObjectDeserializer()
    sto1 = get_storage(cls="memory")
    replayer1 = JournalClient(
        brokers=kafka_server,
        group_id=f"{kafka_consumer_group}-1",
        prefix=prefix1,
        stop_on_eof=True,
        value_deserializer=deserializer.convert,
    )

    worker_fn1 = functools.partial(process_replay_objects, storage=sto1)
    replayer1.process(worker_fn1)

    # Replaying #2
    sto2 = get_storage(cls="memory")
    replayer2 = JournalClient(
        brokers=kafka_server,
        group_id=f"{kafka_consumer_group}-2",
        prefix=prefix2,
        stop_on_eof=True,
        value_deserializer=deserializer.convert,
    )
    worker_fn2 = functools.partial(process_replay_objects, storage=sto2)
    replayer2.process(worker_fn2)

    # Compare storages
    assert isinstance(sto1, InMemoryStorage)  # needed to help mypy
    assert isinstance(sto2, InMemoryStorage)
    check_replayed(sto1, sto2)

    for record in caplog.records:
        assert (
            "this should not happen" not in record.message
        ), "Replayer ignored some message types, see captured logging"
