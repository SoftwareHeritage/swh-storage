# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dataclasses
import datetime
import functools
import logging
import re
from typing import Any, Container, Dict, Optional, cast

import attr
import pytest

from swh.journal.client import JournalClient
from swh.journal.serializers import kafka_to_value, key_to_kafka, value_to_kafka
from swh.model.hashutil import DEFAULT_ALGORITHMS, MultiHash, hash_to_bytes, hash_to_hex
from swh.model.model import Revision, RevisionType
from swh.model.tests.swh_model_data import (
    COMMITTERS,
    DATES,
    DUPLICATE_CONTENTS,
    REVISIONS,
)
from swh.model.tests.swh_model_data import TEST_OBJECTS as _TEST_OBJECTS
from swh.storage import get_storage
from swh.storage.cassandra.model import ContentRow, SkippedContentRow
from swh.storage.exc import StorageArgumentException
from swh.storage.in_memory import InMemoryStorage
from swh.storage.replay import ModelObjectDeserializer, process_replay_objects

UTC = datetime.timezone.utc

TEST_OBJECTS = _TEST_OBJECTS.copy()
# add a revision with metadata to check this later is dropped while being replayed
TEST_OBJECTS["revision"] = list(_TEST_OBJECTS["revision"]) + [
    Revision(
        id=hash_to_bytes("51d9d94ab08d3f75512e3a9fd15132e0a7ca7928"),
        message=b"hello again",
        date=DATES[1],
        committer=COMMITTERS[1],
        author=COMMITTERS[0],
        committer_date=DATES[0],
        type=RevisionType.GIT,
        directory=b"\x03" * 20,
        synthetic=False,
        metadata={"something": "interesting"},
        parents=(REVISIONS[0].id,),
    ),
]
WRONG_ID_REG = re.compile(
    "Object has id [0-9a-f]{40}, but it should be [0-9a-f]{40}: .*"
)


def nullify_ctime(obj):
    if isinstance(obj, (ContentRow, SkippedContentRow)):
        return dataclasses.replace(obj, ctime=None)
    else:
        return obj


@pytest.fixture()
def replayer_storage_and_client(
    kafka_prefix: str, kafka_consumer_group: str, kafka_server: str
):
    journal_writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
    }
    storage_config: Dict[str, Any] = {
        "cls": "memory",
        "journal_writer": journal_writer_config,
    }
    storage = get_storage(**storage_config)
    deserializer = ModelObjectDeserializer()
    replayer = JournalClient(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_on_eof=True,
        value_deserializer=deserializer.convert,
    )

    yield storage, replayer


def test_storage_replayer(replayer_storage_and_client, caplog):
    """Optimal replayer scenario.

    This:
    - writes objects to a source storage
    - replayer consumes objects from the topic and replays them
    - a destination storage is filled from this

    In the end, both storages should have the same content.
    """
    src, replayer = replayer_storage_and_client

    # Fill Kafka using a source storage
    nb_sent = 0
    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(src, object_type + "_add")
        method(objects)
        if object_type == "origin_visit":
            nb_sent += len(objects)  # origin-visit-add adds origin-visit-status as well
        nb_sent += len(objects)

    caplog.set_level(logging.ERROR, "swh.journal.replay")

    # Fill the destination storage from Kafka
    dst = get_storage(cls="memory")
    worker_fn = functools.partial(process_replay_objects, storage=dst)
    nb_inserted = replayer.process(worker_fn)
    assert nb_sent == nb_inserted

    assert isinstance(src, InMemoryStorage)  # needed to help mypy
    assert isinstance(dst, InMemoryStorage)
    check_replayed(src, dst)

    collision = 0
    for record in caplog.records:
        logtext = record.getMessage()
        if "Colliding contents:" in logtext:
            collision += 1

    assert collision == 0, "No collision should be detected"


def test_storage_replay_with_collision(replayer_storage_and_client, caplog):
    """Another replayer scenario with collisions.

    This:
    - writes objects to the topic, including colliding contents
    - replayer consumes objects from the topic and replay them
    - This drops the colliding contents from the replay when detected

    """
    src, replayer = replayer_storage_and_client

    # Fill Kafka using a source storage
    nb_sent = 0
    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(src, object_type + "_add")
        method(objects)
        if object_type == "origin_visit":
            nb_sent += len(objects)  # origin-visit-add adds origin-visit-status as well
        nb_sent += len(objects)

    # Create collision in input data
    # These should not be written in the destination
    producer = src.journal_writer.journal.producer
    prefix = src.journal_writer.journal._prefix
    for content in DUPLICATE_CONTENTS:
        topic = f"{prefix}.content"
        key = content.sha1
        now = datetime.datetime.now(tz=UTC)
        content = attr.evolve(content, ctime=now)
        producer.produce(
            topic=topic,
            key=key_to_kafka(key),
            value=value_to_kafka(content.to_dict()),
        )
        nb_sent += 1

    producer.flush()

    caplog.set_level(logging.ERROR, "swh.journal.replay")

    # Fill the destination storage from Kafka
    dst = get_storage(cls="memory")
    worker_fn = functools.partial(process_replay_objects, storage=dst)
    nb_inserted = replayer.process(worker_fn)
    assert nb_sent == nb_inserted

    # check the logs for the collision being properly detected
    nb_collisions = 0
    actual_collision: Dict
    for record in caplog.records:
        logtext = record.getMessage()
        if "Collision detected:" in logtext:
            nb_collisions += 1
            actual_collision = record.args["collision"]

    assert nb_collisions == 1, "1 collision should be detected"

    algo = "sha1"
    assert actual_collision["algo"] == algo
    expected_colliding_hash = hash_to_hex(DUPLICATE_CONTENTS[0].get_hash(algo))
    assert actual_collision["hash"] == expected_colliding_hash

    actual_colliding_hashes = actual_collision["objects"]
    assert len(actual_colliding_hashes) == len(DUPLICATE_CONTENTS)
    for content in DUPLICATE_CONTENTS:
        expected_content_hashes = {
            k: hash_to_hex(v) for k, v in content.hashes().items()
        }
        assert expected_content_hashes in actual_colliding_hashes

    # all objects from the src should exists in the dst storage
    assert isinstance(src, InMemoryStorage)  # needed to help mypy
    assert isinstance(dst, InMemoryStorage)  # needed to help mypy
    check_replayed(src, dst, exclude=["contents"])
    # but the dst has one content more (one of the 2 colliding ones)
    assert (
        len(list(src._cql_runner._contents.iter_all()))
        == len(list(dst._cql_runner._contents.iter_all())) - 1
    )


def test_replay_skipped_content(replayer_storage_and_client):
    """Test the 'skipped_content' topic is properly replayed."""
    src, replayer = replayer_storage_and_client
    _check_replay_skipped_content(src, replayer, "skipped_content")


# utility functions


def check_replayed(
    src: InMemoryStorage,
    dst: InMemoryStorage,
    exclude: Optional[Container] = None,
    expected_anonymized=False,
):
    """Simple utility function to compare the content of 2 in_memory storages"""

    def fix_expected(attr, row):
        if expected_anonymized:
            if attr == "releases":
                row = dataclasses.replace(
                    row, author=row.author and row.author.anonymize()
                )
            elif attr == "revisions":
                row = dataclasses.replace(
                    row,
                    author=row.author.anonymize(),
                    committer=row.committer.anonymize(),
                )
        if attr == "revisions":
            # the replayer should now drop the metadata attribute; see
            # swh/storgae/replay.py:_insert_objects()
            row.metadata = "null"

        return row

    for attr_ in (
        "contents",
        "skipped_contents",
        "directories",
        "extid",
        "revisions",
        "releases",
        "snapshots",
        "origins",
        "origin_visits",
        "origin_visit_statuses",
        "raw_extrinsic_metadata",
    ):
        if exclude and attr_ in exclude:
            continue
        expected_objects = [
            (id, nullify_ctime(fix_expected(attr_, obj)))
            for id, obj in sorted(getattr(src._cql_runner, f"_{attr_}").iter_all())
        ]
        got_objects = [
            (id, nullify_ctime(obj))
            for id, obj in sorted(getattr(dst._cql_runner, f"_{attr_}").iter_all())
        ]
        assert got_objects == expected_objects, f"Mismatch object list for {attr_}"


def _check_replay_skipped_content(storage, replayer, topic):
    skipped_contents = _gen_skipped_contents(100)
    nb_sent = len(skipped_contents)
    producer = storage.journal_writer.journal.producer
    prefix = storage.journal_writer.journal._prefix

    for i, obj in enumerate(skipped_contents):
        producer.produce(
            topic=f"{prefix}.{topic}",
            key=key_to_kafka({"sha1": obj["sha1"]}),
            value=value_to_kafka(obj),
        )
    producer.flush()

    dst_storage = get_storage(cls="memory")
    worker_fn = functools.partial(process_replay_objects, storage=dst_storage)
    nb_inserted = replayer.process(worker_fn)

    assert nb_sent == nb_inserted
    for content in skipped_contents:
        assert not storage.content_find({"sha1": content["sha1"]})

    # no skipped_content_find API endpoint, so use this instead
    assert not list(dst_storage.skipped_content_missing(skipped_contents))


def _updated(d1, d2):
    d1.update(d2)
    d1.pop("data", None)
    return d1


def _gen_skipped_contents(n=10):
    # we do not use the hypothesis strategy here because this does not play well with
    # pytest fixtures, and it makes test execution very slow
    algos = DEFAULT_ALGORITHMS | {"length"}
    now = datetime.datetime.now(tz=UTC)
    return [
        _updated(
            MultiHash.from_data(data=f"foo{i}".encode(), hash_names=algos).digest(),
            {
                "status": "absent",
                "reason": "why not",
                "origin": f"https://somewhere/{i}",
                "ctime": now,
            },
        )
        for i in range(n)
    ]


@pytest.mark.parametrize("privileged", [True, False])
def test_storage_replay_anonymized(
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
    privileged: bool,
):
    """Optimal replayer scenario.

    This:
    - writes objects to the topic
    - replayer consumes objects from the topic and replay them

    This tests the behavior with both a privileged and non-privileged replayer
    """
    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
        "anonymize": True,
    }
    src_config: Dict[str, Any] = {"cls": "memory", "journal_writer": writer_config}

    storage = get_storage(**src_config)

    # Fill the src storage
    nb_sent = 0
    for obj_type, objs in TEST_OBJECTS.items():
        if obj_type in ("origin_visit", "origin_visit_status"):
            # these are unrelated with what we want to test here
            continue
        method = getattr(storage, obj_type + "_add")
        method(objs)
        nb_sent += len(objs)

    # Fill a destination storage from Kafka, potentially using privileged topics
    dst_storage = get_storage(cls="memory")
    deserializer = ModelObjectDeserializer(
        validate=False
    )  # we cannot validate an anonymized replay
    replayer = JournalClient(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_after_objects=nb_sent,
        privileged=privileged,
        value_deserializer=deserializer.convert,
    )
    worker_fn = functools.partial(process_replay_objects, storage=dst_storage)

    nb_inserted = replayer.process(worker_fn)
    replayer.consumer.commit()

    assert nb_sent == nb_inserted
    # Check the contents of the destination storage, and whether the anonymization was
    # properly used
    assert isinstance(storage, InMemoryStorage)  # needed to help mypy
    assert isinstance(dst_storage, InMemoryStorage)
    check_replayed(storage, dst_storage, expected_anonymized=not privileged)


def test_storage_replayer_with_validation_ok(
    replayer_storage_and_client, caplog, redisdb
):
    """Optimal replayer scenario

    with validation activated and reporter set to a redis db.

    - writes objects to a source storage
    - replayer consumes objects from the topic and replays them
    - a destination storage is filled from this
    - nothing has been reported in the redis db
    - both storages should have the same content
    """
    src, replayer = replayer_storage_and_client
    replayer.deserializer = ModelObjectDeserializer(validate=True, reporter=redisdb.set)

    # Fill Kafka using a source storage
    nb_sent = 0
    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(src, object_type + "_add")
        method(objects)
        if object_type == "origin_visit":
            nb_sent += len(objects)  # origin-visit-add adds origin-visit-status as well
        nb_sent += len(objects)

    # Fill the destination storage from Kafka
    dst = get_storage(cls="memory")
    worker_fn = functools.partial(process_replay_objects, storage=dst)
    nb_inserted = replayer.process(worker_fn)
    assert nb_sent == nb_inserted

    # check we do not have invalid objects reported
    invalid = 0
    for record in caplog.records:
        logtext = record.getMessage()
        if WRONG_ID_REG.match(logtext):
            invalid += 1
    assert invalid == 0, "Invalid objects should not be detected"
    assert not redisdb.keys()
    # so the dst should be the same as src storage
    check_replayed(cast(InMemoryStorage, src), cast(InMemoryStorage, dst))


def test_storage_replayer_with_validation_nok(
    replayer_storage_and_client, caplog, redisdb
):
    """Replayer scenario with invalid objects

    with validation and reporter set to a redis db.

    - writes objects to a source storage
    - replayer consumes objects from the topic and replays them
    - the destination storage is filled with only valid objects
    - the redis db contains the invalid (raw kafka mesg) objects
    """
    src, replayer = replayer_storage_and_client
    replayer.value_deserializer = ModelObjectDeserializer(
        validate=True, reporter=redisdb.set
    ).convert

    caplog.set_level(logging.ERROR, "swh.journal.replay")

    # Fill Kafka using a source storage
    nb_sent = 0
    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(src, object_type + "_add")
        method(objects)
        if object_type == "origin_visit":
            nb_sent += len(objects)  # origin-visit-add adds origin-visit-status as well
        nb_sent += len(objects)

    # insert invalid objects
    for object_type in ("revision", "directory", "release", "snapshot"):
        method = getattr(src, object_type + "_add")
        method([attr.evolve(TEST_OBJECTS[object_type][0], id=b"\x00" * 20)])
        nb_sent += 1

    # Fill the destination storage from Kafka
    dst = get_storage(cls="memory")
    worker_fn = functools.partial(process_replay_objects, storage=dst)
    nb_inserted = replayer.process(worker_fn)
    assert nb_sent == nb_inserted

    # check we do have invalid objects reported
    invalid = 0
    for record in caplog.records:
        logtext = record.getMessage()
        if WRONG_ID_REG.match(logtext):
            invalid += 1
    assert invalid == 4, "Invalid objects should be detected"
    assert set(redisdb.keys()) == {
        f"swh:1:{typ}:{'0'*40}".encode() for typ in ("rel", "rev", "snp", "dir")
    }

    for key in redisdb.keys():
        # check the stored value looks right
        rawvalue = redisdb.get(key)
        value = kafka_to_value(rawvalue)
        assert isinstance(value, dict)
        assert "id" in value
        assert value["id"] == b"\x00" * 20

    # check that invalid objects did not reach the dst storage
    for attr_ in (
        "directories",
        "revisions",
        "releases",
        "snapshots",
    ):
        for id, obj in sorted(getattr(dst._cql_runner, f"_{attr_}").iter_all()):
            assert id != b"\x00" * 20


def test_storage_replayer_with_validation_nok_raises(
    replayer_storage_and_client, caplog, redisdb
):
    """Replayer scenario with invalid objects

    with raise_on_error set to True

    This:
    - writes both valid & invalid objects to a source storage
    - a StorageArgumentException should be raised while replayer consumes
      objects from the topic and replays them
    """
    src, replayer = replayer_storage_and_client
    replayer.value_deserializer = ModelObjectDeserializer(
        validate=True, reporter=redisdb.set, raise_on_error=True
    ).convert

    caplog.set_level(logging.ERROR, "swh.journal.replay")

    # Fill Kafka using a source storage
    nb_sent = 0
    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(src, object_type + "_add")
        method(objects)
        if object_type == "origin_visit":
            nb_sent += len(objects)  # origin-visit-add adds origin-visit-status as well
        nb_sent += len(objects)

    # insert invalid objects
    for object_type in ("revision", "directory", "release", "snapshot"):
        method = getattr(src, object_type + "_add")
        method([attr.evolve(TEST_OBJECTS[object_type][0], id=b"\x00" * 20)])
        nb_sent += 1

    # Fill the destination storage from Kafka
    dst = get_storage(cls="memory")
    worker_fn = functools.partial(process_replay_objects, storage=dst)
    with pytest.raises(StorageArgumentException):
        replayer.process(worker_fn)

    # check we do have invalid objects reported
    invalid = 0
    for record in caplog.records:
        logtext = record.getMessage()
        if WRONG_ID_REG.match(logtext):
            invalid += 1
    assert invalid == 1, "One invalid objects should be detected"
    assert len(redisdb.keys()) == 1
