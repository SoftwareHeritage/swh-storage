# Copyright (C) 2018-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List

from attr import asdict, has
from confluent_kafka import Consumer
from hypothesis import given
from hypothesis.strategies import lists
import pytest

from swh.journal.pytest_plugin import assert_all_objects_consumed, consume_messages
from swh.model.hypothesis_strategies import objects
from swh.model.model import (
    Content,
    Directory,
    ExtID,
    MetadataAuthority,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)
from swh.model.tests.swh_model_data import TEST_OBJECTS
from swh.storage import get_storage

from ..in_memory import InMemoryStorage
from .storage_data import DEFAULT_ALGORITHMS, StorageData


def get_storage_with_writer(
    kafka_prefix: str, kafka_server: str, anonymize: bool
) -> InMemoryStorage:
    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
        "anonymize": anonymize,
        "auto_flush": False,
    }
    storage_config: Dict[str, Any] = {
        "cls": "pipeline",
        "steps": [
            {"cls": "memory", "journal_writer": writer_config},
        ],
    }

    ret = get_storage(**storage_config)
    assert isinstance(ret, InMemoryStorage)
    return ret


def test_storage_direct_writer(kafka_prefix: str, kafka_server, consumer: Consumer):

    storage = get_storage_with_writer(
        kafka_prefix=kafka_prefix, kafka_server=kafka_server, anonymize=False
    )

    object_types = (
        Content.object_type,
        SkippedContent.object_type,
        Directory.object_type,
        ExtID.object_type,
        MetadataAuthority.object_type,
        MetadataFetcher.object_type,
        Revision.object_type,
        Release.object_type,
        Snapshot.object_type,
        Origin.object_type,
        OriginVisit.object_type,
        OriginVisitStatus.object_type,
        RawExtrinsicMetadata.object_type,
    )

    expected_messages = 0

    for obj_type, objs in TEST_OBJECTS.items():
        method = getattr(storage, f"{obj_type}_add")
        if obj_type in object_types:
            method(objs)
            expected_messages += len(objs)
        else:
            assert False, obj_type
    storage.journal_writer.flush()

    existing_topics = set(
        topic
        for topic in consumer.list_topics(timeout=10).topics.keys()
        if topic.startswith(f"{kafka_prefix}.")  # final . to exclude privileged topics
    )
    assert existing_topics == {
        f"{kafka_prefix}.{obj_type}" for obj_type in object_types
    }

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)


@pytest.mark.parametrize(
    "object_types",
    (set(k.value for k in TEST_OBJECTS.keys()) | {"hash_colliding_content"},),
)  # consume from the hash_colliding_content topic, which is not listened to by default
@pytest.mark.parametrize("colliding_hash", DEFAULT_ALGORITHMS)
def test_storage_direct_writer_content_hash_collision_sequential(
    kafka_prefix: str,
    kafka_server,
    consumer: Consumer,
    sample_data: StorageData,
    colliding_hash: str,
):
    storage = get_storage_with_writer(
        kafka_prefix=kafka_prefix, kafka_server=kafka_server, anonymize=False
    )
    for content in sample_data.colliding_contents[colliding_hash]:
        storage.content_add([content])
    assert storage.journal_writer.journal is not None
    storage.journal_writer.journal.flush()

    expected_messages = 4

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    colliding_no_ctime = [
        content.evolve(ctime=None)
        for content in sample_data.colliding_contents[colliding_hash]
    ]

    for msg in consumed_messages["hash_colliding_contents"]:
        assert Content.from_dict(msg[1]) in colliding_no_ctime


@pytest.mark.parametrize(
    "object_types",
    (set(k.value for k in TEST_OBJECTS.keys()) | {"hash_colliding_content"},),
)  # consume from the hash_colliding_content topic, which is not listened to by default
def test_storage_direct_writer_content_hash_collision_same_batch(
    kafka_prefix: str,
    kafka_server,
    consumer: Consumer,
    sample_data: StorageData,
):
    storage = get_storage_with_writer(
        kafka_prefix=kafka_prefix, kafka_server=kafka_server, anonymize=False
    )
    all_colliding: List[Content] = sum(sample_data.colliding_contents.values(), [])
    storage.content_add(all_colliding)
    assert storage.journal_writer.journal is not None
    storage.journal_writer.journal.flush()

    expected_messages = 2 * len(all_colliding)

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    colliding_no_ctime = [content.evolve(ctime=None) for content in all_colliding]

    for msg in consumed_messages["hash_colliding_contents"]:
        assert Content.from_dict(msg[1]) in colliding_no_ctime


def test_storage_direct_writer_anonymized(
    kafka_prefix: str, kafka_server, consumer: Consumer
):
    storage = get_storage_with_writer(
        kafka_prefix=kafka_prefix, kafka_server=kafka_server, anonymize=True
    )

    expected_messages = 0

    for obj_type, objs in TEST_OBJECTS.items():
        if obj_type == OriginVisit.object_type:
            # these have non-consistent API and are unrelated with what we
            # want to test here
            continue
        method = getattr(storage, f"{obj_type}_add")
        method(objs)
        expected_messages += len(objs)
    storage.journal_writer.flush()

    existing_topics = set(
        topic
        for topic in consumer.list_topics(timeout=10).topics.keys()
        if topic.startswith(kafka_prefix)
    )
    assert existing_topics == {
        f"{kafka_prefix}.{obj_type}"
        for obj_type in (
            "content",
            "directory",
            "extid",
            "metadata_authority",
            "metadata_fetcher",
            "origin",
            "origin_visit",
            "origin_visit_status",
            "raw_extrinsic_metadata",
            "release",
            "revision",
            "snapshot",
            "skipped_content",
        )
    } | {
        f"{kafka_prefix}_privileged.{obj_type}"
        for obj_type in (
            "release",
            "revision",
        )
    }


def check_anonymized_obj(obj):
    if has(obj):
        if isinstance(obj, Person):
            assert obj.name is None
            assert obj.email is None
            assert len(obj.fullname) == 32
        else:
            for key, value in asdict(obj, recurse=False).items():
                check_anonymized_obj(value)


@given(lists(objects(split_content=True)))
def test_anonymizer(obj_type_and_objs):
    for obj_type, obj in obj_type_and_objs:
        check_anonymized_obj(obj.anonymize())
