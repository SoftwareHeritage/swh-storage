# Copyright (C) 2018-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from confluent_kafka import Consumer

from swh.storage import get_storage
from swh.model.model import Origin, OriginVisit
from swh.model.hypothesis_strategies import objects
from swh.journal.pytest_plugin import consume_messages, assert_all_objects_consumed
from swh.journal.tests.journal_data import TEST_OBJECTS

from swh.model.model import Person
from attr import asdict, has
from hypothesis import given
from hypothesis.strategies import lists


def test_storage_direct_writer(kafka_prefix: str, kafka_server, consumer: Consumer):

    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
        "anonymize": False,
    }
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "memory", "journal_writer": writer_config},],
    }

    storage = get_storage(**storage_config)

    expected_messages = 0

    for obj_type, objs in TEST_OBJECTS.items():
        method = getattr(storage, obj_type + "_add")
        if obj_type in (
            "content",
            "skipped_content",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
            "origin_visit_status",
        ):
            method(objs)
            expected_messages += len(objs)
        elif obj_type in ("origin_visit",):
            for obj in objs:
                assert isinstance(obj, OriginVisit)
                storage.origin_add_one(Origin(url=obj.origin))
                visit = method([obj])[0]
                expected_messages += 1 + 1  # 1 visit + 1 visit status

                obj_d = obj.to_dict()
                for k in ("visit", "origin", "date", "type"):
                    del obj_d[k]
                storage.origin_visit_update(obj.origin, visit.visit, **obj_d)
                expected_messages += 1 + 1  # 1 visit update + 1 visit status
        else:
            assert False, obj_type

    existing_topics = set(
        topic
        for topic in consumer.list_topics(timeout=10).topics.keys()
        if topic.startswith(f"{kafka_prefix}.")  # final . to exclude privileged topics
    )
    assert existing_topics == {
        f"{kafka_prefix}.{obj_type}"
        for obj_type in (
            "content",
            "directory",
            "origin",
            "origin_visit",
            "origin_visit_status",
            "release",
            "revision",
            "snapshot",
            "skipped_content",
        )
    }

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)


def test_storage_direct_writer_anonymized(
    kafka_prefix: str, kafka_server, consumer: Consumer
):

    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
        "anonymize": True,
    }
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "memory", "journal_writer": writer_config},],
    }

    storage = get_storage(**storage_config)

    expected_messages = 0

    for obj_type, objs in TEST_OBJECTS.items():
        if obj_type == "origin_visit":
            # these have non-consistent API and are unrelated with what we
            # want to test here
            continue
        method = getattr(storage, obj_type + "_add")
        method(objs)
        expected_messages += len(objs)

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
            "origin",
            "origin_visit",
            "origin_visit_status",
            "release",
            "revision",
            "snapshot",
            "skipped_content",
        )
    } | {
        f"{kafka_prefix}_privileged.{obj_type}" for obj_type in ("release", "revision",)
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
