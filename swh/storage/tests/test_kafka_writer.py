# Copyright (C) 2018-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from confluent_kafka import Consumer

from swh.storage import get_storage
from swh.model.model import Origin, OriginVisit
from swh.journal.pytest_plugin import consume_messages, assert_all_objects_consumed
from swh.journal.tests.journal_data import TEST_OBJECTS


def test_storage_direct_writer(kafka_prefix: str, kafka_server, consumer: Consumer):

    writer_config = {
        "cls": "kafka",
        "brokers": [kafka_server],
        "client_id": "kafka_writer",
        "prefix": kafka_prefix,
    }
    storage_config = {
        "cls": "pipeline",
        "steps": [{"cls": "memory", "journal_writer": writer_config},],
    }

    storage = get_storage(**storage_config)

    expected_messages = 0

    for object_type, objects in TEST_OBJECTS.items():
        method = getattr(storage, object_type + "_add")
        if object_type in (
            "content",
            "skipped_content",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
        ):
            method(objects)
            expected_messages += len(objects)
        elif object_type in ("origin_visit",):
            for obj in objects:
                assert isinstance(obj, OriginVisit)
                storage.origin_add_one(Origin(url=obj.origin))
                visit = method(obj.origin, date=obj.date, type=obj.type)
                expected_messages += 1

                obj_d = obj.to_dict()
                for k in ("visit", "origin", "date", "type"):
                    del obj_d[k]
                storage.origin_visit_update(obj.origin, visit.visit, **obj_d)
                expected_messages += 1
        else:
            assert False, object_type

    consumed_messages = consume_messages(consumer, kafka_prefix, expected_messages)
    assert_all_objects_consumed(consumed_messages)
