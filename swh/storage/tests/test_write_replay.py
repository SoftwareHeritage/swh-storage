# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
from unittest.mock import patch

import attr
from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists

from swh.model.hypothesis_strategies import objects
from swh.model.model import Origin
from swh.storage import get_storage
from swh.storage.exc import HashCollision

from swh.storage.replay import process_replay_objects

from swh.journal.tests.utils import MockedJournalClient, MockedKafkaWriter


storage_config = {
    "cls": "memory",
    "journal_writer": {"cls": "memory"},
}


def empty_person_name_email(rev_or_rel):
    """Empties the 'name' and 'email' fields of the author/committer fields
    of a revision or release; leaving only the fullname."""
    if getattr(rev_or_rel, "author", None):
        rev_or_rel = attr.evolve(
            rev_or_rel, author=attr.evolve(rev_or_rel.author, name=b"", email=b"",)
        )

    if getattr(rev_or_rel, "committer", None):
        rev_or_rel = attr.evolve(
            rev_or_rel,
            committer=attr.evolve(rev_or_rel.committer, name=b"", email=b"",),
        )

    return rev_or_rel


@given(lists(objects(blacklist_types=("origin_visit_status",)), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_write_replay_same_order_batches(objects):
    queue = []
    replayer = MockedJournalClient(queue)

    with patch(
        "swh.journal.writer.inmemory.InMemoryJournalWriter",
        return_value=MockedKafkaWriter(queue),
    ):
        storage1 = get_storage(**storage_config)

    # Write objects to storage1
    for (obj_type, obj) in objects:
        if obj_type == "content" and obj.status == "absent":
            obj_type = "skipped_content"

        if obj_type == "origin_visit":
            storage1.origin_add_one(Origin(url=obj.origin))
            storage1.origin_visit_upsert([obj])
        else:
            method = getattr(storage1, obj_type + "_add")
            try:
                method([obj])
            except HashCollision:
                pass

    # Bail out early if we didn't insert any relevant objects...
    queue_size = len(queue)
    assert queue_size != 0, "No test objects found; hypothesis strategy bug?"

    assert replayer.stop_after_objects is None
    replayer.stop_after_objects = queue_size

    storage2 = get_storage(**storage_config)
    worker_fn = functools.partial(process_replay_objects, storage=storage2)

    replayer.process(worker_fn)

    assert replayer.consumer.committed

    for attr_name in (
        "_contents",
        "_directories",
        "_snapshots",
        "_origin_visits",
        "_origins",
    ):
        assert getattr(storage1, attr_name) == getattr(storage2, attr_name), attr_name

    # When hypothesis generates a revision and a release with same
    # author (or committer) fullname but different name or email, then
    # the storage will use the first name/email it sees.
    # This first one will be either the one from the revision or the release,
    # and since there is no order guarantees, storage2 has 1/2 chance of
    # not seeing the same order as storage1, therefore we need to strip
    # them out before comparing.
    for attr_name in ("_revisions", "_releases"):
        items1 = {
            k: empty_person_name_email(v)
            for (k, v) in getattr(storage1, attr_name).items()
        }
        items2 = {
            k: empty_person_name_email(v)
            for (k, v) in getattr(storage2, attr_name).items()
        }
        assert items1 == items2, attr_name
