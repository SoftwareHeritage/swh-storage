# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import call
import uuid

import pytest

from swh.core.db.db_utils import get_database_info
from swh.storage.proxies.masking.db import (
    DuplicateRequest,
    MaskedState,
    MaskedStatus,
    MaskingAdmin,
    MaskingQuery,
    RequestNotFound,
)
from swh.storage.tests.storage_data import StorageData


def test_db_version(masking_admin: MaskingAdmin):
    dbmodule, dbversion, dbflavor = get_database_info(masking_admin.conn.dsn)
    assert dbmodule == "storage.proxies.masking"
    assert dbversion == MaskingAdmin.current_version
    assert dbflavor is None


def test_create_find_request(masking_admin: MaskingAdmin):
    created = masking_admin.create_request(slug="foo", reason="bar")

    assert created.slug == "foo"
    assert created.reason == "bar"
    assert created.id is not None
    assert created.date is not None

    assert masking_admin.find_request("foo") == created


def test_create_request_conflict(masking_admin: MaskingAdmin):
    masking_admin.create_request(slug="foo", reason="bar")

    with pytest.raises(DuplicateRequest) as exc_info:
        masking_admin.create_request(slug="foo", reason="quux")

    assert exc_info.value.args == ("foo",)


def test_find_request_not_found(masking_admin):
    slug = "notfound"
    assert masking_admin.find_request(slug=slug) is None


def test_find_request_by_id(masking_admin: MaskingAdmin):
    created = masking_admin.create_request(slug="foo", reason="bar")

    assert masking_admin.find_request_by_id(created.id) == created


NON_EXISTING_UUID: uuid.UUID = uuid.UUID("da785a27-acab-4a35-b82a-a5ae3714407c")


def test_find_request_by_id_not_found(masking_admin: MaskingAdmin):
    assert masking_admin.find_request_by_id(NON_EXISTING_UUID) is None


@pytest.fixture
def populated_masking_admin(masking_admin):
    pending_request1 = masking_admin.create_request(slug="pending1", reason="one")
    masking_admin.set_object_state(
        request_id=pending_request1.id,
        new_state=MaskedState.DECISION_PENDING,
        swhids=[StorageData.content.swhid().to_extended()],
    )
    pending_request2 = masking_admin.create_request(slug="pending2", reason="two")
    masking_admin.set_object_state(
        request_id=pending_request2.id,
        new_state=MaskedState.VISIBLE,
        swhids=[StorageData.content.swhid().to_extended()],
    )
    masking_admin.set_object_state(
        request_id=pending_request2.id,
        new_state=MaskedState.DECISION_PENDING,
        swhids=[StorageData.content2.swhid().to_extended()],
    )
    masking_admin.create_request(slug="cleared", reason="handled")
    # We add no masks to this last one
    return masking_admin


def test_get_requests_excluding_cleared_requests(populated_masking_admin):
    assert [
        (request.slug, count)
        for request, count in populated_masking_admin.get_requests(
            include_cleared_requests=False
        )
    ] == [("pending2", 2), ("pending1", 1)]


def test_get_requests_including_cleared_requests(populated_masking_admin):
    assert [
        (request.slug, count)
        for request, count in populated_masking_admin.get_requests(
            include_cleared_requests=True
        )
    ] == [("cleared", 0), ("pending2", 2), ("pending1", 1)]


def test_get_states_for_request(populated_masking_admin):
    request = populated_masking_admin.find_request("pending2")
    states = populated_masking_admin.get_states_for_request(request.id)
    assert states == {
        StorageData.content.swhid().to_extended(): MaskedState.VISIBLE,
        StorageData.content2.swhid().to_extended(): MaskedState.DECISION_PENDING,
    }


def test_get_states_for_request_not_found(masking_admin: MaskingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        masking_admin.get_states_for_request(NON_EXISTING_UUID)
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_find_masks(populated_masking_admin: MaskingAdmin):
    swhids = [
        StorageData.content.swhid().to_extended(),
        StorageData.content2.swhid().to_extended(),
        # This one does not exist in the masking db
        StorageData.directory.swhid().to_extended(),
    ]
    masks = populated_masking_admin.find_masks(swhids)
    # The order in the output should be grouped by SWHID
    assert [(mask.swhid, mask.state, mask.request_slug) for mask in masks] == [
        (
            StorageData.content2.swhid().to_extended(),
            MaskedState.DECISION_PENDING,
            "pending2",
        ),
        (StorageData.content.swhid().to_extended(), MaskedState.VISIBLE, "pending2"),
        (
            StorageData.content.swhid().to_extended(),
            MaskedState.DECISION_PENDING,
            "pending1",
        ),
    ]


def test_delete_masks(populated_masking_admin):
    request = populated_masking_admin.find_request("pending2")
    populated_masking_admin.delete_masks(request.id)

    assert populated_masking_admin.get_states_for_request(request.id) == {}


def test_delete_masks_not_found(masking_admin: MaskingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        masking_admin.delete_masks(NON_EXISTING_UUID)
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_record_history(masking_admin: MaskingAdmin):
    messages = [f"message {i}" for i in range(3)]

    request = masking_admin.create_request(slug="foo", reason="bar")

    for message in messages:
        msg = masking_admin.record_history(request.id, message)
        assert msg.date is not None


def test_record_history_not_found(masking_admin: MaskingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        masking_admin.record_history(NON_EXISTING_UUID, "kaboom")
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_get_history(masking_admin: MaskingAdmin):
    request = masking_admin.create_request(slug="foo", reason="bar")
    masking_admin.record_history(request.id, "one")
    masking_admin.record_history(request.id, "two")
    masking_admin.record_history(request.id, "three")

    history = masking_admin.get_history(request.id)
    assert all(record.request == request.id for record in history)
    assert ["three", "two", "one"] == [record.message for record in history]


def test_get_history_not_found(masking_admin: MaskingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        masking_admin.get_history(NON_EXISTING_UUID)
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_swhid_lifecycle(masking_admin: MaskingAdmin, masking_query: MaskingQuery):
    # Create a request
    request = masking_admin.create_request(slug="foo", reason="bar")

    masked_swhids = [
        StorageData.content.swhid().to_extended(),
        StorageData.directory.swhid().to_extended(),
        StorageData.revision.swhid().to_extended(),
        StorageData.release.swhid().to_extended(),
        StorageData.snapshot.swhid().to_extended(),
        StorageData.origin.swhid(),
    ]

    all_swhids = masked_swhids + [
        StorageData.content2.swhid().to_extended(),
        StorageData.directory2.swhid().to_extended(),
        StorageData.revision2.swhid().to_extended(),
        StorageData.release2.swhid().to_extended(),
        StorageData.empty_snapshot.swhid().to_extended(),
        StorageData.origin2.swhid(),
    ]

    masking_admin.set_object_state(
        request_id=request.id,
        new_state=MaskedState.DECISION_PENDING,
        swhids=masked_swhids,
    )

    expected = {
        swhid: [MaskedStatus(state=MaskedState.DECISION_PENDING, request=request.id)]
        for swhid in masked_swhids
    }

    assert masking_query.swhids_are_masked(all_swhids) == expected
    assert dict(masking_query.iter_masked_swhids()) == expected

    restricted = masked_swhids[0:2]

    masking_admin.set_object_state(
        request_id=request.id, new_state=MaskedState.RESTRICTED, swhids=restricted
    )

    for swhid in restricted:
        expected[swhid] = [
            MaskedStatus(state=MaskedState.RESTRICTED, request=request.id)
        ]

    assert masking_query.swhids_are_masked(all_swhids) == expected
    assert dict(masking_query.iter_masked_swhids()) == expected

    visible = masked_swhids[2:4]

    masking_admin.set_object_state(
        request_id=request.id, new_state=MaskedState.VISIBLE, swhids=visible
    )

    for swhid in visible:
        del expected[swhid]

    assert masking_query.swhids_are_masked(all_swhids) == expected
    assert dict(masking_query.iter_masked_swhids()) == expected


def test_set_display_name(masking_admin: MaskingAdmin, masking_query: MaskingQuery):
    assert masking_query.display_name([b"author1@example.com"]) == {}
    assert masking_query.display_name([b"author2@example.com"]) == {}

    masking_admin.set_display_name(
        b"author1@example.com", b"author2 <author2@example.com>"
    )

    assert masking_query.display_name([b"author1@example.com"]) == {
        b"author1@example.com": b"author2 <author2@example.com>"
    }
    assert masking_query.display_name([b"author2@example.com"]) == {}


def test_query_metrics(
    masking_admin: MaskingAdmin, masking_query: MaskingQuery, mocker
):
    increment = mocker.patch("swh.core.statsd.statsd.increment")

    # Create a request
    request = masking_admin.create_request(slug="foo", reason="bar")

    masked_swhids = [
        StorageData.content.swhid().to_extended(),
        StorageData.directory.swhid().to_extended(),
        StorageData.revision.swhid().to_extended(),
        StorageData.release.swhid().to_extended(),
        StorageData.snapshot.swhid().to_extended(),
        StorageData.origin.swhid(),
    ]

    all_swhids = masked_swhids + [
        StorageData.content2.swhid().to_extended(),
        StorageData.directory2.swhid().to_extended(),
        StorageData.revision2.swhid().to_extended(),
        StorageData.release2.swhid().to_extended(),
        StorageData.empty_snapshot.swhid().to_extended(),
        StorageData.origin2.swhid(),
    ]

    # Query with no masked SWHIDs

    assert masking_query.swhids_are_masked(all_swhids) == {}
    increment.assert_called_once_with(
        "swh_storage_masking_queried_total", len(all_swhids)
    )
    increment.reset_mock()

    assert dict(masking_query.iter_masked_swhids()) == {}
    increment.assert_has_calls(
        [
            call("swh_storage_masking_list_requests_total", 1),
            call("swh_storage_masking_listed_total", 0),
        ]
    )
    increment.reset_mock()

    # Mask some SWHIDs

    masking_admin.set_object_state(
        request_id=request.id,
        new_state=MaskedState.DECISION_PENDING,
        swhids=masked_swhids,
    )

    # Query again

    assert len(masking_query.swhids_are_masked(all_swhids)) == len(masked_swhids)
    increment.assert_has_calls(
        [
            call("swh_storage_masking_queried_total", len(all_swhids)),
            call("swh_storage_masking_masked_total", len(masked_swhids)),
        ]
    )
    increment.reset_mock()

    assert set(dict(masking_query.iter_masked_swhids())) == set(masked_swhids)
    increment.assert_has_calls(
        [
            call("swh_storage_masking_list_requests_total", 1),
            call("swh_storage_masking_listed_total", len(masked_swhids)),
        ]
    )
