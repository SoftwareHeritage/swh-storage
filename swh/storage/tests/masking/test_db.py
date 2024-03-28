# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import call
import uuid

import pytest

from swh.storage.proxies.masking.db import (
    DuplicateRequest,
    MaskedState,
    MaskedStatus,
    MaskingAdmin,
    MaskingQuery,
    RequestNotFound,
)
from swh.storage.tests.storage_data import StorageData


def test_create_find_request(masking_admin: MaskingAdmin):
    created = masking_admin.create_request(slug="foo", reason="bar")

    assert created.slug == "foo"
    assert created.reason == "bar"
    assert created.id is not None
    assert created.date is not None

    assert masking_admin.find_request("foo") == created


def test_find_request_not_found(masking_admin):
    slug = "notfound"
    assert masking_admin.find_request(slug=slug) is None


def test_create_request_conflict(masking_admin: MaskingAdmin):
    masking_admin.create_request(slug="foo", reason="bar")

    with pytest.raises(DuplicateRequest) as exc_info:
        masking_admin.create_request(slug="foo", reason="quux")

    assert exc_info.value.args == ("foo",)


def test_record_history(masking_admin: MaskingAdmin):
    messages = [f"message {i}" for i in range(3)]

    request = masking_admin.create_request(slug="foo", reason="bar")

    for message in messages:
        msg = masking_admin.record_history(request.id, message)
        assert msg.date is not None


def test_record_history_not_found(masking_admin: MaskingAdmin):
    request_id = uuid.uuid4()

    with pytest.raises(RequestNotFound) as exc_info:
        masking_admin.record_history(request_id, "kaboom")

    assert exc_info.value.args == (request_id,)


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

    restricted = masked_swhids[0:2]

    masking_admin.set_object_state(
        request_id=request.id, new_state=MaskedState.RESTRICTED, swhids=restricted
    )

    for swhid in restricted:
        expected[swhid] = [
            MaskedStatus(state=MaskedState.RESTRICTED, request=request.id)
        ]

    assert masking_query.swhids_are_masked(all_swhids) == expected

    visible = masked_swhids[2:4]

    masking_admin.set_object_state(
        request_id=request.id, new_state=MaskedState.VISIBLE, swhids=visible
    )

    for swhid in visible:
        del expected[swhid]

    assert masking_query.swhids_are_masked(all_swhids) == expected


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

    assert masking_query.swhids_are_masked(all_swhids) == {}
    increment.assert_called_once_with(
        "swh_storage_masking_queried_total", len(all_swhids)
    )
    increment.reset_mock()

    masking_admin.set_object_state(
        request_id=request.id,
        new_state=MaskedState.DECISION_PENDING,
        swhids=masked_swhids,
    )

    assert len(masking_query.swhids_are_masked(all_swhids)) == len(masked_swhids)
    increment.assert_has_calls(
        [
            call("swh_storage_masking_queried_total", len(all_swhids)),
            call("swh_storage_masking_masked_total", len(masked_swhids)),
        ]
    )
