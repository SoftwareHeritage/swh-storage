# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import call
import uuid

import pytest

from swh.core.db.db_utils import get_database_info
from swh.storage.proxies.blocking.db import (
    BlockingAdmin,
    BlockingQuery,
    BlockingState,
    BlockingStatus,
    DuplicateRequest,
    RequestNotFound,
    get_urls_to_check,
)
from swh.storage.tests.storage_data import StorageData


@pytest.mark.parametrize(
    "url,exact,prefix",
    [
        pytest.param(
            "https://github.com/user1/test1.git",
            [
                "https://github.com/user1/test1.git",
                "https://github.com/user1/test1",
            ],
            ["https://github.com/user1", "https://github.com"],
            id="known-suffix-git",
        ),
        pytest.param(
            "https://foss.heptapod.net/user1/test1.hg",
            [
                "https://foss.heptapod.net/user1/test1.hg",
                "https://foss.heptapod.net/user1/test1",
            ],
            ["https://foss.heptapod.net/user1", "https://foss.heptapod.net"],
            id="known-suffix-hg",
        ),
        pytest.param(
            "svn+ssh://example.com/svnroot/users/user1/test1.svn",
            [
                "svn+ssh://example.com/svnroot/users/user1/test1.svn",
                "svn+ssh://example.com/svnroot/users/user1/test1",
            ],
            [
                "svn+ssh://example.com/svnroot/users/user1",
                "svn+ssh://example.com/svnroot/users",
                "svn+ssh://example.com/svnroot",
                "svn+ssh://example.com",
            ],
            id="known-suffix-svn",
        ),
        pytest.param(
            "https://github.com/user1/test1",
            [
                "https://github.com/user1/test1",
            ],
            ["https://github.com/user1", "https://github.com"],
            id="bare",
        ),
        pytest.param(
            "https://github.com/user1/test1/",
            [
                "https://github.com/user1/test1",
                "https://github.com/user1/test1/",
            ],
            ["https://github.com/user1", "https://github.com"],
            id="slash",
        ),
        pytest.param(
            "https://github.com/user1/test1.git/",
            [
                "https://github.com/user1/test1.git",
                "https://github.com/user1/test1.git/",
                "https://github.com/user1/test1",
            ],
            ["https://github.com/user1", "https://github.com"],
            id="slash-suffix",
        ),
    ],
)
def test_get_urls_to_check(url, exact, prefix):
    assert get_urls_to_check(url) == (exact, prefix)


def test_db_version(blocking_admin: BlockingAdmin):
    dbmodule, dbversion, dbflavor = get_database_info(blocking_admin.conn.dsn)
    assert dbmodule == "storage.proxies.blocking"
    assert dbversion == BlockingAdmin.current_version
    assert dbflavor is None


def test_create_find_request(blocking_admin: BlockingAdmin):
    created = blocking_admin.create_request(slug="foo", reason="bar")

    assert created.slug == "foo"
    assert created.reason == "bar"
    assert created.id is not None
    assert created.date is not None

    assert blocking_admin.find_request("foo") == created


def test_create_request_conflict(blocking_admin: BlockingAdmin):
    blocking_admin.create_request(slug="foo", reason="bar")

    with pytest.raises(DuplicateRequest) as exc_info:
        blocking_admin.create_request(slug="foo", reason="quux")

    assert exc_info.value.args == ("foo",)


def test_find_request_not_found(blocking_admin):
    slug = "notfound"
    assert blocking_admin.find_request(slug=slug) is None


def test_find_request_by_id(blocking_admin: BlockingAdmin):
    created = blocking_admin.create_request(slug="foo", reason="bar")

    assert blocking_admin.find_request_by_id(created.id) == created


NON_EXISTING_UUID: uuid.UUID = uuid.UUID("da785a27-acab-4a35-b82a-a5ae3714407c")


def test_find_request_by_id_not_found(blocking_admin: BlockingAdmin):
    assert blocking_admin.find_request_by_id(NON_EXISTING_UUID) is None


@pytest.fixture
def populated_blocking_admin(blocking_admin):
    pending_request1 = blocking_admin.create_request(slug="pending1", reason="one")
    blocking_admin.set_origins_state(
        request_id=pending_request1.id,
        new_state=BlockingState.DECISION_PENDING,
        urls=[StorageData.origin.url],
    )
    pending_request2 = blocking_admin.create_request(slug="pending2", reason="two")
    blocking_admin.set_origins_state(
        request_id=pending_request2.id,
        new_state=BlockingState.NON_BLOCKED,
        urls=[StorageData.origin.url],
    )
    blocking_admin.set_origins_state(
        request_id=pending_request2.id,
        new_state=BlockingState.DECISION_PENDING,
        urls=[StorageData.origin2.url],
    )
    blocking_admin.create_request(slug="cleared", reason="handled")
    # We add no blocks to this last one
    return blocking_admin


def test_get_requests_excluding_cleared_requests(populated_blocking_admin):
    assert [
        (request.slug, count)
        for request, count in populated_blocking_admin.get_requests(
            include_cleared_requests=False
        )
    ] == [("pending2", 2), ("pending1", 1)]


def test_get_requests_including_cleared_requests(populated_blocking_admin):
    assert [
        (request.slug, count)
        for request, count in populated_blocking_admin.get_requests(
            include_cleared_requests=True
        )
    ] == [("cleared", 0), ("pending2", 2), ("pending1", 1)]


def test_get_states_for_request(populated_blocking_admin):
    request = populated_blocking_admin.find_request("pending2")
    states = populated_blocking_admin.get_states_for_request(request.id)
    assert states == {
        StorageData.origin.url: BlockingState.NON_BLOCKED,
        StorageData.origin2.url: BlockingState.DECISION_PENDING,
    }


def test_get_states_for_request_not_found(blocking_admin: BlockingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        blocking_admin.get_states_for_request(NON_EXISTING_UUID)
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_find_blocking_states(populated_blocking_admin: BlockingAdmin):
    urls = [
        StorageData.origin.url,
        StorageData.origin2.url,
        # This one does not exist in the blocking db
        StorageData.origins[3].url,
    ]
    blocked_origins = populated_blocking_admin.find_blocking_states(urls)
    # The order in the output should be grouped by SWHID
    assert [
        (blocked.url_pattern, blocked.state, blocked.request_slug)
        for blocked in blocked_origins
    ] == [
        (StorageData.origin.url, BlockingState.NON_BLOCKED, "pending2"),
        (
            StorageData.origin.url,
            BlockingState.DECISION_PENDING,
            "pending1",
        ),
        (
            StorageData.origin2.url,
            BlockingState.DECISION_PENDING,
            "pending2",
        ),
    ]


def test_delete_blocking_states(populated_blocking_admin):
    request = populated_blocking_admin.find_request("pending2")
    populated_blocking_admin.delete_blocking_states(request.id)

    assert populated_blocking_admin.get_states_for_request(request.id) == {}


def test_delete_blocking_states_not_found(blocking_admin: BlockingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        blocking_admin.delete_blocking_states(NON_EXISTING_UUID)
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_record_history(blocking_admin: BlockingAdmin):
    messages = [f"message {i}" for i in range(3)]

    request = blocking_admin.create_request(slug="foo", reason="bar")

    for message in messages:
        msg = blocking_admin.record_history(request.id, message)
        assert msg.date is not None


def test_record_history_not_found(blocking_admin: BlockingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        blocking_admin.record_history(NON_EXISTING_UUID, "kaboom")
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_get_history(blocking_admin: BlockingAdmin):
    request = blocking_admin.create_request(slug="foo", reason="bar")
    blocking_admin.record_history(request.id, "one")
    blocking_admin.record_history(request.id, "two")
    blocking_admin.record_history(request.id, "three")

    history = blocking_admin.get_history(request.id)
    assert all(record.request == request.id for record in history)
    assert ["three", "two", "one"] == [record.message for record in history]


def test_get_history_not_found(blocking_admin: BlockingAdmin):
    with pytest.raises(RequestNotFound) as exc_info:
        blocking_admin.get_history(NON_EXISTING_UUID)
    assert exc_info.value.args == (NON_EXISTING_UUID,)


def test_swhid_lifecycle(blocking_admin: BlockingAdmin, blocking_query: BlockingQuery):
    # Create a request
    request = blocking_admin.create_request(slug="foo", reason="bar")

    all_origins = [origin.url for origin in StorageData.origins]
    blocked_origins = all_origins[::2]

    blocking_admin.set_origins_state(
        request_id=request.id,
        new_state=BlockingState.DECISION_PENDING,
        urls=blocked_origins,
    )

    expected = {
        url: BlockingStatus(state=BlockingState.DECISION_PENDING, request=request.id)
        for url in blocked_origins
    }

    assert blocking_query.origins_are_blocked(all_origins) == expected

    restricted = blocked_origins[0:2]

    blocking_admin.set_origins_state(
        request_id=request.id, new_state=BlockingState.BLOCKED, urls=restricted
    )

    for url in restricted:
        expected[url] = BlockingStatus(state=BlockingState.BLOCKED, request=request.id)

    assert blocking_query.origins_are_blocked(all_origins) == expected

    visible = blocked_origins[2:4]

    blocking_admin.set_origins_state(
        request_id=request.id, new_state=BlockingState.NON_BLOCKED, urls=visible
    )

    for url in visible:
        del expected[url]

    assert blocking_query.origins_are_blocked(all_origins) == expected

    for url in visible:
        expected[url] = BlockingStatus(
            state=BlockingState.NON_BLOCKED, request=request.id
        )

    assert (
        blocking_query.origins_are_blocked(all_origins, all_statuses=True) == expected
    )


def test_query_metrics(
    blocking_admin: BlockingAdmin, blocking_query: BlockingQuery, mocker
):
    increment = mocker.patch("swh.core.statsd.statsd.increment")

    # Create a request
    request = blocking_admin.create_request(slug="foo", reason="bar")

    all_origins = [origin.url for origin in StorageData.origins]
    blocked_origins = all_origins[::2]

    assert blocking_query.origins_are_blocked(all_origins) == {}
    assert increment.call_count == len(all_origins)
    increment.assert_has_calls(
        [call("swh_storage_blocking_queried_total", 1)] * len(all_origins)
    )

    increment.reset_mock()
    blocking_admin.set_origins_state(
        request_id=request.id,
        new_state=BlockingState.DECISION_PENDING,
        urls=blocked_origins,
    )

    assert len(blocking_query.origins_are_blocked(all_origins)) == len(blocked_origins)
    assert increment.call_args_list.count(
        call("swh_storage_blocking_queried_total", 1)
    ) == len(all_origins)
    assert increment.call_args_list.count(
        call("swh_storage_blocking_blocked_total", 1)
    ) == len(blocked_origins)
