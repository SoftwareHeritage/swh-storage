# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from itertools import chain
from typing import Iterable

import pytest

from swh.model.model import Origin, OriginVisit, OriginVisitStatus
from swh.storage.exc import BlockedOriginException
from swh.storage.proxies.blocking import BlockingProxyStorage
from swh.storage.proxies.blocking.db import BlockingState


def now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@pytest.fixture
def swh_storage_backend_config():
    return {
        "cls": "memory",
        "journal_writer": {
            "cls": "memory",
        },
    }


@pytest.fixture
def swh_storage(blocking_db_postgresql, swh_storage_backend):
    return BlockingProxyStorage(
        blocking_db=blocking_db_postgresql.info.dsn, storage=swh_storage_backend
    )


def set_origin_visibility(blocking_admin, slug="foo", reason="bar"):
    # Create a request
    request = blocking_admin.create_request(slug=slug, reason=reason)

    def set_visibility(urls: Iterable[str], new_state: BlockingState):
        blocking_admin.set_origins_state(
            request_id=request.id,
            new_state=new_state,
            urls=list(urls),
        )

    return request, set_visibility


def mk_origin_visit(origin: Origin):
    return OriginVisit(origin=origin.url, date=now(), type="git")


def mk_origin_visit_status(origin: Origin):
    return OriginVisitStatus(
        origin=origin.url, visit=1, date=now(), status="created", snapshot=None
    )


OBJ_FACTORY = {
    "origin_add": lambda x: x,
    "origin_visit_add": mk_origin_visit,
    "origin_visit_status_add": mk_origin_visit_status,
}


@pytest.mark.parametrize(
    "endpoint", ["origin_add", "origin_visit_add", "origin_visit_status_add"]
)
def test_blocking_prefix_match_simple(swh_storage, blocking_admin, endpoint):
    request, set_visibility = set_origin_visibility(blocking_admin)
    method = getattr(swh_storage, endpoint)
    obj_factory = OBJ_FACTORY[endpoint]
    backend_storage = swh_storage.storage

    # beware of the mix of example.com vs example.org URLs below
    url_patterns = {
        BlockingState.DECISION_PENDING: ["https://example.com/user1/repo1"],
        BlockingState.BLOCKED: ["https://example.org/user1"],
    }
    for decision, urls in url_patterns.items():
        set_visibility(urls, decision)

    origins = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user1/repo1/",
            "https://example.com/user1/repo1.git",
            "https://example.com/user1/repo1.git/",
            "https://example.com/user1/repo1/subrepo1",
            "https://example.com/user1/repo1/subrepo1.git",
        ],
        BlockingState.BLOCKED: [
            "https://example.org/user1",
            "https://example.org/user1/repo1",
        ],
        BlockingState.NON_BLOCKED: [
            "https://example.com/user1",
            "https://example.com/user1/repo2",
            "https://example.com/user1/repo2/",
            "https://example.com/user1/repo2.git",
            "https://example.com/user1/repo2.git/",
            "https://example.com/user1/repo2/subrepo1",
            "https://example.com/user1/repo2/subrepo1.git",
            "https://example.com/user2/repo1",
            "https://example.com/user2/repo1",
            "https://example.com/user11",
            "https://example.com/user11/repo1",
            "https://example.org/user11",
            "https://example.org/user11/repo1",
        ],
    }
    all_origins = [Origin(url) for url in chain(*(origins.values()))]

    if endpoint != "origin_add":
        # if the endpoint is not 'origin_add', actually insert the origins; no
        # visit or visit status should be able to be inserted nonetheless
        assert backend_storage.origin_add(all_origins)
    if endpoint == "origin_visit_status_add":
        # if the endpoint is 'origin_visit_status_add', actually insert the
        # origin visits in the backend; but no visit status should be able to
        # be inserted via the proxy
        assert backend_storage.origin_visit_add(
            [mk_origin_visit(origin) for origin in all_origins]
        )

    for state, urls in origins.items():
        if state != BlockingState.NON_BLOCKED:
            for url in urls:
                origin = Origin(url=url)
                with pytest.raises(BlockedOriginException) as exc_info:
                    method([obj_factory(origin)])
                    print("URL should not be inserted", origin.url)
                assert {
                    url: status.state for url, status in exc_info.value.blocked.items()
                } == {origin.url: state}
        else:
            # this should not be blocked
            for url in urls:
                origin = Origin(url=url)
                assert method([obj_factory(origin)])


@pytest.mark.parametrize(
    "endpoint", ["origin_add", "origin_visit_add", "origin_visit_status_add"]
)
def test_blocking_prefix_match_with_overload(swh_storage, blocking_admin, endpoint):
    request, set_visibility = set_origin_visibility(blocking_admin)
    method = getattr(swh_storage, endpoint)
    obj_factory = OBJ_FACTORY[endpoint]
    backend_storage = swh_storage.storage

    url_patterns = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user2",
        ],
        BlockingState.NON_BLOCKED: [
            "https://example.com/user2/repo1",
        ],
        BlockingState.BLOCKED: [
            "https://example.com/user2/repo1/subrepo1",
        ],
    }

    for decision, urls in url_patterns.items():
        set_visibility(urls, decision)

    origins = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user1/repo1/",
            "https://example.com/user1/repo1.git",
            "https://example.com/user1/repo1.git/",
            "https://example.com/user1/repo1/subrepo1",
            "https://example.com/user1/repo1/subrepo1.git",
            # everything under https://example.com/user2 should be blocked (but
            # https://example.com/user2/repo1)
            "https://example.com/user2",
            "https://example.com/user2.git",
            "https://example.com/user2.git/",
            "https://example.com/user2/",
            "https://example.com/user2/repo2",
            "https://example.com/user2/repo2/",
            "https://example.com/user2/repo2.git",
            "https://example.com/user2/repo2.git/",
        ],
        BlockingState.BLOCKED: [
            # "https://example.com/user2/repo1/subrepo1 is explicitly blocked
            "https://example.com/user2/repo1/subrepo1",
            "https://example.com/user2/repo1/subrepo1.git",
            "https://example.com/user2/repo1/subrepo1/subsubrepo",
        ],
        BlockingState.NON_BLOCKED: [
            "https://example.com/user1",
            "https://example.com/user1/repo2",
            # https://example.com/user2/repo1 is explicitly enabled
            "https://example.com/user2/repo1",
            "https://example.com/user2/repo1/",
            "https://example.com/user2/repo1.git",
            "https://example.com/user2/repo1/subrepo2",
        ],
    }
    all_origins = [Origin(url) for url in chain(*(origins.values()))]

    if endpoint != "origin_add":
        # if the endpoint is not 'origin_add', actually insert the origins; no
        # visit or visit status should be able to be inserted nonetheless
        assert backend_storage.origin_add(all_origins)
    if endpoint == "origin_visit_status_add":
        # if the endpoint is 'origin_visit_status_add', actually insert the
        # origin visits in the backend; but no visit status should be able to
        # be inserted via the proxy
        assert backend_storage.origin_visit_add(
            [mk_origin_visit(origin) for origin in all_origins]
        )

    for state, urls in origins.items():
        if state != BlockingState.NON_BLOCKED:
            for url in urls:
                origin = Origin(url=url)
                with pytest.raises(BlockedOriginException) as exc_info:
                    method([obj_factory(origin)])
                    print("URL should not be inserted", origin.url)
                assert {
                    url: status.state for url, status in exc_info.value.blocked.items()
                } == {origin.url: state}
        else:
            # this should not be blocked
            for url in urls:
                origin = Origin(url=url)
                assert method([obj_factory(origin)])


@pytest.mark.parametrize(
    "endpoint", ["origin_add", "origin_visit_add", "origin_visit_status_add"]
)
def test_blocking_prefix_match_add_multi(swh_storage, blocking_admin, endpoint):
    request, set_visibility = set_origin_visibility(blocking_admin)
    method = getattr(swh_storage, endpoint)
    obj_factory = OBJ_FACTORY[endpoint]
    backend_storage = swh_storage.storage

    url_patterns = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user2",
        ],
        BlockingState.NON_BLOCKED: [
            "https://example.com/user2/repo1",
        ],
        BlockingState.BLOCKED: [
            "https://example.com/user2/repo1/subrepo1",
        ],
    }

    requests = {}
    set_vs = {}
    for decision, urls in url_patterns.items():
        request, set_visibility = set_origin_visibility(
            blocking_admin, slug=f"foo_{decision.name}"
        )
        set_visibility(urls, decision)
        requests[decision] = request
        set_vs[decision] = set_visibility

    origins = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user1/repo1/",
            "https://example.com/user1/repo1.git",
            "https://example.com/user1/repo1.git/",
            "https://example.com/user1/repo1/subrepo1",
            "https://example.com/user1/repo1/subrepo1.git",
            # everything under https://example.com/user2 should be blocked (but
            # https://example.com/user2/repo1)
            "https://example.com/user2",
            "https://example.com/user2.git",
            "https://example.com/user2.git/",
            "https://example.com/user2/",
            "https://example.com/user2/repo2",
            "https://example.com/user2/repo2/",
            "https://example.com/user2/repo2.git",
            "https://example.com/user2/repo2.git/",
        ],
        BlockingState.NON_BLOCKED: [
            # these should work OK by themselves
            "https://example.com/user1",
            "https://example.com/user1/repo2",
            # https://example.com/user2/repo1 is explicitly enabled
            "https://example.com/user2/repo1",
            "https://example.com/user2/repo1/",
            "https://example.com/user2/repo1.git",
            "https://example.com/user2/repo1/subrepo2",
        ],
        BlockingState.BLOCKED: [
            # "https://example.com/user2/repo1/subrepo1 is explicitly blocked
            "https://example.com/user2/repo1/subrepo1",
            "https://example.com/user2/repo1/subrepo1.git",
            "https://example.com/user2/repo1/subrepo1/subsubrepo",
        ],
    }

    # insert them all at once, none should be inserted and the error give
    # reasons for rejected urls
    all_origins = [Origin(url) for url in chain(*(origins.values()))]

    if endpoint != "origin_add":
        # if the endpoint is not 'origin_add', insert the origins; no
        # visit or visit status should be able to be inserted nonetheless
        assert backend_storage.origin_add(all_origins)
    if endpoint == "origin_visit_status_add":
        # if the endpoint is 'origin_visit_status_add', insert the
        # origin visits in the backend; but no visit status should be able to
        # be inserted via the proxy
        assert backend_storage.origin_visit_add(
            [mk_origin_visit(origin) for origin in all_origins]
        )
    # the insertion of all the objects at once fails because there are blocked
    # elements in the batch
    with pytest.raises(BlockedOriginException) as exc_info:
        method([obj_factory(origin) for origin in all_origins])
    # check the reported list of actually blocked origins is ok
    assert {
        url
        for url, status in exc_info.value.blocked.items()
        if status.state == BlockingState.DECISION_PENDING
    } == set(origins[BlockingState.DECISION_PENDING])
    assert {
        url
        for url, status in exc_info.value.blocked.items()
        if status.state == BlockingState.BLOCKED
    } == set(origins[BlockingState.BLOCKED])

    # now, set the decision pending pattern to non_blocked and rerun the whole insertion thing
    # use the set_visibility helper coming the the request that created decision pending rules
    set_visibility = set_vs[BlockingState.DECISION_PENDING]
    set_visibility(
        url_patterns[BlockingState.DECISION_PENDING], BlockingState.NON_BLOCKED
    )

    with pytest.raises(BlockedOriginException) as exc_info:
        method([obj_factory(origin) for origin in all_origins])
    # urls from the (formerly) decision pending batch should now not be reported
    # as blocked in the exception
    assert not {
        url
        for url, status in exc_info.value.blocked.items()
        if status.state == BlockingState.DECISION_PENDING
    }
    assert {
        url
        for url, status in exc_info.value.blocked.items()
        if status.state == BlockingState.BLOCKED
    } == set(origins[BlockingState.BLOCKED])

    # now if we ingest only non blocked urls, it should be ok (aka all but the
    # urls in origins[BLOCKED])
    assert method(
        [
            obj_factory(origin)
            for origin in all_origins
            if origin.url not in origins[BlockingState.BLOCKED]
        ]
    )


@pytest.mark.parametrize(
    "endpoint", ["origin_add", "origin_visit_add", "origin_visit_status_add"]
)
def test_blocking_log(swh_storage, blocking_admin, endpoint):
    method = getattr(swh_storage, endpoint)
    obj_factory = OBJ_FACTORY[endpoint]
    backend_storage = swh_storage.storage

    url_patterns = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user2",
        ],
        BlockingState.NON_BLOCKED: [
            "https://example.com/user2/repo1",
        ],
        BlockingState.BLOCKED: [
            "https://example.com/user2/repo1/subrepo1",
        ],
    }
    requests = {}
    set_vs = {}
    for decision, urls in url_patterns.items():
        request, set_visibility = set_origin_visibility(
            blocking_admin, slug=f"foo_{decision.name}"
        )
        set_visibility(urls, decision)
        requests[decision] = request
        set_vs[decision] = set_visibility

    origins = {
        BlockingState.DECISION_PENDING: [
            "https://example.com/user1/repo1",
            "https://example.com/user1/repo1/",
            "https://example.com/user1/repo1.git",
            "https://example.com/user1/repo1.git/",
            "https://example.com/user1/repo1/subrepo1",
            "https://example.com/user1/repo1/subrepo1.git",
            # everything under https://example.com/user2 should be blocked (but
            # https://example.com/user2/repo1)
            "https://example.com/user2",
            "https://example.com/user2.git",
            "https://example.com/user2.git/",
            "https://example.com/user2/",
            "https://example.com/user2/repo2",
            "https://example.com/user2/repo2/",
            "https://example.com/user2/repo2.git",
            "https://example.com/user2/repo2.git/",
        ],
        None: [
            # these should work OK by themselves, not matching any rule
            "https://example.com/user1",
            "https://example.com/user1/repo2",
            "https://example.org/user2/repo2",
            "https://example.org/user3/repo2/",
        ],
        BlockingState.NON_BLOCKED: [
            # https://example.com/user2/repo1 is explicitly enabled
            "https://example.com/user2/repo1",
            "https://example.com/user2/repo1/",
            "https://example.com/user2/repo1.git",
            "https://example.com/user2/repo1/subrepo2",
        ],
        BlockingState.BLOCKED: [
            # "https://example.com/user2/repo1/subrepo1 is explicitly blocked
            "https://example.com/user2/repo1/subrepo1",
            "https://example.com/user2/repo1/subrepo1.git",
            "https://example.com/user2/repo1/subrepo1/subsubrepo",
        ],
    }

    # insert them all at once, none should be inserted and the error give
    # reasons for rejected urls
    all_origins = [Origin(url) for url in chain(*(origins.values()))]

    ts_before = now()
    if endpoint != "origin_add":
        # if the endpoint is not 'origin_add', insert the origins; no
        # visit or visit status should be able to be inserted nonetheless
        assert backend_storage.origin_add(all_origins)
    if endpoint == "origin_visit_status_add":
        # if the endpoint is 'origin_visit_status_add', insert the
        # origin visits in the backend; but no visit status should be able to
        # be inserted via the proxy
        assert backend_storage.origin_visit_add(
            [mk_origin_visit(origin) for origin in all_origins]
        )

    with pytest.raises(BlockedOriginException):
        method([obj_factory(origin) for origin in all_origins])
    ts_after = now()

    # Check blocking journal log
    log = blocking_admin.get_log()
    assert set(origins[BlockingState.DECISION_PENDING]) == set(
        x.url for x in log if x.state == BlockingState.DECISION_PENDING
    )
    assert set(origins[BlockingState.BLOCKED]) == set(
        x.url for x in log if x.state == BlockingState.BLOCKED
    )
    assert set(origins[BlockingState.NON_BLOCKED]) == set(
        x.url for x in log if x.state == BlockingState.NON_BLOCKED
    )
    urls_with_event = {x.url for x in log}
    for url in origins[None]:
        assert url not in urls_with_event

    for logentry in log:
        assert logentry.request == requests[logentry.state].id
        assert ts_before < logentry.date < ts_after

    # check the get_log(request_id=xx) method call
    for state, request in requests.items():
        log = blocking_admin.get_log(request_id=request.id)
        assert set(origins[state]) == set(x.url for x in log)

    # check the get_log(url=xx) method call
    for origin in all_origins:
        url = origin.url
        log = blocking_admin.get_log(url=url)
        if url in origins[None]:
            assert not log
        else:
            assert len(log) == 1
            logentry = log[0]
            assert logentry.url == url
            assert logentry.request == requests[logentry.state].id

    # now, set the decision pending pattern to non_blocked and rerun the whole
    # insertion thing; use the set_visibility helper coming with the request
    # that created decision pending rules
    set_visibility = set_vs[BlockingState.DECISION_PENDING]
    set_visibility(
        url_patterns[BlockingState.DECISION_PENDING], BlockingState.NON_BLOCKED
    )

    with pytest.raises(BlockedOriginException):
        method([obj_factory(origin) for origin in all_origins])

    # for each url in origin[BLOCKED], we should have 2 'blocked' log entries
    for url in origins[BlockingState.BLOCKED]:
        log = blocking_admin.get_log(url=url)
        assert len(log) == 2
        assert {entry.state for entry in log} == {BlockingState.BLOCKED}

    # for each url in origin[NON_BLOCKED], we should have 2 'non-blocked' log entries
    for url in origins[BlockingState.NON_BLOCKED]:
        log = blocking_admin.get_log(url=url)
        assert len(log) == 2
        assert {entry.state for entry in log} == {BlockingState.NON_BLOCKED}

    # for each url in origin[DESCISION_PENDING], we should have 1 'decision-pending' and
    # 1 'non-blocked' log entry
    for url in origins[BlockingState.DECISION_PENDING]:
        log = blocking_admin.get_log(url=url)
        assert len(log) == 2
        assert {entry.state for entry in log} == {
            BlockingState.NON_BLOCKED,
            BlockingState.DECISION_PENDING,
        }

    # for each url in origin[NON_BLOCKED], we should have 2 'non-blocked' log entries
    for url in origins[BlockingState.NON_BLOCKED]:
        log = blocking_admin.get_log(url=url)
        assert len(log) == 2
        assert {entry.state for entry in log} == {BlockingState.NON_BLOCKED}

    # for each url in origin[None], we should have no log entry
    for url in origins[None]:
        log = blocking_admin.get_log(url=url)
        assert len(log) == 0


def test_proxy_config_deprecation(blocking_db_postgresql, swh_storage_backend):
    with pytest.warns(DeprecationWarning):
        assert BlockingProxyStorage(
            blocking_db=blocking_db_postgresql.info.dsn, storage=swh_storage_backend
        )
