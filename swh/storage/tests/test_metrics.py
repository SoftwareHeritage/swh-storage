# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

import pytest

from swh.storage.metrics import (
    OPERATIONS_METRIC,
    OPERATIONS_UNIT_METRIC,
    DifferentialTimer,
    send_metric,
)


def test_send_metric_unknown_unit():
    r = send_metric("content", count=10, method_name="content_add")
    assert r is False
    r = send_metric("sthg:add:bytes:extra", count=10, method_name="sthg_add")
    assert r is False


def test_send_metric_no_value():
    r = send_metric("content:add", count=0, method_name="content_add")
    assert r is False


@patch("swh.storage.metrics.statsd.increment")
def test_send_metric_no_unit(mock_statsd):
    r = send_metric("content:add", count=10, method_name="content_add")

    mock_statsd.assert_called_with(
        OPERATIONS_METRIC,
        10,
        tags={
            "endpoint": "content_add",
            "object_type": "content",
            "operation": "add",
        },
    )

    assert r


@patch("swh.storage.metrics.statsd.increment")
def test_send_metric_unit(mock_statsd):
    unit_ = "bytes"
    r = send_metric("c:add:%s" % unit_, count=100, method_name="c_add")

    expected_metric = OPERATIONS_UNIT_METRIC.format(unit=unit_)
    mock_statsd.assert_called_with(
        expected_metric,
        100,
        tags={
            "endpoint": "c_add",
            "object_type": "c",
            "operation": "add",
        },
    )

    assert r


def test_differential_timer(mocker):
    mocked_timing = mocker.patch("swh.core.statsd.statsd.timing")
    mocked_monotonic = mocker.patch("swh.storage.metrics.monotonic")
    mocked_monotonic.side_effect = [
        0.0,  # outer start
        1.0,  # inner.start 1
        1.5,  # inner.done 1 (0.5s elapsed)
        1.7,  # inner.start 2
        1.9,  # inner.done 2 (0.2s elapsed)
        4.5,  # outer done (4.5s elapsed)
        Exception("Should not have been called again!"),
    ]

    timer = DifferentialTimer("metric_difftimer_seconds", tags={"test": "value"})

    def do_something():
        pass

    def do_something_else():
        pass

    with timer:
        with timer.inner():
            do_something()
        with timer.inner():
            do_something()
        do_something_else()

    mocked_timing.assert_called_once_with(
        "metric_difftimer_seconds",
        # The differential timing is 3800 ms: 4.5s - 0.5s - 0.2s
        pytest.approx(3800.0),
        tags={"test": "value"},
    )


def test_differential_timer_inner_exception(mocker):
    mocked_timing = mocker.patch("swh.core.statsd.statsd.timing")
    mocked_monotonic = mocker.patch("swh.storage.metrics.monotonic")
    mocked_monotonic.side_effect = [
        5.0,  # outer start
        6.0,  # inner start
        # exception raised by inner
        6.1,  # inner finished (0.1s elapsed)
        6.2,  # outer finished (1.2s elapsed)
        Exception("Should not have been called again!"),
    ]

    timer = DifferentialTimer("metric_innerexception_seconds", tags={"test2": "value"})

    class InnerException(Exception):
        pass

    with pytest.raises(InnerException):
        with timer:
            with timer.inner():
                raise InnerException

            assert False, "unreachable"

    mocked_timing.assert_called_once_with(
        "metric_innerexception_seconds",
        # The differential timing is 1100 ms: 1.2s - 0.1s
        pytest.approx(1100.0),
        tags={"test2": "value", "inner_exc": "InnerException"},
    )


def test_differential_timer_outer_exception(mocker):
    mocked_timing = mocker.patch("swh.core.statsd.statsd.timing")
    mocked_monotonic = mocker.patch("swh.storage.metrics.monotonic")
    mocked_monotonic.side_effect = [
        15.0,  # outer start
        17.0,  # inner start
        17.7,  # inner finished (0.7s elapsed)
        # exception raised by outer
        18.2,  # outer finished (3.2s elapsed)
        Exception("Should not have been called again!"),
    ]

    timer = DifferentialTimer("metric_outerexception_seconds")

    class OuterException(Exception):
        pass

    with pytest.raises(OuterException):
        with timer:
            with timer.inner():
                pass
            raise OuterException

            assert False, "unreachable"

    mocked_timing.assert_called_once_with(
        "metric_outerexception_seconds",
        # The differential timing is 2500 ms: 3.2s - 0.7s
        pytest.approx(2500.0),
        tags={"outer_exc": "OuterException"},
    )
