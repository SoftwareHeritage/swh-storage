# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
from functools import wraps
import logging
from time import monotonic
from types import TracebackType
from typing import Dict, Optional, Type

from swh.core.statsd import statsd

logger = logging.getLogger(__name__)

OPERATIONS_METRIC = "swh_storage_operations_total"
OPERATIONS_UNIT_METRIC = "swh_storage_operations_{unit}_total"
DURATION_METRIC = "swh_storage_request_duration_seconds"


def timed(f):
    """Time that function!"""

    @wraps(f)
    def d(*a, **kw):
        with statsd.timed(DURATION_METRIC, tags={"endpoint": f.__name__}):
            return f(*a, **kw)

    return d


class DifferentialTimer:
    """Compute differential timing metrics and send them to StatsD.

    The DifferentialTimer will send the ``metric`` to statsd, computing the
    difference between the time spent within the whole block, with the time
    spent within any blocks wrapped in the :meth:`inner` decorator.

    If an exception is raised,when sending the metric, the DifferentialTimer
    will add a ``inner_exc`` (when the exception is raised by an inner-timed
    section) or ``outer_exc`` tag with the name of the exception class that was
    raised .

    Arguments:
      metric: name of the timing metric that is sent to StatsD when the context
        manager exits
      tags: tags to attach to the metric when it is sent

    Example:
      To generate a ``metric_seconds`` timed metric, recording the overhead of
      running ``run_pre_processing`` and ``run_post_processing``, use:

      .. code-block:: python

          with DifferentialTimer("metric_seconds") as t:
              run_pre_processing()
              with t.inner():
                  run_inner_method()
              run_post_processing()

    """

    def __init__(self, metric: str, tags: Optional[Dict[str, str]] = None) -> None:
        self.start_ts: Optional[float] = None
        self.inner_elapsed = 0
        self.exc_source = "outer"
        self.metric = metric
        self.tags = tags or {}

    def __enter__(self) -> "DifferentialTimer":
        self.start_ts = monotonic()
        return self

    @contextmanager
    def inner(self):
        """Add the duration of this block to the inner elapsed time counter."""
        start = monotonic()
        try:
            yield
        except BaseException:
            self.exc_source = "inner"
            raise
        finally:
            self.inner_elapsed += monotonic() - start

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        _exc_value: Optional[BaseException],
        _traceback: Optional[TracebackType],
    ) -> None:
        if self.start_ts is None:
            logger.warning("DifferentialTimer exited without being started")
            return

        total_elapsed = monotonic() - self.start_ts

        extra_tags = {}
        if exc_type:
            extra_tags[f"{self.exc_source}_exc"] = exc_type.__name__

        if self.inner_elapsed == 0 and not exc_type:
            logger.warning("DifferentialTimer didn't record calls to `inner`?")

        # Statsd expects timings in milliseconds
        value = (total_elapsed - self.inner_elapsed) * 1000

        statsd.timing(
            self.metric,
            value,
            tags={**self.tags, **extra_tags},
        )


def send_metric(metric, count, method_name):
    """Send statsd metric with count for method `method_name`

    If count is 0, the metric is discarded.  If the metric is not
    parseable, the metric is discarded with a log message.

    Args:
        metric (str): Metric's name (e.g content:add, content:add:bytes)
        count (int): Associated value for the metric
        method_name (str): Method's name

    Returns:
        Bool to explicit if metric has been set or not
    """
    if count == 0:
        return False

    metric_type = metric.split(":")
    _length = len(metric_type)
    if _length == 2:
        object_type, operation = metric_type
        metric_name = OPERATIONS_METRIC
    elif _length == 3:
        object_type, operation, unit = metric_type
        metric_name = OPERATIONS_UNIT_METRIC.format(unit=unit)
    else:
        logging.warning("Skipping unknown metric {%s: %s}" % (metric, count))
        return False

    statsd.increment(
        metric_name,
        count,
        tags={
            "endpoint": method_name,
            "object_type": object_type,
            "operation": operation,
        },
    )
    return True


def process_metrics(f):
    """Increment object counters for the decorated function."""

    @wraps(f)
    def d(*a, **kw):
        r = f(*a, **kw)
        for metric, count in r.items():
            send_metric(metric=metric, count=count, method_name=f.__name__)

        return r

    return d
