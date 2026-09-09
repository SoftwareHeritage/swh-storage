# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import dataclasses
import functools
import logging
import pprint
import time
from typing import Callable, TypeVar

from .statsd import StatsdProxyStorage

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Callable)


@dataclasses.dataclass
class Stats:
    calls: dict[str, int] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(int)
    )
    """Total number of calls of each method"""

    total_time: dict[str, float] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(float)
    )
    """Total time spent in each method"""

    arg_lengths: dict[str, int] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(int)
    )
    """Sum of the length of the main argument of each method over all its calls"""


class CountingProxyStorage(StatsdProxyStorage):
    """Storage implementation which times and counts calls to each endpoint,
    as well as the number of items in its list arguments (eg. the number of revision ids
    as parameter to ``revision_get`` or the number of revisions as parameter to
    ``revision_add`) and prints them when deleted

    Configuration: see :class:`swh.storage.proxies.statsd.StatsdProxyStorage`.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.stats = Stats()

    def __del__(self):
        logger.info("Total calls:\n%s", pprint.pformat(dict(self.stats.calls)))
        logger.info(
            "Total time (seconds):\n%s", pprint.pformat(dict(self.stats.total_time))
        )
        logger.info(
            "Summed argument length:\n%s", pprint.pformat(dict(self.stats.arg_lengths))
        )

    def _timed(self, f: T) -> T:
        @functools.wraps(f)
        def newf(*args, **kwargs):
            start = time.monotonic()
            try:
                return f(*args, **kwargs)
            finally:
                end = time.monotonic()
                self.stats.total_time[f.__name__] += end - start
                self.stats.calls[f.__name__] += 1

        return newf  # type: ignore[return-value]

    def _increment(self, metric: str, value: int, tags: dict[str, str]) -> None:
        assert metric == "swh_storage_request_args_total"
        assert set(tags) == {"endpoint"}

        self.stats.arg_lengths[tags["endpoint"]] += value
