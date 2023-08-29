# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter, deque
from functools import partial
import logging
from typing import Counter as CounterT
from typing import Deque, Dict, Iterable, List, Optional

from swh.model.model import BaseModel
from swh.storage import get_storage
from swh.storage.exc import HashCollision

logger = logging.getLogger(__name__)


class RateQueue:
    def __init__(self, size: int, max_errors: int):
        assert size > max_errors
        self._size = size
        self._max_errors = max_errors
        self._errors: Deque[bool] = deque(maxlen=size)

    def add_ok(self, n_ok: int = 1) -> None:
        self._errors.extend([False] * n_ok)

    def add_error(self, n_error: int = 1) -> None:
        self._errors.extend([True] * n_error)

    def limit_reached(self) -> bool:
        return sum(self._errors) > self._max_errors

    def reset(self):
        # mainly for testing purpose
        self._errors.clear()


class TenaciousProxyStorage:
    """Storage proxy that have a tenacious insertion behavior.

    When an xxx_add method is called, it's first attempted as is against the backend
    storage. If a failure occurs, split the list of inserted objects in pieces until
    erroneous objects have been identified, so all the valid objects are guaranteed to
    be inserted.

    Also provides a error-rate limit feature: if more than n errors occurred during the
    insertion of the last p (window_size) objects, stop accepting any insertion.

    The number of insertion retries for a single object can be specified via
    the 'retries' parameter.

    This proxy is mainly intended to be used in a replayer configuration (aka a
    mirror stack), where insertion errors are mostly unexpected (which explains
    the low default ratio errors/window_size).

    Conversely, it should not be used in a loader configuration, as it may
    drop objects without stopping the loader, which leads to holes in the graph.

    Deployments using this proxy should carefully monitor their logs to check any
    failure is expected (because the failed object is corrupted),
    not because of transient errors or issues with the storage backend.

    Sample configuration use case for tenacious storage:

    .. code-block:: yaml

        storage:
          cls: tenacious
        storage:
          cls: remote
          args: http://storage.internal.staging.swh.network:5002/
        error-rate-limit:
          errors: 10
          window_size: 1000

    """

    tenacious_methods: Dict[str, str] = {
        "content_add": "content",
        "content_add_metadata": "content",
        "skipped_content_add": "skipped_content",
        "directory_add": "directory",
        "revision_add": "revision",
        "extid_add": "extid",
        "release_add": "release",
        "snapshot_add": "snapshot",
        "origin_add": "origin",
    }

    def __init__(
        self,
        storage,
        error_rate_limit: Optional[Dict[str, int]] = None,
        retries: int = 3,
    ):
        self.storage = get_storage(**storage)
        if error_rate_limit is None:
            error_rate_limit = {"errors": 10, "window_size": 1000}
        assert "errors" in error_rate_limit
        assert "window_size" in error_rate_limit
        self.rate_queue = RateQueue(
            size=error_rate_limit["window_size"],
            max_errors=error_rate_limit["errors"],
        )
        self._single_object_retries: int = retries

    def __getattr__(self, key):
        if key in self.tenacious_methods:
            return partial(self._tenacious_add, key)
        return getattr(self.storage, key)

    def _tenacious_add(self, func_name, objects: Iterable[BaseModel]) -> Dict[str, int]:
        """Enqueue objects to write to the storage. This checks if the queue's
        threshold is hit. If it is actually write those to the storage.

        """
        add_function = getattr(self.storage, func_name)
        object_type = self.tenacious_methods[func_name]

        # list of lists of objects; note this to_add list is consumed from the tail
        to_add: List[List[BaseModel]] = [list(objects)]
        n_objs: int = len(to_add[0])

        results: CounterT[str] = Counter()
        retries: int = self._single_object_retries

        while to_add:
            if self.rate_queue.limit_reached():
                logger.error(
                    "Too many insertion errors have been detected; "
                    "disabling insertions"
                )
                raise RuntimeError(
                    "Too many insertion errors have been detected; "
                    "disabling insertions"
                )
            objs = to_add.pop()
            try:
                results.update(add_function(objs))
                self.rate_queue.add_ok(len(objs))
            except Exception as exc:
                if len(objs) > 1:
                    logger.info(
                        "%s: failed to insert a batch of %s %s objects, splitting",
                        func_name,
                        len(objs),
                        object_type,
                    )
                    # reinsert objs split in 2 parts at the end of to_add
                    to_add.append(objs[(len(objs) // 2) :])
                    to_add.append(objs[: (len(objs) // 2)])
                    # each time we append a batch in the to_add bag, reset the
                    # one-object-batch retries counter
                    retries = self._single_object_retries
                else:
                    retries -= 1
                    if retries:
                        logger.info(
                            "%s: failed to insert an %s, retrying",
                            func_name,
                            object_type,
                        )
                        # give it another chance
                        to_add.append(objs)
                    else:
                        logger.error(
                            "%s: failed to insert an object, excluding %s (from a batch of %s)",
                            func_name,
                            objs,
                            n_objs,
                        )
                        logger.error(
                            "Exception was: %s",
                            repr(exc),
                            exc_info=not isinstance(exc, HashCollision),
                        )
                        results.update({f"{object_type}:add:errors": 1})
                        self.rate_queue.add_error()
                        # reset the retries counter (needed in case the next
                        # batch is also 1 element only)
                        retries = self._single_object_retries
        return dict(results)

    def reset(self):
        self.rate_queue.reset()
