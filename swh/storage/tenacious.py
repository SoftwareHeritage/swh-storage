# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter, deque
from functools import partial
import logging
from typing import Counter as CounterT
from typing import Deque, Dict, Iterable, List

from swh.model.model import BaseModel
from swh.storage import get_storage

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

    tenacious_methods = (
        "content_add",
        "skipped_content_add",
        "directory_add",
        "revision_add",
        "extid_add",
        "release_add",
        "snapshot_add",
        "origin_add",
    )

    def __init__(self, storage, error_rate_limit=None):
        self.storage = get_storage(**storage)
        if error_rate_limit is None:
            error_rate_limit = {"errors": 10, "window_size": 1000}
        assert "errors" in error_rate_limit
        assert "window_size" in error_rate_limit
        self.rate_queue = RateQueue(
            size=error_rate_limit["window_size"], max_errors=error_rate_limit["errors"],
        )

    def __getattr__(self, key):
        if key in self.tenacious_methods:
            return partial(self._tenacious_add, key)
        return getattr(self.storage, key)

    def _tenacious_add(self, func_name, objects: Iterable[BaseModel]) -> Dict[str, int]:
        """Enqueue objects to write to the storage. This checks if the queue's
           threshold is hit. If it is actually write those to the storage.

        """
        add_function = getattr(self.storage, func_name)
        object_type = func_name[:-4]  # remove the _add suffix

        # list of lists of objects; note this to_add list is consumed from the tail
        to_add: List[List[BaseModel]] = [list(objects)]
        results: CounterT[str] = Counter()

        while to_add:
            if self.rate_queue.limit_reached():
                logging.error(
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
                        f"{func_name}: failed to insert a batch of "
                        f"{len(objs)} {object_type} objects, splitting"
                    )
                    # reinsert objs split in 2 parts at the end of to_add
                    to_add.append(objs[(len(objs) // 2) :])
                    to_add.append(objs[: (len(objs) // 2)])
                else:
                    logger.error(
                        f"{func_name}: failed to insert an object, excluding {objs}"
                    )
                    # logger.error(f"Exception: {exc}")
                    logger.exception(f"Exception was: {exc}")
                    results.update({f"{object_type}:add:errors": 1})
                    self.rate_queue.add_error()
        return dict(results)

    def reset(self):
        self.rate_queue.reset()
