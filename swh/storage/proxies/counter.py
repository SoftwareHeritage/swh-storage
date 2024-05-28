# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Callable

from swh.counters import get_counters
from swh.counters.interface import CountersInterface
from swh.storage import StorageSpec, get_storage
from swh.storage.interface import StorageInterface

OBJECT_TYPES = [
    "content",
    "directory",
    "snapshot",
    "origin_visit_status",
    "origin_visit",
    "origin",
]


class CountingProxyStorage:
    """Counting Storage Proxy.

    This is in charge of adding objects directly to swh-counters, without
    going through Kafka/swh-journal.
    This is meant as a simple way to setup counters for experiments; production
    should use swh-journal to reduce load/latency of the storage server.

    Additionally, unlike the journal-based counting, it does not count persons
    or the number of origins per netloc.

    Sample configuration use case for filtering storage:

    .. code-block: yaml

        storage:
          cls: counter
          counters:
            cls: remote
            url: http://counters.internal.staging.swh.network:5011/
          storage:
            cls: remote
            url: http://storage.internal.staging.swh.network:5002/

    """

    def __init__(self, counters, storage: StorageSpec) -> None:
        self.counters: CountersInterface = get_counters(**counters)
        self.storage: StorageInterface = get_storage(**storage)

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        if key.endswith("_add"):
            return self._adder(key[0:-4], getattr(self.storage, key))
        return getattr(self.storage, key)

    def _adder(self, collection: str, backend_function: Callable):
        def f(objs):
            self.counters.add(collection, [obj.unique_key() for obj in objs])
            return backend_function(objs)

        return f
