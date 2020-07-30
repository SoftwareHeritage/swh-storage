# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import importlib
from typing import Any, Dict, List
import warnings

from .interface import StorageInterface


STORAGE_IMPLEMENTATIONS = {
    "local": ".storage.Storage",
    "remote": ".api.client.RemoteStorage",
    "memory": ".in_memory.InMemoryStorage",
    "filter": ".filter.FilteringProxyStorage",
    "buffer": ".buffer.BufferingProxyStorage",
    "retry": ".retry.RetryingProxyStorage",
    "cassandra": ".cassandra.CassandraStorage",
}


def get_storage(cls: str, **kwargs) -> StorageInterface:
    """Get a storage object of class `storage_class` with arguments
    `storage_args`.

    Args:
        storage (dict): dictionary with keys:
        - cls (str): storage's class, either local, remote, memory, filter,
            buffer
        - args (dict): dictionary with keys

    Returns:
        an instance of swh.storage.Storage or compatible class

    Raises:
        ValueError if passed an unknown storage class.

    """
    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    if cls == "pipeline":
        return get_storage_pipeline(**kwargs)

    class_path = STORAGE_IMPLEMENTATIONS.get(cls)
    if class_path is None:
        raise ValueError(
            "Unknown storage class `%s`. Supported: %s"
            % (cls, ", ".join(STORAGE_IMPLEMENTATIONS))
        )

    (module_path, class_name) = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path, package=__package__)
    Storage = getattr(module, class_name)
    return Storage(**kwargs)


def get_storage_pipeline(steps: List[Dict[str, Any]]) -> StorageInterface:
    """Recursively get a storage object that may use other storage objects
    as backends.

    Args:
        steps (List[dict]): List of dicts that may be used as kwargs for
            `get_storage`.

    Returns:
        an instance of swh.storage.Storage or compatible class

    Raises:
        ValueError if passed an unknown storage class.
    """
    storage_config = None
    for step in reversed(steps):
        if "args" in step:
            warnings.warn(
                'Explicit "args" key is deprecated, use keys directly ' "instead.",
                DeprecationWarning,
            )
            step = {
                "cls": step["cls"],
                **step["args"],
            }
        if storage_config:
            step["storage"] = storage_config
        storage_config = step

    if storage_config is None:
        raise ValueError("'pipeline' has no steps.")

    return get_storage(**storage_config)
