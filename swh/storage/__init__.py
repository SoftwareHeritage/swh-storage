# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import importlib
from typing import TYPE_CHECKING, Any, Dict, List
import warnings

if TYPE_CHECKING:
    from .interface import StorageInterface


STORAGE_IMPLEMENTATIONS = {
    "local": ".postgresql.storage.Storage",
    "remote": ".api.client.RemoteStorage",
    "memory": ".in_memory.InMemoryStorage",
    "filter": ".filter.FilteringProxyStorage",
    "buffer": ".buffer.BufferingProxyStorage",
    "retry": ".retry.RetryingProxyStorage",
    "cassandra": ".cassandra.CassandraStorage",
    "validate": ".validate.ValidatingProxyStorage",
}


def get_storage(cls: str, **kwargs) -> "StorageInterface":
    """Get a storage object of class `storage_class` with arguments
    `storage_args`.

    Args:
        cls (str): storage's class, can be:
          - ``local`` to use a postgresql database
          - ``cassandra`` to use a cassandra database
          - ``remote`` to connect to a swh-storage server
          - ``memory`` for an in-memory storage, useful for fast tests
          - ``filter``, ``buffer``, ... to use specific storage "proxies", see their
            respective documentations
        args (dict): dictionary with keys

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
    check_config = kwargs.pop("check_config", {})
    storage = Storage(**kwargs)
    if check_config:
        if not storage.check_config(**check_config):
            raise EnvironmentError("storage check config failed")
    return storage


def get_storage_pipeline(
    steps: List[Dict[str, Any]], check_config=None
) -> "StorageInterface":
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
        step["check_config"] = check_config
        storage_config = step

    if storage_config is None:
        raise ValueError("'pipeline' has no steps.")

    return get_storage(**storage_config)
