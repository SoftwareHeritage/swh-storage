# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import warnings


class HashCollision(Exception):
    pass


STORAGE_IMPLEMENTATION = {
    'pipeline', 'local', 'remote', 'memory', 'filter', 'buffer', 'retry',
    'validate', 'cassandra',
}


def get_storage(cls, **kwargs):
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
    if cls not in STORAGE_IMPLEMENTATION:
        raise ValueError('Unknown storage class `%s`. Supported: %s' % (
            cls, ', '.join(STORAGE_IMPLEMENTATION)))

    if 'args' in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning)
        kwargs = kwargs['args']

    if cls == 'pipeline':
        return get_storage_pipeline(**kwargs)

    if cls == 'remote':
        from .api.client import RemoteStorage as Storage
    elif cls == 'local':
        from .storage import Storage
    elif cls == 'cassandra':
        from .cassandra import CassandraStorage as Storage
    elif cls == 'memory':
        from .in_memory import InMemoryStorage as Storage
    elif cls == 'filter':
        from .filter import FilteringProxyStorage as Storage
    elif cls == 'buffer':
        from .buffer import BufferingProxyStorage as Storage
    elif cls == 'retry':
        from .retry import RetryingProxyStorage as Storage
    elif cls == 'validate':
        from .validate import ValidatingProxyStorage as Storage

    return Storage(**kwargs)


def get_storage_pipeline(steps):
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
        if 'args' in step:
            warnings.warn(
                'Explicit "args" key is deprecated, use keys directly '
                'instead.',
                DeprecationWarning)
            step = {
                'cls': step['cls'],
                **step['args'],
            }
        if storage_config:
            step['storage'] = storage_config
        storage_config = step

    return get_storage(**storage_config)
