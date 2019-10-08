# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from . import storage

Storage = storage.Storage


class HashCollision(Exception):
    pass


STORAGE_IMPLEMENTATION = {'local', 'remote', 'memory', 'filter', 'buffer'}


def get_storage(cls, args):
    """Get a storage object of class `storage_class` with arguments
    `storage_args`.

    Args:
        storage (dict): dictionary with keys:
        - cls (str): storage's class, either local, remote, memory, filter,
            buffer
        - args (dict): dictionary with keys

    Returns:
        an instance of swh.storage.Storage (either local or remote)

    Raises:
        ValueError if passed an unknown storage class.

    """
    if cls not in STORAGE_IMPLEMENTATION:
        raise ValueError('Unknown storage class `%s`. Supported: %s' % (
            cls, ', '.join(STORAGE_IMPLEMENTATION)))

    if cls == 'remote':
        from .api.client import RemoteStorage as Storage
    elif cls == 'local':
        from .storage import Storage
    elif cls == 'memory':
        from .in_memory import Storage
    elif cls == 'filter':
        from .filter import FilteringProxyStorage as Storage
    elif cls == 'buffer':
        from .buffer import BufferingProxyStorage as Storage

    return Storage(**args)
