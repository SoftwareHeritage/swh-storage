from . import storage

Storage = storage.Storage


def get_storage(storage_class, storage_args):
    """
    Get a storage object of class `storage_class` with arguments
    `storage_args`.

    Args:
        storage_class: one of 'remote_storage', 'local_storage'
        storage_args: the arguments to pass to the storage class
    Returns:
        an instance of swh.storage.Storage (either local or remote)
    Raises:
        ValueError if passed an unknown storage_class.
    """

    if storage_class == 'remote_storage':
        from .api.client import RemoteStorage as Storage
    elif storage_class == 'local_storage':
        from .storage import Storage
    else:
        raise ValueError('Unknown storage class `%s`' % storage_class)

    return Storage(*storage_args)
