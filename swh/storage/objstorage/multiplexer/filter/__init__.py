import functools

from .read_write_filter import ReadObjStorageFilter
from .id_filter import RegexIdObjStorageFilter, PrefixIdObjStorageFilter


_FILTERS_CLASSES = {
    'readonly': ReadObjStorageFilter,
    'regex': RegexIdObjStorageFilter,
    'prefix': PrefixIdObjStorageFilter
}


_FILTERS_PRIORITY = {
    'readonly': 0,
    'prefix': 1,
    'regex': 2
}


def read_only():
    return {'type': 'readonly'}


def id_prefix(prefix):
    return {'type': 'prefix', 'prefix': prefix}


def id_regex(regex):
    return {'type': 'regex', 'regex': regex}


def _filter_priority(self, filter_type):
    """ Get the priority of this filter.

    Priority is a value that indicates if the operation of the
    filter is time-consuming (smaller values means quick execution),
    or very likely to be almost always the same value (False being small,
    and True high).

    In case the filters are chained, they will be ordered in a way that
    small priorities (quick execution or instantly break the chain) are
    executed first.

    Default value is 1. Value 0 is recommended for storages that change
    behavior only by disabling some operations (making the method return
    None).
    """
    return _FILTERS_PRIORITY.get(filter_type, 1)


def add_filter(storage, filter_conf):
    """ Add a filter to the given storage.

    Args:
        storage (ObjStorage): storage which will be filtered.
        filter_conf (dict): configuration of an ObjStorageFilter, given as
            a dictionnary that contains the keys:
                - type: which represent the type of filter, one of the keys
                    of FILTERS
                - Every arguments that this type of filter require.

    Returns:
        A filtered storage that perform only the valid operations.
    """
    type = filter_conf['type']
    args = {k: v for k, v in filter_conf.items() if k is not 'type'}
    filter = _FILTERS_CLASSES[type](storage=storage, **args)
    return filter


def add_filters(storage, *filter_confs):
    """ Add multiple filters to the given storage.

    (See filter.add_filter)

    Args:
        storage (ObjStorage): storage which will be filtered.
        filter_confs (list): any number of filter conf, as a dict with:
            - type: which represent the type of filter, one of the keys of
                FILTERS.
            - Every arguments that this type of filter require.

    Returns:
        A filtered storage that fulfill the requirement of all the given
        filters.
    """
    # Reverse sorting in order to put the filter with biggest priority first.
    filter_confs.sort(key=lambda conf: _filter_priority(conf['type']),
                      reverse=True)

    # Add the bigest filter to the storage, and reduce it to accumulate filters
    # on top of it, until the smallest (fastest, see filter.filter_priority) is
    # added.
    return functools.reduce(
        lambda stor, conf: add_filter(stor, conf),
        [storage] + filter_confs
    )
