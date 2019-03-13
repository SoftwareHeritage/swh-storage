# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def iter_origins(storage, origin_from=1, origin_to=None, batch_size=10000):
    """Iterates over all origins in the storage.

    Args:
        storage: the storage object used for queries.
        batch_size: number of origins per query
    Yields:
        dict: the origin dictionary with the keys:

        - id: origin's id
        - type: origin's type
        - url: origin's url
    """
    start = origin_from
    while True:
        if origin_to:
            origin_count = min(origin_to - start, batch_size)
        else:
            origin_count = batch_size
        origins = list(storage.origin_get_range(
            origin_from=start, origin_count=origin_count))
        if not origins:
            break
        start = origins[-1]['id'] + 1
        yield from origins
        if origin_to and start > origin_to:
            break
