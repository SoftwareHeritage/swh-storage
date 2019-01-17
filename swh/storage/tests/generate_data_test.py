# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from hypothesis.strategies import (binary, composite, just, sets)

from swh.model.hashutil import MultiHash

from swh.storage.tests.algos.test_snapshot import origins


def gen_raw_content():
    """Generate raw content binary.

    """
    return binary(min_size=20, max_size=100)


@composite
def gen_contents(draw, *, min_size=0, max_size=100):
    """Generate valid and consistent content.

    Context: Test purposes

    Args:
        **draw**: Used by hypothesis to generate data
        **min_size** (int): Minimal number of elements to generate
                            (default: 0)
        **max_size** (int): Maximal number of elements to generate
                            (default: 100)

    Returns:
        [dict] representing contents. The list's size is between
        [min_size:max_size].

    """
    raw_contents = draw(sets(
        gen_raw_content(),
        min_size=min_size, max_size=max_size))

    contents = []
    for raw_content in raw_contents:
        contents.append({
            'data': raw_content,
            'length': len(raw_content),
            'status': 'visible',
            **MultiHash.from_data(raw_content).digest()
        })

    return contents


def gen_origins(min_size=10, max_size=100, unique=True):
    """Generate a list of origins.

    Args:
        **min_size** (int): Minimal number of elements to generate
                            (default: 10)
        **max_size** (int): Maximal number of elements to generate
                            (default: 100)
        **unique** (bool): Specify if all generated origins must be unique

    Returns:
        [dict] representing origins. The list's size is between
        [min_size:max_size].
    """
    size = random.randint(min_size, max_size)
    new_origins = []
    origins_set = set()
    while len(new_origins) != size:
        new_origin = origins().example()
        if unique:
            key = (new_origin['type'], new_origin['url'])
            if key in origins_set:
                continue
            origins_set.add(key)
        new_origins.append(new_origin)
    return just(new_origins)
