# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path
import swh.storage

from hypothesis.strategies import (binary, composite, lists)

from swh.model.hashutil import MultiHash


SQL_DIR = path.join(path.dirname(swh.storage.__file__), 'sql')


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
        List of swh.model contents whose size is between [min_size:max_size].

    """
    raw_contents = draw(lists(
        gen_raw_content(), min_size=min_size, max_size=max_size))

    contents = []
    for raw_content in raw_contents:
        content = {
            'data': raw_content,
            'length': len(raw_content),
            'status': 'visible',
        }
        hashes = MultiHash.from_data(raw_content).digest()
        content.update(hashes)
        contents.append(content)

    return contents
