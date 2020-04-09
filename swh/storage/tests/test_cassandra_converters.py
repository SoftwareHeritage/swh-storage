# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import namedtuple
from typing import List

from swh.storage.cassandra import converters

from swh.model.hashutil import DEFAULT_ALGORITHMS


# Test purposes
field_names: List[str] = list(DEFAULT_ALGORITHMS)
Row = namedtuple("Row", field_names)  # type: ignore


def test_row_to_content_hashes():
    for row in [
        Row(
            sha1=b"4\x972t\xcc\xefj\xb4\xdf\xaa\xf8e\x99y/\xa9\xc3\xfeF\x89",
            sha1_git=b"\xd8\x1c\xc0q\x0e\xb6\xcf\x9e\xfd[\x92\n\x84S\xe1\xe0qW\xb6\xcd",  # noqa
            sha256=b"g6P\xf96\xcb;\n/\x93\xce\t\xd8\x1b\xe1\x07H\xb1\xb2\x03\xc1\x9e\x81v\xb4\xee\xfc\x19d\xa0\xcf:",  # noqa
            blake2s256=b"\xd5\xfe\x199We'\xe4,\xfdv\xa9EZ$2\xfe\x7fVf\x95dW}\xd9<B\x80\xe7mf\x1d",  # noqa
        ),
        Row(
            sha1=b"4\x972t\xcc\xefj\xb4\xdf\xaa\xf8e\x99y/\xa9\xc3\xfeF\x89",  # noqa
            sha1_git=b"\xd8\x1c\xc0q\x0e\xb6\xcf\x9e\xfd[\x92\n\x84S\xe1\xe0qW\xb6\xcd",  # noqa
            sha256=b"h6P\xf96\xcb;\n/\x93\xce\t\xd8\x1b\xe1\x07H\xb1\xb2\x03\xc1\x9e\x81v\xb4\xee\xfc\x19d\xa0\xcf:",  # noqa
            blake2s256=b"\xd5\xfe\x199We'\xe4,\xfdv\xa9EZ$2\xfe\x7fVf\x95dW}\xd9<B\x80\xe7mf\x1d",  # noqa
        ),
    ]:
        actual_hashes = converters.row_to_content_hashes(row)

        assert len(actual_hashes) == len(DEFAULT_ALGORITHMS)
        for algo in DEFAULT_ALGORITHMS:
            assert actual_hashes[algo] == getattr(row, algo)
