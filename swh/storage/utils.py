# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional, Tuple


def _is_power_of_two(n: int) -> bool:
    return n > 0 and n & (n-1) == 0


def get_partition_bounds_bytes(
        i: int, n: int, nb_bytes: int) -> Tuple[bytes, Optional[bytes]]:
    r"""Splits the range [0; 2^(nb_bytes*8)) into n same-length intervals,
    and returns the boundaries of this interval (both inclusive); or None
    as upper bound, if this is the last partition

    n must be a power of 2.

    >>> get_partition_bounds_bytes(0, 16, 2) == (b'\x00\x00', b'\x10\x00')
    True
    >>> get_partition_bounds_bytes(1, 16, 2) == (b'\x10\x00', b'\x20\x00')
    True
    >>> get_partition_bounds_bytes(14, 16, 2) == (b'\xe0\x00', b'\xf0\x00')
    True
    >>> get_partition_bounds_bytes(15, 16, 2) == (b'\xf0\x00', None)
    True
    """
    if not _is_power_of_two(n):
        raise ValueError('number of partitions must be a power of two')
    if not 0 <= i < n:
        raise ValueError(
            'partition index must be between 0 and the number of partitions.')

    space_size = 1 << (nb_bytes*8)
    partition_size = space_size//n

    start = (partition_size*i).to_bytes(nb_bytes, 'big')
    end = None if i == n-1 \
        else (partition_size*(i+1)).to_bytes(nb_bytes, 'big')
    return (start, end)
