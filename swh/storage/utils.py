# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import re
from typing import Callable, Dict, Optional, Tuple, TypeVar

from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_bytes, hash_to_hex


def now() -> datetime:
    return datetime.now(tz=timezone.utc)


T1 = TypeVar("T1")
T2 = TypeVar("T2")


def map_optional(f: Callable[[T1], T2], x: Optional[T1]) -> Optional[T2]:
    if x is None:
        return None
    else:
        return f(x)


def _is_power_of_two(n: int) -> bool:
    return n > 0 and n & (n - 1) == 0


def get_partition_bounds_bytes(
    i: int, n: int, nb_bytes: int
) -> Tuple[bytes, Optional[bytes]]:
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
        raise ValueError("number of partitions must be a power of two")
    if not 0 <= i < n:
        raise ValueError(
            "partition index must be between 0 and the number of partitions."
        )

    space_size = 1 << (nb_bytes * 8)
    partition_size = space_size // n

    start = (partition_size * i).to_bytes(nb_bytes, "big")
    end = None if i == n - 1 else (partition_size * (i + 1)).to_bytes(nb_bytes, "big")
    return (start, end)


def extract_collision_hash(error_message: str) -> Optional[Tuple[str, bytes]]:
    """Utilities to extract the hash information from a hash collision error.

    Hash collision error message are of the form:
    'Key (<hash-type>)=(<double-escaped-hash) already exists.'

    for example:
    'Key (sha1)=(\\x34973274ccef6ab4dfaaf86599792fa9c3fe4689) already exists.'

    Return:
        A formatted string

    """
    pattern = r"\w* \((?P<type>[^)]+)\)=\(\\x(?P<id>[a-f0-9]+)\) \w*"
    result = re.match(pattern, error_message)
    if result:
        hash_type = result.group("type")
        hash_id = result.group("id")
        return hash_type, hash_to_bytes(hash_id)
    return None


def content_hex_hashes(content: Dict[str, bytes]) -> Dict[str, str]:
    """Convert bytes hashes into hex hashes.

    """
    return {algo: hash_to_hex(content[algo]) for algo in DEFAULT_ALGORITHMS}


def content_bytes_hashes(content: Dict[str, str]) -> Dict[str, bytes]:
    """Convert bytes hashes into hex hashes.

    """
    return {algo: hash_to_bytes(content[algo]) for algo in DEFAULT_ALGORITHMS}


def round_to_milliseconds(date):
    """Round datetime to milliseconds before insertion, so equality doesn't fail after a
    round-trip through a DB (eg. Cassandra)

    """
    return date.replace(microsecond=(date.microsecond // 1000) * 1000)
