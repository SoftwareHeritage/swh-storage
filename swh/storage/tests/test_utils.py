# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model import hashutil
from swh.storage.utils import (
    content_bytes_hashes,
    content_hex_hashes,
    extract_collision_hash,
    map_optional,
    now,
    round_to_milliseconds,
)


def test_extract_collision_hash():
    for msg, expected_result in [
        (
            "Key (sha1)=(\\x34973274ccef6ab4dfaaf86599792fa9c3fe4689) ...",
            (
                "sha1",
                hashutil.hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4689"),
            ),
        ),
        (
            "Key (sha1_git)=(\\x34973274ccef6ab4dfaaf86599792fa9c3fe4699) already exists",  # noqa
            (
                "sha1_git",
                hashutil.hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4699"),
            ),
        ),
        (
            "Key (sha256)=(\\x673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a) ...",  # noqa
            (
                "sha256",
                hashutil.hash_to_bytes(
                    "673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a"
                ),
            ),  # noqa
        ),
        (
            "Key (blake2s)=(\\xd5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d) ...",  # noqa
            (
                "blake2s",
                hashutil.hash_to_bytes(
                    "d5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d"
                ),
            ),  # noqa
        ),
    ]:
        assert extract_collision_hash(msg) == expected_result

    assert extract_collision_hash("Nothing matching") is None


def test_content_hex_hashes():
    input_content = {
        "blake2s256": hashutil.hash_to_bytes(
            "8f677e3214ca8b2acad91884a1571ef3f12b786501f9a6bedfd6239d82095dd2"
        ),
        "sha1_git": hashutil.hash_to_bytes("ba9aaa145ccd24ef760cf31c74d8f7ca1a2e47b0"),
        "sha256": hashutil.hash_to_bytes(
            "2bb787a73e37352f92383abe7e2902936d1059ad9f1ba6daaa9c1e58ee6970d0"
        ),
        "sha1": hashutil.hash_to_bytes("38762cf7f55934b34d179ae6a4c80cadccbb7f0a"),
    }

    expected_content = {
        "blake2s256": (
            "8f677e3214ca8b2acad91884a1571ef3" "f12b786501f9a6bedfd6239d82095dd2"
        ),
        "sha1_git": "ba9aaa145ccd24ef760cf31c74d8f7ca1a2e47b0",
        "sha256": "2bb787a73e37352f92383abe7e2902936d1059ad9f1ba6daaa9c1e58ee6970d0",
        "sha1": "38762cf7f55934b34d179ae6a4c80cadccbb7f0a",
    }

    actual_content = content_hex_hashes(input_content)

    assert len(actual_content) == len(expected_content)
    for algo in hashutil.DEFAULT_ALGORITHMS:
        assert actual_content[algo] == expected_content[algo]


def test_content_bytes_hashes():
    input_content = {
        "blake2s256": (
            "8f677e3214ca8b2acad91884a1571ef3" "f12b786501f9a6bedfd6239d82095dd2"
        ),
        "sha1_git": "ba9aaa145ccd24ef760cf31c74d8f7ca1a2e47b0",
        "sha256": "2bb787a73e37352f92383abe7e2902936d1059ad9f1ba6daaa9c1e58ee6970d0",
        "sha1": "38762cf7f55934b34d179ae6a4c80cadccbb7f0a",
    }

    expected_content = {
        "blake2s256": hashutil.hash_to_bytes(
            "8f677e3214ca8b2acad91884a1571ef3f12b786501f9a6bedfd6239d82095dd2"
        ),
        "sha1_git": hashutil.hash_to_bytes("ba9aaa145ccd24ef760cf31c74d8f7ca1a2e47b0"),
        "sha256": hashutil.hash_to_bytes(
            "2bb787a73e37352f92383abe7e2902936d1059ad9f1ba6daaa9c1e58ee6970d0"
        ),
        "sha1": hashutil.hash_to_bytes("38762cf7f55934b34d179ae6a4c80cadccbb7f0a"),
    }

    actual_content = content_bytes_hashes(input_content)

    assert len(actual_content) == len(expected_content)
    for algo in hashutil.DEFAULT_ALGORITHMS:
        assert actual_content[algo] == expected_content[algo]


def test_round_to_milliseconds():
    date = now()

    for ms, expected_ms in [(0, 0), (1000, 1000), (555555, 555000), (999500, 999000)]:
        date = date.replace(microsecond=ms)
        actual_date = round_to_milliseconds(date)
        assert actual_date.microsecond == expected_ms


@pytest.mark.parametrize(
    "f,x,expected_result",
    [
        (int, "10", 10),
        (int, None, None),
        (str, 10, "10"),
        (str, None, None),
        (lambda x: x + 1, 99, 100),
        (lambda x: x + 1, None, None),
    ],
)
def test_map_optional(f, x, expected_result):
    """map_optional should execute function over input if input is not None, returns
    None otherwise.

    """
    assert map_optional(f, x) == expected_result
