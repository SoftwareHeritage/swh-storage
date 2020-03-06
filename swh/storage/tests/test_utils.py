# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.hashutil import hash_to_bytes
from swh.storage.utils import extract_collision_hash


def test_extract_collision_hash():
    for msg, expected_result in [
            (
                'Key (sha1)=(\\x34973274ccef6ab4dfaaf86599792fa9c3fe4689) ...',
                ('sha1', hash_to_bytes(
                    '34973274ccef6ab4dfaaf86599792fa9c3fe4689')),
            ),
            (
                'Key (sha1_git)=(\\x34973274ccef6ab4dfaaf86599792fa9c3fe4699) already exists',  # noqa
                ('sha1_git', hash_to_bytes(
                    '34973274ccef6ab4dfaaf86599792fa9c3fe4699')),
            ),
            (
                'Key (sha256)=(\\x673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a) ...',  # noqa
                ('sha256', hash_to_bytes(
                    '673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a'))  # noqa
            ),
            (
                'Key (blake2s)=(\\xd5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d) ...',  # noqa
                ('blake2s', hash_to_bytes(
                    'd5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d'))  # noqa
            ),
    ]:
        assert extract_collision_hash(msg) == expected_result

    assert extract_collision_hash('Nothing matching') is None
