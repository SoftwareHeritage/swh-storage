# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil

from swh.storage.exc import HashCollision
from swh.storage.utils import content_hex_hashes


def test_hash_collision_exception():
    hex_hash_id = "38762cf7f55934b34d179ae6a4c80cadccbb7f0a"
    hash_id = hashutil.hash_to_bytes(hex_hash_id)

    content = {
        "blake2s256": hashutil.hash_to_bytes(
            "8f677e3214ca8b2acad91884a1571ef3f12b786501f9a6bedfd6239d82095dd2"
        ),
        "sha1_git": hashutil.hash_to_bytes("ba9aaa145ccd24ef760cf31c74d8f7ca1a2e47b0"),
        "sha256": hashutil.hash_to_bytes(
            "2bb787a73e37352f92383abe7e2902936d1059ad9f1ba6daaa9c1e58ee6970d0"
        ),
        "sha1": hash_id,
    }

    exc = HashCollision("sha1", hash_id, [content])

    assert exc.algo == "sha1"
    assert exc.hash_id == hex_hash_id
    assert exc.colliding_contents == [content_hex_hashes(content)]

    assert exc.colliding_content_hashes() == [content]
