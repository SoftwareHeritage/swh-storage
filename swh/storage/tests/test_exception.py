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


def test_masked_object_exception_str() -> None:
    from uuid import UUID

    from swh.model.swhids import ExtendedSWHID

    from ..exc import MaskedObjectException
    from ..proxies.masking.db import MaskedState, MaskedStatus

    swhid_cnt = ExtendedSWHID.from_string(
        "swh:1:cnt:a6d1888d25fd8e84986f386ee0cdc8a1fc3c09b4"
    )
    swhid_ori = ExtendedSWHID.from_string(
        "swh:1:ori:155291d5b9ada4570672510509f93fcfd9809882"
    )
    status1 = MaskedStatus(
        state=MaskedState.DECISION_PENDING,
        request=UUID("da785a27-7e59-4a35-b82a-a5ae3714407c"),
    )
    status2 = MaskedStatus(
        state=MaskedState.RESTRICTED,
        request=UUID("4fd42e35-2b6c-4536-8447-bc213cd0118b"),
    )
    masked = {swhid_cnt: [status1], swhid_ori: [status1, status2]}
    exc = MaskedObjectException(masked)

    assert str(exc) == (
        "Some objects are masked: "
        "swh:1:cnt:a6d1888d25fd8e84986f386ee0cdc8a1fc3c09b4 by request "
        "da785a27-7e59-4a35-b82a-a5ae3714407c (DECISION_PENDING), "
        "swh:1:ori:155291d5b9ada4570672510509f93fcfd9809882 by request "
        "da785a27-7e59-4a35-b82a-a5ae3714407c (DECISION_PENDING) and others"
    )
