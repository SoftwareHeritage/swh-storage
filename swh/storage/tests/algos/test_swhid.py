# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import hashlib

from swh.model.swhids import CoreSWHID
from swh.storage.algos.swhid import known_swhids, swhid_is_known

from ..storage_data import StorageData


def test_known_swhids(swh_storage):
    swh_storage.snapshot_add([StorageData.snapshot])
    swh_storage.revision_add([StorageData.revision])
    swh_storage.release_add([StorageData.release])
    swh_storage.directory_add([StorageData.directory])
    swh_storage.content_add([StorageData.content])

    missing_snapshot_swhid = CoreSWHID.from_string(
        f"swh:1:snp:{hashlib.sha1(b'test snapshot').hexdigest()}"
    )
    missing_revision_swhid = CoreSWHID.from_string(
        f"swh:1:rev:{hashlib.sha1(b'test revision').hexdigest()}"
    )
    missing_release_swhid = CoreSWHID.from_string(
        f"swh:1:rel:{hashlib.sha1(b'test release').hexdigest()}"
    )
    missing_directory_swhid = CoreSWHID.from_string(
        f"swh:1:dir:{hashlib.sha1(b'test directory').hexdigest()}"
    )
    missing_content_swhid = CoreSWHID.from_string(
        f"swh:1:cnt:{hashlib.sha1(b'test content').hexdigest()}"
    )
    results = known_swhids(
        swh_storage,
        [
            StorageData.snapshot.swhid(),
            StorageData.revision.swhid(),
            StorageData.release.swhid(),
            StorageData.directory.swhid(),
            StorageData.content.swhid(),
            missing_snapshot_swhid,
            missing_revision_swhid,
            missing_release_swhid,
            missing_directory_swhid,
            missing_content_swhid,
        ],
    )

    assert len(results) == 10

    assert results[StorageData.snapshot.swhid()]
    assert not results[missing_snapshot_swhid]
    assert results[StorageData.revision.swhid()]
    assert not results[missing_revision_swhid]
    assert results[StorageData.release.swhid()]
    assert not results[missing_release_swhid]
    assert results[StorageData.directory.swhid()]
    assert not results[missing_directory_swhid]
    assert results[StorageData.content.swhid()]
    assert not results[missing_content_swhid]


def test_swhid_is_known(swh_storage):
    swh_storage.snapshot_add([StorageData.snapshot])
    missing_snapshot_swhid = CoreSWHID.from_string(
        f"swh:1:snp:{hashlib.sha1(b'test snapshot').hexdigest()}"
    )
    assert swhid_is_known(swh_storage, StorageData.snapshot.swhid())
    assert not swhid_is_known(swh_storage, missing_snapshot_swhid)
