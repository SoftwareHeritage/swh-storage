# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import random

from swh.model.model import ObjectType, Sha1Git
from swh.storage.algos.identifier import identifiers_missing
from ..storage_data import StorageData


@pytest.fixture(scope="function")
def missing_id() -> Sha1Git:
    return bytes(random.randint(0, 255) for _ in range(20))


@pytest.mark.parametrize("content_id", [c.sha1_git for c in StorageData.contents])
def test_missing_contents(swh_storage, content_id, missing_id):
    hashes = [content_id, missing_id]

    result = identifiers_missing(swh_storage, ObjectType.CONTENT, hashes)
    assert set(result) == {missing_id}


@pytest.mark.parametrize("directory_id", [o.id for o in StorageData.directories])
def test_missing_directories(swh_storage, directory_id, missing_id):
    hashes = [directory_id, missing_id]

    result = identifiers_missing(swh_storage, ObjectType.Directories, hashes)
    assert set(result) == {missing_id}


@pytest.mark.parametrize("revision_id", [o.id for o in StorageData.revisions])
def test_missing_revisions(swh_storage, revision_id, missing_id):
    hashes = [revision_id, missing_id]

    result = identifiers_missing(swh_storage, ObjectType.REVISION, hashes)
    assert set(result) == {missing_id}


@pytest.mark.parametrize("release_id", [o.id for o in StorageData.releases])
def test_missing_releases(swh_storage, release_id, missing_id):
    hashes = [release_id, missing_id]

    result = identifiers_missing(swh_storage, ObjectType.RELEASE, hashes)
    assert set(result) == {missing_id}


@pytest.mark.parametrize("snapshot_id", [o.id for o in StorageData.snapshots])
def test_missing_snapshots(swh_storage, snapshot_id, missing_id):
    hashes = [snapshot_id, missing_id]

    result = identifiers_missing(swh_storage, ObjectType.CONTENT, hashes)
    assert set(result) == {missing_id}
