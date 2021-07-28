# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import pytest

from swh.model.model import BaseModel
from swh.storage.tests.storage_data import StorageData


def test_storage_data():
    data = StorageData()

    for attribute_key in [
        "contents",
        "skipped_contents",
        "directories",
        "revisions",
        "releases",
        "snapshots",
        "origins",
        "origin_visits",
        "fetchers",
        "authorities",
        "origin_metadata",
        "content_metadata",
        "extids",
    ]:
        for obj in getattr(data, attribute_key):
            assert isinstance(obj, BaseModel)


@pytest.mark.parametrize(
    "collection",
    ("directories", "git_revisions", "hg_revisions", "releases", "snapshots"),
)
def test_storage_data_hash(collection):
    data = StorageData()

    for obj in getattr(data, collection):
        assert (
            obj.compute_hash() == obj.id
        ), f"{obj.compute_hash().hex()} != {obj.id.hex()}"
