# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

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
    ]:
        for obj in getattr(data, attribute_key):
            assert isinstance(obj, BaseModel)
