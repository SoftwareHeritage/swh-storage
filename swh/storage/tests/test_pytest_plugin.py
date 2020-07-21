# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.storage.pytest_plugin import OBJECT_FACTORY


from swh.model.model import BaseModel
from swh.storage.interface import StorageInterface


def test_sample_data(sample_data, sample_data_model):
    assert set(sample_data.keys()) == set(
        [
            "content",
            "skipped_content",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
            "origin_visit",
            "fetcher",
            "authority",
            "origin_metadata",
            "content_metadata",
        ]
    )
    for object_type, objs in sample_data.items():
        if object_type in [
            "content",
            "skipped_content",
            "directory",
            "revision",
            "origin",
            "fetcher",
            "authority",
            "origin_metadata",
            "content_metadata",
        ]:
            type_ = BaseModel
        else:
            type_ = dict

        for obj in objs:
            assert isinstance(obj, type_)


def test_sample_data_model(sample_data, sample_data_model):
    assert set(sample_data_model.keys()) == set(
        [
            "content",
            "skipped_content",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
            "origin_visit",
            "fetcher",
            "authority",
            "origin_metadata",
            "content_metadata",
        ]
    )

    for object_type, objs in sample_data_model.items():
        assert object_type in OBJECT_FACTORY

        for obj in objs:
            assert isinstance(obj, BaseModel)

        assert len(objs) == len(sample_data[object_type])


def test_swh_storage(swh_storage: StorageInterface):
    assert isinstance(swh_storage, StorageInterface) is not None


def test_swh_storage_backend_config(swh_storage_backend_config):
    assert isinstance(swh_storage_backend_config, dict)
