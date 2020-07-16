# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.storage.pytest_plugin import OBJECT_FACTORY


from swh.model.model import BaseModel


def test_sample_data(sample_data, sample_data_model):
    assert set(sample_data.keys()) == set(
        [
            "content",
            "content_metadata",
            "skipped_content",
            "person",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
            "fetcher",
            "authority",
            "origin_metadata",
        ]
    )
    for object_type, objs in sample_data.items():
        for obj in objs:
            assert isinstance(obj, dict)

        if sample_data_model.get(object_type):
            # metadata keys are missing because conversion is not possible yet
            assert len(objs) == len(sample_data_model[object_type])


def test_sample_data_model(sample_data, sample_data_model):
    assert set(sample_data_model.keys()) == set(
        [
            "content",
            "content_metadata",
            "skipped_content",
            "person",
            "directory",
            "revision",
            "release",
            "snapshot",
            "origin",
        ]
    )

    for object_type, objs in sample_data_model.items():
        assert object_type in OBJECT_FACTORY

        for obj in objs:
            assert isinstance(obj, BaseModel)

        assert len(objs) == len(sample_data[object_type])
