# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

import pytest

from swh.model.model import Directory, DirectoryEntry
from swh.storage.algos.directory import directory_get, directory_get_many

from ..storage_data import StorageData


@pytest.mark.parametrize("directory_id", [d.id for d in StorageData.directories])
def test_directory_small(swh_storage, directory_id):
    swh_storage.directory_add(StorageData.directories)

    (expected_directory,) = [d for d in StorageData.directories if d.id == directory_id]
    returned_directory = directory_get(swh_storage, directory_id)
    assert returned_directory.id == expected_directory.id
    assert set(returned_directory.entries) == set(expected_directory.entries)
    assert returned_directory.raw_manifest == expected_directory.raw_manifest


def test_directory_missing(swh_storage):
    swh_storage.directory_add(StorageData.directories)

    assert directory_get(swh_storage, b"\x42" * 20) is None


def test_directory_large(swh_storage):
    expected_directory = Directory(
        entries=tuple(
            DirectoryEntry(
                name=f"entry{i:04}".encode(),
                type="file",
                target=b"\x00" * 20,
                perms=0o000664,
            )
            for i in range(10)
        )
    )

    swh_storage.directory_add([expected_directory])

    returned_directory = directory_get(swh_storage, expected_directory.id)

    assert returned_directory.id == expected_directory.id
    assert set(returned_directory.entries) == set(expected_directory.entries)
    assert returned_directory.raw_manifest == expected_directory.raw_manifest


def test_directories_small(swh_storage):
    swh_storage.directory_add(StorageData.directories)

    directory_ids = [d.id for d in StorageData.directories]

    returned_directories = list(directory_get_many(swh_storage, directory_ids))

    assert sorted(d.id for d in returned_directories) == sorted(directory_ids)
    for returned_directory in returned_directories:
        (expected_directory,) = [
            d for d in StorageData.directories if d.id == returned_directory.id
        ]
        assert set(returned_directory.entries) == set(expected_directory.entries)
        assert returned_directory.raw_manifest == expected_directory.raw_manifest


def test_directories_missing(swh_storage):
    swh_storage.directory_add(StorageData.directories)

    missing_ids = [b"\x42" * 20, b"\x24" * 20]
    directory_ids = [d.id for d in StorageData.directories]
    directory_ids += missing_ids
    random.shuffle(directory_ids)

    returned_directories = list(directory_get_many(swh_storage, directory_ids))

    assert [d and d.id for d in returned_directories] == [
        None if id_ in missing_ids else id_ for id_ in directory_ids
    ]


def test_directories_large(swh_storage):
    expected_directory = Directory(
        entries=tuple(
            DirectoryEntry(
                name=f"entry{i:04}".encode(),
                type="file",
                target=b"\x00" * 20,
                perms=0o000664,
            )
            for i in range(10)
        )
    )

    swh_storage.directory_add([expected_directory])

    returned_directories = list(
        directory_get_many(swh_storage, [expected_directory.id, b"\x42" * 20])
    )

    (returned_directory, none) = returned_directories
    assert none is None

    assert returned_directory.id == expected_directory.id
    assert set(returned_directory.entries) == set(expected_directory.entries)
    assert returned_directory.raw_manifest == expected_directory.raw_manifest
