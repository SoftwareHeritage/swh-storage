# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

import attr
import pytest

from swh.model.model import Directory, DirectoryEntry
from swh.storage.algos.directory import (
    directory_get,
    directory_get_many,
    directory_get_many_with_possibly_duplicated_entries,
)

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


@pytest.mark.parametrize("with_possibly_duplicated_entries", [True, False])
def test_directories_small(swh_storage, with_possibly_duplicated_entries):
    swh_storage.directory_add(StorageData.directories)

    directory_ids = [d.id for d in StorageData.directories]

    if with_possibly_duplicated_entries:
        ret = list(
            directory_get_many_with_possibly_duplicated_entries(
                swh_storage, directory_ids
            )
        )
        assert not any(is_corrupt for (is_corrupt, _) in ret)
        returned_directories = [d for (_, d) in ret]
    else:
        returned_directories = list(directory_get_many(swh_storage, directory_ids))

    assert sorted(d.id for d in returned_directories) == sorted(directory_ids)
    for returned_directory in returned_directories:
        (expected_directory,) = [
            d for d in StorageData.directories if d.id == returned_directory.id
        ]
        assert set(returned_directory.entries) == set(expected_directory.entries)
        assert returned_directory.raw_manifest == expected_directory.raw_manifest


@pytest.mark.parametrize("with_possibly_duplicated_entries", [True, False])
def test_directories_missing(swh_storage, with_possibly_duplicated_entries):
    swh_storage.directory_add(StorageData.directories)

    missing_ids = [b"\x42" * 20, b"\x24" * 20]
    directory_ids = [d.id for d in StorageData.directories]
    directory_ids += missing_ids
    random.shuffle(directory_ids)

    if with_possibly_duplicated_entries:
        ret = list(
            directory_get_many_with_possibly_duplicated_entries(
                swh_storage, directory_ids
            )
        )
        assert not any(
            False if x is None else x[0] for x in ret
        ), "Unexpectedly marked corrupt"
        returned_directories = [None if x is None else x[1] for x in ret]
    else:
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


@pytest.mark.parametrize("allow_duplicated_entries", [False, True])
def test_directories_duplicate_entries(swh_storage, allow_duplicated_entries):
    run_validators = attr.get_run_validators()
    attr.set_run_validators(False)
    try:
        invalid_directory = Directory(
            entries=(
                DirectoryEntry(name=b"foo", type="dir", target=b"\x01" * 20, perms=1),
                DirectoryEntry(name=b"foo", type="file", target=b"\x00" * 20, perms=0),
            )
        )
    finally:
        attr.set_run_validators(run_validators)
    swh_storage.directory_add([invalid_directory])

    deduplicated_directory = Directory(
        id=invalid_directory.id,
        entries=(
            DirectoryEntry(name=b"foo", type="dir", target=b"\x01" * 20, perms=1),
            DirectoryEntry(
                name=b"foo_0000000000", type="file", target=b"\x00" * 20, perms=0
            ),
        ),
        raw_manifest=(
            # fmt: off
            b"tree 52\x00"
            + b"0 foo\x00" + b"\x00" * 20
            + b"1 foo\x00" + b"\x01" * 20
            # fmt: on
        ),
    )

    # Make sure we successfully inserted a corrupt directory, otherwise this test
    # is pointless
    db = swh_storage.get_db()
    with db.conn.cursor() as cur:
        cur.execute("select id, dir_entries, file_entries, raw_manifest from directory")
        (row,) = cur
        (id_, (dir_entry,), (file_entry,), raw_manifest) = row
        assert id_ == invalid_directory.id
        assert raw_manifest is None
        cur.execute("select id, name, target from directory_entry_dir")
        assert list(cur) == [(dir_entry, b"foo", b"\x01" * 20)]
        cur.execute("select id, name, target from directory_entry_file")
        assert list(cur) == [(file_entry, b"foo", b"\x00" * 20)]

    if allow_duplicated_entries:
        ((is_corrupt, returned_directory),) = list(
            directory_get_many_with_possibly_duplicated_entries(
                swh_storage, [invalid_directory.id]
            )
        )

        assert is_corrupt
        assert (
            returned_directory.id == invalid_directory.id == deduplicated_directory.id
        )
        assert set(returned_directory.entries) == set(deduplicated_directory.entries)
        assert returned_directory.raw_manifest == deduplicated_directory.raw_manifest
    else:
        with pytest.raises(ValueError):
            list(directory_get_many(swh_storage, [invalid_directory.id]))
