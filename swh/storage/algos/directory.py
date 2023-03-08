# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable, List, Optional

from swh.core.api.classes import stream_results_optional
from swh.model.model import Directory, DirectoryEntry, Sha1Git
from swh.storage.interface import StorageInterface


def directory_get(
    storage: StorageInterface, directory_id: Sha1Git
) -> Optional[Directory]:
    """Get all the entries for a given directory

    Args:
        storage (swh.storage.interface.StorageInterface): the storage instance
        directory_id (bytes): the directory's identifier
    Returns:
        The directory if it could be properly put back together.
    """
    entries: Optional[Iterable[DirectoryEntry]] = stream_results_optional(
        storage.directory_get_entries,
        directory_id=directory_id,
    )
    if entries is None:
        return None
    return Directory(
        id=directory_id,
        entries=tuple(entries),
        raw_manifest=storage.directory_get_raw_manifest([directory_id])[directory_id],
    )


def directory_get_many(
    storage: StorageInterface, directory_ids: List[Sha1Git]
) -> Iterable[Optional[Directory]]:
    """Same as :func:`directory_get`, but fetches directories slightly more
    effectively by batching requests to ``directory_get_raw_manifest``.

    Args:
        storage (swh.storage.interface.StorageInterface): the storage instance
        directory_ids (bytes): the directories' identifiers
    Yields:
        The directories which could be properly put back together
    """
    raw_manifests = storage.directory_get_raw_manifest(directory_ids)
    for directory_id in directory_ids:
        if directory_id not in raw_manifests:
            yield None
        else:
            entries = stream_results_optional(
                storage.directory_get_entries,
                directory_id=directory_id,
            )
            assert entries, f"Directory {directory_id.hex()} stopped existing"
            yield Directory(
                id=directory_id,
                entries=tuple(entries),
                raw_manifest=raw_manifests[directory_id],
            )
