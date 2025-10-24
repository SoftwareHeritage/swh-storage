# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from collections import defaultdict
from typing import Iterable

from swh.model.hashutil import hash_to_bytes
from swh.model.swhids import ObjectType, QualifiedSWHID
from swh.storage.interface import StorageInterface


def known_swhids(
    storage: StorageInterface, swhids: Iterable[QualifiedSWHID]
) -> dict[QualifiedSWHID, bool]:
    """Query the storage to check if ``swhids`` exist.

    We group SWHIDs by type and then call the corresponding storage ``_missing`` method
    (directory_missing, snapshot_missing, etc.) and switch the results as we want to
    know what exists, not what's missing.

    Args:
        storage: a ``StorageInterface``
        swhid: a list of ``QualifiedSWHID``

    Returns:
        A dictionary with SWHIDs as keys and a boolean indicating their existence in the
        storage
    """

    grouped_swhids: dict[ObjectType, list[QualifiedSWHID]] = defaultdict(list)
    results: dict[QualifiedSWHID, bool] = {}

    for swhid in swhids:
        grouped_swhids[swhid.object_type].append(swhid)

    for object_type, swhids in grouped_swhids.items():
        storage_missing_method = getattr(storage, f"{object_type.name.lower()}_missing")
        missing_ids: list[bytes] = list(
            storage_missing_method([hash_to_bytes(swhid.object_id) for swhid in swhids])
        )
        for swhid in swhids:
            results[swhid] = hash_to_bytes(swhid.object_id) not in missing_ids

    return results


def known_swhid(storage: StorageInterface, swhid: QualifiedSWHID) -> bool:
    """Query the storage to check if ``swhid`` exists.

    Like ``known_swhids`` but for a single SWHID.

    Args:
        storage: a ``StorageInterface``
        swhid: a ``QualifiedSWHID``

    Returns:
        True if ``swhid`` exists in the storage
    """
    return known_swhids(storage, [swhid])[swhid]
