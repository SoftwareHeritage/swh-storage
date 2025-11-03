# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from collections import defaultdict
from typing import Iterable

from swh.model.hashutil import hash_to_bytes
from swh.model.swhids import (
    CoreSWHID,
    ExtendedObjectType,
    ExtendedSWHID,
    ObjectType,
    QualifiedSWHID,
)
from swh.storage.interface import StorageInterface


def known_swhids(
    storage: StorageInterface, swhids: Iterable[CoreSWHID | ExtendedSWHID]
) -> dict[CoreSWHID | ExtendedSWHID, bool]:
    """Query the storage to check if ``swhids`` exist.

    We group SWHIDs by type and then call the corresponding storage ``_missing`` method
    (directory_missing, snapshot_missing, etc.) and switch the results as we want to
    know what exists, not what's missing.

    As the storage does not use values from the qualifier (snapshot, etc.) we exclude
    ``QualifiedSWHID`` from this method because
    ``known_swhids(storage, swh:1:cnt:1234567890;visit=swh:1:snp:098654321)``
    could return that `swh:1:cnt:1234567890` exists even if `swh:1:snp:098654321`
    doesn't.

    Args:
        storage: a ``StorageInterface``
        swhid: a list of SWHIDs

    Raises:
        AssertionError: received a ``QualifiedSWHID`` in the ``swhids`` list

    Returns:
        A dictionary with SWHIDs as keys and a boolean indicating their existence in the
        storage
    """

    grouped_swhids: dict[
        ObjectType | ExtendedObjectType, list[CoreSWHID | ExtendedSWHID]
    ] = defaultdict(list)
    results: dict[CoreSWHID | ExtendedSWHID, bool] = {}

    for swhid in swhids:
        if isinstance(swhid, QualifiedSWHID):
            raise AssertionError(
                f"This method can't properly handle QualifiedSWHID like {swhid} but "
                "but CoreSWHID or ExtendedSWHID"
            )
        grouped_swhids[swhid.object_type].append(swhid)

    for object_type, swhids in grouped_swhids.items():
        if object_type == ObjectType.CONTENT:
            storage_missing_method = storage.content_missing_per_sha1_git
        else:
            storage_missing_method = getattr(
                storage, f"{object_type.name.lower()}_missing"
            )
        missing_ids: list[bytes] = list(
            storage_missing_method([hash_to_bytes(swhid.object_id) for swhid in swhids])
        )
        for swhid in swhids:
            results[swhid] = hash_to_bytes(swhid.object_id) not in missing_ids

    return results


def swhid_is_known(storage: StorageInterface, swhid: CoreSWHID | ExtendedSWHID) -> bool:
    """Query the storage to check if ``swhid`` exists.

    A wrapper for ``known_swhids`` but for a single SWHID.

    Args:
        storage: a ``StorageInterface``
        swhid: a SWHID

    Returns:
        True if ``swhid`` exists in the storage
    """
    return known_swhids(storage, [swhid])[swhid]
