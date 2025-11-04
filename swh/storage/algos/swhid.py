# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from collections import defaultdict
from typing import Iterable, TypeVar

from swh.model.hashutil import hash_to_bytes
from swh.model.swhids import (
    CoreSWHID,
    ExtendedObjectType,
    ExtendedSWHID,
    ObjectType,
    QualifiedSWHID,
)
from swh.storage.interface import StorageInterface

T = TypeVar("T", CoreSWHID, ExtendedSWHID, CoreSWHID | ExtendedSWHID)


def known_swhids(storage: StorageInterface, swhids: Iterable[T]) -> set[T]:
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
        TypeError: received a ``QualifiedSWHID`` in the ``swhids`` list

    Returns:
        A set of SWHIDs found in the storage
    """

    grouped_swhids: dict[ObjectType | ExtendedObjectType, list[T]] = defaultdict(list)
    missing: set[bytes] = set()

    for swhid in swhids:
        if isinstance(swhid, QualifiedSWHID):
            raise TypeError(
                f"This method can't properly handle QualifiedSWHID like {swhid} "
                "but only CoreSWHID or ExtendedSWHID"
            )
        grouped_swhids[swhid.object_type].append(swhid)

    for object_type, objects in grouped_swhids.items():
        if object_type == ObjectType.CONTENT:
            storage_missing_method = storage.content_missing_per_sha1_git
        else:
            storage_missing_method = getattr(
                storage, f"{object_type.name.lower()}_missing"
            )

        missing |= set(
            storage_missing_method(
                [hash_to_bytes(swhid.object_id) for swhid in objects]
            )
        )
    return {swhid for swhid in swhids if hash_to_bytes(swhid.object_id) not in missing}


def swhid_is_known(storage: StorageInterface, swhid: CoreSWHID | ExtendedSWHID) -> bool:
    """Query the storage to check if ``swhid`` exists.

    A wrapper for ``known_swhids`` but for a single SWHID.

    Args:
        storage: a ``StorageInterface``
        swhid: a SWHID

    Returns:
        True if ``swhid`` exists in the storage
    """
    return swhid in known_swhids(storage, [swhid])
