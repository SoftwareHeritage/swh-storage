# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable, List

from swh.model.model import ObjectType, Sha1Git
from swh.storage.interface import StorageInterface


def identifiers_missing(
    storage: StorageInterface, obj_type: ObjectType, obj_ids: List[Sha1Git]
) -> Iterable[Sha1Git]:
    """Lookup missing Software Heritage hashes, choosing the correct
    underlying method based on the type.

    The idea is that conceptually we are looking up a list of Core
    SWHIDs with the same object type part, and to enforce that we pash
    the type and the hashes separately.

    Args:
        storage (swh.storage.interface.StorageInterface): the storage
        instance

        obj_type: What type of object are the hashes supposed to point
        to.

        obj_ids: list of hashes to look up

    Yields:
        missing hashes for the given object type
    """

    fun = {
        ObjectType.CONTENT: storage.content_missing_per_sha1_git,
        ObjectType.DIRECTORY: storage.directory_missing,
        ObjectType.REVISION: storage.revision_missing,
        ObjectType.RELEASE: storage.release_missing,
        ObjectType.SNAPSHOT: storage.snapshot_missing,
    }[obj_type]

    return fun(obj_ids)
