# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Dict, Iterable, List

from swh.model.hashutil import MultiHash, hash_to_bytes, hash_to_hex
from swh.model.model import Content, Directory, Release, Revision, Snapshot
from swh.storage import get_storage
from swh.storage.exc import StorageArgumentException
from swh.storage.interface import StorageInterface


class ValidatingProxyStorage:
    """Proxy for storage classes, which checks inserted objects have a correct hash.

    Sample configuration use case for filtering storage:

    .. code-block: yaml

        storage:
          cls: validate
          storage:
            cls: remote
            url: http://storage.internal.staging.swh.network:5002/

    """

    def __init__(self, storage):
        self.storage: StorageInterface = get_storage(**storage)

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    def _check_hashes(self, objects: Iterable):
        for obj in objects:
            id_ = hash_to_bytes(obj.compute_hash())
            if id_ != obj.id:
                raise StorageArgumentException(
                    f"Object has id {hash_to_hex(obj.id)}, "
                    f"but it should be {hash_to_hex(id_)}: {obj}"
                )

    def content_add(self, content: List[Content]) -> Dict:
        for cont in content:
            hashes = MultiHash.from_data(cont.data).digest()
            if hashes != cont.hashes():
                raise StorageArgumentException(
                    f"Object has hashes {cont.hashes()}, but they should be {hashes}"
                )
        return self.storage.content_add(content)

    def directory_add(self, directories: List[Directory]) -> Dict:
        self._check_hashes(directories)
        return self.storage.directory_add(directories)

    def revision_add(self, revisions: List[Revision]) -> Dict:
        self._check_hashes(revisions)
        return self.storage.revision_add(revisions)

    def release_add(self, releases: List[Release]) -> Dict:
        self._check_hashes(releases)
        return self.storage.release_add(releases)

    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict:
        self._check_hashes(snapshots)
        return self.storage.snapshot_add(snapshots)
