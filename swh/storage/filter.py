# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Dict, Iterable, List, Set

from swh.model.model import (
    Content,
    SkippedContent,
    Directory,
    Revision,
    Sha1Git,
)

from swh.storage import get_storage
from swh.storage.interface import StorageInterface


class FilteringProxyStorage:
    """Filtering Storage implementation. This is in charge of transparently
       filtering out known objects prior to adding them to storage.

    Sample configuration use case for filtering storage:

    .. code-block: yaml

        storage:
          cls: filter
          storage:
            cls: remote
            url: http://storage.internal.staging.swh.network:5002/

    """

    object_types = ["content", "skipped_content", "directory", "revision"]

    def __init__(self, storage):
        self.storage: StorageInterface = get_storage(**storage)

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    def content_add(self, content: List[Content]) -> Dict:
        contents_to_add = self._filter_missing_contents(content)
        return self.storage.content_add(
            [x for x in content if x.sha256 in contents_to_add]
        )

    def skipped_content_add(self, content: List[SkippedContent]) -> Dict:
        contents_to_add = self._filter_missing_skipped_contents(content)
        return self.storage.skipped_content_add(
            [x for x in content if x.sha1_git is None or x.sha1_git in contents_to_add]
        )

    def directory_add(self, directories: List[Directory]) -> Dict:
        missing_ids = self._filter_missing_ids("directory", (d.id for d in directories))
        return self.storage.directory_add(
            [d for d in directories if d.id in missing_ids]
        )

    def revision_add(self, revisions: List[Revision]) -> Dict:
        missing_ids = self._filter_missing_ids("revision", (r.id for r in revisions))
        return self.storage.revision_add([r for r in revisions if r.id in missing_ids])

    def _filter_missing_contents(self, contents: List[Content]) -> Set[bytes]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha256 to check for existence in swh
                storage

        """
        missing_contents = []
        for content in contents:
            missing_contents.append(content.hashes())

        return set(self.storage.content_missing(missing_contents, key_hash="sha256",))

    def _filter_missing_skipped_contents(
        self, contents: List[SkippedContent]
    ) -> Set[Sha1Git]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha1_git to check for existence in swh
                storage

        """
        missing_contents = [c.hashes() for c in contents if c.sha1_git is not None]

        ids = set()
        for c in self.storage.skipped_content_missing(missing_contents):
            if c is None or c.get("sha1_git") is None:
                continue
            ids.add(c["sha1_git"])
        return ids

    def _filter_missing_ids(self, object_type: str, ids: Iterable[bytes]) -> Set[bytes]:
        """Filter missing ids from the storage for a given object type.

        Args:
            object_type: object type to use {revision, directory}
            ids: List of object_type ids

        Returns:
            Missing ids from the storage for object_type

        """
        missing_ids = []
        for id in ids:
            missing_ids.append(id)

        fn_by_object_type = {
            "revision": self.storage.revision_missing,
            "directory": self.storage.directory_missing,
        }

        fn = fn_by_object_type[object_type]
        return set(fn(missing_ids))
