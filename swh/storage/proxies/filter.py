# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Dict, Iterable, List, Set, cast

from swh.model.model import (
    Content,
    Directory,
    Release,
    Revision,
    Sha1Git,
    SkippedContent,
)
from swh.storage import StorageSpec, get_storage
from swh.storage.interface import HashDict, StorageInterface


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

    object_types = ["content", "skipped_content", "directory", "revision", "release"]

    def __init__(self, storage: StorageSpec) -> None:
        self.storage: StorageInterface = get_storage(**storage)

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    def content_add(self, content: List[Content]) -> Dict[str, int]:
        empty_stat = {
            "content:add": 0,
            "content:add:bytes": 0,
        }
        if not content:
            return empty_stat
        contents_to_add = self._filter_missing_contents(content)
        if not contents_to_add:
            return empty_stat
        return self.storage.content_add(
            [x for x in content if x.sha256 in contents_to_add]
        )

    def skipped_content_add(self, content: List[SkippedContent]) -> Dict[str, int]:
        empty_stat = {"skipped_content:add": 0}
        if not content:
            return empty_stat
        contents_to_add = self._filter_missing_skipped_contents(content)
        if not contents_to_add and not any(c.sha1_git is None for c in content):
            return empty_stat
        return self.storage.skipped_content_add(
            [x for x in content if x.sha1_git is None or x.sha1_git in contents_to_add]
        )

    def directory_add(self, directories: List[Directory]) -> Dict[str, int]:
        empty_stat = {"directory:add": 0}
        if not directories:
            return empty_stat
        missing_ids = self._filter_missing_ids("directory", (d.id for d in directories))
        if not missing_ids:
            return empty_stat
        return self.storage.directory_add(
            [d for d in directories if d.id in missing_ids]
        )

    def revision_add(self, revisions: List[Revision]) -> Dict[str, int]:
        empty_stat = {"revision:add": 0}
        if not revisions:
            return empty_stat
        missing_ids = self._filter_missing_ids("revision", (r.id for r in revisions))
        if not missing_ids:
            return empty_stat
        return self.storage.revision_add([r for r in revisions if r.id in missing_ids])

    def release_add(self, releases: List[Release]) -> Dict[str, int]:
        empty_stat = {"release:add": 0}
        if not releases:
            return empty_stat
        missing_ids = self._filter_missing_ids("release", (r.id for r in releases))
        if not missing_ids:
            return empty_stat
        return self.storage.release_add([r for r in releases if r.id in missing_ids])

    def _filter_missing_contents(self, contents: List[Content]) -> Set[bytes]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha256 to check for existence in swh
                storage

        """
        missing_contents = []
        for content in contents:
            missing_contents.append(cast(HashDict, content.hashes()))

        return set(
            self.storage.content_missing(
                missing_contents,
                key_hash="sha256",
            )
        )

    def _filter_missing_skipped_contents(
        self, contents: List[SkippedContent]
    ) -> Set[Sha1Git]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha1_git to check for existence in swh
                storage

        """
        missing_contents = [
            dict(c.hashes()) for c in contents if c.sha1_git is not None
        ]

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
        return set(getattr(self.storage, f"{object_type}_missing")(list(ids)))
