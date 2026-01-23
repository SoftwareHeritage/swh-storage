# Copyright (C) 2023-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable, List

from swh.model import discovery, model
from swh.model.model import Sha1Git
from swh.storage.interface import StorageInterface


class DiscoveryStorageConnection(discovery.ArchiveDiscoveryInterface):
    """Use the storage APIs to query the archive"""

    def __init__(
        self,
        contents: List[model.Content],
        skipped_contents: List[model.SkippedContent],
        directories: List[model.Directory],
        swh_storage: StorageInterface,
    ) -> None:
        self.contents = contents
        self.skipped_contents = skipped_contents
        self.directories = directories
        self.storage = swh_storage

    def content_missing(self, contents: List[Sha1Git]) -> Iterable[Sha1Git]:
        """List content missing from the archive by sha1"""
        return self.storage.content_missing_per_sha1_git(contents)

    def skipped_content_missing(
        self, skipped_contents: List[Sha1Git]
    ) -> Iterable[Sha1Git]:
        """List skipped content missing from the archive by sha1"""
        contents = [
            {"sha1_git": s, "sha1": None, "sha256": None, "blake2s256": None}
            for s in skipped_contents
        ]
        return (d["sha1_git"] for d in self.storage.skipped_content_missing(contents))

    def directory_missing(self, directories: List[Sha1Git]) -> Iterable[Sha1Git]:
        """List directories missing from the archive by sha1"""
        return self.storage.directory_missing(directories)
