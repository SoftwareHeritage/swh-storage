# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Dict, List, Mapping

from swh.model.model import Directory, OriginVisitStatus, Release, Revision, Snapshot
from swh.storage import get_storage
from swh.storage.interface import ObjectReference, StorageInterface


class RecordReferencesProxyStorage:
    """Automatically store object references when adding objects that have them"""

    def __init__(self, storage: Mapping):
        self.storage: StorageInterface = get_storage(**storage)

    def __getattr__(self, key: str):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    def directory_add(self, directories: List[Directory]) -> Dict[str, int]:
        object_references = [
            ObjectReference(
                source=directory.swhid().to_extended(),
                target=entry.swhid().to_extended(),
            )
            for directory in directories
            for entry in directory.entries
        ]
        stats = self.storage.object_references_add(object_references)
        stats.update(self.storage.directory_add(directories))
        return stats

    def revision_add(self, revisions: List[Revision]) -> Dict[str, int]:
        object_references = []
        for revision in revisions:
            object_references.append(
                ObjectReference(
                    source=revision.swhid().to_extended(),
                    target=revision.directory_swhid().to_extended(),
                )
            )
            for swhid in revision.parent_swhids():
                object_references.append(
                    ObjectReference(
                        source=revision.swhid().to_extended(),
                        target=swhid.to_extended(),
                    )
                )

        stats = self.storage.object_references_add(object_references)
        stats.update(self.storage.revision_add(revisions))
        return stats

    def release_add(self, releases: List[Release]) -> Dict[str, int]:
        object_references = []
        for release in releases:
            target_swhid = release.target_swhid()
            if target_swhid:
                object_references.append(
                    ObjectReference(
                        source=release.swhid().to_extended(),
                        target=target_swhid.to_extended(),
                    )
                )
        stats = self.storage.object_references_add(object_references)
        stats.update(self.storage.release_add(releases))
        return stats

    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict[str, int]:
        object_references = []
        for snapshot in snapshots:
            snapshot_swhid = snapshot.swhid().to_extended()
            for branch in snapshot.branches.values():
                if not branch:
                    continue
                target_swhid = branch.swhid()
                if target_swhid:
                    object_references.append(
                        ObjectReference(
                            source=snapshot_swhid,
                            target=target_swhid.to_extended(),
                        )
                    )

        stats = self.storage.object_references_add(object_references)
        stats.update(self.storage.snapshot_add(snapshots))
        return stats

    def origin_visit_status_add(
        self,
        visit_statuses: List[OriginVisitStatus],
    ) -> Dict[str, int]:
        object_references = []
        for visit_status in visit_statuses:
            snapshot_swhid = visit_status.snapshot_swhid()
            if snapshot_swhid:
                object_references.append(
                    ObjectReference(
                        source=visit_status.origin_swhid(),
                        target=snapshot_swhid.to_extended(),
                    )
                )

        stats = self.storage.object_references_add(object_references)
        stats.update(self.storage.origin_visit_status_add(visit_statuses))
        return stats
