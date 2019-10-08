# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Dict, Generator, Sequence, Set

from swh.storage import get_storage


class FilteringProxyStorage:
    """Filtering Storage implementation. This is in charge of transparently
       filtering out known objects prior to adding them to storage.

    Sample configuration use case for filtering storage:

    .. code-block: yaml

        storage:
          cls: filter
          args:
            storage:
              cls: remote
              args: http://storage.internal.staging.swh.network:5002/

    """
    def __init__(self, storage):
        self.storage = get_storage(**storage)
        self.objects_seen = {
            'content': set(),    # set of content hashes (sha256) seen
            'directory': set(),
            'revision': set(),
        }

    def __getattr__(self, key):
        return getattr(self.storage, key)

    def content_add(self, content: Sequence[Dict]) -> Dict:
        contents = list(content)
        contents_to_add = self._filter_missing_contents(contents)
        return self.storage.content_add(
            x for x in contents if x['sha256'] in contents_to_add
        )

    def directory_add(self, directories: Sequence[Dict]) -> Dict:
        directories = list(directories)
        missing_ids = self._filter_missing_ids(
            'directory',
            (d['id'] for d in directories)
        )
        return self.storage.directory_add(
            d for d in directories if d['id'] in missing_ids
        )

    def revision_add(self, revisions):
        revisions = list(revisions)
        missing_ids = self._filter_missing_ids(
            'revision',
            (d['id'] for d in revisions)
        )
        return self.storage.revision_add(
            r for r in revisions if r['id'] in missing_ids
        )

    def _filter_missing_contents(
            self, content_hashes: Sequence[Dict]) -> Set[bytes]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha256 to check for existence in swh
                storage

        """
        objects_seen = self.objects_seen['content']
        missing_hashes = []
        for hashes in content_hashes:
            if hashes['sha256'] in objects_seen:
                continue
            objects_seen.add(hashes['sha256'])
            missing_hashes.append(hashes)

        return set(self.storage.content_missing(
            missing_hashes,
            key_hash='sha256',
        ))

    def _filter_missing_ids(
            self,
            object_type: str,
            ids: Generator[bytes, None, None]) -> Set[bytes]:
        """Filter missing ids from the storage for a given object type.

        Args:
            object_type: object type to use {revision, directory}
            ids: Sequence of object_type ids

        Returns:
            Missing ids from the storage for object_type

        """
        objects_seen = self.objects_seen[object_type]
        missing_ids = []
        for id in ids:
            if id in objects_seen:
                continue
            objects_seen.add(id)
            missing_ids.append(id)

        fn_by_object_type = {
            'revision': self.storage.revision_missing,
            'directory': self.storage.directory_missing,
        }

        fn = fn_by_object_type[object_type]
        return set(fn(missing_ids))
