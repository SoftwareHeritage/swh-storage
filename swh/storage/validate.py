# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Union

from swh.model.model import (
    SkippedContent,
    Content,
    Directory,
    Revision,
    Release,
    Snapshot,
    OriginVisit,
    Origin,
)

from . import get_storage
from .exc import StorageArgumentException


VALIDATION_EXCEPTIONS = [
    KeyError,
    TypeError,
    ValueError,
]


@contextlib.contextmanager
def convert_validation_exceptions():
    """Catches validation errors arguments, and re-raises a
    StorageArgumentException."""
    try:
        yield
    except tuple(VALIDATION_EXCEPTIONS) as e:
        raise StorageArgumentException(str(e))


class ValidatingProxyStorage:
    """Storage implementation converts dictionaries to swh-model objects
    before calling its backend, and back to dicts before returning results

    For test purposes.
    """

    def __init__(self, storage):
        self.storage = get_storage(**storage)

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    def content_add(self, content: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            contents = [Content.from_dict(c) for c in content]
        return self.storage.content_add(contents)

    def content_add_metadata(self, content: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            contents = [Content.from_dict(c) for c in content]
        return self.storage.content_add_metadata(contents)

    def skipped_content_add(self, content: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            contents = [SkippedContent.from_dict(c) for c in content]
        return self.storage.skipped_content_add(contents)

    def directory_add(self, directories: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            directories = [Directory.from_dict(d) for d in directories]
        return self.storage.directory_add(directories)

    def revision_add(self, revisions: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            revisions = [Revision.from_dict(r) for r in revisions]
        return self.storage.revision_add(revisions)

    def revision_get(self, revisions: Iterable[bytes]) -> Iterator[Optional[Dict]]:
        rev_dicts = self.storage.revision_get(revisions)
        with convert_validation_exceptions():
            for rev_dict in rev_dicts:
                if rev_dict is None:
                    yield None
                else:
                    yield Revision.from_dict(rev_dict).to_dict()

    def revision_log(
        self, revisions: Iterable[bytes], limit: Optional[int] = None
    ) -> Iterator[Dict]:
        for rev_dict in self.storage.revision_log(revisions, limit):
            with convert_validation_exceptions():
                rev_obj = Revision.from_dict(rev_dict)
            yield rev_obj.to_dict()

    def revision_shortlog(
        self, revisions: Iterable[bytes], limit: Optional[int] = None
    ) -> Iterator[Tuple[bytes, Tuple]]:
        for rev, parents in self.storage.revision_shortlog(revisions, limit):
            yield (rev, tuple(parents))

    def release_add(self, releases: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            releases = [Release.from_dict(r) for r in releases]
        return self.storage.release_add(releases)

    def snapshot_add(self, snapshots: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            snapshots = [Snapshot.from_dict(s) for s in snapshots]
        return self.storage.snapshot_add(snapshots)

    def origin_visit_add(self, visits: Iterable[OriginVisit]) -> Iterable[OriginVisit]:
        return self.storage.origin_visit_add(visits)

    def origin_add(self, origins: Union[Iterable[Dict], Iterable[Origin]]) -> List:
        origins_: List[Origin] = []
        for o in origins:
            ori: Origin
            if isinstance(o, Dict):
                with convert_validation_exceptions():
                    ori = Origin.from_dict(o)
            else:
                ori = o
            origins_.append(ori)
        return self.storage.origin_add(origins_)

    def origin_add_one(self, origin: Union[Dict, Origin]) -> int:
        origin_: Origin
        if isinstance(origin, Dict):
            with convert_validation_exceptions():
                origin_ = Origin.from_dict(origin)
        else:
            origin_ = origin
        return self.storage.origin_add_one(origin_)

    def clear_buffers(self, object_types: Optional[Iterable[str]] = None) -> None:
        return self.storage.clear_buffers(object_types)

    def flush(self, object_types: Optional[Iterable[str]] = None) -> Dict:
        return self.storage.flush(object_types)
