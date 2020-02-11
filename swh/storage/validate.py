# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
from typing import Dict, Iterable, List, Union

from swh.model.model import (
    SkippedContent, Content, Directory, Revision, Release, Snapshot,
    OriginVisit, Origin
)

from . import get_storage
from .exc import StorageArgumentException


VALIDATION_EXCEPTIONS = (
    KeyError,
    TypeError,
    ValueError,
)


@contextlib.contextmanager
def convert_validation_exceptions():
    """Catches validation errors arguments, and re-raises a
    StorageArgumentException."""
    try:
        yield
    except VALIDATION_EXCEPTIONS as e:
        raise StorageArgumentException(*e.args)


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class ValidatingProxyStorage:
    """Storage implementation converts dictionaries to swh-model objects
    before calling its backend, and back to dicts before returning results

    """
    def __init__(self, storage):
        self.storage = get_storage(**storage)

    def __getattr__(self, key):
        if key == 'storage':
            raise AttributeError(key)
        return getattr(self.storage, key)

    def content_add(self, content: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            contents = [Content.from_dict({**c, 'ctime': now()})
                        for c in content]
        return self.storage.content_add(contents)

    def content_add_metadata(self, content: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            contents = [Content.from_dict(c)
                        for c in content]
        return self.storage.content_add_metadata(contents)

    def skipped_content_add(self, content: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            contents = [SkippedContent.from_dict({**c, 'ctime': now()})
                        for c in content]
        return self.storage.skipped_content_add(contents)

    def directory_add(self, directories: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            directories = [Directory.from_dict(d) for d in directories]
        return self.storage.directory_add(directories)

    def revision_add(self, revisions: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            revisions = [Revision.from_dict(r) for r in revisions]
        return self.storage.revision_add(revisions)

    def release_add(self, releases: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            releases = [Release.from_dict(r) for r in releases]
        return self.storage.release_add(releases)

    def snapshot_add(self, snapshots: Iterable[Dict]) -> Dict:
        with convert_validation_exceptions():
            snapshots = [Snapshot.from_dict(s) for s in snapshots]
        return self.storage.snapshot_add(snapshots)

    def origin_visit_add(
            self, origin, date, type) -> Dict[str, Union[str, int]]:
        with convert_validation_exceptions():
            visit = OriginVisit(origin=origin, date=date, type=type,
                                status='ongoing', snapshot=None)
        return self.storage.origin_visit_add(
            visit.origin, visit.date, visit.type)

    def origin_add(self, origins: Iterable[Dict]) -> List[Dict]:
        with convert_validation_exceptions():
            origins = [Origin.from_dict(o) for o in origins]
        return self.storage.origin_add(origins)

    def origin_add_one(self, origin: Dict) -> int:
        with convert_validation_exceptions():
            origin = Origin.from_dict(origin)
        return self.storage.origin_add_one(origin)
