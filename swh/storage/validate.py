# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
from typing import Dict, Iterable, Iterator, Optional, Tuple, Type, TypeVar, Union

from deprecated import deprecated

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


ModelObject = TypeVar(
    "ModelObject",
    Content,
    SkippedContent,
    Directory,
    Revision,
    Release,
    Snapshot,
    OriginVisit,
    Origin,
)


def dict_converter(
    model: Type[ModelObject], obj: Union[Dict, ModelObject]
) -> ModelObject:
    """Convert dicts to model objects; Passes through model objects as well."""
    if isinstance(obj, dict):
        with convert_validation_exceptions():
            return model.from_dict(obj)
    else:
        return obj


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

    def content_add(self, content: Iterable[Union[Content, Dict]]) -> Dict:
        return self.storage.content_add([dict_converter(Content, c) for c in content])

    def content_add_metadata(self, content: Iterable[Union[Content, Dict]]) -> Dict:
        return self.storage.content_add_metadata(
            [dict_converter(Content, c) for c in content]
        )

    def skipped_content_add(
        self, content: Iterable[Union[SkippedContent, Dict]]
    ) -> Dict:
        return self.storage.skipped_content_add(
            [dict_converter(SkippedContent, c) for c in content]
        )

    def directory_add(self, directories: Iterable[Union[Directory, Dict]]) -> Dict:
        return self.storage.directory_add(
            [dict_converter(Directory, d) for d in directories]
        )

    def revision_add(self, revisions: Iterable[Union[Revision, Dict]]) -> Dict:
        return self.storage.revision_add(
            [dict_converter(Revision, r) for r in revisions]
        )

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

    def release_add(self, releases: Iterable[Union[Dict, Release]]) -> Dict:
        return self.storage.release_add(
            [dict_converter(Release, release) for release in releases]
        )

    def snapshot_add(self, snapshots: Iterable[Union[Dict, Snapshot]]) -> Dict:
        return self.storage.snapshot_add(
            [dict_converter(Snapshot, snapshot) for snapshot in snapshots]
        )

    def origin_visit_add(self, visits: Iterable[OriginVisit]) -> Iterable[OriginVisit]:
        return self.storage.origin_visit_add(visits)

    def origin_add(self, origins: Iterable[Union[Dict, Origin]]) -> Dict[str, int]:
        return self.storage.origin_add([dict_converter(Origin, o) for o in origins])

    @deprecated("Use origin_add([origin]) instead")
    def origin_add_one(self, origin: Union[Dict, Origin]) -> int:
        return self.storage.origin_add_one(dict_converter(Origin, origin))

    def clear_buffers(self, object_types: Optional[Iterable[str]] = None) -> None:
        return self.storage.clear_buffers(object_types)

    def flush(self, object_types: Optional[Iterable[str]] = None) -> Dict:
        return self.storage.flush(object_types)
