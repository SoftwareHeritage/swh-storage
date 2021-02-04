# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
from typing import Dict, Iterable, Mapping, Sequence, Tuple

from typing_extensions import Literal

from swh.core.utils import grouper
from swh.model.model import BaseModel, Content, SkippedContent
from swh.storage import get_storage
from swh.storage.interface import StorageInterface

LObjectType = Literal[
    "content", "skipped_content", "directory", "revision", "release", "snapshot"
]
OBJECT_TYPES: Tuple[LObjectType, ...] = (
    "content",
    "skipped_content",
    "directory",
    "revision",
    "release",
    "snapshot",
)

DEFAULT_BUFFER_THRESHOLDS: Dict[str, int] = {
    "content": 10000,
    "content_bytes": 100 * 1024 * 1024,
    "skipped_content": 10000,
    "directory": 25000,
    "revision": 100000,
    "release": 100000,
    "snapshot": 25000,
}


class BufferingProxyStorage:
    """Storage implementation in charge of accumulating objects prior to
       discussing with the "main" storage.

    Deduplicates values based on a tuple of keys depending on the object type.

    Sample configuration use case for buffering storage:

    .. code-block:: yaml

        storage:
          cls: buffer
          args:
            storage:
              cls: remote
              args: http://storage.internal.staging.swh.network:5002/
            min_batch_size:
              content: 10000
              content_bytes: 100000000
              skipped_content: 10000
              directory: 5000
              revision: 1000
              release: 10000
              snapshot: 5000

    """

    def __init__(self, storage: Mapping, min_batch_size: Mapping = {}):
        self.storage: StorageInterface = get_storage(**storage)

        self._buffer_thresholds = {**DEFAULT_BUFFER_THRESHOLDS, **min_batch_size}

        self._objects: Dict[LObjectType, Dict[Tuple[str, ...], BaseModel]] = {
            k: {} for k in OBJECT_TYPES
        }
        self._contents_size: int = 0

    def __getattr__(self, key: str):
        if key.endswith("_add"):
            object_type = key.rsplit("_", 1)[0]
            if object_type in OBJECT_TYPES:
                return partial(self.object_add, object_type=object_type, keys=["id"],)
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    def content_add(self, contents: Sequence[Content]) -> Dict:
        """Push contents to write to the storage in the buffer.

        Following policies apply:
            - if the buffer's threshold is hit, flush content to the storage.
            - otherwise, if the total size of buffered contents's threshold is hit,
            flush content to the storage.

        """
        stats = self.object_add(
            contents,
            object_type="content",
            keys=["sha1", "sha1_git", "sha256", "blake2s256"],
        )
        if not stats:  # We did not flush already
            self._contents_size += sum(c.length for c in contents)
            if self._contents_size >= self._buffer_thresholds["content_bytes"]:
                return self.flush(["content"])

        return stats

    def skipped_content_add(self, contents: Sequence[SkippedContent]) -> Dict:
        return self.object_add(
            contents,
            object_type="skipped_content",
            keys=["sha1", "sha1_git", "sha256", "blake2s256"],
        )

    def object_add(
        self,
        objects: Sequence[BaseModel],
        *,
        object_type: LObjectType,
        keys: Iterable[str],
    ) -> Dict[str, int]:
        """Push objects to write to the storage in the buffer. Flushes the
        buffer to the storage if the threshold is hit.

        """
        buffer_ = self._objects[object_type]
        for obj in objects:
            obj_key = tuple(getattr(obj, key) for key in keys)
            buffer_[obj_key] = obj
        if len(buffer_) >= self._buffer_thresholds[object_type]:
            return self.flush()

        return {}

    def flush(
        self, object_types: Sequence[LObjectType] = OBJECT_TYPES
    ) -> Dict[str, int]:
        summary: Dict[str, int] = {}

        def update_summary(stats):
            for k, v in stats.items():
                summary[k] = v + summary.get(k, 0)

        for object_type in object_types:
            buffer_ = self._objects[object_type]
            batches = grouper(buffer_.values(), n=self._buffer_thresholds[object_type])
            for batch in batches:
                add_fn = getattr(self.storage, "%s_add" % object_type)
                stats = add_fn(list(batch))
                update_summary(stats)

        # Flush underlying storage
        stats = self.storage.flush(object_types)
        update_summary(stats)

        self.clear_buffers(object_types)

        return summary

    def clear_buffers(self, object_types: Sequence[LObjectType] = OBJECT_TYPES) -> None:
        """Clear objects from current buffer.

        WARNING:

            data that has not been flushed to storage will be lost when this
            method is called. This should only be called when `flush` fails and
            you want to continue your processing.

        """
        for object_type in object_types:
            buffer_ = self._objects[object_type]
            buffer_.clear()
            if object_type == "content":
                self._contents_size = 0

        self.storage.clear_buffers(object_types)
