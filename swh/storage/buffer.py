# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import deque
from functools import partial
from typing import Optional, Iterable, Dict

from swh.core.utils import grouper
from swh.storage import get_storage


class BufferingProxyStorage:
    """Storage implementation in charge of accumulating objects prior to
       discussing with the "main" storage.

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

    """
    def __init__(self, storage, min_batch_size=None):
        self.storage = get_storage(**storage)

        if min_batch_size is None:
            min_batch_size = {}

        self.min_batch_size = {
            'content': min_batch_size.get('content', 10000),
            'content_bytes': min_batch_size.get('content_bytes',
                                                100*1024*1024),
            'skipped_content': min_batch_size.get('skipped_content', 10000),
            'directory': min_batch_size.get('directory', 25000),
            'revision': min_batch_size.get('revision', 100000),
            'release': min_batch_size.get('release', 100000),
        }
        self.object_types = [
            'content', 'skipped_content', 'directory', 'revision', 'release']
        self._objects = {k: deque() for k in self.object_types}

    def __getattr__(self, key):
        if key.endswith('_add'):
            object_type = key.rsplit('_', 1)[0]
            if object_type in self.object_types:
                return partial(
                    self.object_add, object_type=object_type
                )
        if key == 'storage':
            raise AttributeError(key)
        return getattr(self.storage, key)

    def content_add(self, content: Iterable[Dict]) -> Dict:
        """Enqueue contents to write to the storage.

        Following policies apply:

            - First, check if the queue's threshold is hit.
              If it is flush content to the storage.

            - If not, check if the total size of enqueued contents's
              threshold is hit. If it is flush content to the storage.

        """
        s = self.object_add(content, object_type='content')
        if not s:
            q = self._objects['content']
            total_size = sum(c['length'] for c in q)
            if total_size >= self.min_batch_size['content_bytes']:
                return self.flush(['content'])

        return s

    def flush(self, object_types: Optional[Iterable[str]] = None) -> Dict:
        if object_types is None:
            object_types = self.object_types
        summary = {}  # type: Dict[str, Dict]
        for object_type in object_types:
            q = self._objects[object_type]
            for objs in grouper(q, n=self.min_batch_size[object_type]):
                add_fn = getattr(self.storage, '%s_add' % object_type)
                s = add_fn(objs)
                summary = {k: v + summary.get(k, 0)
                           for k, v in s.items()}
            q.clear()

        return summary

    def object_add(self, objects: Iterable[Dict], *, object_type: str) -> Dict:
        """Enqueue objects to write to the storage. This checks if the queue's
           threshold is hit. If it is actually write those to the storage.

        """
        q = self._objects[object_type]
        threshold = self.min_batch_size[object_type]
        q.extend(objects)
        if len(q) >= threshold:
            return self.flush()

        return {}
