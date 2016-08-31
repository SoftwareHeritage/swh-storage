# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core import hashutil
from swh.core.config import load_named_config

from swh.scheduler.task import Task
from swh.storage import get_storage

DEFAULT_CONFIG = {
    'storage_class': ('str', 'remote_storage'),
    'storage_args': ('list[str]', [
        'http://localhost:5000/',
    ]),
}


class PopulateCacheContentRevision(Task):
    """Import one svn repository to Software Heritage.

    """
    task_queue = 'swh_populate_cache_content_revision'

    @property
    def config(self):
        if not hasattr(self, '__config'):
            self.__config = load_named_config(
                'cache/contents.ini',
                DEFAULT_CONFIG,
            )
        return self.__config

    def run(self, revisions):
        """Cache the cache_content_revision table for the revisions provided.

        Args:
            revisions: List of revisions to cache populate.

        """
        config = self.config
        storage = get_storage(
            config['storage_class'],
            config['storage_args'],
        )

        for rev in revisions:
            revision = hashutil.hex_to_hash(rev)
            storage.cache_content_revision_add(revision)
