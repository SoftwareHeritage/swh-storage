# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import group

from swh.core import hashutil
from swh.core.config import load_named_config

from swh.scheduler.task import Task
from swh.storage import get_storage

BASE_CONFIG_PATH = 'storage/provenance_cache'
DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'remote_storage',
        'args': ('list[str]', ['http://localhost:5000/']),
    }),
    'revision_packet_size': ('int', 100),
}


class PopulateCacheContentRevision(Task):
    """Populate the content -> revision provenance cache for some revisions"""

    task_queue = 'swh_populate_cache_content_revision'

    @property
    def config(self):
        if not hasattr(self, '__config'):
            self.__config = load_named_config(BASE_CONFIG_PATH, DEFAULT_CONFIG)
        return self.__config

    def run(self, revisions):
        """Cache the cache_content_revision table for the revisions provided.

        Args:
            revisions: List of revisions to cache populate.

        """
        config = self.config
        storage = get_storage(
            config['storage']['cls'],
            config['storage']['args']
        )

        storage.cache_content_revision_add(
            hashutil.hex_to_hash(revision) for revision in revisions
        )


class PopulateCacheRevisionOrigin(Task):
    """Populate the revision -> origin provenance cache for one origin's
    visit"""

    task_queue = 'swh_populate_cache_revision_origin'

    @property
    def config(self):
        if not hasattr(self, '__config'):
            self.__config = load_named_config(BASE_CONFIG_PATH, DEFAULT_CONFIG)
        return self.__config

    def run(self, origin_id, visit_id):
        """Cache the cache_revision_origin for the given origin visit

        Args:
            origin_id: the origin id to cache
            visit_id: the visit id to cache

        This task also creates the revision cache tasks, as well as the task to
        cache the next origin visit available

        """
        config = self.config
        storage = get_storage(
            config['storage']['cls'],
            config['storage']['args']
        )

        packet_size = config['revision_packet_size']

        pipelined_tasks = []

        visits = sorted(
            visit['visit']
            for visit in storage.origin_visit_get(origin_id)
        )

        if visit_id in visits:
            revision_task = PopulateCacheContentRevision()
            new_revisions = [
                hashutil.hash_to_hex(revision)
                for revision in storage.cache_revision_origin_add(
                        origin_id, visit_id)
            ]

            if new_revisions:
                split_new_revisions = [
                    new_revisions[i:i + packet_size]
                    for i in range(0, packet_size, len(new_revisions))
                ]
                for packet in split_new_revisions:
                    pipelined_tasks.append(revision_task.s(packet))

        try:
            next_visit = min(visit for visit in visits if visit > visit_id)
        except ValueError:
            # no next visit, stop pipelining further visits
            pass
        else:
            pipelined_tasks.append(self.s(origin_id, next_visit))

        if pipelined_tasks:
            group(pipelined_tasks).delay()
