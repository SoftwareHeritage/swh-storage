# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from .worker import ArchiverWorker


class SWHArchiverTask(Task):
    """ Main task that archive a batch of content.
    """
    task_queue = 'swh_storage_archive_worker'

    def run(self, batch, storages, dbconn, archival_policy):
        aw = ArchiverWorker(batch, storages, dbconn, archival_policy)
        aw.run()
