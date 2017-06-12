# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from .worker import ArchiverWithRetentionPolicyWorker
from .worker import ArchiverToBackendWorker


class SWHArchiverWithRetentionPolicyTask(Task):
    """Main task that archive a batch of content.

    """
    task_queue = 'swh_storage_archive_worker'

    def run_task(self, *args, **kwargs):
        ArchiverWithRetentionPolicyWorker(*args, **kwargs).run()


class SWHArchiverToBackendTask(Task):
    """Main task that archive a batch of content in the cloud.

    """
    task_queue = 'swh_storage_archive_worker_to_backend'

    def run_task(self, *args, **kwargs):
        ArchiverToBackendWorker(*args, **kwargs).run()
