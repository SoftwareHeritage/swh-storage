# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import swh
import logging

from datetime import datetime

from swh.core import hashutil
from swh.scheduler.celery_backend.config import app
from . import tasks  # NOQA


DEFAULT_CONFIG = {
    'objstorage_path': '/tmp/swh-storage/objects',
    'batch_max_size': 50,
    'archival_max_age': 3600,
    'retention_policy': 2,
    'asynchronous': True,

    'dbname': 'softwareheritage',
    'user': 'root'
}

task_name = 'swh.storage.archiver.tasks.SWHArchiverTask'

logger = logging.getLogger()


class ArchiverDirector():
    """Process the files in order to know which one is needed as backup.

    The archiver director processes the files in the local storage in order
    to know which one needs archival and it delegates this task to
    archiver workers.

    Attributes:
        master_storage: the local storage of the master server.
        slave_storages: Iterable of remote obj storages to the slaves servers
            used for backup.
        config: Archiver_configuration. A dictionary that must contain
            the following keys.
            objstorage_path (string): the path of the objstorage of the
                master.
            batch_max_size (int): The number of content items that can be
                given to the same archiver worker.
            archival_max_age (int): Delay given to the worker to copy all
                the files in a given batch.
            retention_policy (int): Required number of copies for the
                content to be considered safe.
            asynchronous (boolean): Indicate whenever the archival should
                run in asynchronous mode or not.
    """

    def __init__(self, db_conn, config):
        """ Constructor of the archiver director.

        Args:
            db_conn: db_conn: Either a libpq connection string,
                or a psycopg2 connection.
            config: Archiver_configuration. A dictionary that must contains
                the following keys.
                objstorage_path (string): the path of the objstorage of the
                    master.
                batch_max_size (int): The number of content items that can be
                    given to the same archiver worker.
                archival_max_age (int): Delay given to the worker to copy all
                    the files in a given batch.
                retention_policy (int): Required number of copies for the
                    content to be considered safe.
                asynchronous (boolean): Indicate whenever the archival should
                    run in asynchronous mode or not.
        """
        # Get the local storage of the master and remote ones for the slaves.
        self.master_storage_args = [db_conn, config['objstorage_path']]
        master_storage = swh.storage.get_storage('local_storage',
                                                 self.master_storage_args)
        slaves = {
            id: url
            for id, url
            in master_storage.db.archive_ls()
        }

        # TODO Database should be initialized somehow before going in
        # production. For now, assumes that the database contains
        # datas for all the current content.

        self.master_storage = master_storage
        self.slave_storages = slaves
        self.config = config

    def run(self):
        """ Run the archiver director.

        The archiver director will check all the contents of the archiver
        database and do the required backup jobs.
        """
        if self.config['asynchronous']:
            run_fn = self.run_async_worker
        else:
            run_fn = self.run_sync_worker
        for batch in self.get_unarchived_content():
            run_fn(batch)

    def run_async_worker(self, batch):
        """ Produce a worker that will be added to the task queue.
        """
        task = app.tasks[task_name]
        task.delay(batch, self.master_storage_args,
                   self.slave_storages, self.config['retention_policy'])

    def run_sync_worker(self, batch):
        """ Run synchronously a worker on the given batch.
        """
        task = app.tasks[task_name]
        task(batch, self.master_storage_args,
             self.slave_storages, self.config)

    def get_unarchived_content(self):
        """ get all the contents that needs to be archived.

        Yields:
            A batch of contents. Batches are dictionaries which associates
            a content id to the data about servers that contains it or not.

            {'id1':
                {'present': [('slave1', 'slave1_url')],
                 'missing': [('slave2', 'slave2_url'),
                             ('slave3', 'slave3_url')]
                },
             'id2':
                {'present': [],
                 'missing': [
                     ('slave1', 'slave1_url'),
                     ('slave2', 'slave2_url'),
                     ('slave3', 'slave3_url')
                 ]}
            }

            Where keys (idX) are sha1 of the content and (slaveX, slaveX_url)
            are ids and urls of the storage slaves.

            At least all the content that don't have enough copies on the
            backups servers are distributed into these batches.
        """
        # Get the data about each content referenced into the archiver.
        missing_copy = {}
        for content_id in self.master_storage.db.content_archive_ls():
            db_content_id = '\\x' + hashutil.hash_to_hex(content_id[0])

            # Fetch the datas about archival status of the content
            backups = self.master_storage.db.content_archive_get(
                content=db_content_id
            )
            for _content_id, server_id, status, mtime in backups:
                virtual_status = self.get_virtual_status(status, mtime)
                server_data = (server_id, self.slave_storages[server_id])

                missing_copy.setdefault(
                    db_content_id,
                    {'present': [], 'missing': []}
                ).setdefault(virtual_status, []).append(server_data)

                # Check the content before archival.
                try:
                    self.master_storage.objstorage.check(content_id[0])
                except Exception as e:
                    # Exception can be Error or ObjNotFoundError.
                    logger.error(e)
                    # TODO Do something to restore the content?

                if len(missing_copy) >= self.config['batch_max_size']:
                    yield missing_copy
                    missing_copy = {}

        if len(missing_copy) > 0:
            yield missing_copy

    def get_virtual_status(self, status, mtime):
        """ Compute the virtual presence of a content.

        If the status is ongoing but the time is not elasped, the archiver
        consider it will be present in the futur, and so consider it as
        present.
        However, if the time is elasped, the copy may have failed, so consider
        the content as missing.

        Arguments:
            status (string): One of ('present', 'missing', 'ongoing'). The
                status of the content.
            mtime (datetime): Time at which the content have been updated for
                the last time.

        Returns:
            The virtual status of the studied content, which is 'present' or
            'missing'.

        Raises:
            ValueError: if the status is not one 'present', 'missing'
                or 'ongoing'
        """
        if status in ('present', 'missing'):
            return status

        # If the status is 'ongoing' but there is still time, another worker
        # may still be on the task.
        if status == 'ongoing':
            mtime = mtime.replace(tzinfo=None)
            elapsed = (datetime.now() - mtime).total_seconds()
            if elapsed <= self.config['archival_max_age']:
                return 'present'
            else:
                return 'missing'
        else:
            raise ValueError("status must be either 'present', 'missing' "
                             "or 'ongoing'")
