# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import swh
import logging
import click

from datetime import datetime

from swh.core import hashutil, config
from swh.scheduler.celery_backend.config import app

from . import tasks  # NOQA
from .storage import ArchiverStorage


DEFAULT_CONFIG = {
    'objstorage_path': ('str', '/tmp/swh-storage/objects'),
    'batch_max_size': ('int', 50),
    'archival_max_age': ('int', 3600),
    'retention_policy': ('int', 2),
    'asynchronous': ('bool', True),

    'dbconn': ('str', 'dbname=softwareheritage-archiver-dev user=guest'),
    'dbconn_storage': ('str', 'dbname=softwareheritage-dev user=guest')
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
            objstorage_path (string): master's objstorage path

            batch_max_size (int): The number of content items that can be
                given to the same archiver worker.
            archival_max_age (int): Delay given to the worker to copy all
                the files in a given batch.
            retention_policy (int): Required number of copies for the
                content to be considered safe.
            asynchronous (boolean): Indicate whenever the archival should
                run in asynchronous mode or not.
    """

    def __init__(self, db_conn_archiver, db_conn_storage, config):
        """ Constructor of the archiver director.

        Args:
            db_conn_archiver: Either a libpq connection string,
                or a psycopg2 connection for the archiver db connection.
            db_conn_storage: Either a libpq connection string,
                or a psycopg2 connection for the db storage connection.
            config: Archiver_configuration. A dictionary that must contain
                the following keys.
                objstorage_path (string): master's objstorage path
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
        self.db_conn_archiver = db_conn_archiver
        self.archiver_storage = ArchiverStorage(db_conn_archiver)

        self.master_storage_args = [db_conn_storage, config['objstorage_path']]
        master_storage = swh.storage.get_storage('local_storage',
                                                 self.master_storage_args)
        slaves = {
            id: url
            for id, url
            in self.archiver_storage.archive_ls()
        }

        # TODO Database should be initialized somehow before going in
        # production. For now, assumes that the database contains
        # data for all the current content.

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
        task.delay(batch,
                   archiver_args=self.db_conn_archiver,
                   master_storage_args=self.master_storage_args,
                   slave_storages=self.slave_storages,
                   config=self.config)

    def run_sync_worker(self, batch):
        """ Run synchronously a worker on the given batch.
        """
        task = app.tasks[task_name]
        task(batch,
             archiver_args=self.db_conn_archiver,
             master_storage_args=self.master_storage_args,
             slave_storages=self.slave_storages,
             config=self.config)

    def get_unarchived_content(self):
        """ Get contents that need to be archived.

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
        for content_id in self.archiver_storage.content_archive_ls():
            db_content_id = '\\x' + hashutil.hash_to_hex(content_id[0])

            # Fetch the datas about archival status of the content
            backups = self.archiver_storage.content_archive_get(
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


@click.command()
@click.argument('config-path', required=1)
@click.option('--dbconn', default=DEFAULT_CONFIG['dbconn'][1],
              help="Connection string for the archiver database")
@click.option('--dbconn-storage', default=DEFAULT_CONFIG['dbconn_storage'][1],
              help="Connection string for the storage database")
@click.option('--async/--sync', default=DEFAULT_CONFIG['asynchronous'][1],
              help="Indicates if the archiver should run asynchronously")
def launch(config_path, dbconn, dbconn_storage, async):
    # The configuration have following priority :
    # command line > file config > default config
    cl_config = {
        'dbconn': dbconn,
        'dbconn_storage': dbconn_storage,
        'asynchronous': async
    }
    conf = config.read(config_path, DEFAULT_CONFIG)
    conf.update(cl_config)
    # Create connection data and run the archiver.
    archiver = ArchiverDirector(conf['dbconn'], conf['dbconn_storage'], conf)
    logger.info("Starting an archival at", datetime.now())
    archiver.run()


if __name__ == '__main__':
    launch()
