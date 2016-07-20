# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import click

from datetime import datetime

from swh.core import hashutil, config
from swh.objstorage import PathSlicingObjStorage
from swh.objstorage.api.client import RemoteObjStorage
from swh.scheduler.celery_backend.config import app

from . import tasks  # NOQA
from .storage import ArchiverStorage


DEFAULT_CONFIG = {
    'objstorage_type': ('str', 'local_storage'),
    'objstorage_path': ('str', '/tmp/swh-storage/objects'),
    'objstorage_slicing': ('str', '0:2/2:4/4:6'),
    'objstorage_url': ('str', 'http://localhost:5003/'),

    'batch_max_size': ('int', 50),
    'archival_max_age': ('int', 3600),
    'retention_policy': ('int', 2),
    'asynchronous': ('bool', True),

    'dbconn': ('str', 'dbname=softwareheritage-archiver-dev user=guest')
}

task_name = 'swh.storage.archiver.tasks.SWHArchiverTask'

logger = logging.getLogger()


class ArchiverDirector():
    """Process the files in order to know which one is needed as backup.

    The archiver director processes the files in the local storage in order
    to know which one needs archival and it delegates this task to
    archiver workers.
    Attributes:
        master_objstorage: the local storage of the master server.
        master_objstorage_args (dict): arguments of the master objstorage
            initialization.

        archiver_storage: a wrapper for archiver db operations.
        db_conn_archiver: Either a libpq connection string,
                or a psycopg2 connection for the archiver db.

        slave_objstorages: Iterable of remote obj storages to the slaves
            servers used for backup.
        config: Archiver_configuration. A dictionary that must contain
                the following keys:

                objstorage_type (str): type of objstorage used (local_storage
                    or remote_storage).
                    If the storage is local, the arguments keys must be present
                        objstorage_path (str): master's objstorage path
                        objstorage_slicing (str): masters's objstorage slicing
                    Otherwise, if it's a remote objstorage, the keys must be:
                        objstorage_url (str): url of the remote objstorage

                batch_max_size (int): The number of content items that can be
                    given to the same archiver worker.
                archival_max_age (int): Delay given to the worker to copy all
                    the files in a given batch.
                retention_policy (int): Required number of copies for the
                    content to be considered safe.
                asynchronous (boolean): Indicate whenever the archival should
                    run in asynchronous mode or not.
    """

    def __init__(self, db_conn_archiver, config):
        """ Constructor of the archiver director.

        Args:
            db_conn_archiver: Either a libpq connection string,
                or a psycopg2 connection for the archiver db.
            config: Archiver_configuration. A dictionary that must contain
                the following keys:

                objstorage_type (str): type of objstorage used
                    (local_objstorage or remote_objstorage).
                    If the storage is local, the arguments keys must be present
                        objstorage_path (str): master's objstorage path
                        objstorage_slicing (str): masters's objstorage slicing
                    Otherwise, if it's a remote objstorage, the keys must be:
                        objstorage_url (str): url of the remote objstorage

                batch_max_size (int): The number of content items that can be
                    given to the same archiver worker.
                archival_max_age (int): Delay given to the worker to copy all
                    the files in a given batch.
                retention_policy (int): Required number of copies for the
                    content to be considered safe.
                asynchronous (boolean): Indicate whenever the archival should
                    run in asynchronous mode or not.
        """
        # Get the slave storages
        self.db_conn_archiver = db_conn_archiver
        self.archiver_storage = ArchiverStorage(db_conn_archiver)
        self.slave_objstorages = {
            id: url
            for id, url
            in self.archiver_storage.archive_ls()
        }
        # Check that there is enough backup servers for the retention policy
        if config['retention_policy'] > len(self.slave_objstorages) + 1:
            raise ValueError(
                "Can't have a retention policy of %d with %d backup servers"
                % (config['retention_policy'], len(self.slave_objstorages))
            )

        # Get the master storage that contains content to be archived
        if config['objstorage_type'] == 'local_objstorage':
            master_objstorage_args = {
                'root': config['objstorage_path'],
                'slicing': config['objstorage_slicing']
            }
            master_objstorage = PathSlicingObjStorage(
                **master_objstorage_args
            )
        elif config['objstorage_type'] == 'remote_objstorage':
            master_objstorage_args = {'base_url': config['objstorage_url']}
            master_objstorage = RemoteObjStorage(**master_objstorage_args)
        else:
            raise ValueError(
                'Unknow objstorage class `%s`' % config['objstorage_type']
            )
        self.master_objstorage = master_objstorage
        self.master_objstorage_args = master_objstorage_args

        # Keep the full configuration
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
                   master_objstorage_args=self.master_objstorage_args,
                   slave_objstorages=self.slave_objstorages,
                   config=self.config)

    def run_sync_worker(self, batch):
        """ Run synchronously a worker on the given batch.
        """
        task = app.tasks[task_name]
        task(batch,
             archiver_args=self.db_conn_archiver,
             master_objstorage_args=self.master_objstorage_args,
             slave_objstorages=self.slave_objstorages,
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
                server_data = (server_id, self.slave_objstorages[server_id])

                missing_copy.setdefault(
                    db_content_id,
                    {'present': [], 'missing': []}
                ).setdefault(virtual_status, []).append(server_data)

                # Check the content before archival.
                try:
                    self.master_objstorage.check(content_id[0])
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
@click.option('--async/--sync', default=DEFAULT_CONFIG['asynchronous'][1],
              help="Indicates if the archiver should run asynchronously")
def launch(config_path, dbconn, async):
    # The configuration have following priority :
    # command line > file config > default config
    cl_config = {
        'dbconn': dbconn,
        'asynchronous': async
    }
    conf = config.read(config_path, DEFAULT_CONFIG)
    conf.update(cl_config)
    # Create connection data and run the archiver.
    archiver = ArchiverDirector(conf['dbconn'], conf)
    logger.info("Starting an archival at", datetime.now())
    archiver.run()


if __name__ == '__main__':
    launch()
