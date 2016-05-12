# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import swh

from datetime import datetime

from swh.core import hashutil, config
from swh.scheduler.celery_backend.config import app
from . import tasks  # NOQA
from ..db import cursor_to_bytes


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


class ArchiverDirector():
    """Process the files in order to know which one is needed as backup.

    The archiver director processes the files in the local storage in order
    to know which one needs archival and it delegates this task to
    archiver workers.

    Attributes:
        master_storage: the local storage of the master server.
        slave_storages: Iterable of remote obj storages to the slaves servers
            used for backup.
        batch_max_size: The number of content items that can be given
                to the same archiver worker.
        archival_max_age: Delay given to the worker to copy all the files
            in a given batch.
        retention_policy: Required number of copies for the content to
            be considered safe.
    """

    def __init__(self, db_conn, config):
        """ Constructor of the archiver director.

        Args:
            db_conn: db_conn: Either a libpq connection string,
                or a psycopg2 connection.
            config: Archiver_configuration. A dictionnary that must contains
                the following keys.
                objstorage_path (string): the path of the objstorage of the
                    master.
                batch_max_size (int): The number of content items that can be
                    given to the same archiver worker.
                archival_max_age (int): Delay given to the worker to cpy all
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
        self.batch_max_size = config['batch_max_size']
        self.archival_max_age = config['archival_max_age']
        self.retention_policy = config['retention_policy']
        self.is_asynchronous = config['asynchronous']

    def run(self):
        """ Run the archiver director.

        The archiver director will check all the contents of the archiver
        database and do the required backup jobs.
        """
        run_fn = (self.run_async_worker
                  if self.is_asynchronous
                  else self.run_sync_worker)
        for batch in self.get_unarchived_content():
            run_fn(batch)

    def run_async_worker(self, batch):
        """ Produce a worker that will be added to the task queue.
        """
        task = app.tasks[task_name]
        task.delay(batch, self.master_storage_args,
                   self.slave_storages, self.retention_policy)

    def run_sync_worker(self, batch):
        """ Run synchronously a worker on the given batch.
        """
        task = app.tasks[task_name]
        task(batch, self.master_storage_args,
             self.slave_storages, self.retention_policy)

    def get_unarchived_content(self):
        """ get all the contents that needs to be archived.

        Yields:
            A batch of contents. Batches are dictionnaries which associates
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
                 ]
            }

            Where keys (idX) are sha1 of the content and (slaveX, slaveX_url)
            are ids and urls of the storage slaves.

            At least all the content that don't have enough copies on the
            backups servers are distributed into these batches.
        """
        # Get the data about each content referenced into the archiver.
        missing_copy = {}
        for content_id in self.master_storage.db.content_archive_ls():
            # Do some initializations
            db_content_id = '\\x' + hashutil.hash_to_hex(content_id[0])

            # Query in order to know in which servers the content is saved.
            backups = self.master_storage.db.content_archive_get(
                content=db_content_id
            )
            for _content_id, server_id, status, mtime in backups:

                # If the content is ongoing but still have time, there is
                # another worker working on this content.
                if status == 'ongoing':
                    mtime = mtime.replace(tzinfo=None)
                    elapsed = (datetime.now() - mtime).total_seconds()
                    if elapsed < self.archival_max_age:
                        continue
                server_data = (server_id, self.slave_storages[server_id])
                missing_copy.setdefault(
                    db_content_id,
                    {'present': [], 'missing': []}
                ).setdefault(status, []).append(server_data)

                # Check the content before archival.
                # TODO catch exception and try to restore the file from an
                # archive?
                self.master_storage.objstorage.check(content_id[0])

                if len(missing_copy) >= self.batch_max_size:
                    yield missing_copy
                    missing_copy = {}

        if len(missing_copy) > 0:
            yield missing_copy


def initialize_content_archive(db, sample_size, names=['Local']):
    """ Initialize the content_archive table with a sample.

    From the content table, get a sample of id, and fill the
    content_archive table with those id in order to create a test sample
    for the archiver.

    Args:
        db: The database of the storage.
        sample_size (int): The size of the sample to create.
        names: A list of archive names. Those archives must already exists.
            Archival status of the archives content will be erased on db.

    Returns:
        Tha amount of entry created.
    """
    with db.transaction() as cur:
        cur.execute('DELETE FROM content_archive')

    with db.transaction() as cur:
        cur.execute('SELECT sha1 from content limit %d' % sample_size)
        ids = list(cursor_to_bytes(cur))

    for id, in ids:
        tid = r'\x' + hashutil.hash_to_hex(id)

        with db.transaction() as cur:
            for name in names:
                s = """INSERT INTO content_archive
                    VALUES('%s'::sha1, '%s', 'missing', now())
                    """ % (tid, name)
                cur.execute(s)

    print('Initialized database with', sample_size * len(names), 'items')
    return sample_size * len(names)


def add_content_to_objstore(director, source, content_ids):
    """ Fill the objstore according to the database

    Get the current status of the database and fill the objstorage of the
    master storage according to these data.
    Content are fetched from the source, which is a storage.

    Args:
        director (ArchiverDirector): The archiver director containing
            the master storage to fill.
        source (Storage): A storage that contains the content for all the
            ids in content_ids.
        content_ids: A list of ids that should be added to the master object
            storage.
    """
    for res in source.content_get(content_ids):
        content_data = res['data']
        director.master_storage.objstorage.add_bytes(content_data)


if __name__ == '__main__':
    import sys

    conf = config.read(sys.argv[1], DEFAULT_CONFIG)
    cstring = 'dbname={} user={}'.format(conf['dbname'], conf['user'])

    director = ArchiverDirector(cstring, conf)
    director.run()
