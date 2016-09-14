# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import click

from swh.core import config
from swh.scheduler.celery_backend.config import app

from . import tasks  # noqa
from ..storage import Storage
from .storage import ArchiverStorage


class ArchiverDirectorBase(config.SWHConfig, metaclass=abc.ABCMeta):
    """Abstract Director class

    An archiver director is in charge of dispatching batch of
    contents to archiver workers (for them to archive).

    Inherit from this class and provide:
    - ADDITIONAL_CONFIG: Some added configuration needed for the
      director to work
    - CONFIG_BASE_FILENAME: relative path to lookup for the
      configuration file
    - def get_contents_to_archive(self): Implementation method to read
      contents to archive

    """
    DEFAULT_CONFIG = {
        'batch_max_size': ('int', 1500),
        'asynchronous': ('bool', True),

        'dbconn': ('str', 'dbname=softwareheritage-archiver-dev user=guest')
    }

    # Destined to be overridden by subclass
    ADDITIONAL_CONFIG = {}

    # We use the same configuration file as the worker
    CONFIG_BASE_FILENAME = 'archiver/worker'

    # The worker's task queue name to use
    TASK_NAME = None

    def __init__(self):
        """ Constructor of the archiver director.

        Args:
            db_conn_archiver: Either a libpq connection string,
                or a psycopg2 connection for the archiver db.
            config: optionnal additional configuration. Keys in the dict will
                override the one parsed from the configuration file.
        """
        super().__init__()
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        self.archiver_storage = ArchiverStorage(self.config['dbconn'])

    def run(self):
        """ Run the archiver director.

        The archiver director will check all the contents of the archiver
        database and do the required backup jobs.
        """
        if self.config['asynchronous']:
            run_fn = self.run_async_worker
        else:
            run_fn = self.run_sync_worker

        for batch in self.read_batch_contents():
            run_fn(batch)

    def run_async_worker(self, batch):
        """ Produce a worker that will be added to the task queue.
        """
        task = app.tasks[self.TASK_NAME]
        task.delay(batch=batch)

    def run_sync_worker(self, batch):
        """ Run synchronously a worker on the given batch.
        """
        task = app.tasks[self.TASK_NAME]
        task(batch=batch)

    def read_batch_contents(self):
        """ Create batch of contents that needs to be archived

        Yields:
            batch of sha1 that corresponds to contents that needs more archive
            copies.
        """
        contents = []
        for content in self.get_contents_to_archive():
            contents.append(content)
            if len(contents) > self.config['batch_max_size']:
                yield contents
                contents = []
        if len(contents) > 0:
            yield contents

    @abc.abstractmethod
    def get_contents_to_archive(self):
        """Retrieve generator of sha1 to archive

        Yields:
            sha1 to archive

        """
        pass


class ArchiverWithRetentionPolicyDirector(ArchiverDirectorBase):
    """Process the files in order to know which one is needed as backup.

    The archiver director processes the files in the local storage in order
    to know which one needs archival and it delegates this task to
    archiver workers.
    """

    ADDITIONAL_CONFIG = {
        'retention_policy': ('int', 2),
    }

    TASK_NAME = 'swh.storage.archiver.tasks.SWHArchiverWithRetentionPolicyTask'

    def get_contents_to_archive(self):
        """Create batch of contents that needs to be archived

         Yields:
            Datas about a content as a tuple
            (content_id, present_copies, ongoing_copies) where ongoing_copies
            is a dict mapping copy to mtime.

         """
        last_object = b''
        while True:
            archiver_contents = list(
                self.archiver_storage.content_archive_get_unarchived_copies(
                    last_content=last_object,
                    retention_policy=self.config['retention_policy']))
            if not archiver_contents:
                return
            for content_id, _, _ in archiver_contents:
                last_object = content_id
                yield content_id


class ArchiverToBackendDirector(ArchiverDirectorBase):
    """A cloud archiver director in charge of reading contents and send
    them in batch in the cloud.

    The archiver director processes the files in the local storage in
    order to know which one needs archival and it delegates this task
    to archiver workers.

    """
    ADDITIONAL_CONFIG = {
        'storage': ('dict', {
            'dbconn': 'dbname=softwareheritage-dev user=guest',
            'objroot': '/srv/softwareheritage/storage',
        })
    }

    CONFIG_BASE_FILENAME = 'archiver/worker-to-backend'

    TASK_NAME = 'swh.storage.archiver.tasks.SWHArchiverToBackendTask'

    def __init__(self):
        super().__init__()
        storage = self.config['storage']
        self.storage = Storage(storage['dbconn'], storage['objroot'])

    def get_contents_to_archive(self):
        """Create batch of contents that needs to be archived

         Yields:
            sha1 of content to archive

         """
        last_object = b''
        while True:
            contents = list(self.storage.cache_content_get(
                last_content=last_object,
                limit=self.config['batch_max_size']))

            if not contents:
                return

            last_object = contents[-1]['sha1_git']
            for content in contents:
                yield content['sha1']


@click.command()
@click.option('--direct', is_flag=True,
              help="""With this flag, the archiver sends content for backup to
one storage.""")
def launch(direct):
    if direct:
        archiver = ArchiverToBackendDirector()
    else:
        archiver = ArchiverWithRetentionPolicyDirector()

    archiver.run()

if __name__ == '__main__':
    launch()
