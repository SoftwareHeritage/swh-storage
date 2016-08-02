# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core import config
from swh.scheduler.celery_backend.config import app

from . import tasks  # NOQA
from .storage import ArchiverStorage


task_name = 'swh.storage.archiver.tasks.SWHArchiverTask'


class ArchiverDirector(config.SWHConfig):
    """Process the files in order to know which one is needed as backup.

    The archiver director processes the files in the local storage in order
    to know which one needs archival and it delegates this task to
    archiver workers.
    """

    DEFAULT_CONFIG = {
        'batch_max_size': ('int', 1500),
        'retention_policy': ('int', 2),
        'asynchronous': ('bool', True),

        'dbconn': ('str', 'dbname=softwareheritage-archiver-dev user=guest')
    }
    CONFIG_BASE_FILENAME = 'archiver/director'

    def __init__(self):
        """ Constructor of the archiver director.

        Args:
            db_conn_archiver: Either a libpq connection string,
                or a psycopg2 connection for the archiver db.
            config: optionnal additional configuration. Keys in the dict will
                override the one parsed from the configuration file.
        """
        self.config = self.parse_config_file()
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

        for batch in self.get_unarchived_content_batch():
            run_fn(batch)

    def run_async_worker(self, batch):
        """ Produce a worker that will be added to the task queue.
        """
        task = app.tasks[task_name]
        task.delay(batch=batch)

    def run_sync_worker(self, batch):
        """ Run synchronously a worker on the given batch.
        """
        task = app.tasks[task_name]
        task(batch=batch)

    def get_unarchived_content_batch(self):
        """ Create batch of contents that needs to be archived

        Yields:
            batch of sha1 that corresponds to contents that needs more archive
            copies.
        """
        contents = []
        for content in self._get_unarchived_content():
            contents.append(content)
            if len(contents) > self.config['batch_max_size']:
                yield contents
                contents = []
        if len(contents) > 0:
            yield contents

    def _get_unarchived_content(self):
        """ Get all the content ids in the db that needs more copies

        Yields:
            sha1 of contents that needs to be archived.
        """
        for content_id, present, _ongoing in self._get_all_contents():
            if len(present) < self.config['retention_policy']:
                yield content_id
            else:
                continue

    def _get_all_contents(self):
        """ Get batchs from the archiver db and yield it as continous stream

        Yields:
            Datas about a content as a tuple
            (content_id, present_copies, ongoing_copies) where ongoing_copies
            is a dict mapping copy to mtime.
        """
        last_object = b''
        while True:
            archiver_contents = list(
                self.archiver_storage.content_archive_get_copies(last_object)
            )
            if not archiver_contents:
                return
            for content in archiver_contents:
                last_object = content[0]
                yield content


def launch():
    archiver = ArchiverDirector()
    archiver.run()

if __name__ == '__main__':
    launch()
