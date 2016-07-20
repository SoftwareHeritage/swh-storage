# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import logging

from datetime import datetime

from swh.objstorage import PathSlicingObjStorage
from swh.objstorage.api.client import RemoteObjStorage

from .storage import ArchiverStorage
from .copier import ArchiverCopier


logger = logging.getLogger()


class ArchiverWorker():
    """ Do the required backups on a given batch of contents.

    Process the content of a content batch in order to do the needed backups on
    the slaves servers.

    Attributes:
        batch: The content this worker has to archive, which is a dictionary
            that associates a content's sha1 id to the list of servers where
            the content is present or missing
            (see ArchiverDirector::get_unarchived_content).
        master_objstorage_args: The connection argument to initialize the
            master storage with the db connection url & the object storage
            path.
        slave_objstorages: A map that associates server_id to the remote server
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
    def __init__(self, batch, archiver_args, master_objstorage_args,
                 slave_objstorages, config):
        """ Constructor of the ArchiverWorker class.

        Args:
            batch: A batch of content, which is a dictionary that associates
                a content's sha1 id to the list of servers where the content
                is present.
            archiver_args: The archiver's arguments to establish connection to
                db.
            master_objstorage_args: The master storage arguments.
            slave_objstorages: A map that associates server_id to the remote
                server.
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
        self.batch = batch
        self.archiver_storage = ArchiverStorage(archiver_args)
        self.slave_objstorages = slave_objstorages
        self.config = config

        if config['objstorage_type'] == 'local_objstorage':
            master_objstorage = PathSlicingObjStorage(**master_objstorage_args)
        else:
            master_objstorage = RemoteObjStorage(**master_objstorage_args)
        self.master_objstorage = master_objstorage

    def _choose_backup_servers(self, allowed_storage, backup_number):
        """ Choose the slave servers for archival.

        Choose the given amount of servers among those which don't already
        contain a copy of the content.

        Args:
            allowed_storage: servers when the content is not already present.
            backup_number (int): The number of servers we have to choose in
                order to fullfill the objective.
        """
        # In case there is not enough backup servers to get all the backups
        # we need, just do our best.
        # Such situation should not happen.
        backup_number = min(backup_number, len(allowed_storage))

        # TODO Find a better (or a good) policy to choose the backup servers.
        # The random choice should be equivalently distributed between
        # servers for a great amount of data, but don't take care of servers
        # capacities.
        return random.sample(allowed_storage, backup_number)

    def _get_archival_status(self, content_id, server):
        """ Get the archival status of the required content.

        Attributes:
            content_id (string): Sha1 of the content.
            server: Tuple (archive_id, archive_url) of the archive server.
        Returns:
            A dictionary that contains all the required data : 'content_id',
            'archive_id', 'status', and 'mtime'
        """
        t, = list(
            self.archiver_storage.content_archive_get(content_id, server[0])
        )
        return {
            'content_id': t[0],
            'archive_id': t[1],
            'status': t[2],
            'mtime': t[3]
        }

    def _content_archive_update(self, content_id, archive_id,
                                new_status=None):
        """ Update the status of a archive content and set it's mtime to now()

        Change the last modification time of an archived content and change
        its status to the given one.

        Args:
            content_id (string): The content id.
            archive_id (string): The id of the concerned archive.
            new_status (string): One of missing, ongoing or present, this
                status will replace the previous one. If not given, the
                function only changes the mtime of the content.
        """
        self.archiver_storage.content_archive_update(
            content_id,
            archive_id,
            new_status
        )

    def need_archival(self, content, destination):
        """ Indicates whenever a content need archivage.

        Filter function that returns True if a given content
        still require to be archived.

        Args:
            content (str): Sha1 of a content.
            destination: Tuple (archive id, archive url).
        """
        archival_status = self._get_archival_status(
            content,
            destination
        )
        status = archival_status['status']
        mtime = archival_status['mtime']
        # If the archive is already present, no need to backup.
        if status == 'present':
            return False
        # If the content is ongoing but still have time, there is
        # another worker working on this content.
        elif status == 'ongoing':
            mtime = mtime.replace(tzinfo=None)
            elapsed = (datetime.now() - mtime).total_seconds()
            if elapsed <= self.config['archival_max_age']:
                return False
        return True

    def sort_content_by_archive(self):
        """ Create a map {archive_server -> list of content)

        Create a mapping that associate to a archive server all the
        contents that needs to be archived in it by the current worker.

        The map is in the form of :
        {
            (archive_1, archive_1_url): [content1, content2, content_3]
            (archive_2, archive_2_url): [content1, content3]
        }

        Returns:
            The created mapping.
        """
        slaves_copy = {}
        for content_id in self.batch:
            # Choose some servers to upload the content among the missing ones.
            server_data = self.batch[content_id]
            nb_present = len(server_data['present'])
            nb_backup = self.config['retention_policy'] - nb_present
            backup_servers = self._choose_backup_servers(
                server_data['missing'],
                nb_backup
            )
            # Fill the map destination -> content to upload
            for server in backup_servers:
                slaves_copy.setdefault(server, []).append(content_id)
        return slaves_copy

    def run(self):
        """ Do the task expected from the archiver worker.

        Process the content in the batch, ensure that the elements still need
        an archival, and spawn copiers to copy files in each destinations.
        """
        # Get a map (archive -> [contents])
        slaves_copy = self.sort_content_by_archive()

        # At this point, re-check the archival status in order to know if the
        # job have been done by another worker.
        for destination in slaves_copy:
            # list() is needed because filter's result will be consumed twice.
            slaves_copy[destination] = list(filter(
                lambda content_id: self.need_archival(content_id, destination),
                slaves_copy[destination]
            ))
            for content_id in slaves_copy[destination]:
                self._content_archive_update(content_id, destination[0],
                                             new_status='ongoing')

        # Spawn a copier for each destination
        for destination in slaves_copy:
            try:
                self.run_copier(destination, slaves_copy[destination])
            except:
                logger.error('Unable to copy a batch to %s' % destination)

    def run_copier(self, destination, contents):
        """ Run a copier in order to archive the given contents

        Upload the given contents to the given archive.
        If the process fail, the whole content is considered uncopied
        and remains 'ongoing', waiting to be rescheduled as there is
        a delay.

        Attributes:
            destination: Tuple (archive_id, archive_url) of the destination.
            contents: List of contents to archive.
        """
        ac = ArchiverCopier(destination, contents, self.master_objstorage)
        if ac.run():
            # Once the archival complete, update the database.
            for content_id in contents:
                self._content_archive_update(content_id, destination[0],
                                             new_status='present')
