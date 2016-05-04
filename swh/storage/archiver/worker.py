# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random

from .copier import ArchiverCopier

from datetime import datetime


class ArchiverWorker():  # This class should probably extend a Celery Task.
    """ Do the required backups on a given batch of contents.

    Process the content of a content batch in order to do the needed backups on
    the slaves servers.

    Attributes:
        batch: The content this worker has to archive, which is a dictionary
            that associates a content's sha1 id to the list of servers where
            the content is present or missing
            (see ArchiverDirector::get_unarchived_content).
        master_storage: The master storage where is the content location.
        slave_storages: A map that associates server_id to the remote server.
        retention_policy: The required number of copies for a content to be
            considered safe.
    """
    def __init__(self, batch, master_storage,
                 slave_storages, retention_policy):
        """ Constructor of the ArchiverWorker class.

        Args:
            batch: A batch of content, which is a dictionnary that associates
                a content's sha1 id to the list of servers where the content
                is present.
            master_storage: The master storage where is the whole content.
            slave_storages: A map that associates server_id to the remote
                server.
            retention_policy: The required number of copies for a content to
                be considered safe.
        """
        self.batch = batch
        self.master_storage = master_storage
        self.slave_storages = slave_storages
        self.retention_policy = retention_policy

    def __choose_backup_servers(self, allowed_storage, backup_number):
        """ Choose the slave servers for archival.

        Choose the given amount of servers among those which don't already
        contain a copy of the content.

        Args:
            server_missing: a list of servers where the content is missing.
            backup_number (int): The number of servers we have to choose in
                order to fullfill the objective.
        """
        # In case there is not enough backup servers to get all the backups
        # we need, just do our best.
        # TODO such situation can only be caused by an incorrect configuration
        # setting. Do a verification previously.
        backup_number = min(backup_number, len(allowed_storage))

        # TODO Find a better (or a good) policy to choose the backup servers.
        # The random choice should be equivalently distributed between
        # servers for a great amount of data, but don't take care of servers
        # capacities.
        return random.sample(allowed_storage, backup_number)

    def __get_archival_status(self, content_id, server):
        t, = list(
            self.master_storage.db.content_archive_get(content_id, server[0])
        )
        return {
            'content_id': t[0],
            'archive_id': t[1],
            'status': t[2],
            'mtime': t[3]
        }

    def __content_archive_update(self, content_id, archive_id,
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
        query = """UPDATE content_archive
                SET %(fields)s
                WHERE content_id='%(content_id)s'
                    and archive_id='%(archive_id)s'
                """
        fields = []
        if new_status:
            fields.append("status='%s'" % new_status)
        fields.append("mtime=now()")

        d = {'fields': ', '.join(fields),
             'content_id': content_id,
             'archive_id': archive_id}

        with self.master_storage.db.transaction() as cur:
            cur.execute(query % d)

    def run(self):
        """ Do the task expected from the archiver worker.

        Process the content in the batch, ensure that the elements still need
        an archival, and spawn copiers to copy files in each destinations.
        """

        def content_filter(content, destination):
            """ Indicates whenever a content need archivage.

            Filter function that returns True if a given content
            still require to be archived.

            Args:
                content (str):
            """
            archival_status = self.__get_archival_status(
                content,
                destination
            )
            if archival_status:
                status = archival_status['status']
                # If the archive is already present, no need to backup.
                if status == 'present':
                    return False
                # If the content is ongoing but still have time, there is
                # another worker working on this content.
                elif status == 'ongoing':
                    elapsed = datetime.now() - archival_status['mtime']\
                              .total_seconds()
                    if elapsed < self.master_storage.archival_max_age:
                        return False
                return True
            else:
                # TODO this is an error case, the content should always exists.
                return None

        slaves_copy = {}
        for content_id in self.batch:
            # Choose some servers to upload the content
            server_data = self.batch[content_id]

            backup_servers = self.__choose_backup_servers(
                server_data['missing'],
                self.retention_policy - len(server_data['present'])
            )
            # Fill the map destination -> content to upload
            for server in backup_servers:
                slaves_copy.setdefault(server, []).append(content_id)

        # At this point, check the archival status of the content in order to
        # know if it is still needed.
        for destination in slaves_copy:
            contents = []
            for content in slaves_copy[destination]:
                if content_filter(content, destination):
                    contents.append(content)
            slaves_copy[destination] = contents

        # Spawn a copier for each destination that will copy all the
        # needed content.
        for destination in slaves_copy:
            ac = ArchiverCopier(
                destination, slaves_copy[destination],
                self.master_storage)
            if ac.run():
                # Once the archival complete, update the database.
                for content_id in slaves_copy[destination]:
                    self.__content_archive_update(content_id, destination[0],
                                                  new_status='present')
