# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import logging
import time

from collections import defaultdict

from swh.core import hashutil
from swh.objstorage import get_objstorage

from .storage import ArchiverStorage
from .copier import ArchiverCopier


logger = logging.getLogger('archiver.worker')


class ArchiverWorker():
    """ Do the required backups on a given batch of contents.

    Process the content of a content batch in order to do the needed backups on
    the slaves servers.
    """
    def __init__(self, batch, storages, dbconn, archival_policy):
        """ Constructor of the ArchiverWorker class.
        """
        self.batch = batch
        self.archival_policy = archival_policy

        self.archiver_db = ArchiverStorage(dbconn)
        self.objstorages = {
            storage['host']: get_objstorage(storage['cls'], storage['args'])
            for storage in storages
        }

    def run(self):
        """ Do the task expected from the archiver worker.

        Process the content in the batch, ensure that the elements still need
        an archival, and spawn copiers to copy files in each destinations.
        """
        # Defaultdict so the d[key] with non-existant key automatically
        # create the given type (here list).
        transferts = defaultdict(list)
        for obj_id in self.batch:
            # Get dict {'missing': [servers], 'present': [servers]}
            # for contents ignoring those who don't need archival.
            copies = self._compute_copies(obj_id)
            if not self._need_archival(copies):
                continue
            present = copies.get('present', [])
            missing = copies.get('missing', [])
            if len(present) == 0:
                logger.critical('Content have been lost %s' % obj_id)
                continue
            # Choose randomly some servers to be used as srcs and dests.
            for src_dest in self._choose_backup_servers(present, missing):
                transferts[src_dest].append(obj_id)

        # Then run copiers for each of the required transferts.
        for (src, dest), content_ids in transferts.items():
            self.run_copier(self.objstorages[src],
                            self.objstorages[dest], content_ids)

    def _compute_copies(self, content_id):
        """ From a content_id, return present and missing copies.

        Returns:
            A dictionary with keys 'present' and 'missing' that are mapped to
            lists of copies ids depending on whenever the content is present
            or missing on the copy.
        """
        copies = self.archiver_db.content_archive_get(content_id)
        _, present, ongoing = copies
        # Initialize the archival status with all known present
        content_data = {'present': set(present), 'missing': set()}
        # Add data about the ongoing items
        for copy, mtime in ongoing.items():
            content_data[
                self._get_virtual_status('ongoing', mtime)
            ].add(copy)
        # Add to the archival status datas about servers that were not
        # in the db; they are missing.
        content_data['missing'].update(
            set(self.objstorages.keys()) - set(content_data['present'])
        )
        return content_data

    def _get_virtual_status(self, status, mtime):
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
            elapsed = time.time() - mtime
            if elapsed <= self.archival_policy['archival_max_age']:
                return 'present'
            else:
                return 'missing'
        else:
            raise ValueError("status must be either 'present', 'missing' "
                             "or 'ongoing'")

    def _need_archival(self, content_data):
        """ Indicate if the content need to be archived.

        Args:
            content_data (dict): dict that contains two lists 'present' and
                'missing' with copies id corresponding to this status.
        Returns: True if there is not enough copies, False otherwise.
        """
        nb_present = len(content_data.get('present', []))
        retention_policy = self.archival_policy['retention_policy']
        print(content_data['present'], nb_present, retention_policy)
        return nb_present < retention_policy

    def _choose_backup_servers(self, present, missing):
        """ Choose and yield the required amount of couple source/destination

        For each required copy, choose a unique destination server among the
        missing copies and a source server among the presents.

        Each destination server is unique so after archival, the retention
        policy requiremen will be fulfilled. However, the source server may be
        used multiple times.

        Yields:
            tuple (source, destination) for each required copy.
        """
        # Transform from set to list to allow random selections
        missing = list(missing)
        present = list(present)
        nb_required = self.archival_policy['retention_policy'] - len(present)
        destinations = random.sample(missing, nb_required)
        sources = [random.choice(present) for dest in destinations]
        yield from zip(sources, destinations)

    def run_copier(self, source, destination, content_ids):
        """ Run a copier in order to archive the given contents

        Upload the given contents from the source to the destination.
        If the process fail, the whole content is considered uncopied
        and remains 'ongoing', waiting to be rescheduled as there is
        a delay.

        Args:
            source (ObjStorage): source storage to get the contents.
            destination (ObjStorage): Storage where the contents will be copied
            content_ids: list of content's id to archive.
        """
        ac = ArchiverCopier(source, destination, content_ids)
        if ac.run():
            # Once the archival complete, update the database.
            for content_id in content_ids:
                self._content_archive_update(content_id, destination,
                                             new_status='present')

    def _content_archive_update(self, content_id, archive_id,
                                new_status=None):
        """ Update the status of a archive content and set its mtime to now.

        Change the last modification time of an archived content and change
        its status to the given one.

        Args:
            content_id (string): The content id.
            archive_id (string): The id of the concerned archive.
            new_status (string): One of missing, ongoing or present, this
                status will replace the previous one. If not given, the
                function only changes the mtime of the content.
        """
        db_obj_id = r'\x' + hashutil.hash_to_hex(content_id)
        self.archiver_db.content_archive_update(
            db_obj_id,
            archive_id,
            new_status
        )
