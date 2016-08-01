# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import logging
import time

from collections import defaultdict

from swh.core import hashutil
from swh.core import config
from swh.objstorage import get_objstorage
from swh.objstorage.exc import Error, ObjNotFoundError

from .storage import ArchiverStorage
from .copier import ArchiverCopier


logger = logging.getLogger('archiver.worker')


class ArchiverWorker(config.SWHConfig):
    """ Do the required backups on a given batch of contents.

    Process the content of a content batch in order to do the needed backups on
    the slaves servers.
    """

    DEFAULT_CONFIG = {
        'retention_policy': ('int', 2),
        'archival_max_age': ('int', 3600),
        'dbconn': ('str', 'dbname=softwareheritage-archiver-dev'),

        'storages': ('dict',
                     [
                         {'host': 'uffizi',
                          'cls': 'pathslicing',
                          'args': {'root': '/tmp/softwareheritage/objects',
                                   'slicing': '0:2/2:4/4:6'}},
                         {'host': 'banco',
                          'cls': 'remote',
                          'args': {'base_url': 'http://banco:5003/'}}
                     ])
    }
    CONFIG_BASE_FILENAME = 'archiver-worker'

    def __init__(self, batch, add_config={}):
        """ Constructor of the ArchiverWorker class.
        """
        self.batch = batch
        config = self.parse_config_file(additional_configs=[add_config])
        self.retention_policy = config['retention_policy']
        self.archival_max_age = config['archival_max_age']

        self.archiver_db = ArchiverStorage(config['dbconn'])
        self.objstorages = {
            storage['host']: get_objstorage(storage['cls'], storage['args'])
            for storage in config.get('storages', [])
        }

        if len(self.objstorages) < self.retention_policy:
            raise ValueError('Retention policy is too high for the number of '
                             'provided servers')

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
            if elapsed <= self.archival_max_age:
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
        return nb_present < self.retention_policy

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
        nb_required = self.retention_policy - len(present)
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
        # Check if there is any error among the contents.
        content_status = self._get_contents_error(content_ids, source)

        # Iterates over the error detected.
        for content_id, real_status in content_status.items():
            # Remove them from the to-archive list,
            # as they cannot be retrieved correclty.
            content_ids.remove(content_id)
            # Update their status to reflect their real state.
            self._content_archive_update(content_id, source,
                                         new_status=real_status)

        # Now perform the copy on the remaining contents
        ac = ArchiverCopier(source, destination, content_ids)
        if ac.run():
            # Once the archival complete, update the database.
            for content_id in content_ids:
                self._content_archive_update(content_id, destination,
                                             new_status='present')

    def _get_contents_error(self, content_ids, storage):
        """ Indicates what is the error associated to a content when needed

        Check the given content on the given storage. If an error is detected,
        it will be reported through the returned dict.

        Args:
            content_ids: a list of content id to check
            storage: the storage where are the content to check.

        Returns:
            a dict that map {content_id -> error_status} for each content_id
            with an error. The `error_status` result may be 'missing' or
            'corrupted'.
        """
        content_status = {}
        for content_id in content_ids:
            try:
                storage.check(content_id)
            except Error:
                content_status[content_id] = 'corrupted'
                logger.error('Content is corrupted: %s' % content_id)
            except ObjNotFoundError:
                content_status[content_id] = 'missing'
                logger.error('A content referenced present is missing: %s'
                             % content_id)
        return content_status

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
