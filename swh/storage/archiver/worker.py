# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import logging
import random
import time

from collections import defaultdict

from swh.objstorage import get_objstorage

from swh.core import hashutil, config

from swh.objstorage.exc import Error, ObjNotFoundError

from .storage import ArchiverStorage
from .copier import ArchiverCopier


logger = logging.getLogger('archiver.worker')


class BaseArchiveWorker(config.SWHConfig, metaclass=abc.ABCMeta):
    """Base archive worker.

    Inherit from this class and override:
    - ADDITIONAL_CONFIG: Some added configuration needed for the
      director to work
    - CONFIG_BASE_FILENAME: relative path to lookup for the
      configuration file
    - def need_archival(self, content_data): Determine if a content
      needs archival or not
    - def choose_backup_servers(self, present, missing): Choose
      which backup server to send copies to

    """
    DEFAULT_CONFIG = {
        'dbconn': ('str', 'dbname=softwareheritage-archiver-dev'),
    }

    ADDITIONAL_CONFIG = {}

    CONFIG_BASE_FILENAME = 'archiver/director'

    objstorages = {}

    def __init__(self, batch):
        super().__init__()
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        self.batch = batch
        self.archiver_db = ArchiverStorage(self.config['dbconn'])

    def run(self):
        """Do the task expected from the archiver worker.

        Process the contents in self.batch, ensure that the elements
        still need an archival (using archiver db), and spawn copiers
        to copy files in each destination according to the
        archiver-worker's policy.

        """
        transfers = defaultdict(list)
        set_objstorages = set(self.objstorages)
        for obj_id in self.batch:
            # Get dict {'missing': [servers], 'present': [servers]}
            # for contents ignoring those who don't need archival.
            copies = self.compute_copies(set_objstorages, obj_id)
            if not self.need_archival(copies):
                continue
            present = copies.get('present', [])
            missing = copies.get('missing', [])
            if len(present) == 0:
                logger.critical('Content have been lost %s' %
                                hashutil.hash_to_hex(obj_id))
                continue
            # Choose servers to be used as srcs and dests.
            for src_dest in self.choose_backup_servers(present, missing):
                transfers[src_dest].append(obj_id)

        # Then run copiers for each of the required transfers.
        for (src, dest), content_ids in transfers.items():
            self.run_copier(src, dest, content_ids)

    def compute_copies(self, set_objstorages, content_id):
        """From a content_id, return present and missing copies.

        Args:
            objstorages (set): objstorage's id name
            content_id: the content concerned

        Returns:
            A dictionary with keys 'present' and 'missing'.
            that are mapped to lists of copies ids depending on
            whenever the content is present or missing on the copy.

            There is also the key 'ongoing' which is associated with a
            dict that map to a copy name the mtime of the ongoing
            status update.

        """
        _, present, ongoing = self.archiver_db.content_archive_get(content_id)
        set_present = set(present)
        set_ongoing = set(ongoing)
        set_missing = set_objstorages - set_present - set_ongoing
        return {
            'present': set_present,
            'missing': set_missing,
            'ongoing': ongoing
        }

    def run_copier(self, source, destination, content_ids):
        """Run a copier in order to archive the given contents.

        Upload the given contents from the source to the destination.
        If the process fails, the whole content is considered uncopied
        and remains 'ongoing', waiting to be rescheduled as there is a
        delay.

        Args:
            source (str): source storage's identifier
            destination (str): destination storage's identifier
            content_ids ([sha1]): list of content ids to archive.

        """
        # Check if there are any errors among the contents.
        content_status = self.get_contents_error(content_ids, source)

        # Iterates over the error detected.
        for content_id, real_status in content_status.items():
            # Remove them from the to-archive list,
            # as they cannot be retrieved correctly.
            content_ids.remove(content_id)
            # Update their status to reflect their real state.
            self.content_archive_update(
                content_id, archive_id=source, new_status=real_status)

        # Now perform the copy on the remaining contents
        ac = ArchiverCopier(
            source=self.objstorages[source],
            destination=self.objstorages[destination],
            content_ids=content_ids)

        if ac.run():
            # Once the archival complete, update the database.
            for content_id in content_ids:
                self.content_archive_update(
                    content_id, archive_id=destination, new_status='present')

    def get_contents_error(self, content_ids, storage):
        """ Indicates what is the error associated to a content when needed

        Check the given content on the given storage. If an error is detected,
        it will be reported through the returned dict.

        Args:
            content_ids ([sha1]): list of content ids to check
            storage (str): the storage where are the content to check.

        Returns:
            a dict that map {content_id -> error_status} for each content_id
            with an error. The `error_status` result may be 'missing' or
            'corrupted'.
        """
        content_status = {}
        for content_id in content_ids:
            try:
                self.objstorages[storage].check(content_id)
            except Error as e:
                content_status[content_id] = 'corrupted'
                content_id = hashutil.hash_to_hex(content_id)
                logger.error(e)
            except ObjNotFoundError as e:
                content_status[content_id] = 'missing'
                content_id = hashutil.hash_to_hex(content_id)
                logger.error(e)

        return content_status

    def content_archive_update(self, content_id, archive_id, new_status=None):
        """Update the status of a archive content and set its mtime to now.

        Change the last modification time of an archived content and change
        its status to the given one.

        Args:
            content_id (str): The content id.
            archive_id (str): The id of the concerned archive.
            new_status (str): One of missing, ongoing or present, this
                status will replace the previous one. If not given, the
                function only changes the mtime of the content.
        """
        db_obj_id = r'\x' + hashutil.hash_to_hex(content_id)
        self.archiver_db.content_archive_update(
            db_obj_id,
            archive_id,
            new_status
        )

    @abc.abstractmethod
    def need_archival(self, content_data):
        """Indicate if the content needs to be archived.

        Args:
            content_data (dict): dict that contains two lists 'present' and
                'missing' with copies id corresponding to this status.

        Returns:
            True if there is not enough copies, False otherwise.

        """
        pass

    @abc.abstractmethod
    def choose_backup_servers(self, present, missing):
        """Choose and yield the required amount of couple source/destination

        For each required copy, choose a unique destination server
        among the missing copies and a source server among the
        presents.

        Yields:
            tuple (source (str), destination (src)) for each required copy.

        """
        pass


class ArchiverWithRetentionPolicyWorker(BaseArchiveWorker):
    """ Do the required backups on a given batch of contents.

    Process the content of a content batch in order to do the needed backups on
    the slaves servers.
    """

    ADDITIONAL_CONFIG = {
        'retention_policy': ('int', 2),
        'archival_max_age': ('int', 3600),

        'storages': ('list[dict]',
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
    CONFIG_BASE_FILENAME = 'archiver/worker'

    def __init__(self, batch):
        """ Constructor of the ArchiverWorker class.

        Args:
            batch: list of object's sha1 that potentially need archival.
        """
        super().__init__(batch)
        config = self.config
        self.retention_policy = config['retention_policy']
        self.archival_max_age = config['archival_max_age']

        self.objstorages = {
            storage['host']: get_objstorage(storage['cls'], storage['args'])
            for storage in config.get('storages', [])
        }

        if len(self.objstorages) < self.retention_policy:
            raise ValueError('Retention policy is too high for the number of '
                             'provided servers')

    def need_archival(self, content_data):
        """ Indicate if the content need to be archived.

        Args:
            content_data (dict): dict that contains two lists 'present' and
                'missing' with copies id corresponding to this status.
        Returns: True if there is not enough copies, False otherwise.
        """
        nb_presents = len(content_data.get('present', []))
        for copy, mtime in content_data.get('ongoing', {}).items():
            if not self._is_archival_delay_elasped(mtime):
                nb_presents += 1
        return nb_presents < self.retention_policy

    def _is_archival_delay_elapsed(self, start_time):
        """ Indicates if the archival delay is elapsed given the start_time

        Args:
            start_time (float): time at which the archival started.

        Returns:
            True if the archival delay is elasped, False otherwise
        """
        elapsed = time.time() - start_time
        return elapsed > self.archival_max_age

    def choose_backup_servers(self, present, missing):
        """Choose and yield the required amount of couple source/destination

        For each required copy, choose a unique destination server
        among the missing copies and a source server among the
        presents.

        Each destination server is unique so after archival, the
        retention policy requirement will be fulfilled. However, the
        source server may be used multiple times.

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


class ArchiverToBackendWorker(BaseArchiveWorker):
    """Worker that send copies over to backend without retention policy.

    Process the content of a content batch from source objstorage to
    destination objstorage.

    """

    ADDITIONAL_CONFIG = {
        'source': ('dict', {
            'host': 'uffizi',
            'cls': 'pathslicing',
            'args': {
                'root': '/srv/softwareheritage/objects',
                'slicing': '0:2/2:4/4:6'
            }
        }),
        'destination': ('dict', {
            'host': 'azure',
            'cls': 'objstorage_azure',
            'args': {
                'account_name': 'accout-name',
                'api_secret_key': 'secret-key',
                'container_name': 'container-name',
            }
        })
    }

    CONFIG_BASE_FILENAME = 'archiver/worker-to-backend'

    def __init__(self, batch):
        """Constructor of the ArchiverWorkerToBackend class.

        Args:
            batch: list of object's sha1 that potentially need archival.

        """
        super().__init__(batch)

        source = self.config['source']
        self.source_host = source['host']
        self.objstorages[self.source_host] = get_objstorage(
            source['cls'], source['args'])

        destination = self.config['destination']
        self.destination_host = destination['host']
        self.objstorages[self.destination_host] = get_objstorage(
            destination['cls'], destination['args'])

    def need_archival(self, content_data):
        """Indicate if the content needs to be archived.

        Args:
            content_data (dict): dict that contains 3 lists 'present',
            'ongoing' and 'missing' with copies id corresponding to
            this status.

        Returns:
            True if we need to archive, False otherwise

        """
        if self.destination_host in content_data.get('missing', {}):
            return True
        return False

    def choose_backup_servers(self, present, missing):
        """Only 1 couple source/destination as backup so we send back always
        from the same source to the destination.

        """
        return [(self.source_host, self.destination_host)]
