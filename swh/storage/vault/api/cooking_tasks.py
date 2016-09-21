# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from swh.core import hashutil
from ..cache import VaultCache
from ..cooker import DirectoryVaultCooker
from ... import get_storage


COOKER_TYPES = {
    'directory': DirectoryVaultCooker
}


class SWHCookingTask(Task):
    """ Main task that archive a batch of content.
    """
    task_queue = 'swh_storage_vault_cooking'

    def run(self, type, hex_dir_id, storage_args, cache_args):
        # Initialize elements
        storage = get_storage(**storage_args)
        cache = VaultCache(**cache_args)
        # Initialize cooker
        vault_cooker_class = COOKER_TYPES[type]
        cooker = vault_cooker_class(storage, cache)
        # Perform the cooking
        dir_id = hashutil.hex_to_hash(hex_dir_id)
        cooker.cook(dir_id)
