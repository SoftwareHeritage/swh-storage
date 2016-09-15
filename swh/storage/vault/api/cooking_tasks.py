# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from swh.core import hashutil
from ..cache import VaultCache
from ..cooker import DirectoryVaultCooker
from ... import get_storage


class SWHCookingTask(Task):
    """ Main task that archive a batch of content.
    """
    task_queue = 'swh_storage_vault_cooking'

    def run(self, hex_dir_id, storage_args, cache_args):
        storage = get_storage(**storage_args)
        cache = VaultCache(**cache_args)
        directory_cooker = DirectoryVaultCooker(storage, cache)

        dir_id = hashutil.hex_to_hash(hex_dir_id)
        directory_cooker.cook(dir_id)
