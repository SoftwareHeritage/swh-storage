# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from swh.model import hashutil
from ..cache import VaultCache
from ..cookers import COOKER_TYPES
from ... import get_storage


class SWHCookingTask(Task):
    """Main task which archives a contents batch.

    """
    task_queue = 'swh_storage_vault_cooking'

    def run(self, type, hex_id, storage_args, cache_args):
        # Initialize elements
        storage = get_storage(**storage_args)
        cache = VaultCache(**cache_args)
        # Initialize cooker
        obj_id = hashutil.hash_to_bytes(hex_id)
        cooker = COOKER_TYPES[type](storage, cache, obj_id)
        # Perform the cooking
        cooker.cook()
