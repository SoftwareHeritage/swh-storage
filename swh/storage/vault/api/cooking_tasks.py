# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from swh.core import hashutil
from ..cache import VaultCache
from ..cooker import COOKER_TYPES
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
        cooker = COOKER_TYPES[type](storage, cache)
        # Perform the cooking
        cooker.cook(obj_id=hashutil.hex_to_hash(hex_id))
