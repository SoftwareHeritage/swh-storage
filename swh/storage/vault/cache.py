# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from swh.core import hashutil
from swh.objstorage import get_objstorage
from swh.objstorage.objstorage_pathslicing import DIR_MODE

BUNDLE_TYPES = {
    'directory': 'd',
}


class VaultCache():
    """The vault cache is an object storage that stores bundles

    The current implementation uses a PathSlicingObjStorage to store
    the bundles. The id of a content if prefixed to specify its type
    and store different types of bundle in different folders.

    """

    def __init__(self, root):
        for subdir in BUNDLE_TYPES.values():
            fp = os.path.join(root, subdir)
            if not os.path.isdir(fp):
                os.makedirs(fp, DIR_MODE, exist_ok=True)

        self.storages = {
            type: get_objstorage(
                'pathslicing', {'root': os.path.join(root, subdir),
                                'slicing': '0:1/0:5'}
            )
            for type, subdir in BUNDLE_TYPES.items()
        }

    def __contains__(self, obj_id):
        return obj_id in self.storage

    def add(self, obj_type, obj_id, content):
        storage = self._get_storage(obj_type)
        return storage.add(content, obj_id)

    def get(self, obj_type, obj_id):
        storage = self._get_storage(obj_type)
        return storage.get(hashutil.hex_to_hash(obj_id))

    def is_cached(self, obj_type, obj_id):
        storage = self._get_storage(obj_type)
        return hashutil.hex_to_hash(obj_id) in storage

    def ls(self, obj_type):
        storage = self._get_storage(obj_type)
        yield from storage

    def _get_storage(self, obj_type):
        """Get the storage that corresponds to the object type"""
        try:
            return self.storages[obj_type]
        except:
            raise ValueError('Wrong bundle type: ' + obj_type)
