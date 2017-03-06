# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .base import BaseVaultCooker, DirectoryBuilder
from swh.core import hashutil


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """
    CACHE_TYPE_KEY = 'directory'

    def cook(self, obj_id):
        """Cook the requested directory into a Bundle

        Args:
            obj_id (bytes): the id of the directory to be cooked.

        Returns:
            bytes that correspond to the bundle

        """
        # Create the bytes that corresponds to the compressed
        # directory.
        directory_cooker = DirectoryBuilder(self.storage)
        bundle_content = directory_cooker.get_directory_bytes(obj_id)
        # Cache the bundle
        self.update_cache(obj_id, bundle_content)
        # Make a notification that the bundle have been cooked
        # NOT YET IMPLEMENTED see TODO in function.
        self.notify_bundle_ready(
            notif_data='Bundle %s ready' % hashutil.hash_to_hex(obj_id),
            bundle_id=obj_id)

    def notify_bundle_ready(self, notif_data, bundle_id):
        # TODO plug this method with the notification method once
        # done.
        pass
