# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .base import BaseVaultCooker, DirectoryBuilder


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """
    CACHE_TYPE_KEY = 'directory'

    def prepare_bundle(self, obj_id):
        """Cook the requested directory into a Bundle

        Args:
            obj_id (bytes): the id of the directory to be cooked.

        Returns:
            bytes that correspond to the bundle

        """
        directory_builder = DirectoryBuilder(self.storage)
        return directory_builder.get_directory_bytes(obj_id)
