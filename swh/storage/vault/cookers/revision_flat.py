# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
from pathlib import Path

from swh.core import hashutil

from .base import BaseVaultCooker, DirectoryBuilder, get_tar_bytes


class RevisionFlatCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """
    CACHE_TYPE_KEY = 'revision_flat'

    def cook(self, obj_id):
        """Cook the requested revision into a Bundle

        Args:
            obj_id (bytes): the id of the revision to be cooked.

        Returns:
            bytes that correspond to the bundle

        """
        directory_cooker = DirectoryBuilder(self.storage)
        with tempfile.TemporaryDirectory(suffix='.cook') as root_tmp:
            root = Path(root_tmp)
            for revision in self.storage.revision_log([obj_id]):
                revdir = root / hashutil.hash_to_hex(revision['id'])
                revdir.mkdir()
                directory_cooker.build_directory(revision['directory'],
                                                 str(revdir).encode())
            bundle_content = get_tar_bytes(root_tmp,
                                           hashutil.hash_to_hex(obj_id))
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
