# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core import hashutil
from swh.core.api import SWHRemoteAPI
from swh.storage.exc import StorageAPIError


class RemoteVaultCache(SWHRemoteAPI):
    """Client to the Software Heritage vault cache."""

    def __init__(self, base_url):
        super().__init__(api_exception=StorageAPIError, url=base_url)

    def directory_ls(self):
        return self.get('vault/directory/')

    def directory_get(self, obj_id):
        return self.get('vault/directory/%s/' % (hashutil.hash_to_hex(obj_id)))

    def directory_cook(self, obj_id):
        return self.post('vault/directory/%s/' % hashutil.hash_to_hex(obj_id),
                         data={})

    def revision_ls(self):
        return self.get('vault/revision/')

    def revision_get(self, obj_id):
        return self.get('vault/revision/%s/' % (hashutil.hash_to_hex(obj_id)))

    def revision_cook(self, obj_id):
        return self.post('vault/revision/%s/' % hashutil.hash_to_hex(obj_id),
                         data={})
