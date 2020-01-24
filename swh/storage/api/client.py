# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient

from ..exc import StorageAPIError
from ..interface import StorageInterface


class RemoteStorage(RPCClient):
    """Proxy to a remote storage API"""
    api_exception = StorageAPIError
    backend_class = StorageInterface

    def reset(self):
        return self.post('reset', {})

    def stat_counters(self):
        return self.get('stat/counters')

    def refresh_stat_counters(self):
        return self.get('stat/refresh')
