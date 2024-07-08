# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List

from swh.core.api import RemoteException, RPCClient
from swh.model.model import Content

from ..exc import (
    BlockedOriginException,
    HashCollision,
    MaskedObjectException,
    NonRetryableException,
    QueryTimeout,
    StorageAPIError,
    StorageArgumentException,
    UnknownMetadataAuthority,
    UnknownMetadataFetcher,
)
from ..interface import StorageInterface
from .serializers import DECODERS, ENCODERS


class RemoteStorage(RPCClient):
    """Proxy to a remote storage API"""

    api_exception = StorageAPIError
    backend_class = StorageInterface
    reraise_exceptions = [
        BlockedOriginException,
        MaskedObjectException,
        NonRetryableException,
        QueryTimeout,
        StorageArgumentException,
        UnknownMetadataAuthority,
        UnknownMetadataFetcher,
    ]
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS

    def raise_for_status(self, response) -> None:
        try:
            super().raise_for_status(response)
        except RemoteException as e:
            if (
                e.response is not None
                and e.response.status_code == 500
                and e.args
                and e.args[0].get("type") == "HashCollision"
            ):
                # XXX: workaround until we fix these HashCollisions happening
                # when they shouldn't
                raise HashCollision(*e.args[0]["args"])
            else:
                raise

    def content_add(self, content: List[Content]) -> Dict[str, int]:
        return self._post("content/add", {"content": [c.with_data() for c in content]})

    def reset(self):
        return self._post("reset", {})

    def stat_counters(self):
        return self.get("stat/counters")

    def refresh_stat_counters(self):
        return self.get("stat/refresh")
