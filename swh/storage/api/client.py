# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Union

from swh.core.api import RPCClient, RemoteException
from swh.model.model import Content

from ..exc import StorageAPIError, StorageArgumentException, HashCollision
from ..interface import StorageInterface

from .serializers import ENCODERS, DECODERS


class RemoteStorage(RPCClient):
    """Proxy to a remote storage API"""

    api_exception = StorageAPIError
    backend_class = StorageInterface
    reraise_exceptions = [
        StorageArgumentException,
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

    def content_add(self, content: Iterable[Union[Content, Dict[str, Any]]]):
        content = [c.with_data() if isinstance(c, Content) else c for c in content]
        return self.post("content/add", {"content": content})

    def reset(self):
        return self.post("reset", {})

    def stat_counters(self):
        return self.get("stat/counters")

    def refresh_stat_counters(self):
        return self.get("stat/refresh")
