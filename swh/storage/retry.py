# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import psycopg2
import traceback

from datetime import datetime
from typing import Dict, List, Optional, Union

from requests.exceptions import ConnectionError
from tenacity import (
    retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
)

from swh.storage import get_storage, HashCollision


logger = logging.getLogger(__name__)


RETRY_EXCEPTIONS = [
    # raised when two parallel insertions insert the same data
    psycopg2.IntegrityError,
    HashCollision,
    # when the server is restarting
    ConnectionError,
]


def should_retry_adding(error: Exception) -> bool:
    """Retry if the error/exception if one of the RETRY_EXCEPTIONS type.

    """
    for exc in RETRY_EXCEPTIONS:
        if retry_if_exception_type(exc)(error):
            error_name = error.__module__ + '.' + error.__class__.__name__
            logger.warning('Retry adding a batch', exc_info=False, extra={
                'swh_type': 'storage_retry',
                'swh_exception_type': error_name,
                'swh_exception': traceback.format_exc(),
            })
            return True
    return False


swh_retry = retry(retry=should_retry_adding,
                  wait=wait_random_exponential(multiplier=1, max=10),
                  stop=stop_after_attempt(3))


class RetryingProxyStorage:
    """Storage implementation which retries adding objects when it specifically
       fails (hash collision, integrity error).

    """
    def __init__(self, storage):
        self.storage = get_storage(**storage)

    def __getattr__(self, key):
        return getattr(self.storage, key)

    @swh_retry
    def content_add(self, content: List[Dict]) -> Dict:
        contents = list(content)
        return self.storage.content_add(contents)

    @swh_retry
    def content_add_metadata(self, content: List[Dict]) -> Dict:
        contents = list(content)
        return self.storage.content_add_metadata(contents)

    @swh_retry
    def origin_add_one(self, origin: Dict) -> str:
        return self.storage.origin_add_one(origin)

    @swh_retry
    def origin_visit_add(self, origin: Dict,
                         date: Union[datetime, str], type: str) -> Dict:
        return self.storage.origin_visit_add(origin, date, type)

    @swh_retry
    def origin_visit_update(
            self, origin: str, visit_id: int, status: Optional[str] = None,
            metadata: Optional[Dict] = None,
            snapshot: Optional[Dict] = None) -> Dict:
        return self.storage.origin_visit_update(
            origin, visit_id, status=status,
            metadata=metadata, snapshot=snapshot)

    @swh_retry
    def tool_add(self, tools: List[Dict]) -> List[Dict]:
        tools = list(tools)
        return self.storage.tool_add(tools)

    @swh_retry
    def metadata_provider_add(
            self, provider_name: str, provider_type: str, provider_url: str,
            metadata: Dict) -> Union[str, int]:
        return self.storage.metadata_provider_add(
            provider_name, provider_type, provider_url, metadata)

    @swh_retry
    def origin_metadata_add(
            self, origin_url: str, ts: Union[str, datetime],
            provider_id: int, tool_id: int, metadata: Dict) -> None:
        return self.storage.origin_metadata_add(
            origin_url, ts, provider_id, tool_id, metadata)

    @swh_retry
    def directory_add(self, directories: List[Dict]) -> Dict:
        directories = list(directories)
        return self.storage.directory_add(directories)

    @swh_retry
    def revision_add(self, revisions: List[Dict]) -> Dict:
        revisions = list(revisions)
        return self.storage.revision_add(revisions)

    @swh_retry
    def release_add(self, releases: List[Dict]) -> Dict:
        releases = list(releases)
        return self.storage.release_add(releases)

    @swh_retry
    def snapshot_add(self, snapshot: List[Dict]) -> Dict:
        snapshots = list(snapshot)
        return self.storage.snapshot_add(snapshots)
