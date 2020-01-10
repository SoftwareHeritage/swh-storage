# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import psycopg2
import traceback

from typing import Dict, List

from retrying import retry

from swh.storage import get_storage, HashCollision


logger = logging.getLogger(__name__)


RETRY_EXCEPTIONS = [
    # raised when two parallel insertions insert the same data
    psycopg2.IntegrityError,
    HashCollision,
]


def should_retry_adding(error: Exception) -> bool:
    """Retry policy when some kind of failures occur (database integrity error,
       hash collision, etc...)

    """
    retry = any(isinstance(error, exc) for exc in RETRY_EXCEPTIONS)
    if retry:
        error_name = error.__module__ + '.' + error.__class__.__name__
        logger.warning('Retry adding a batch', exc_info=False, extra={
            'swh_type': 'storage_retry',
            'swh_exception_type': error_name,
            'swh_exception': traceback.format_exception(
                error.__class__,
                error,
                error.__traceback__,
            ),
        })
    return retry


class RetryingProxyStorage:
    """Storage implementation which retries adding objects when it specifically
       fails (hash collision, integrity error).

    """
    def __init__(self, storage):
        self.storage = get_storage(**storage)

    def __getattr__(self, key):
        return getattr(self.storage, key)

    @retry(retry_on_exception=should_retry_adding, stop_max_attempt_number=3)
    def content_add(self, content: List[Dict]) -> Dict:
        return self.storage.content_add(content)

    @retry(retry_on_exception=should_retry_adding, stop_max_attempt_number=3)
    def origin_add_one(self, origin: Dict) -> str:
        return self.storage.origin_add_one(origin)
