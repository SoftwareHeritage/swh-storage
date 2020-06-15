# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import traceback

from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)

from swh.model.model import (
    Content,
    SkippedContent,
    Directory,
    Revision,
    Release,
    Snapshot,
    Origin,
    OriginVisit,
)

from swh.storage import get_storage
from swh.storage.exc import StorageArgumentException


logger = logging.getLogger(__name__)


def should_retry_adding(retry_state) -> bool:
    """Retry if the error/exception is (probably) not about a caller error

    """
    try:
        attempt = retry_state.outcome
    except AttributeError:
        # tenacity < 5.0
        attempt = retry_state

    if attempt.failed:
        error = attempt.exception()
        if isinstance(error, StorageArgumentException):
            # Exception is due to an invalid argument
            return False
        else:
            # Other exception
            module = getattr(error, "__module__", None)
            if module:
                error_name = error.__module__ + "." + error.__class__.__name__
            else:
                error_name = error.__class__.__name__
            logger.warning(
                "Retry adding a batch",
                exc_info=False,
                extra={
                    "swh_type": "storage_retry",
                    "swh_exception_type": error_name,
                    "swh_exception": traceback.format_exc(),
                },
            )
            return True
    else:
        # No exception
        return False


swh_retry = retry(
    retry=should_retry_adding,
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)


class RetryingProxyStorage:
    """Storage implementation which retries adding objects when it specifically
       fails (hash collision, integrity error).

    """

    def __init__(self, storage):
        self.storage = get_storage(**storage)

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        return getattr(self.storage, key)

    @swh_retry
    def content_add(self, content: Iterable[Content]) -> Dict:
        return self.storage.content_add(content)

    @swh_retry
    def content_add_metadata(self, content: Iterable[Content]) -> Dict:
        return self.storage.content_add_metadata(content)

    @swh_retry
    def skipped_content_add(self, content: Iterable[SkippedContent]) -> Dict:
        return self.storage.skipped_content_add(content)

    @swh_retry
    def origin_add_one(self, origin: Origin) -> str:
        return self.storage.origin_add_one(origin)

    @swh_retry
    def origin_visit_add(self, visits: Iterable[OriginVisit]) -> Iterable[OriginVisit]:
        return self.storage.origin_visit_add(visits)

    @swh_retry
    def metadata_fetcher_add(
        self, name: str, version: str, metadata: Dict[str, Any]
    ) -> None:
        return self.storage.metadata_fetcher_add(name, version, metadata)

    @swh_retry
    def metadata_authority_add(
        self, type: str, url: str, metadata: Dict[str, Any]
    ) -> None:
        return self.storage.metadata_authority_add(type, url, metadata)

    @swh_retry
    def origin_metadata_add(
        self,
        origin_url: str,
        discovery_date: datetime,
        authority: Dict[str, Any],
        fetcher: Dict[str, Any],
        format: str,
        metadata: bytes,
    ) -> None:
        return self.storage.origin_metadata_add(
            origin_url, discovery_date, authority, fetcher, format, metadata
        )

    @swh_retry
    def directory_add(self, directories: Iterable[Directory]) -> Dict:
        return self.storage.directory_add(directories)

    @swh_retry
    def revision_add(self, revisions: Iterable[Revision]) -> Dict:
        return self.storage.revision_add(revisions)

    @swh_retry
    def release_add(self, releases: Iterable[Release]) -> Dict:
        return self.storage.release_add(releases)

    @swh_retry
    def snapshot_add(self, snapshots: Iterable[Snapshot]) -> Dict:
        return self.storage.snapshot_add(snapshots)

    def clear_buffers(self, object_types: Optional[Iterable[str]] = None) -> None:
        return self.storage.clear_buffers(object_types)

    def flush(self, object_types: Optional[Iterable[str]] = None) -> Dict:
        """Specific case for buffer proxy storage failing to flush data

        """
        return self.storage.flush(object_types)
