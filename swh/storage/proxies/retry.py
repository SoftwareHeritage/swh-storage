# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import traceback

from tenacity import RetryCallState, retry, stop_after_attempt, wait_random_exponential
from tenacity.wait import wait_base

from swh.core.api import TransientRemoteException
from swh.storage import StorageSpec, get_storage
from swh.storage.exc import NonRetryableException
from swh.storage.interface import StorageInterface

logger = logging.getLogger(__name__)


def should_retry_adding(retry_state: RetryCallState) -> bool:
    """Retry if the error/exception is (probably) not about a caller error"""
    attempt = retry_state.outcome
    assert attempt

    if attempt.failed:
        error = attempt.exception()
        if isinstance(error, NonRetryableException):
            # Don't issue retries for persistent exceptions
            return False
        elif isinstance(error, (KeyboardInterrupt, SystemExit)):
            return False
        else:
            # Other exception
            module = getattr(error, "__module__", None)
            if module:
                error_name = error.__module__ + "." + error.__class__.__name__
            else:
                error_name = error.__class__.__name__
            logger.warning(
                "Retrying RPC call",
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


class wait_transient_exceptions(wait_base):
    """Wait longer when servers return HTTP 503."""

    def __init__(self, wait: float) -> None:
        self.wait = wait

    def __call__(self, retry_state: RetryCallState) -> float:
        attempt = retry_state.outcome
        assert attempt

        if attempt.failed and isinstance(attempt.exception(), TransientRemoteException):
            return self.wait
        else:
            return 0.0


swh_retry = retry(
    retry=should_retry_adding,
    wait=wait_random_exponential(multiplier=1, max=10) + wait_transient_exceptions(10),
    stop=stop_after_attempt(3),
    reraise=True,
)


def retry_function(storage, attribute_name):
    @swh_retry
    def newf(*args, **kwargs):
        return getattr(storage, attribute_name)(*args, **kwargs)

    return newf


class RetryingProxyStorage:
    """Storage implementation which retries adding objects when it specifically
    fails (hash collision, integrity error).

    """

    def __init__(self, storage: StorageSpec):
        self.storage: StorageInterface = get_storage(**storage)
        for attribute_name in dir(StorageInterface):
            if attribute_name.startswith("_"):
                continue
            attribute = getattr(self.storage, attribute_name)
            if hasattr(attribute, "__call__"):
                setattr(
                    self, attribute_name, retry_function(self.storage, attribute_name)
                )
