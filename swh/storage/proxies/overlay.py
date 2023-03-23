# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import functools
import random
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Sequence,
    TypeVar,
)
import warnings

from swh.core.api.classes import PagedResult
from swh.model.model import OriginVisit, Sha1Git
from swh.storage import get_storage
from swh.storage.exc import StorageArgumentException
from swh.storage.interface import StorageInterface

OBJECT_TYPES = [
    "content",
    "directory",
    "snapshot",
    "origin_visit_status",
    "origin_visit",
    "origin",
]


TKey = TypeVar("TKey", bound=Hashable)
TValue = TypeVar("TValue")


class UntestedCodeWarning(UserWarning):
    pass


class OverlayProxyStorage:
    """Overlay storage proxy

    This storage proxy is in front of several backends (or other proxies).

    It always writes to the first backend.
    When reading, it returns aggregated results for all backends (or from the
    first backend to have a result, for endpoints which provide a single result).

    Sample configuration use case for filtering storage:

    .. code-block: yaml

        storage:
          cls: counter
          storages:
          - cls: remote
            url: http://storage-rw.internal.staging.swh.network:5002/
          - cls: remote
            url: http://storage-ro2.internal.staging.swh.network:5002/
          - cls: remote
            url: http://storage-ro1.internal.staging.swh.network:5002/

    """

    def __init__(self, storages):
        warnings.warn(
            "OverlayProxyStorage is not well-tested and should not be used "
            "in production.",
            UntestedCodeWarning,
        )
        self.storages: List[StorageInterface] = [
            get_storage(**storage) if isinstance(storage, dict) else storage
            for storage in storages
        ]

    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        elif key == "journal_writer":
            # Useful for tests
            return self.storages[0].journal_writer
        elif key.endswith("_add") or key in ("content_update", "content_add_metadata"):
            return getattr(self.storages[0], key)
        elif key in (
            "content_get_data",
            "directory_get_entries",
            "directory_entry_get_by_path",
            "snapshot_get",
            "snapshot_get_branches",
            "snapshot_count_branches",
            "origin_visit_get_by",
            "origin_visit_status_get",
            "origin_visit_status_get_latest",
            "metadata_authority_get",
            "metadata_fetcher_get",
        ):
            return self._getter_optional(key)
        elif key in (
            "origin_list",
            "origin_visit_get",
            "origin_search",
            "raw_extrinsic_metadata_get",
            "origin_visit_get_with_statuses",
        ):
            return self._getter_pagedresult(key)
        elif key.endswith("_get") or key in ("origin_get_by_sha1",):
            return self._getter_list_optional(key)
        elif key in (
            "content_missing",  # TODO: could be optimized
            "content_missing_per_sha1_git",  # TODO: could be optimized
        ):
            return self._getter_intersection(key)
        elif key.endswith("_missing") or key in ("content_missing_per_sha1",):
            return self._missing(key)
        elif key in ("refresh_stat_counters", "stat_counters"):
            return getattr(self.storages[0], key)
        elif key.endswith("_get_random"):
            return self._getter_random(key)
        elif key in (
            "content_find",
            "origin_snapshot_get_all",
            "extid_get_from_extid",
            "extid_get_from_target",
            "raw_extrinsic_metadata_get_by_ids",  # TODO: could be optimized
            "raw_extrinsic_metadata_get_authorities",
        ):
            return self._getter_union(key)
        else:
            raise NotImplementedError(key)

    def _getter_optional(self, method_name: str) -> Callable[[TKey], Optional[TValue]]:
        """Generates a function which take an id and return, queries underlying
        storages in order until one returns a non-None value"""

        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(id_: TKey, *args, **kwargs) -> Optional[TValue]:
            method: Callable[[TKey], Optional[TValue]]

            for storage in self.storages:
                method = getattr(storage, method_name)
                result = method(id_, *args, **kwargs)
                if result is not None:
                    return result

            return None

        return newf

    def _getter_list_optional(
        self,
        method_name: str,
    ) -> Callable[[List[TKey]], List[Optional[TValue]]]:
        """Generates a function which take a list of ids and return a list of optional
        objects in the same order, implemented by querying all underlying storages."""

        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(ids: List[TKey], *args, **kwargs) -> List[Optional[TValue]]:
            method: Callable[[List[TKey]], List[Optional[TValue]]]

            missing_ids = list(ids)
            results = {}
            for storage in self.storages:
                method = getattr(storage, method_name)
                new_results = dict(
                    zip(missing_ids, method(missing_ids, *args, **kwargs))
                )
                results.update(new_results)
                missing_ids = [id_ for id_ in missing_ids if new_results[id_] is None]

            return [results[id_] for id_ in ids]

        return newf

    def _missing(self, method_name: str) -> Callable[[List[TKey]], Iterable[TKey]]:
        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(ids: List[TKey]) -> List[TKey]:
            method: Callable[[List[TKey]], Iterable[TKey]]

            missing_ids = list(ids)
            for storage in self.storages:
                method = getattr(storage, method_name)
                missing_ids = list(method(missing_ids))
            return missing_ids

        return newf

    def _getter_random(self, method_name: str) -> Callable[[], Optional[TValue]]:
        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(*args, **kwargs) -> Optional[TValue]:
            method: Callable[[], Optional[TValue]]

            # Not uniform sampling, but we don't care.
            storages = list(self.storages)
            random.shuffle(storages)

            for storage in storages:
                method = getattr(storage, method_name)
                try:
                    result = method(*args, **kwargs)
                except IndexError:
                    # in-memory storage when empty
                    result = None
                if result is not None:
                    return result

            return None

        return newf

    def _getter_intersection(self, method_name) -> Callable[..., List[TKey]]:
        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(*args, **kwargs) -> List[TKey]:
            (head, *tail) = self.storages
            results = set(getattr(head, method_name)(*args, **kwargs))
            for storage in tail:
                method = getattr(storage, method_name)
                results.intersection_update(method(*args, **kwargs))
            return list(results)

        return newf

    def _getter_union(self, method_name) -> Callable[..., List[TKey]]:
        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(*args, **kwargs) -> List[TKey]:
            results = set()
            for storage in self.storages:
                method = getattr(storage, method_name)
                results.update(method(*args, **kwargs))
            return list(results)

        return newf

    def _getter_pagedresult(self, method_name: str) -> Callable[..., PagedResult]:
        @functools.wraps(getattr(self.storages[0], method_name))
        def newf(*args, page_token: Optional[bytes] = None, **kwargs) -> PagedResult:
            if page_token is None:
                storage_id = 0
            else:
                if isinstance(page_token, str):
                    (storage_id_str, page_token) = page_token.split(" ", 1)
                elif isinstance(page_token, bytes):
                    (storage_id_bytes, page_token) = page_token.split(b" ", 1)
                    storage_id_str = storage_id_bytes.decode()
                else:
                    raise StorageArgumentException(
                        "page_token must be a string or bytes"
                    )
                storage_id = int(storage_id_str)
                page_token = page_token or None

            prepend_results = []

            for storage in self.storages[storage_id:]:
                method = getattr(storage, method_name)
                results = method(*args, page_token=page_token, **kwargs)
                if results.results:
                    if results.next_page_token is None:
                        prepend_results = results.results
                        continue
                    elif isinstance(results.next_page_token, str):
                        next_page_token = f"{storage_id} {results.next_page_token}"
                    else:
                        next_page_token = f"{storage_id} ".encode() + (
                            results.next_page_token
                        )
                    return PagedResult(
                        next_page_token=next_page_token,
                        results=prepend_results + results.results,
                    )
                else:
                    storage_id += 1
                    page_token = None

            return PagedResult(
                next_page_token=None,
                results=prepend_results,
            )

        return newf

    def check_config(self, *, check_write: bool) -> bool:
        (rw_storage, *ro_storages) = self.storages
        return rw_storage.check_config(check_write=check_write) and all(
            storage.check_config(check_write=False) for storage in ro_storages
        )

    def directory_ls(
        self, directory: Sha1Git, recursive: bool = False
    ) -> Iterable[Dict[str, Any]]:
        for storage in self.storages:
            it = iter(storage.directory_ls(directory, recursive=recursive))
            try:
                yield next(it)
            except StopIteration:
                # Note: this is slightly wasteful for the empty directory
                continue
            else:
                yield from it
                return

    def directory_get_raw_manifest(
        self, directory_ids: List[Sha1Git]
    ) -> Dict[Sha1Git, Optional[bytes]]:
        results = {}
        missing_ids = set(directory_ids)
        for storage in self.storages:
            new_results = storage.directory_get_raw_manifest(list(missing_ids))
            missing_ids.difference_update(set(new_results))
            results.update(new_results)
        return results

    def object_find_by_sha1_git(self, ids: List[Sha1Git]) -> Dict[Sha1Git, List[Dict]]:
        results: Dict[Sha1Git, List[Dict]] = {id_: [] for id_ in ids}
        for storage in self.storages:
            for (id_, objects) in storage.object_find_by_sha1_git(ids).items():
                # note: this is quadratic in the number of hash conflicts:
                for object_ in objects:
                    if object_ not in results[id_]:
                        results[id_].append(object_)

        return results

    def origin_visit_get_latest(self, *args, **kwargs) -> Optional[OriginVisit]:
        return max(
            (
                storage.origin_visit_get_latest(*args, **kwargs)
                for storage in self.storages
            ),
            key=lambda ov: (-1000, None) if ov is None else (ov.visit, ov.date),
        )

    def origin_visit_find_by_date(
        self, origin: str, visit_date: datetime.datetime
    ) -> Optional[OriginVisit]:
        return min(
            (
                storage.origin_visit_find_by_date(origin, visit_date)
                for storage in self.storages
            ),
            key=lambda ov: (datetime.timedelta.max, None)
            if ov is None
            else (abs(visit_date - ov.date), -(ov.visit or 0)),
        )

    def clear_buffers(self, object_types: Sequence[str] = ()) -> None:
        return self.storages[0].clear_buffers(object_types)

    def flush(self, object_types: Sequence[str] = ()) -> Dict[str, int]:
        return self.storages[0].flush(object_types)
