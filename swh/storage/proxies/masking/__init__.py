# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import functools
import inspect
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

import psycopg2.pool

from swh.core.api.classes import PagedResult
from swh.model.model import Origin
from swh.model.swhids import ExtendedObjectType, ExtendedSWHID
from swh.storage import get_storage
from swh.storage.exc import MaskedObjectException
from swh.storage.interface import Content, HashDict, Sha1, StorageInterface
from swh.storage.proxies.masking.db import MaskedStatus

from .db import MaskingQuery

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


class MaskingProxyStorage:
    """Masking storage proxy

    This proxy filters out objects that are present in the archive but should
    not be displayed for policy reasons.

    It uses a specific PostgreSQL database (which for now is colocated with the
    swh.storage PostgreSQL database), the access to which is implemented in the
    :mod:`.db` submodule.

    Sample configuration

    .. code-block: yaml

        storage:
          cls: masking
          masking_db: 'dbname=swh-storage'
          max_pool_conns: 10
          storage:
          - cls: remote
            url: http://storage.internal.staging.swh.network:5002/

    """

    def __init__(
        self,
        masking_db: str,
        storage: Union[Dict, StorageInterface],
        min_pool_conns: int = 1,
        max_pool_conns: int = 5,
    ):
        self.storage: StorageInterface = (
            get_storage(**storage) if isinstance(storage, dict) else storage
        )

        self.masking_pool = psycopg2.pool.ThreadedConnectionPool(
            min_pool_conns, max_pool_conns, masking_db
        )

    @contextmanager
    def masking_query(self) -> Iterator[MaskingQuery]:
        ret = None
        try:
            ret = MaskingQuery.from_pool(self.masking_pool)
            yield ret
        finally:
            if ret:
                ret.put_conn()

    def get_swhids_for_result_of_method(
        self, method_name: str, result: Any
    ) -> List[ExtendedSWHID]:
        if result is None:
            raise TypeError(f"Filtering of Nones missing in {method_name}")

        result_type = getattr(result, "object_type", None)
        if result_type == "raw_extrinsic_metadata":
            # they have a swhid, we need to add the extra target first
            return [result.swhid(), result.target]

        if hasattr(result, "swhid"):
            swhid = result.swhid()
            if hasattr(swhid, "to_extended"):
                swhid = swhid.to_extended()
            assert isinstance(
                swhid, ExtendedSWHID
            ), f"{method_name} returned object with unexpected swhid() method return type"
            return [swhid]

        if result_type:
            if result_type == "extid":
                return [result.target.to_extended()]
            if result_type.startswith("origin_visit"):
                return [Origin(url=result.origin).swhid()]

        if (
            object_type := method_name.removesuffix("_get_random").removesuffix(
                "_get_id_partition"
            )
        ) != method_name:
            # Returns bare sha1_gits
            assert isinstance(result, bytes), f"{method_name} returned unexpected type"
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType[object_type.upper()],
                    object_id=result,
                )
            ]
        elif method_name == "revision_log":
            # returns dicts of revisions
            assert (
                isinstance(result, dict) and "id" in result
            ), f"{method_name} returned unexpected type"
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.REVISION, object_id=result["id"]
                )
            ]
        elif method_name == "revision_shortlog":
            # Returns tuples (revision.id, revision.parents)
            assert isinstance(
                result[0], bytes
            ), f"{method_name} returned unexpected type"
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.REVISION, object_id=result[0]
                )
            ]

        elif method_name == "origin_get_by_sha1":
            # Returns origin dicts, because why not
            assert (
                isinstance(result, dict) and "url" in result
            ), f"{method_name} returned unexpected type"
            return [Origin(url=result["url"]).swhid()]
        elif method_name == "origin_visit_get_with_statuses":
            # Returns an OriginVisitWithStatuses
            return [Origin(url=result.visit.origin).swhid()]
        elif method_name == "snapshot_get":
            # Returns a snapshot dict
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.SNAPSHOT, object_id=result["id"]
                )
            ]

        raise ValueError(f"Cannot get swhid for result of method {method_name}")

    def raise_if_masked(self, swhids: List[ExtendedSWHID]) -> None:
        with self.masking_query() as q:
            masked = q.swhids_are_masked(swhids)

        if masked:
            raise MaskedObjectException(masked)

    def masked_result(
        self, method_name: str, result: Any
    ) -> Optional[Dict[ExtendedSWHID, List[MaskedStatus]]]:
        """Check if the result object is masked, returning the associated information."""
        with self.masking_query() as q:
            return q.swhids_are_masked(
                self.get_swhids_for_result_of_method(method_name, result)
            )

    def raise_if_masked_result(self, method_name: str, result: Any) -> None:
        """Raise a :exc:`MaskedObjectException` if result is masked."""
        if masked := self.masked_result(method_name, result):
            raise MaskedObjectException(masked)

    def raise_if_masked_results(self, method_name: str, results: Iterable[Any]) -> None:
        """Raise a :exc:`MaskedObjectException` if any object in `results` is masked."""
        result_swhids = set()
        for result in results:
            if result is not None:
                result_swhids.update(
                    self.get_swhids_for_result_of_method(method_name, result)
                )

        if result_swhids:
            with self.masking_query() as q:
                masked = q.swhids_are_masked(list(result_swhids))

            if masked:
                raise MaskedObjectException(masked)

    @functools.lru_cache(128)
    def __getattr__(self, key):
        if key == "storage":
            raise AttributeError(key)
        elif key == "journal_writer":
            # Useful for tests
            return self.storage.journal_writer
        elif key.endswith("_add") or key in ("content_update", "content_add_metadata"):
            return getattr(self.storage, key)
        elif key in ("extid_get_from_extid",):
            return getattr(self.storage, key)
        elif key in ("refresh_stat_counters", "stat_counters", "origin_count"):
            return getattr(self.storage, key)
        elif key in ("check_config",):
            return getattr(self.storage, key)
        elif key in ("clear_buffers", "flush"):
            return getattr(self.storage, key)
        elif key in (
            "object_find_recent_references",
            "metadata_authority_get",
            "metadata_fetcher_get",
            "object_find_by_sha1_git",
        ):
            return getattr(self.storage, key)
        elif key in (
            "snapshot_get",
            "origin_visit_find_by_date",
            "origin_visit_get_by",
            "origin_visit_status_get_latest",
            "origin_visit_get_latest",
        ):
            return self._getter_optional(key)
        elif key in (
            "directory_entry_get_by_path",
            "directory_get_entries",
            "directory_get_raw_manifest",
            "directory_ls",
            "raw_extrinsic_metadata_get_authorities",
            "snapshot_branch_get_by_name",
            "snapshot_count_branches",
            "snapshot_get_branches",
            "origin_snapshot_get_all",
        ):
            return self._getter_filtering_arguments(key)
        elif key in (
            "origin_list",
            "origin_visit_get",
            "origin_search",
            "raw_extrinsic_metadata_get",
            "origin_visit_get_with_statuses",
            "origin_visit_status_get",
        ) or key.endswith("_partition"):
            return self._getter_pagedresult(key)
        elif key.endswith("_get") or key in (
            "origin_get_by_sha1",
            "content_find",
            "skipped_content_find",
            "revision_log",
            "revision_shortlog",
            "extid_get_from_target",
            "raw_extrinsic_metadata_get_by_ids",
        ):
            return self._getter_list_optional(key)
        elif key.endswith("_get_random"):
            return self._getter_random(key)
        elif key.endswith("_missing") or key.startswith("content_missing_"):
            return getattr(self.storage, key)
        else:
            raise NotImplementedError(key)

    def content_get_data(self, content: Union[HashDict, Sha1]) -> Optional[bytes]:
        ret = self.storage.content_get_data(content)
        if ret is None:
            return None

        if isinstance(content, dict) and "sha1_git" in content:
            self.raise_if_masked(
                [
                    ExtendedSWHID(
                        object_type=ExtendedObjectType.CONTENT,
                        object_id=content["sha1_git"],
                    )
                ]
            )

        else:
            swhid = Content.from_data(ret).swhid().to_extended()
            self.raise_if_masked([swhid])

        return ret

    def _get_swhids_for_method_args(
        self, method_name: str, parsed_args: Dict[str, Any]
    ) -> List[ExtendedSWHID]:
        if method_name in ("directory_entry_get_by_path", "directory_ls"):
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.DIRECTORY,
                    object_id=parsed_args["directory"],
                )
            ]
        elif method_name == "directory_get_entries":
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.DIRECTORY,
                    object_id=parsed_args["directory_id"],
                )
            ]
        elif method_name.startswith("snapshot_"):
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.SNAPSHOT,
                    object_id=parsed_args["snapshot_id"],
                )
            ]
        elif method_name == "raw_extrinsic_metadata_get_authorities":
            return [parsed_args["target"].to_extended()]
        elif method_name == "directory_get_raw_manifest":
            return [
                ExtendedSWHID(
                    object_type=ExtendedObjectType.DIRECTORY,
                    object_id=object_id,
                )
                for object_id in parsed_args["directory_ids"]
            ]
        elif method_name == "origin_snapshot_get_all":
            return [Origin(url=parsed_args["origin_url"]).swhid()]
        else:
            raise ValueError(f"Cannot get swhid for arguments of method {method_name}")

    def _getter_filtering_arguments(self, method_name: str):
        """Generates a function which takes some arguments, calls the underlying
        storage with them, then, if the underlying storage returns any data,
        raises :exc:`MaskedObjectException` if the requested object is masked"""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            method = getattr(self.storage, method_name)
            result = method(*args, **kwargs)
            if result is None:
                return None

            parsed_args = inspect.getcallargs(method, *args, **kwargs)
            self.raise_if_masked(
                self._get_swhids_for_method_args(method_name, parsed_args)
            )

            return result

        return newf

    def _getter_random(self, method_name: str):
        """Generates a function calls the underlying method until it returns an object
        that is not masked"""

        RANDOM_ATTEMPTS = 5

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            method = getattr(self.storage, method_name)
            for _ in range(RANDOM_ATTEMPTS):
                result = method(*args, **kwargs)
                if result is None:
                    return None

                if not self.masked_result(method_name, result):
                    return result

        return newf

    def _getter_optional(self, method_name: str):
        """Generates a function which take an id and return, queries underlying
        storages in order until one returns a non-None value"""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            method = getattr(self.storage, method_name)
            result = method(*args, **kwargs)
            if result is None:
                return None

            self.raise_if_masked_result(method_name, result)

            return result

        return newf

    def _getter_list_optional(
        self,
        method_name: str,
    ):
        """Generates a function which take a list of ids and return a list of
        optional objects in the same order, raising :exc:`MaskedObjectException`
        if some objects in the batch are masked."""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            method = getattr(self.storage, method_name)

            results = list(method(*args, **kwargs))

            self.raise_if_masked_results(method_name, results)
            return results

        return newf

    def _getter_pagedresult(self, method_name: str) -> Callable[..., PagedResult]:
        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs) -> PagedResult:
            method = getattr(self.storage, method_name)
            results = method(*args, **kwargs)

            self.raise_if_masked_results(method_name, results.results)

            return results

        return newf
