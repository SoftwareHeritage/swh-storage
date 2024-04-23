# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import functools
import inspect
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Union

import psycopg2.pool

from swh.core.api.classes import PagedResult
from swh.model.hashutil import MultiHash
from swh.model.model import Origin
from swh.model.swhids import ExtendedObjectType, ExtendedSWHID
from swh.storage import get_storage
from swh.storage.exc import MaskedObjectException
from swh.storage.interface import HashDict, Sha1, StorageInterface
from swh.storage.metrics import DifferentialTimer
from swh.storage.proxies.masking.db import MaskedStatus

from .db import MaskingQuery

MASKING_OVERHEAD_METRIC = "swh_storage_masking_overhead_seconds"


def get_datastore(cls, db):
    assert cls == "postgresql"
    from .db import MaskingAdmin

    return MaskingAdmin.connect(db)


def masking_overhead_timer(method_name: str) -> DifferentialTimer:
    """Return a properly setup DifferentialTimer for ``method_name`` of the storage"""
    return DifferentialTimer(MASKING_OVERHEAD_METRIC, tags={"endpoint": method_name})


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

        self._masking_pool = psycopg2.pool.ThreadedConnectionPool(
            min_pool_conns, max_pool_conns, masking_db
        )

        # Generate the method dictionaries once per instantiation, instead of
        # doing it on every (first) __getattr__ call.
        self._gen_method_dicts()

    @contextmanager
    def _masking_query(self) -> Iterator[MaskingQuery]:
        ret = None
        try:
            ret = MaskingQuery.from_pool(self._masking_pool)
            yield ret
        finally:
            if ret:
                ret.put_conn()

    @staticmethod
    def _get_swhids_in_result(method_name: str, result: Any) -> List[ExtendedSWHID]:
        if result is None:
            raise TypeError(f"Filtering of Nones missing in {method_name}")

        result_type = getattr(result, "object_type", None)
        if result_type == "raw_extrinsic_metadata":
            # Raw Extrinsic Metadata have a swhid, but we also mask them if the target is masked
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

    def _masked_result(
        self, method_name: str, result: Any
    ) -> Optional[Dict[ExtendedSWHID, List[MaskedStatus]]]:
        """Find the SWHIDs of the ``result`` object, and check if any of them is
        masked, returning the associated masking information."""
        with self._masking_query() as q:
            return q.swhids_are_masked(self._get_swhids_in_result(method_name, result))

    def _raise_if_masked_result(self, method_name: str, result: Any) -> None:
        """Raise a :exc:`MaskedObjectException` if ``result`` is masked."""
        masked = self._masked_result(method_name, result)
        if masked:
            raise MaskedObjectException(masked)

    def _raise_if_masked_swhids(self, swhids: List[ExtendedSWHID]) -> None:
        """Raise a :exc:`MaskedObjectException` if any SWHID is masked."""
        with self._masking_query() as q:
            masked = q.swhids_are_masked(swhids)

        if masked:
            raise MaskedObjectException(masked)

    def __getattr__(self, key):
        method = None

        if key in self._methods_by_name:
            method = self._methods_by_name[key](key)
        else:
            suffix = key.rsplit("_", 1)[-1]

            if suffix in self._methods_by_suffix:
                method = self._methods_by_suffix[suffix](key)

        if method:
            # Avoid going through __getattr__ again next time
            setattr(self, key, method)
            return method

        # Raise a NotImplementedError to make sure we don't forget to add
        # masking to any new storage functions
        raise NotImplementedError(key)

    def _gen_method_dicts(self):
        """Generate the :attr:`_methods_by_name` and :attr:`_methods_by_suffix`
        used by :meth:`__getattr__`"""
        _passthrough = functools.partial(getattr, self.storage)

        self._methods_by_name = {
            # Returns a single object
            "snapshot_get": self._getter_optional,
            "origin_visit_find_by_date": self._getter_optional,
            "origin_visit_get_by": self._getter_optional,
            "origin_visit_status_get_latest": self._getter_optional,
            "origin_visit_get_latest": self._getter_optional,
            # Returns a PagedResult
            "origin_list": self._getter_pagedresult,
            "origin_visit_get": self._getter_pagedresult,
            "origin_search": self._getter_pagedresult,
            "raw_extrinsic_metadata_get": self._getter_pagedresult,
            "origin_visit_get_with_statuses": self._getter_pagedresult,
            "origin_visit_status_get": self._getter_pagedresult,
            # Returns a list of (optional) objects
            "origin_get_by_sha1": self._getter_list,
            "content_find": self._getter_list,
            "skipped_content_find": self._getter_list,
            "revision_log": self._getter_list,
            "revision_shortlog": self._getter_list,
            "extid_get_from_target": self._getter_list,
            "raw_extrinsic_metadata_get_by_ids": self._getter_list,
            # Filter arguments
            "directory_entry_get_by_path": self._getter_filtering_arguments,
            "directory_get_entries": self._getter_filtering_arguments,
            "directory_get_raw_manifest": self._getter_filtering_arguments,
            "directory_ls": self._getter_filtering_arguments,
            "raw_extrinsic_metadata_get_authorities": self._getter_filtering_arguments,
            "snapshot_branch_get_by_name": self._getter_filtering_arguments,
            "snapshot_count_branches": self._getter_filtering_arguments,
            "snapshot_get_branches": self._getter_filtering_arguments,
            "origin_snapshot_get_all": self._getter_filtering_arguments,
            # Content functions that don't match common getter or adder suffixes
            "content_add_metadata": _passthrough,
            "content_missing_per_sha1": _passthrough,
            "content_missing_per_sha1_git": _passthrough,
            "content_update": _passthrough,
            # These objects aren't maskable
            "extid_get_from_extid": _passthrough,
            "object_find_by_sha1_git": _passthrough,
            "object_find_recent_references": _passthrough,
            "metadata_authority_get": _passthrough,
            "metadata_fetcher_get": _passthrough,
            # Utility methods
            "check_config": _passthrough,
            "clear_buffers": _passthrough,
            "flush": _passthrough,
            "origin_count": _passthrough,
            "refresh_stat_counters": _passthrough,
            "stat_counters": _passthrough,
            # For tests
            "journal_writer": _passthrough,
        }

        self._methods_by_suffix = {
            # These methods will never need do any masking
            "add": _passthrough,
            "missing": _passthrough,
            # Partitions return PagedResults
            "partition": self._getter_pagedresult,
            # Getters return lists of optional objects
            "get": self._getter_list,
            "random": self._getter_random,
        }

    def content_get_data(self, content: Union[HashDict, Sha1]) -> Optional[bytes]:
        with masking_overhead_timer("content_get_data") as t:
            with t.inner():
                ret = self.storage.content_get_data(content)

            if ret is None:
                return None

            if isinstance(content, dict) and "sha1_git" in content:
                self._raise_if_masked_swhids(
                    [
                        ExtendedSWHID(
                            object_type=ExtendedObjectType.CONTENT,
                            object_id=content["sha1_git"],
                        )
                    ]
                )

            else:
                # We did not get the SWHID of the object as argument, so we need to
                # hash the resulting content to check if its SWHID was masked.
                self._raise_if_masked_swhids(
                    [
                        ExtendedSWHID(
                            object_type=ExtendedObjectType.CONTENT,
                            object_id=MultiHash.from_data(ret, ["sha1_git"]).digest()[
                                "sha1_git"
                            ],
                        )
                    ]
                )

            return ret

    def _get_swhids_in_args(
        self, method_name: str, parsed_args: Dict[str, Any]
    ) -> List[ExtendedSWHID]:
        """Extract SWHIDs from the parsed arguments of ``method_name``.

        Arguments:
          method_name: name of the called method
          parsed_args: arguments of the method parsed with :func:`inspect.getcallargs`
        """

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
            return [parsed_args["target"]]
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
        """Handles methods that should filter on their argument, instead of the
        returned value. If the underlying storage returns :const:`None`, return
        it, else, raise :exc:`MaskedObjectException` if the requested object is
        masked"""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            with masking_overhead_timer(method_name) as t:
                method = getattr(self.storage, method_name)

                with t.inner():
                    result = method(*args, **kwargs)

                if result is None:
                    return None

                signature = inspect.signature(getattr(StorageInterface, method_name))
                bound_args = signature.bind(self, *args, **kwargs)
                self._raise_if_masked_swhids(
                    self._get_swhids_in_args(method_name, bound_args.arguments)
                )

                return result

        return newf

    RANDOM_ATTEMPTS = 5

    def _getter_random(self, method_name: str):
        """Handles methods returning a random object. Try
        :const:`RANDOM_ATTEMPTS` times for a non-masked object, and return it,
        else return :const:`None`."""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            with masking_overhead_timer(method_name) as t:
                method = getattr(self.storage, method_name)
                for _ in range(self.RANDOM_ATTEMPTS):
                    with t.inner():
                        result = method(*args, **kwargs)

                    if result is None:
                        return None

                    if not self._masked_result(method_name, result):
                        return result

        return newf

    def _getter_optional(self, method_name: str):
        """Handles methods returning an optional object: if the return value is
        :const:`None`, return it, else, raise a :exc:`MaskedObjectException` if
        the return value should be masked."""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            with masking_overhead_timer(method_name) as t:
                method = getattr(self.storage, method_name)
                with t.inner():
                    result = method(*args, **kwargs)
                if result is None:
                    return None

                self._raise_if_masked_result(method_name, result)

                return result

        return newf

    def _raise_if_masked_result_in_list(
        self, method_name: str, results: Iterable[Any]
    ) -> None:
        """Raise a :exc:`MaskedObjectException` if any non-:const:`None` object
        in ``results`` is masked."""
        result_swhids = set()
        for result in results:
            if result is not None:
                result_swhids.update(self._get_swhids_in_result(method_name, result))

        if result_swhids:
            self._raise_if_masked_swhids(list(result_swhids))

    def _getter_list(
        self,
        method_name: str,
    ):
        """Handle methods returning a list (or a generator) of optional objects,
        raising :exc:`MaskedObjectException` for all the masked objects in the
        batch."""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs):
            with masking_overhead_timer(method_name) as t:
                method = getattr(self.storage, method_name)

                with t.inner():
                    results = list(method(*args, **kwargs))

                self._raise_if_masked_result_in_list(method_name, results)
                return results

        return newf

    def _getter_pagedresult(self, method_name: str) -> Callable[..., PagedResult]:
        """Handle methods returning a :cls:`PagedResult`, raising
        :exc:`MaskedObjectException` if some objects in the returned page are
        masked."""

        @functools.wraps(getattr(self.storage, method_name))
        def newf(*args, **kwargs) -> PagedResult:
            with masking_overhead_timer(method_name) as t:
                method = getattr(self.storage, method_name)
                with t.inner():
                    results = method(*args, **kwargs)

                self._raise_if_masked_result_in_list(method_name, results.results)

                return results

        return newf
