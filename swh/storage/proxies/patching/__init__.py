# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import itertools
from typing import Any, Dict, Iterable, Iterator, List, Optional, TypeVar, Union

import attr
import psycopg2.pool

from swh.core.utils import grouper
from swh.model.model import Person, Release, Revision, Sha1Git
from swh.storage import get_storage
from swh.storage.interface import PagedResult, StorageInterface

from .db import PatchingQuery

BATCH_SIZE = 1024
"""In ``revision_log``, number of revisions to group together to check for
display names."""


class PatchingProxyStorage:
    """Patching storage proxy

    This proxy rewrites objects present in the archive to replace some values of these
    objects.

    Currently, it only applies display names (to revision and release authors and
    committers).

    It uses a specific PostgreSQL database (which for now is colocated with the
    swh.storage PostgreSQL database), the access to which is implemented in the
    :mod:`.db` submodule.

    Sample configuration

    .. code-block: yaml

        storage:
          cls: patching
          patching_db: 'dbname=swh-storage'
          max_pool_conns: 10
          storage:
          - cls: remote
            url: http://storage.internal.staging.swh.network:5002/

    """

    def __init__(
        self,
        patching_db: str,
        storage: Union[Dict, StorageInterface],
        min_pool_conns: int = 1,
        max_pool_conns: int = 5,
    ):
        self.storage: StorageInterface = (
            get_storage(**storage) if isinstance(storage, dict) else storage
        )

        self.patching_pool = psycopg2.pool.ThreadedConnectionPool(
            min_pool_conns, max_pool_conns, patching_db
        )

    @contextmanager
    def patching_query(self) -> Iterator[PatchingQuery]:
        ret = None
        try:
            ret = PatchingQuery.from_pool(self.patching_pool)
            yield ret
        finally:
            if ret:
                ret.put_conn()

    def __getattr__(self, key):
        method = self._get_method(key)
        # Don't go through the lookup in the next calls to self.key
        setattr(self, key, method)
        return method

    def _get_method(self, key):
        if key == "journal_writer":
            # Useful for tests
            return self.storage.journal_writer
        elif key.startswith(
            (
                "content_",
                "directory_",
                "snapshot_",
                "origin_",
                "extid_",
                "raw_extrinsic_metadata_",
                "metadata_fetcher_",
                "metadata_authority_",
            )
        ):
            return getattr(self.storage, key)
        elif key in ("refresh_stat_counters", "stat_counters"):
            return getattr(self.storage, key)
        elif key in ("check_config",):
            return getattr(self.storage, key)
        elif key in ("clear_buffers", "flush"):
            return getattr(self.storage, key)
        elif key in (
            "revision_missing",
            "revision_shortlog",  # returns only revision ids
            "revision_get_partition",
            "revision_get_random",
            "release_missing",
            "release_get_partition",
            "release_get_random",
        ):
            return getattr(self.storage, key)
        else:
            raise NotImplementedError(key)

    TRevision = TypeVar("TRevision", Revision, Optional[Revision])

    def _apply_revision_display_names(
        self, revisions: List[TRevision]
    ) -> List[TRevision]:
        emails = set()
        for rev in revisions:
            if (
                rev is not None
                and rev.author is not None
                and rev.author.email is not None
            ):
                emails.add(rev.author.email)
            if (
                rev is not None
                and rev.committer is not None
                and rev.committer.email is not None
            ):
                emails.add(rev.committer.email)

        with self.patching_query() as q:
            display_names = q.display_name(list(emails))

        # Short path for the common case
        if not display_names:
            return revisions

        persons: Dict[Optional[bytes], Person] = {
            email: Person.from_fullname(display_name)
            for (email, display_name) in display_names.items()
        }

        return [
            None
            if revision is None
            else attr.evolve(
                revision,
                author=revision.author
                if revision.author is None
                else persons.get(revision.author.email, revision.author),
                committer=revision.committer
                if revision.committer is None
                else persons.get(revision.committer.email, revision.committer),
            )
            for revision in revisions
        ]

    TRelease = TypeVar("TRelease", Release, Optional[Release])

    def _apply_release_display_names(self, releases: List[TRelease]) -> List[TRelease]:
        emails = set()
        for rel in releases:
            if (
                rel is not None
                and rel.author is not None
                and rel.author.email is not None
            ):
                emails.add(rel.author.email)

        with self.patching_query() as q:
            display_names = q.display_name(list(emails))

        # Short path for the common case
        if not display_names:
            return releases

        persons: Dict[Optional[bytes], Person] = {
            email: Person.from_fullname(display_name)
            for (email, display_name) in display_names.items()
        }

        return [
            None
            if release is None
            else attr.evolve(
                release,
                author=release.author
                if release.author is None
                else persons.get(release.author.email, release.author),
            )
            for release in releases
        ]

    def revision_get(
        self, revision_ids: List[Sha1Git], ignore_displayname: bool = False
    ) -> List[Optional[Revision]]:
        return self._apply_revision_display_names(
            self.storage.revision_get(revision_ids)
        )

    def revision_log(
        self,
        revisions: List[Sha1Git],
        ignore_displayname: bool = False,
        limit: Optional[int] = None,
    ) -> Iterable[Optional[Dict[str, Any]]]:
        revision_batches = grouper(
            self.storage.revision_log(revisions, limit=limit), BATCH_SIZE
        )
        yield from itertools.chain.from_iterable(
            self._apply_revision_display_names(list(revision_batch))
            for revision_batch in revision_batches
        )

    def revision_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Revision]:
        page = self.storage.revision_get_partition(
            partition_id, nb_partitions, page_token, limit
        )
        return PagedResult(
            results=self._apply_revision_display_names(page.results),
            next_page_token=page.next_page_token,
        )

    def release_get(
        self, releases: List[Sha1Git], ignore_displayname: bool = False
    ) -> List[Optional[Release]]:
        return self._apply_release_display_names(self.storage.release_get(releases))

    def release_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Release]:
        page = self.storage.release_get_partition(
            partition_id, nb_partitions, page_token, limit
        )
        return PagedResult(
            results=self._apply_revision_display_names(page.results),
            next_page_token=page.next_page_token,
        )
