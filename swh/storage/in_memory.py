# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import bisect
import collections
import copy
import datetime
import functools
import itertools
import random

from collections import defaultdict
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import attr

from swh.core.api.serializers import msgpack_loads, msgpack_dumps
from swh.model.identifiers import SWHID
from swh.model.model import (
    BaseContent,
    Content,
    SkippedContent,
    OriginVisit,
    OriginVisitStatus,
    Origin,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    RawExtrinsicMetadata,
    Sha1Git,
)
from swh.model.hashutil import DEFAULT_ALGORITHMS

from swh.storage.cassandra import CassandraStorage
from swh.storage.cassandra.model import (
    BaseRow,
    ContentRow,
    DirectoryRow,
    DirectoryEntryRow,
    ObjectCountRow,
    OriginRow,
    ReleaseRow,
    RevisionRow,
    RevisionParentRow,
    SkippedContentRow,
    SnapshotRow,
    SnapshotBranchRow,
)
from swh.storage.interface import (
    ListOrder,
    PagedResult,
    VISIT_STATUSES,
)
from swh.storage.objstorage import ObjStorage
from swh.storage.utils import now

from .converters import origin_url_to_sha1
from .exc import StorageArgumentException
from .writer import JournalWriter

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


SortedListItem = TypeVar("SortedListItem")
SortedListKey = TypeVar("SortedListKey")

FetcherKey = Tuple[str, str]


class SortedList(collections.UserList, Generic[SortedListKey, SortedListItem]):
    data: List[Tuple[SortedListKey, SortedListItem]]

    # https://github.com/python/mypy/issues/708
    # key: Callable[[SortedListItem], SortedListKey]

    def __init__(
        self,
        data: List[SortedListItem] = None,
        key: Optional[Callable[[SortedListItem], SortedListKey]] = None,
    ):
        if key is None:

            def key(item):
                return item

        assert key is not None  # for mypy
        super().__init__(sorted((key(x), x) for x in data or []))

        self.key: Callable[[SortedListItem], SortedListKey] = key

    def add(self, item: SortedListItem):
        k = self.key(item)
        bisect.insort(self.data, (k, item))

    def __iter__(self) -> Iterator[SortedListItem]:
        for (k, item) in self.data:
            yield item

    def iter_from(self, start_key: Any) -> Iterator[SortedListItem]:
        """Returns an iterator over all the elements whose key is greater
        or equal to `start_key`.
        (This is an efficient equivalent to:
        `(x for x in L if key(x) >= start_key)`)
        """
        from_index = bisect.bisect_left(self.data, (start_key,))
        for (k, item) in itertools.islice(self.data, from_index, None):
            yield item

    def iter_after(self, start_key: Any) -> Iterator[SortedListItem]:
        """Same as iter_from, but using a strict inequality."""
        it = self.iter_from(start_key)
        for item in it:
            if self.key(item) > start_key:  # type: ignore
                yield item
                break

        yield from it


TRow = TypeVar("TRow", bound=BaseRow)


class Table(Generic[TRow]):
    def __init__(self, row_class: Type[TRow]):
        self.row_class = row_class
        self.primary_key_cols = row_class.PARTITION_KEY + row_class.CLUSTERING_KEY

        # Map from tokens to clustering keys to rows
        # These are not actually partitions (or rather, there is one partition
        # for each token) and they aren't sorted.
        # But it is good enough if we don't care about performance;
        # and makes the code a lot simpler.
        self.data: Dict[int, Dict[Tuple, TRow]] = defaultdict(dict)

    def __repr__(self):
        return f"<__module__.Table[{self.row_class.__name__}] object>"

    def partition_key(self, row: Union[TRow, Dict[str, Any]]) -> Tuple:
        """Returns the partition key of a row (ie. the cells which get hashed
        into the token."""
        if isinstance(row, dict):
            row_d = row
        else:
            row_d = row.to_dict()
        return tuple(row_d[col] for col in self.row_class.PARTITION_KEY)

    def clustering_key(self, row: Union[TRow, Dict[str, Any]]) -> Tuple:
        """Returns the clustering key of a row (ie. the cells which are used
        for sorting rows within a partition."""
        if isinstance(row, dict):
            row_d = row
        else:
            row_d = row.to_dict()
        return tuple(row_d[col] for col in self.row_class.CLUSTERING_KEY)

    def primary_key_from_dict(self, d: Dict[str, Any]) -> Tuple:
        """Returns the primary key (ie. concatenation of partition key and
        clustering key) of the given dictionary interpreted as a row."""
        return tuple(d[col] for col in self.primary_key_cols)

    def token(self, key: Tuple):
        """Returns the token if a row (ie. the hash of its partition key)."""
        return hash(key)

    def get_partition(self, token: int) -> Dict[Tuple, TRow]:
        """Returns the partition that contains this token."""
        return self.data[token]

    def insert(self, row: TRow):
        partition = self.data[self.token(self.partition_key(row))]
        partition[self.clustering_key(row)] = row

    def split_primary_key(self, key: Tuple) -> Tuple[Tuple, Tuple]:
        """Returns (partition_key, clustering_key) from a partition key"""
        assert len(key) == len(self.primary_key_cols)

        partition_key = key[0 : len(self.row_class.PARTITION_KEY)]
        clustering_key = key[len(self.row_class.PARTITION_KEY) :]

        return (partition_key, clustering_key)

    def get_from_partition_key(self, partition_key: Tuple) -> Iterable[TRow]:
        """Returns at most one row, from its partition key."""
        token = self.token(partition_key)
        for row in self.get_from_token(token):
            if self.partition_key(row) == partition_key:
                yield row

    def get_from_primary_key(self, primary_key: Tuple) -> Optional[TRow]:
        """Returns at most one row, from its primary key."""
        (partition_key, clustering_key) = self.split_primary_key(primary_key)

        token = self.token(partition_key)
        partition = self.get_partition(token)

        return partition.get(clustering_key)

    def get_from_token(self, token: int) -> Iterable[TRow]:
        """Returns all rows whose token (ie. non-cryptographic hash of the
        partition key) is the one passed as argument."""
        return (v for (k, v) in sorted(self.get_partition(token).items()))

    def get_random(self) -> Optional[TRow]:
        return random.choice(
            [row for partition in self.data.values() for row in partition.values()]
        )


class InMemoryCqlRunner:
    def __init__(self):
        self._contents = Table(ContentRow)
        self._content_indexes = defaultdict(lambda: defaultdict(set))
        self._skipped_contents = Table(ContentRow)
        self._skipped_content_indexes = defaultdict(lambda: defaultdict(set))
        self._directories = Table(DirectoryRow)
        self._directory_entries = Table(DirectoryEntryRow)
        self._revisions = Table(RevisionRow)
        self._revision_parents = Table(RevisionParentRow)
        self._releases = Table(RevisionRow)
        self._snapshots = Table(SnapshotRow)
        self._snapshot_branches = Table(SnapshotBranchRow)
        self._origins = Table(OriginRow)
        self._stat_counters = defaultdict(int)

    def increment_counter(self, object_type: str, nb: int):
        self._stat_counters[object_type] += nb

    def stat_counters(self) -> Iterable[ObjectCountRow]:
        for (object_type, count) in self._stat_counters.items():
            yield ObjectCountRow(partition_key=0, object_type=object_type, count=count)

    ##########################
    # 'content' table
    ##########################

    def _content_add_finalize(self, content: ContentRow) -> None:
        self._contents.insert(content)
        self.increment_counter("content", 1)

    def content_add_prepare(self, content: ContentRow):
        finalizer = functools.partial(self._content_add_finalize, content)
        return (self._contents.token(self._contents.partition_key(content)), finalizer)

    def content_get_from_pk(
        self, content_hashes: Dict[str, bytes]
    ) -> Optional[ContentRow]:
        primary_key = self._contents.primary_key_from_dict(content_hashes)
        return self._contents.get_from_primary_key(primary_key)

    def content_get_from_token(self, token: int) -> Iterable[ContentRow]:
        return self._contents.get_from_token(token)

    def content_get_random(self) -> Optional[ContentRow]:
        return self._contents.get_random()

    def content_get_token_range(
        self, start: int, end: int, limit: int,
    ) -> Iterable[Tuple[int, ContentRow]]:
        matches = [
            (token, row)
            for (token, partition) in self._contents.data.items()
            for (clustering_key, row) in partition.items()
            if start <= token <= end
        ]
        matches.sort()
        return matches[0:limit]

    ##########################
    # 'content_by_*' tables
    ##########################

    def content_missing_by_sha1_git(self, ids: List[bytes]) -> List[bytes]:
        missing = []
        for id_ in ids:
            if id_ not in self._content_indexes["sha1_git"]:
                missing.append(id_)
        return missing

    def content_index_add_one(self, algo: str, content: Content, token: int) -> None:
        self._content_indexes[algo][content.get_hash(algo)].add(token)

    def content_get_tokens_from_single_hash(
        self, algo: str, hash_: bytes
    ) -> Iterable[int]:
        return self._content_indexes[algo][hash_]

    ##########################
    # 'skipped_content' table
    ##########################

    def _skipped_content_add_finalize(self, content: SkippedContentRow) -> None:
        self._skipped_contents.insert(content)
        self.increment_counter("skipped_content", 1)

    def skipped_content_add_prepare(self, content: SkippedContentRow):
        finalizer = functools.partial(self._skipped_content_add_finalize, content)
        return (
            self._skipped_contents.token(self._contents.partition_key(content)),
            finalizer,
        )

    def skipped_content_get_from_pk(
        self, content_hashes: Dict[str, bytes]
    ) -> Optional[SkippedContentRow]:
        primary_key = self._skipped_contents.primary_key_from_dict(content_hashes)
        return self._skipped_contents.get_from_primary_key(primary_key)

    ##########################
    # 'skipped_content_by_*' tables
    ##########################

    def skipped_content_index_add_one(
        self, algo: str, content: SkippedContent, token: int
    ) -> None:
        self._skipped_content_indexes[algo][content.get_hash(algo)].add(token)

    ##########################
    # 'directory' table
    ##########################

    def directory_missing(self, ids: List[bytes]) -> List[bytes]:
        missing = []
        for id_ in ids:
            if self._directories.get_from_primary_key((id_,)) is None:
                missing.append(id_)
        return missing

    def directory_add_one(self, directory: DirectoryRow) -> None:
        self._directories.insert(directory)
        self.increment_counter("directory", 1)

    def directory_get_random(self) -> Optional[DirectoryRow]:
        return self._directories.get_random()

    ##########################
    # 'directory_entry' table
    ##########################

    def directory_entry_add_one(self, entry: DirectoryEntryRow) -> None:
        self._directory_entries.insert(entry)

    def directory_entry_get(
        self, directory_ids: List[Sha1Git]
    ) -> Iterable[DirectoryEntryRow]:
        for id_ in directory_ids:
            yield from self._directory_entries.get_from_partition_key((id_,))

    ##########################
    # 'revision' table
    ##########################

    def revision_missing(self, ids: List[bytes]) -> Iterable[bytes]:
        missing = []
        for id_ in ids:
            if self._revisions.get_from_primary_key((id_,)) is None:
                missing.append(id_)
        return missing

    def revision_add_one(self, revision: RevisionRow) -> None:
        self._revisions.insert(revision)
        self.increment_counter("revision", 1)

    def revision_get_ids(self, revision_ids) -> Iterable[int]:
        for id_ in revision_ids:
            if self._revisions.get_from_primary_key((id_,)) is not None:
                yield id_

    def revision_get(self, revision_ids: List[Sha1Git]) -> Iterable[RevisionRow]:
        for id_ in revision_ids:
            row = self._revisions.get_from_primary_key((id_,))
            if row:
                yield row

    def revision_get_random(self) -> Optional[RevisionRow]:
        return self._revisions.get_random()

    ##########################
    # 'revision_parent' table
    ##########################

    def revision_parent_add_one(self, revision_parent: RevisionParentRow) -> None:
        self._revision_parents.insert(revision_parent)

    def revision_parent_get(self, revision_id: Sha1Git) -> Iterable[bytes]:
        for parent in self._revision_parents.get_from_partition_key((revision_id,)):
            yield parent.parent_id

    ##########################
    # 'release' table
    ##########################

    def release_missing(self, ids: List[bytes]) -> List[bytes]:
        missing = []
        for id_ in ids:
            if self._releases.get_from_primary_key((id_,)) is None:
                missing.append(id_)
        return missing

    def release_add_one(self, release: ReleaseRow) -> None:
        self._releases.insert(release)
        self.increment_counter("release", 1)

    def release_get(self, release_ids: List[str]) -> Iterable[ReleaseRow]:
        for id_ in release_ids:
            row = self._releases.get_from_primary_key((id_,))
            if row:
                yield row

    def release_get_random(self) -> Optional[ReleaseRow]:
        return self._releases.get_random()

    ##########################
    # 'snapshot' table
    ##########################

    def snapshot_missing(self, ids: List[bytes]) -> List[bytes]:
        missing = []
        for id_ in ids:
            if self._snapshots.get_from_primary_key((id_,)) is None:
                missing.append(id_)
        return missing

    def snapshot_add_one(self, snapshot: SnapshotRow) -> None:
        self._snapshots.insert(snapshot)
        self.increment_counter("snapshot", 1)

    def snapshot_get_random(self) -> Optional[SnapshotRow]:
        return self._snapshots.get_random()

    ##########################
    # 'snapshot_branch' table
    ##########################

    def snapshot_branch_add_one(self, branch: SnapshotBranchRow) -> None:
        self._snapshot_branches.insert(branch)

    def snapshot_count_branches(self, snapshot_id: Sha1Git) -> Dict[Optional[str], int]:
        """Returns a dictionary from type names to the number of branches
        of that type."""
        counts: Dict[Optional[str], int] = defaultdict(int)
        for branch in self._snapshot_branches.get_from_partition_key((snapshot_id,)):
            if branch.target_type is None:
                target_type = None
            else:
                target_type = branch.target_type
            counts[target_type] += 1
        return counts

    def snapshot_branch_get(
        self, snapshot_id: Sha1Git, from_: bytes, limit: int
    ) -> Iterable[SnapshotBranchRow]:
        count = 0
        for branch in self._snapshot_branches.get_from_partition_key((snapshot_id,)):
            if branch.name >= from_:
                count += 1
                yield branch
            if count >= limit:
                break

    ##########################
    # 'origin' table
    ##########################

    def origin_add_one(self, origin: OriginRow) -> None:
        self._origins.insert(origin)
        self.increment_counter("origin", 1)

    def origin_get_by_sha1(self, sha1: bytes) -> Iterable[OriginRow]:
        return self._origins.get_from_partition_key((sha1,))

    def origin_get_by_url(self, url: str) -> Iterable[OriginRow]:
        return self.origin_get_by_sha1(origin_url_to_sha1(url))

    def origin_list(
        self, start_token: int, limit: int
    ) -> Iterable[Tuple[int, OriginRow]]:
        """Returns an iterable of (token, origin)"""
        matches = [
            (token, row)
            for (token, partition) in self._origins.data.items()
            for (clustering_key, row) in partition.items()
            if token >= start_token
        ]
        matches.sort()
        return matches[0:limit]

    def origin_iter_all(self) -> Iterable[OriginRow]:
        return (
            row
            for (token, partition) in self._origins.data.items()
            for (clustering_key, row) in partition.items()
        )


class InMemoryStorage(CassandraStorage):
    _cql_runner: InMemoryCqlRunner  # type: ignore

    def __init__(self, journal_writer=None):
        self.reset()
        self.journal_writer = JournalWriter(journal_writer)

    def reset(self):
        self._cql_runner = InMemoryCqlRunner()
        self._origin_visits = {}
        self._origin_visit_statuses: Dict[Tuple[str, int], List[OriginVisitStatus]] = {}
        self._persons = {}

        # {object_type: {id: {authority: [metadata]}}}
        self._raw_extrinsic_metadata: Dict[
            MetadataTargetType,
            Dict[
                Union[str, SWHID],
                Dict[
                    Hashable,
                    SortedList[
                        Tuple[datetime.datetime, FetcherKey], RawExtrinsicMetadata
                    ],
                ],
            ],
        ] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(
                    lambda: SortedList(
                        key=lambda x: (
                            x.discovery_date,
                            self._metadata_fetcher_key(x.fetcher),
                        )
                    )
                )
            )
        )  # noqa

        self._metadata_fetchers: Dict[FetcherKey, MetadataFetcher] = {}
        self._metadata_authorities: Dict[Hashable, MetadataAuthority] = {}
        self._objects = defaultdict(list)
        self._sorted_sha1s = SortedList[bytes, bytes]()

        self.objstorage = ObjStorage({"cls": "memory", "args": {}})

    def check_config(self, *, check_write: bool) -> bool:
        return True

    def origin_add(self, origins: List[Origin]) -> Dict[str, int]:
        for origin in origins:
            if origin.url not in self._origin_visits:
                self._origin_visits[origin.url] = []
        return super().origin_add(origins)

    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get_one(visit.origin)
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        for visit in visits:
            origin_url = visit.origin
            if list(self._cql_runner.origin_get_by_url(origin_url)):
                if visit.visit:
                    self.journal_writer.origin_visit_add([visit])
                    while len(self._origin_visits[origin_url]) < visit.visit:
                        self._origin_visits[origin_url].append(None)
                    self._origin_visits[origin_url][visit.visit - 1] = visit
                else:
                    # visit ids are in the range [1, +inf[
                    visit_id = len(self._origin_visits[origin_url]) + 1
                    visit = attr.evolve(visit, visit=visit_id)
                    self.journal_writer.origin_visit_add([visit])
                    self._origin_visits[origin_url].append(visit)
                    visit_key = (origin_url, visit.visit)
                    self._objects[visit_key].append(("origin_visit", None))
                assert visit.visit is not None
                self._origin_visit_status_add_one(
                    OriginVisitStatus(
                        origin=visit.origin,
                        visit=visit.visit,
                        date=visit.date,
                        status="created",
                        snapshot=None,
                    )
                )
                all_visits.append(visit)

        self._cql_runner.increment_counter("origin_visit", len(all_visits))

        return all_visits

    def _origin_visit_status_add_one(self, visit_status: OriginVisitStatus) -> None:
        """Add an origin visit status without checks. If already present, do nothing.

        """
        self.journal_writer.origin_visit_status_add([visit_status])
        visit_key = (visit_status.origin, visit_status.visit)
        self._origin_visit_statuses.setdefault(visit_key, [])
        visit_statuses = self._origin_visit_statuses[visit_key]
        if visit_status not in visit_statuses:
            visit_statuses.append(visit_status)

    def origin_visit_status_add(self, visit_statuses: List[OriginVisitStatus],) -> None:
        # First round to check existence (fail early if any is ko)
        for visit_status in visit_statuses:
            origin_url = self.origin_get_one(visit_status.origin)
            if not origin_url:
                raise StorageArgumentException(f"Unknown origin {visit_status.origin}")

        for visit_status in visit_statuses:
            self._origin_visit_status_add_one(visit_status)

    def _origin_visit_status_get_latest(
        self, origin: str, visit_id: int
    ) -> Tuple[OriginVisit, OriginVisitStatus]:
        """Return a tuple of OriginVisit, latest associated OriginVisitStatus.

        """
        assert visit_id >= 1
        visit = self._origin_visits[origin][visit_id - 1]
        assert visit is not None
        visit_key = (origin, visit_id)

        visit_update = max(self._origin_visit_statuses[visit_key], key=lambda v: v.date)
        return visit, visit_update

    def _origin_visit_get_updated(self, origin: str, visit_id: int) -> Dict[str, Any]:
        """Merge origin visit and latest origin visit status

        """
        visit, visit_update = self._origin_visit_status_get_latest(origin, visit_id)
        assert visit is not None and visit_update is not None
        return {
            # default to the values in visit
            **visit.to_dict(),
            # override with the last update
            **visit_update.to_dict(),
            # but keep the date of the creation of the origin visit
            "date": visit.date,
        }

    def origin_visit_get(
        self,
        origin: str,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisit]:
        next_page_token = None
        page_token = page_token or "0"
        if not isinstance(order, ListOrder):
            raise StorageArgumentException("order must be a ListOrder value")
        if not isinstance(page_token, str):
            raise StorageArgumentException("page_token must be a string.")

        visit_from = int(page_token)
        origin_url = self._get_origin_url(origin)
        extra_limit = limit + 1
        visits = sorted(
            self._origin_visits.get(origin_url, []),
            key=lambda v: v.visit,
            reverse=(order == ListOrder.DESC),
        )

        if visit_from > 0 and order == ListOrder.ASC:
            visits = [v for v in visits if v.visit > visit_from]
        elif visit_from > 0 and order == ListOrder.DESC:
            visits = [v for v in visits if v.visit < visit_from]
        visits = visits[:extra_limit]

        assert len(visits) <= extra_limit
        if len(visits) == extra_limit:
            visits = visits[:limit]
            next_page_token = str(visits[-1].visit)

        return PagedResult(results=visits, next_page_token=next_page_token)

    def origin_visit_find_by_date(
        self, origin: str, visit_date: datetime.datetime
    ) -> Optional[OriginVisit]:
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits:
            visits = self._origin_visits[origin_url]
            visit = min(visits, key=lambda v: (abs(v.date - visit_date), -v.visit))
            return visit
        return None

    def origin_visit_get_by(self, origin: str, visit: int) -> Optional[OriginVisit]:
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits and visit <= len(
            self._origin_visits[origin_url]
        ):
            found_visit, _ = self._origin_visit_status_get_latest(origin, visit)
            return found_visit
        return None

    def origin_visit_get_latest(
        self,
        origin: str,
        type: Optional[str] = None,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
    ) -> Optional[OriginVisit]:
        if allowed_statuses and not set(allowed_statuses).intersection(VISIT_STATUSES):
            raise StorageArgumentException(
                f"Unknown allowed statuses {','.join(allowed_statuses)}, only "
                f"{','.join(VISIT_STATUSES)} authorized"
            )

        if not list(self._cql_runner.origin_get_by_url(origin)):
            return None

        visits = sorted(
            self._origin_visits[origin], key=lambda v: (v.date, v.visit), reverse=True,
        )
        for visit in visits:
            if type is not None and visit.type != type:
                continue
            visit_statuses = self._origin_visit_statuses[origin, visit.visit]

            if allowed_statuses is not None:
                visit_statuses = [
                    vs for vs in visit_statuses if vs.status in allowed_statuses
                ]
            if require_snapshot:
                visit_statuses = [vs for vs in visit_statuses if vs.snapshot]

            if visit_statuses:  # we found visit statuses matching criteria
                visit_status = max(visit_statuses, key=lambda vs: (vs.date, vs.visit))
                assert visit.origin == visit_status.origin
                assert visit.visit == visit_status.visit
                return visit

        return None

    def origin_visit_status_get(
        self,
        origin: str,
        visit: int,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisitStatus]:
        next_page_token = None
        date_from = None
        if page_token is not None:
            date_from = datetime.datetime.fromisoformat(page_token)

        visit_statuses = sorted(
            self._origin_visit_statuses.get((origin, visit), []),
            key=lambda v: v.date,
            reverse=(order == ListOrder.DESC),
        )

        if date_from is not None:
            if order == ListOrder.ASC:
                visit_statuses = [v for v in visit_statuses if v.date >= date_from]
            elif order == ListOrder.DESC:
                visit_statuses = [v for v in visit_statuses if v.date <= date_from]

        # Take one more visit status so we can reuse it as the next page token if any
        visit_statuses = visit_statuses[: limit + 1]

        if len(visit_statuses) > limit:
            # last visit status date is the next page token
            next_page_token = str(visit_statuses[-1].date)
            # excluding that visit status from the result to respect the limit size
            visit_statuses = visit_statuses[:limit]

        return PagedResult(results=visit_statuses, next_page_token=next_page_token)

    def origin_visit_status_get_latest(
        self,
        origin_url: str,
        visit: int,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
    ) -> Optional[OriginVisitStatus]:
        if allowed_statuses and not set(allowed_statuses).intersection(VISIT_STATUSES):
            raise StorageArgumentException(
                f"Unknown allowed statuses {','.join(allowed_statuses)}, only "
                f"{','.join(VISIT_STATUSES)} authorized"
            )

        if not list(self._cql_runner.origin_get_by_url(origin_url)):
            return None

        visit_key = (origin_url, visit)
        visits = self._origin_visit_statuses.get(visit_key)
        if not visits:
            return None

        if allowed_statuses is not None:
            visits = [visit for visit in visits if visit.status in allowed_statuses]
        if require_snapshot:
            visits = [visit for visit in visits if visit.snapshot]

        visit_status = max(visits, key=lambda v: (v.date, v.visit), default=None)
        return visit_status

    def _select_random_origin_visit_by_type(self, type: str) -> str:
        while True:
            url = random.choice(list(self._origin_visits.keys()))
            random_origin_visits = self._origin_visits[url]
            if random_origin_visits[0].type == type:
                return url

    def origin_visit_status_get_random(
        self, type: str
    ) -> Optional[Tuple[OriginVisit, OriginVisitStatus]]:

        url = self._select_random_origin_visit_by_type(type)
        random_origin_visits = copy.deepcopy(self._origin_visits[url])
        random_origin_visits.reverse()
        back_in_the_day = now() - timedelta(weeks=12)  # 3 months back
        # This should be enough for tests
        for visit in random_origin_visits:
            origin_visit, latest_visit_status = self._origin_visit_status_get_latest(
                url, visit.visit
            )
            assert latest_visit_status is not None
            if (
                origin_visit.date > back_in_the_day
                and latest_visit_status.status == "full"
            ):
                return origin_visit, latest_visit_status
        else:
            return None

    def raw_extrinsic_metadata_add(self, metadata: List[RawExtrinsicMetadata],) -> None:
        self.journal_writer.raw_extrinsic_metadata_add(metadata)
        for metadata_entry in metadata:
            authority_key = self._metadata_authority_key(metadata_entry.authority)
            if authority_key not in self._metadata_authorities:
                raise StorageArgumentException(
                    f"Unknown authority {metadata_entry.authority}"
                )
            fetcher_key = self._metadata_fetcher_key(metadata_entry.fetcher)
            if fetcher_key not in self._metadata_fetchers:
                raise StorageArgumentException(
                    f"Unknown fetcher {metadata_entry.fetcher}"
                )

            raw_extrinsic_metadata_list = self._raw_extrinsic_metadata[
                metadata_entry.type
            ][metadata_entry.id][authority_key]

            for existing_raw_extrinsic_metadata in raw_extrinsic_metadata_list:
                if (
                    self._metadata_fetcher_key(existing_raw_extrinsic_metadata.fetcher)
                    == fetcher_key
                    and existing_raw_extrinsic_metadata.discovery_date
                    == metadata_entry.discovery_date
                ):
                    # Duplicate of an existing one; ignore it.
                    break
            else:
                raw_extrinsic_metadata_list.add(metadata_entry)

    def raw_extrinsic_metadata_get(
        self,
        type: MetadataTargetType,
        id: Union[str, SWHID],
        authority: MetadataAuthority,
        after: Optional[datetime.datetime] = None,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
    ) -> PagedResult[RawExtrinsicMetadata]:
        authority_key = self._metadata_authority_key(authority)

        if type == MetadataTargetType.ORIGIN:
            if isinstance(id, SWHID):
                raise StorageArgumentException(
                    f"raw_extrinsic_metadata_get called with type='origin', "
                    f"but provided id is an SWHID: {id!r}"
                )
        else:
            if not isinstance(id, SWHID):
                raise StorageArgumentException(
                    f"raw_extrinsic_metadata_get called with type!='origin', "
                    f"but provided id is not an SWHID: {id!r}"
                )

        if page_token is not None:
            (after_time, after_fetcher) = msgpack_loads(base64.b64decode(page_token))
            after_fetcher = tuple(after_fetcher)
            if after is not None and after > after_time:
                raise StorageArgumentException(
                    "page_token is inconsistent with the value of 'after'."
                )
            entries = self._raw_extrinsic_metadata[type][id][authority_key].iter_after(
                (after_time, after_fetcher)
            )
        elif after is not None:
            entries = self._raw_extrinsic_metadata[type][id][authority_key].iter_from(
                (after,)
            )
            entries = (entry for entry in entries if entry.discovery_date > after)
        else:
            entries = iter(self._raw_extrinsic_metadata[type][id][authority_key])

        if limit:
            entries = itertools.islice(entries, 0, limit + 1)

        results = []
        for entry in entries:
            entry_authority = self._metadata_authorities[
                self._metadata_authority_key(entry.authority)
            ]
            entry_fetcher = self._metadata_fetchers[
                self._metadata_fetcher_key(entry.fetcher)
            ]
            if after:
                assert entry.discovery_date > after
            results.append(
                attr.evolve(
                    entry,
                    authority=attr.evolve(entry_authority, metadata=None),
                    fetcher=attr.evolve(entry_fetcher, metadata=None),
                )
            )

        if len(results) > limit:
            results.pop()
            assert len(results) == limit
            last_result = results[-1]
            next_page_token: Optional[str] = base64.b64encode(
                msgpack_dumps(
                    (
                        last_result.discovery_date,
                        self._metadata_fetcher_key(last_result.fetcher),
                    )
                )
            ).decode()
        else:
            next_page_token = None

        return PagedResult(next_page_token=next_page_token, results=results,)

    def metadata_fetcher_add(self, fetchers: List[MetadataFetcher]) -> None:
        self.journal_writer.metadata_fetcher_add(fetchers)
        for fetcher in fetchers:
            if fetcher.metadata is None:
                raise StorageArgumentException(
                    "MetadataFetcher.metadata may not be None in metadata_fetcher_add."
                )
            key = self._metadata_fetcher_key(fetcher)
            if key not in self._metadata_fetchers:
                self._metadata_fetchers[key] = fetcher

    def metadata_fetcher_get(
        self, name: str, version: str
    ) -> Optional[MetadataFetcher]:
        return self._metadata_fetchers.get(
            self._metadata_fetcher_key(MetadataFetcher(name=name, version=version))
        )

    def metadata_authority_add(self, authorities: List[MetadataAuthority]) -> None:
        self.journal_writer.metadata_authority_add(authorities)
        for authority in authorities:
            if authority.metadata is None:
                raise StorageArgumentException(
                    "MetadataAuthority.metadata may not be None in "
                    "metadata_authority_add."
                )
            key = self._metadata_authority_key(authority)
            self._metadata_authorities[key] = authority

    def metadata_authority_get(
        self, type: MetadataAuthorityType, url: str
    ) -> Optional[MetadataAuthority]:
        return self._metadata_authorities.get(
            self._metadata_authority_key(MetadataAuthority(type=type, url=url))
        )

    def _get_origin_url(self, origin):
        if isinstance(origin, str):
            return origin
        else:
            raise TypeError("origin must be a string.")

    def _person_add(self, person):
        key = ("person", person.fullname)
        if key not in self._objects:
            self._persons[person.fullname] = person
            self._objects[key].append(key)

        return self._persons[person.fullname]

    @staticmethod
    def _content_key(content):
        """ A stable key and the algorithm for a content"""
        if isinstance(content, BaseContent):
            content = content.to_dict()
        return tuple((key, content.get(key)) for key in sorted(DEFAULT_ALGORITHMS))

    @staticmethod
    def _metadata_fetcher_key(fetcher: MetadataFetcher) -> FetcherKey:
        return (fetcher.name, fetcher.version)

    @staticmethod
    def _metadata_authority_key(authority: MetadataAuthority) -> Hashable:
        return (authority.type, authority.url)

    def diff_directories(self, from_dir, to_dir, track_renaming=False):
        raise NotImplementedError("InMemoryStorage.diff_directories")

    def diff_revisions(self, from_rev, to_rev, track_renaming=False):
        raise NotImplementedError("InMemoryStorage.diff_revisions")

    def diff_revision(self, revision, track_renaming=False):
        raise NotImplementedError("InMemoryStorage.diff_revision")

    def clear_buffers(self, object_types: Optional[List[str]] = None) -> None:
        """Do nothing

        """
        return None

    def flush(self, object_types: Optional[List[str]] = None) -> Dict:
        return {}
