# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import bisect
import collections
import datetime
import functools
import itertools
import random

from collections import defaultdict
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
    OriginVisitRow,
    OriginVisitStatusRow,
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
)
from swh.storage.objstorage import ObjStorage

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
        self._origin_visits = Table(OriginVisitRow)
        self._origin_visit_statuses = Table(OriginVisitStatusRow)
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

    def origin_generate_unique_visit_id(self, origin_url: str) -> int:
        origin = list(self.origin_get_by_url(origin_url))[0]
        visit_id = origin.next_visit_id
        origin.next_visit_id += 1
        return visit_id

    ##########################
    # 'origin_visit' table
    ##########################

    def origin_visit_get(
        self,
        origin_url: str,
        last_visit: Optional[int],
        limit: Optional[int],
        order: ListOrder,
    ) -> Iterable[OriginVisitRow]:
        visits = list(self._origin_visits.get_from_partition_key((origin_url,)))

        if last_visit is not None:
            if order == ListOrder.ASC:
                visits = [v for v in visits if v.visit > last_visit]
            else:
                visits = [v for v in visits if v.visit < last_visit]

        visits.sort(key=lambda v: v.visit, reverse=order == ListOrder.DESC)

        if limit is not None:
            visits = visits[0:limit]

        return visits

    def origin_visit_add_one(self, visit: OriginVisitRow) -> None:
        self._origin_visits.insert(visit)
        self.increment_counter("origin_visit", 1)

    def origin_visit_get_one(
        self, origin_url: str, visit_id: int
    ) -> Optional[OriginVisitRow]:
        return self._origin_visits.get_from_primary_key((origin_url, visit_id))

    def origin_visit_get_all(self, origin_url: str) -> Iterable[OriginVisitRow]:
        return self._origin_visits.get_from_partition_key((origin_url,))

    def origin_visit_iter(self, start_token: int) -> Iterator[OriginVisitRow]:
        """Returns all origin visits in order from this token,
        and wraps around the token space."""
        return (
            row
            for (token, partition) in self._origin_visits.data.items()
            for (clustering_key, row) in partition.items()
        )

    ##########################
    # 'origin_visit_status' table
    ##########################

    def origin_visit_status_get_range(
        self,
        origin: str,
        visit: int,
        date_from: Optional[datetime.datetime],
        limit: int,
        order: ListOrder,
    ) -> Iterable[OriginVisitStatusRow]:
        statuses = list(self.origin_visit_status_get(origin, visit))

        if date_from is not None:
            if order == ListOrder.ASC:
                statuses = [s for s in statuses if s.date >= date_from]
            else:
                statuses = [s for s in statuses if s.date <= date_from]

        statuses.sort(key=lambda s: s.date, reverse=order == ListOrder.DESC)

        return statuses[0:limit]

    def origin_visit_status_add_one(self, visit_update: OriginVisitStatusRow) -> None:
        self._origin_visit_statuses.insert(visit_update)
        self.increment_counter("origin_visit_status", 1)

    def origin_visit_status_get_latest(
        self, origin: str, visit: int,
    ) -> Optional[OriginVisitStatusRow]:
        """Given an origin visit id, return its latest origin_visit_status

         """
        return next(self.origin_visit_status_get(origin, visit), None)

    def origin_visit_status_get(
        self, origin: str, visit: int,
    ) -> Iterator[OriginVisitStatusRow]:
        """Return all origin visit statuses for a given visit

        """
        statuses = [
            s
            for s in self._origin_visit_statuses.get_from_partition_key((origin,))
            if s.visit == visit
        ]
        statuses.sort(key=lambda s: s.date, reverse=True)
        return iter(statuses)


class InMemoryStorage(CassandraStorage):
    _cql_runner: InMemoryCqlRunner  # type: ignore

    def __init__(self, journal_writer=None):
        self.reset()
        self.journal_writer = JournalWriter(journal_writer)

    def reset(self):
        self._cql_runner = InMemoryCqlRunner()
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
