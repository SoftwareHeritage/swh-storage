# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
from collections import Counter, defaultdict
import datetime
import itertools
import logging
import operator
import random
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import attr

from swh.core.api.classes import stream_results
from swh.core.api.serializers import msgpack_dumps, msgpack_loads
from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_hex
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ExtID,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Release,
    Revision,
    Sha1Git,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.model.swhids import ObjectType as SwhidObjectType
from swh.storage.interface import (
    VISIT_STATUSES,
    HashDict,
    ListOrder,
    ObjectReference,
    OriginVisitWithStatuses,
    PagedResult,
    PartialBranches,
    Sha1,
    SnapshotBranchByNameResponse,
    TotalHashDict,
)
from swh.storage.objstorage import ObjStorage
from swh.storage.utils import now
from swh.storage.writer import JournalWriter

from . import converters
from ..exc import (
    HashCollision,
    StorageArgumentException,
    UnknownMetadataAuthority,
    UnknownMetadataFetcher,
)
from ..utils import remove_keys
from .common import TOKEN_BEGIN, TOKEN_END, hash_url
from .cql import CqlRunner
from .model import (
    BaseRow,
    ContentRow,
    DirectoryEntryRow,
    DirectoryRow,
    ExtIDByTargetRow,
    ExtIDRow,
    MetadataAuthorityRow,
    MetadataFetcherRow,
    OriginRow,
    OriginVisitRow,
    OriginVisitStatusRow,
    RawExtrinsicMetadataByIdRow,
    RawExtrinsicMetadataRow,
    RevisionParentRow,
    RevisionRow,
    SkippedContentRow,
    SnapshotBranchRow,
    SnapshotRow,
)
from .schema import HASH_ALGORITHMS

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000

DIRECTORY_ENTRIES_INSERT_ALGOS = ["one-by-one", "concurrent", "batch"]


TResult = TypeVar("TResult")
TRow = TypeVar("TRow", bound=BaseRow)


logger = logging.getLogger(__name__)


def _get_paginated_sha1_partition(
    partition_id: int,
    nb_partitions: int,
    page_token: Optional[str],
    limit: int,
    get_range: Callable[[int, int, int], Iterator[Tuple[int, TRow]]],
    convert: Callable[[TRow], TResult],
    skip_row: Callable[[TRow], bool] = lambda row: False,
) -> PagedResult[TResult]:
    """Implements the bulk of ``content_get_partition``, ``directory_get_partition``,
    ...:

    1. computes range bounds
    2. applies pagination token
    3. converts to final objects
    4. extracts next pagination token
    """
    if limit is None:
        raise StorageArgumentException("limit should not be None")

    # Compute start and end of the range of tokens covered by the
    # requested partition
    partition_size = (TOKEN_END - TOKEN_BEGIN) // nb_partitions
    range_start = TOKEN_BEGIN + partition_id * partition_size
    range_end = TOKEN_BEGIN + (partition_id + 1) * partition_size

    # offset the range start according to the `page_token`.
    if page_token is not None:
        if not (range_start <= int(page_token) <= range_end):
            raise StorageArgumentException("Invalid page_token.")
        range_start = int(page_token)

    next_page_token: Optional[str] = None

    rows = get_range(range_start, range_end, limit + 1)
    results = []
    for counter, (tok, row) in enumerate(rows):
        if skip_row(row):
            continue
        if counter >= limit:
            next_page_token = str(tok)
            break
        results.append(convert(row))

    assert len(results) <= limit
    return PagedResult(results=results, next_page_token=next_page_token)


class CassandraStorage:
    def __init__(
        self,
        hosts,
        keyspace,
        objstorage=None,
        port=9042,
        journal_writer=None,
        allow_overwrite=False,
        consistency_level="ONE",
        directory_entries_insert_algo="one-by-one",
        auth_provider: Optional[Dict] = None,
    ):
        """
        A backend of swh-storage backed by Cassandra

        Args:
            hosts: Seed Cassandra nodes, to start connecting to the cluster
            keyspace: Name of the Cassandra database to use
            objstorage: Passed as argument to :class:`ObjStorage`; if unset,
               use a NoopObjStorage
            port: Cassandra port
            journal_writer: Passed as argument to :class:`JournalWriter`
            allow_overwrite: Whether ``*_add`` functions will check if an object
                already exists in the database before sending it in an INSERT.
                ``False`` is the default as it is more efficient when there is
                a moderately high probability the object is already known,
                but ``True`` can be useful to overwrite existing objects
                (eg. when applying a schema update),
                or when the database is known to be mostly empty.
                Note that a ``False`` value does not guarantee there won't be
                any overwrite.
            consistency_level: The default read/write consistency to use
            directory_entries_insert_algo: Must be one of:
                * one-by-one: naive, one INSERT per directory entry, serialized
                * concurrent: one INSERT per directory entry, concurrent
                * batch: using UNLOGGED BATCH to insert many entries in a few statements
            auth_provider: An optional dict describing the authentication provider to use.
                Must contain at least a ``cls`` entry and the parameters to pass to the
                constructor. For example::

                    auth_provider:
                        cls: cassandra.auth.PlainTextAuthProvider
                        username: myusername
                        password: mypassword
        """
        self._hosts = hosts
        self._keyspace = keyspace
        self._port = port
        self._consistency_level = consistency_level
        self._auth_provider = auth_provider
        self._set_cql_runner()
        self.journal_writer: JournalWriter = JournalWriter(journal_writer)
        self.objstorage: ObjStorage = ObjStorage(self, objstorage)
        self._allow_overwrite = allow_overwrite

        if directory_entries_insert_algo not in DIRECTORY_ENTRIES_INSERT_ALGOS:
            raise ValueError(
                f"directory_entries_insert_algo must be one of: "
                f"{', '.join(DIRECTORY_ENTRIES_INSERT_ALGOS)}"
            )
        self._directory_entries_insert_algo = directory_entries_insert_algo

    @property
    def hosts(self) -> List[str]:
        return self._hosts

    @property
    def keyspace(self) -> str:
        return self._keyspace

    @property
    def port(self) -> int:
        return self._port

    def _set_cql_runner(self) -> None:
        """Used by tests when they need to reset the CqlRunner"""
        self._cql_runner: CqlRunner = CqlRunner(
            self._hosts,
            self._keyspace,
            self._port,
            self._consistency_level,
            self._auth_provider,
        )

    def check_config(self, *, check_write: bool) -> bool:
        self._cql_runner.check_read()

        return True

    ##########################
    # Content
    ##########################

    def _content_get_from_hashes(self, algo, hashes: List[bytes]) -> Iterable:
        """From the name of a hash algorithm and a value of that hash,
        looks up the "hash -> token" secondary table (content_by_{algo})
        to get tokens.
        Then, looks up the main table (content) to get all contents with
        that token, and filters out contents whose hash doesn't match."""
        found_tokens = list(
            self._cql_runner.content_get_tokens_from_single_algo(algo, hashes)
        )
        assert all(isinstance(token, int) for token in found_tokens)

        # Query the main table ('content').
        rows = self._cql_runner.content_get_from_tokens(found_tokens)
        for row in rows:
            # re-check the the hash (in case of murmur3 collision)
            if getattr(row, algo) in hashes:
                yield row

    def _content_add(self, contents: List[Content], with_data: bool) -> Dict[str, int]:
        # Filter-out content already in the database.
        if not self._allow_overwrite:
            contents = [
                c
                for c in contents
                if not self._cql_runner.content_get_from_pk(c.to_dict())
            ]

        if with_data:
            # First insert to the objstorage, if the endpoint is
            # `content_add` (as opposed to `content_add_metadata`).

            # Must add to the objstorage before the DB and journal. Otherwise:
            # 1. in case of a crash the DB may "believe" we have the content, but
            #    we didn't have time to write to the objstorage before the crash
            # 2. the objstorage mirroring, which reads from the journal, may attempt to
            #    read from the objstorage before we finished writing it
            summary = self.objstorage.content_add(
                c for c in contents if c.status != "absent"
            )
            content_add_bytes = summary["content:add:bytes"]

        self.journal_writer.content_add(contents)

        content_add = 0
        for content in contents:
            content_add += 1

            # Check for sha1 or sha1_git collisions. This test is not atomic
            # with the insertion, so it won't detect a collision if both
            # contents are inserted at the same time, but it's good enough.
            #
            # The proper way to do it would probably be a BATCH, but this
            # would be inefficient because of the number of partitions we
            # need to affect (len(HASH_ALGORITHMS)+1, which is currently 5)
            if not self._allow_overwrite:
                for algo in {"sha1", "sha1_git"}:
                    collisions = []
                    # Get tokens of 'content' rows with the same value for
                    # sha1/sha1_git
                    # TODO: batch these requests, instead of sending them one by one
                    rows = self._content_get_from_hashes(algo, [content.get_hash(algo)])
                    for row in rows:
                        if getattr(row, algo) != content.get_hash(algo):
                            # collision of token(partition key), ignore this
                            # row
                            continue

                        for other_algo in HASH_ALGORITHMS:
                            if getattr(row, other_algo) != content.get_hash(other_algo):
                                # This hash didn't match; discard the row.
                                collisions.append(
                                    {k: getattr(row, k) for k in HASH_ALGORITHMS}
                                )

                    if collisions:
                        collisions.append(dict(content.hashes()))
                        raise HashCollision(algo, content.get_hash(algo), collisions)

            (token, insertion_finalizer) = self._cql_runner.content_add_prepare(
                ContentRow(**remove_keys(content.to_dict(), ("data",)))
            )

            # Then add to index tables
            for algo in HASH_ALGORITHMS:
                self._cql_runner.content_index_add_one(algo, content, token)

            # Then to the main table
            insertion_finalizer()

        summary = {
            "content:add": content_add,
        }

        if with_data:
            summary["content:add:bytes"] = content_add_bytes

        return summary

    def content_add(self, content: List[Content]) -> Dict[str, int]:
        to_add = {
            (c.sha1, c.sha1_git, c.sha256, c.blake2s256): c for c in content
        }.values()
        contents = [attr.evolve(c, ctime=now()) for c in to_add]
        return self._content_add(list(contents), with_data=True)

    def content_update(
        self, contents: List[Dict[str, Any]], keys: List[str] = []
    ) -> None:
        raise NotImplementedError(
            "content_update is not supported by the Cassandra backend"
        )

    def content_add_metadata(self, content: List[Content]) -> Dict[str, int]:
        return self._content_add(content, with_data=False)

    def content_get_data(self, content: Union[Sha1, HashDict]) -> Optional[bytes]:
        # FIXME: Make this method support slicing the `data`
        return self.objstorage.content_get(content)

    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Content]:
        def convert(row: ContentRow) -> Content:
            row_d = row.to_dict()
            row_d.pop("ctime")
            return Content(**row_d)

        def is_absent(row: ContentRow) -> bool:
            return row.status == "absent"

        return _get_paginated_sha1_partition(
            partition_id,
            nb_partitions,
            page_token,
            limit,
            self._cql_runner.content_get_token_range,
            convert,
            skip_row=is_absent,
        )

    def content_get(
        self, contents: List[bytes], algo: str = "sha1"
    ) -> List[Optional[Content]]:
        if algo not in DEFAULT_ALGORITHMS:
            raise StorageArgumentException(
                "algo should be one of {','.join(DEFAULT_ALGORITHMS)}"
            )

        key = operator.attrgetter(algo)
        contents_by_hash: Dict[Sha1, Optional[Content]] = {}
        for row in self._content_get_from_hashes(algo, contents):
            # Get all (sha1, sha1_git, sha256, blake2s256) whose sha1/sha1_git
            # matches the argument, from the index table ('content_by_*')
            row_d = row.to_dict()
            row_d.pop("ctime")
            content = Content(**row_d)
            contents_by_hash[key(content)] = content
        return [contents_by_hash.get(hash_) for hash_ in contents]

    def content_find(self, content: HashDict) -> List[Content]:
        return self._content_find_many([content])

    def _content_find_many(self, contents: List[HashDict]) -> List[Content]:
        # Find an algorithm that is common to all the requested contents.
        # It will be used to do an initial filtering efficiently.
        # TODO: prioritize sha256, we can do more efficient lookups from this hash.
        filter_algos = set(HASH_ALGORITHMS)
        for content in contents:
            filter_algos &= set(content)
        if not filter_algos:
            raise StorageArgumentException(
                "content keys must contain at least one "
                f"of: {', '.join(sorted(HASH_ALGORITHMS))}"
            )
        common_algo = list(filter_algos)[0]

        results = []
        rows = self._content_get_from_hashes(
            common_algo,
            [content[common_algo] for content in cast(List[dict], contents)],
        )
        for row in rows:
            # Re-check all the hashes, in case of collisions (either of the
            # hash of the partition key, or the hashes in it)
            for content in contents:
                for algo in HASH_ALGORITHMS:
                    hash_ = content.get(algo)
                    if hash_ and getattr(row, algo) != hash_:
                        # This hash didn't match; discard the row.
                        break
                else:
                    # All hashes match, keep this row.
                    row_d = row.to_dict()
                    row_d["ctime"] = row.ctime.replace(tzinfo=datetime.timezone.utc)
                    results.append(Content(**row_d))
                    break
            else:
                # No content matched; skip it
                pass
        return results

    def content_missing(
        self, contents: List[HashDict], key_hash: str = "sha1"
    ) -> Iterable[bytes]:
        if key_hash not in DEFAULT_ALGORITHMS:
            raise StorageArgumentException(
                "key_hash should be one of {','.join(DEFAULT_ALGORITHMS)}"
            )

        contents_with_all_hashes: List[TotalHashDict] = []
        contents_with_missing_hashes: List[HashDict] = []
        for content in contents:
            if DEFAULT_ALGORITHMS <= set(content):
                contents_with_all_hashes.append(content)
            else:
                contents_with_missing_hashes.append(content)

        # These contents can be queried efficiently directly in the main table
        for content in self._cql_runner.content_missing_from_all_hashes(
            contents_with_all_hashes
        ):
            yield content[key_hash]  # type: ignore

        if contents_with_missing_hashes:
            # For these, we need the expensive index lookups + main table.

            # Get all contents in the database that match (at least) one of the
            # requested contents, concurrently.
            found_contents = self._content_find_many(contents_with_missing_hashes)

            # Bucket the known contents by hash
            found_contents_by_hash: Dict[str, Dict[str, list]] = {
                algo: defaultdict(list) for algo in DEFAULT_ALGORITHMS
            }
            for found_content in found_contents:
                for algo in DEFAULT_ALGORITHMS:
                    found_contents_by_hash[algo][found_content.get_hash(algo)].append(
                        found_content
                    )

            # For each of the requested contents, check if they are in the
            # 'found_contents' set (via 'found_contents_by_hash' for efficient access,
            # since we need to check using dict inclusion instead of hash+equality)
            for missing_content in contents_with_missing_hashes:
                # Pick any of the algorithms provided in missing_content
                algo = next(algo for (algo, hash_) in missing_content.items() if hash_)

                # Get the list of found_contents that match this hash in the
                # missing_content. (its length is at most 1, unless there is a
                # collision)
                found_contents_with_same_hash = found_contents_by_hash[algo][
                    missing_content[algo]  # type: ignore
                ]

                # Check if there is a found_content that matches all hashes in the
                # missing_content.
                # This is functionally equivalent to 'for found_content in
                # found_contents', but runs almost in constant time (it is linear
                # in the number of hash collisions) instead of linear.
                # This allows this function to run in linear time overall instead of
                # quadratic.
                for found_content in found_contents_with_same_hash:
                    # check if the found_content.hashes() dictionary contains a superset
                    # of the (key, value) pairs in missing_content
                    if missing_content.items() <= found_content.hashes().items():
                        # Found!
                        break
                else:
                    # Not found
                    yield missing_content[key_hash]  # type: ignore

    def content_missing_per_sha1(self, contents: List[bytes]) -> Iterable[bytes]:
        return self.content_missing([{"sha1": c} for c in contents])

    def content_missing_per_sha1_git(
        self, contents: List[Sha1Git]
    ) -> Iterable[Sha1Git]:
        return self.content_missing(
            [{"sha1_git": c} for c in contents], key_hash="sha1_git"
        )

    def content_get_random(self) -> Sha1Git:
        content = self._cql_runner.content_get_random()
        assert content, "Could not find any content"
        return content.sha1_git

    ##########################
    # SkippedContent
    ##########################

    def _skipped_content_add(self, contents: List[SkippedContent]) -> Dict[str, int]:
        # Filter-out content already in the database.
        if not self._allow_overwrite:
            contents = [
                c
                for c in contents
                if not self._cql_runner.skipped_content_get_from_pk(c.to_dict())
            ]

        self.journal_writer.skipped_content_add(contents)

        for content in contents:
            # Compute token of the row in the main table
            (token, insertion_finalizer) = self._cql_runner.skipped_content_add_prepare(
                SkippedContentRow.from_dict({"origin": None, **content.to_dict()})
            )

            # Then add to index tables
            for algo in HASH_ALGORITHMS:
                self._cql_runner.skipped_content_index_add_one(algo, content, token)

            # Then to the main table
            insertion_finalizer()

        return {"skipped_content:add": len(contents)}

    def skipped_content_add(self, content: List[SkippedContent]) -> Dict[str, int]:
        contents = [attr.evolve(c, ctime=now()) for c in content]
        return self._skipped_content_add(contents)

    def skipped_content_find(self, content: HashDict) -> List[SkippedContent]:
        if not set(content).intersection(HASH_ALGORITHMS):
            raise StorageArgumentException(
                "content keys must contain at least one "
                f"of: {', '.join(sorted(HASH_ALGORITHMS))}"
            )
        # get first algo that was given
        algo, hash_ = next(iter(cast(Dict[str, bytes], content).items()))
        # because of collisions, we might get multiple tokens for different skipped contents
        tokens = self._cql_runner.skipped_content_get_tokens_from_single_hash(
            algo, hash_
        )
        # that means that now we need to filter out those that do not match the search criteria…
        results = []
        for token in tokens:
            # and the token might actually also correspond to multiple,
            # unrelated skipped content
            for row in self._cql_runner.skipped_content_get_from_token(token):
                row_d = row.to_dict()
                if all(row_d[algo] == hash_ for algo, hash_ in content.items()):
                    row_d["ctime"] = row_d["ctime"].replace(
                        tzinfo=datetime.timezone.utc
                    )
                    results.append(SkippedContent(**row_d))
        return results

    def skipped_content_missing(
        self, contents: List[Dict[str, Any]]
    ) -> Iterable[Dict[str, Any]]:
        for content in contents:
            if not self._cql_runner.skipped_content_get_from_pk(content):
                yield {algo: content[algo] for algo in DEFAULT_ALGORITHMS}

    ##########################
    # Directory
    ##########################

    def directory_add(self, directories: List[Directory]) -> Dict[str, int]:
        to_add = {d.id: d for d in directories}.values()
        if not self._allow_overwrite:
            # Filter out directories that are already inserted.
            missing = self.directory_missing([dir_.id for dir_ in to_add])
            directories = [dir_ for dir_ in directories if dir_.id in missing]

        self.journal_writer.directory_add(directories)

        for directory in directories:
            # Add directory entries to the 'directory_entry' table
            rows = [
                DirectoryEntryRow(directory_id=directory.id, **entry.to_dict())
                for entry in directory.entries
            ]
            if self._directory_entries_insert_algo == "one-by-one":
                for row in rows:
                    self._cql_runner.directory_entry_add_one(row)
            elif self._directory_entries_insert_algo == "concurrent":
                self._cql_runner.directory_entry_add_concurrent(rows)
            elif self._directory_entries_insert_algo == "batch":
                self._cql_runner.directory_entry_add_batch(rows)
            else:
                raise ValueError(
                    f"Unexpected value for directory_entries_insert_algo: "
                    f"{self._directory_entries_insert_algo}"
                )

            # Add the directory *after* adding all the entries, so someone
            # calling snapshot_get_branch in the meantime won't end up
            # with half the entries.
            self._cql_runner.directory_add_one(
                DirectoryRow(id=directory.id, raw_manifest=directory.raw_manifest)
            )

        return {"directory:add": len(directories)}

    def directory_missing(self, directories: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.directory_missing(directories)

    def _join_dentry_to_content(
        self, dentry: DirectoryEntry, contents: List[Content]
    ) -> Dict[str, Any]:
        content: Union[None, Content, SkippedContentRow]
        keys = (
            "status",
            "sha1",
            "sha1_git",
            "sha256",
            "length",
        )
        ret = dict.fromkeys(keys)
        ret.update(dentry.to_dict())
        if ret["type"] == "file":
            for content in contents:
                if dentry.target == content.sha1_git:
                    break
            else:
                target = ret["target"]
                assert target is not None
                tokens = list(
                    self._cql_runner.skipped_content_get_tokens_from_single_hash(
                        "sha1_git", target
                    )
                )
                if tokens:
                    content = list(
                        self._cql_runner.skipped_content_get_from_token(tokens[0])
                    )[0]
                else:
                    content = None
            if content:
                for key in keys:
                    ret[key] = getattr(content, key)
        return ret

    def _directory_ls(
        self, directory_id: Sha1Git, recursive: bool, prefix: bytes = b""
    ) -> Iterable[Dict[str, Any]]:
        if self.directory_missing([directory_id]):
            return
        rows = list(self._cql_runner.directory_entry_get([directory_id]))

        # TODO: dedup to be fast in case the directory contains the same subdir/file
        # multiple times
        contents = self._content_find_many([{"sha1_git": row.target} for row in rows])

        for row in rows:
            entry_d = row.to_dict()
            # Build and yield the directory entry dict
            del entry_d["directory_id"]
            entry = DirectoryEntry.from_dict(entry_d)
            ret = self._join_dentry_to_content(entry, contents)
            ret["name"] = prefix + ret["name"]
            ret["dir_id"] = directory_id
            yield ret

            if recursive and ret["type"] == "dir":
                yield from self._directory_ls(
                    ret["target"], True, prefix + ret["name"] + b"/"
                )

    def directory_entry_get_by_path(
        self, directory: Sha1Git, paths: List[bytes]
    ) -> Optional[Dict[str, Any]]:
        return self._directory_entry_get_by_path(directory, paths, b"")

    def _directory_entry_get_by_path(
        self, directory: Sha1Git, paths: List[bytes], prefix: bytes
    ) -> Optional[Dict[str, Any]]:
        if not paths:
            return None

        contents = list(self.directory_ls(directory))

        if not contents:
            return None

        def _get_entry(entries, name):
            """Finds the entry with the requested name, prepends the
            prefix (to get its full path), and returns it.

            If no entry has that name, returns None."""
            for entry in entries:
                if entry["name"] == name:
                    entry = entry.copy()
                    entry["name"] = prefix + entry["name"]
                    return entry

        first_item = _get_entry(contents, paths[0])

        if len(paths) == 1:
            return first_item

        if not first_item or first_item["type"] != "dir":
            return None

        return self._directory_entry_get_by_path(
            first_item["target"], paths[1:], prefix + paths[0] + b"/"
        )

    def directory_ls(
        self, directory: Sha1Git, recursive: bool = False
    ) -> Iterable[Dict[str, Any]]:
        yield from self._directory_ls(directory, recursive)

    def directory_get_entries(
        self,
        directory_id: Sha1Git,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
    ) -> Optional[PagedResult[DirectoryEntry]]:
        if self.directory_missing([directory_id]):
            return None

        entries_from: bytes = page_token or b""
        rows = self._cql_runner.directory_entry_get_from_name(
            directory_id, entries_from, limit + 1
        )
        entries = [
            DirectoryEntry.from_dict(remove_keys(row.to_dict(), ("directory_id",)))
            for row in rows
        ]
        if len(entries) > limit:
            last_entry = entries.pop()
            next_page_token = last_entry.name
        else:
            next_page_token = None
        return PagedResult(results=entries, next_page_token=next_page_token)

    def directory_get_raw_manifest(
        self, directory_ids: List[Sha1Git]
    ) -> Dict[Sha1Git, Optional[bytes]]:
        return {
            dir_.id: dir_.raw_manifest
            for dir_ in self._cql_runner.directory_get(directory_ids)
        }

    def directory_get_random(self) -> Sha1Git:
        directory = self._cql_runner.directory_get_random()
        assert directory, "Could not find any directory"
        return directory.id

    def directory_get_id_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1Git]:
        return _get_paginated_sha1_partition(
            partition_id,
            nb_partitions,
            page_token,
            limit,
            self._cql_runner.directory_get_token_range,
            operator.attrgetter("id"),
        )

    ##########################
    # Revision
    ##########################

    def revision_add(self, revisions: List[Revision]) -> Dict[str, int]:
        # Filter-out revisions already in the database
        if not self._allow_overwrite:
            to_add = {r.id: r for r in revisions}.values()
            missing = self.revision_missing([rev.id for rev in to_add])
            revisions = [rev for rev in revisions if rev.id in missing]
        self.journal_writer.revision_add(revisions)

        for revision in revisions:
            revobject = converters.revision_to_db(revision)
            if revobject:
                # Add parents first
                for rank, parent in enumerate(revision.parents):
                    self._cql_runner.revision_parent_add_one(
                        RevisionParentRow(
                            id=revobject.id, parent_rank=rank, parent_id=parent
                        )
                    )

                # Then write the main revision row.
                # Writing this after all parents were written ensures that
                # read endpoints don't return a partial view while writing
                # the parents
                self._cql_runner.revision_add_one(revobject)

        return {"revision:add": len(revisions)}

    def revision_missing(self, revisions: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.revision_missing(revisions)

    def revision_get(
        self, revision_ids: List[Sha1Git], ignore_displayname: bool = False
    ) -> List[Optional[Revision]]:
        rows = self._cql_runner.revision_get(revision_ids)
        revisions: Dict[Sha1Git, Revision] = {}
        for row in rows:
            # TODO: use a single query to get all parents?
            # (it might have lower latency, but requires more code and more
            # bandwidth, because revision id would be part of each returned
            # row)
            parents = tuple(self._cql_runner.revision_parent_get(row.id))
            # parent_rank is the clustering key, so results are already
            # sorted by rank.
            rev = converters.revision_from_db(row, parents=parents)
            revisions[rev.id] = rev

        return [revisions.get(rev_id) for rev_id in revision_ids]

    def _get_parent_revs(
        self,
        rev_ids: Iterable[Sha1Git],
        seen: Set[Sha1Git],
        limit: Optional[int],
        short: bool,
    ) -> Union[
        Iterable[Dict[str, Any]],
        Iterable[Tuple[Sha1Git, Tuple[Sha1Git, ...]]],
    ]:
        if limit and len(seen) >= limit:
            return
        rev_ids = [id_ for id_ in rev_ids if id_ not in seen]
        if not rev_ids:
            return
        seen |= set(rev_ids)

        # We need this query, even if short=True, to return consistent
        # results (ie. not return only a subset of a revision's parents
        # if it is being written)
        if short:
            ids = self._cql_runner.revision_get_ids(rev_ids)
            for id_ in ids:
                # TODO: use a single query to get all parents?
                # (it might have less latency, but requires less code and more
                # bandwidth (because revision id would be part of each returned
                # row)
                parents = tuple(self._cql_runner.revision_parent_get(id_))

                # parent_rank is the clustering key, so results are already
                # sorted by rank.

                yield (id_, parents)
                yield from self._get_parent_revs(parents, seen, limit, short)
        else:
            rows = self._cql_runner.revision_get(rev_ids)

            for row in rows:
                # TODO: use a single query to get all parents?
                # (it might have less latency, but requires less code and more
                # bandwidth (because revision id would be part of each returned
                # row)
                parents = tuple(self._cql_runner.revision_parent_get(row.id))

                # parent_rank is the clustering key, so results are already
                # sorted by rank.

                rev = converters.revision_from_db(row, parents=parents)
                yield rev.to_dict()
                yield from self._get_parent_revs(parents, seen, limit, short)

    def revision_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Revision]:
        def convert(row: RevisionRow) -> Revision:
            return converters.revision_from_db(
                row, parents=tuple(self._cql_runner.revision_parent_get(row.id))
            )

        return _get_paginated_sha1_partition(
            partition_id,
            nb_partitions,
            page_token,
            limit,
            self._cql_runner.revision_get_token_range,
            convert,
        )

    def revision_log(
        self,
        revisions: List[Sha1Git],
        ignore_displayname: bool = False,
        limit: Optional[int] = None,
    ) -> Iterable[Optional[Dict[str, Any]]]:
        seen: Set[Sha1Git] = set()
        yield from cast(
            Iterable[Optional[Dict[str, Any]]],
            self._get_parent_revs(revisions, seen, limit, short=False),
        )

    def revision_shortlog(
        self, revisions: List[Sha1Git], limit: Optional[int] = None
    ) -> Iterable[Optional[Tuple[Sha1Git, Tuple[Sha1Git, ...]]]]:
        seen: Set[Sha1Git] = set()
        yield from cast(
            Iterable[Optional[Tuple[Sha1Git, Tuple[Sha1Git, ...]]]],
            self._get_parent_revs(revisions, seen, limit, short=True),
        )

    def revision_get_random(self) -> Sha1Git:
        revision = self._cql_runner.revision_get_random()
        assert revision, "Could not find any revision"
        return revision.id

    ##########################
    # Release
    ##########################

    def release_add(self, releases: List[Release]) -> Dict[str, int]:
        if not self._allow_overwrite:
            to_add = {r.id: r for r in releases}.values()
            missing = set(self.release_missing([rel.id for rel in to_add]))
            releases = [rel for rel in to_add if rel.id in missing]
        self.journal_writer.release_add(releases)

        for release in releases:
            if release:
                self._cql_runner.release_add_one(converters.release_to_db(release))

        return {"release:add": len(releases)}

    def release_missing(self, releases: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.release_missing(releases)

    def release_get(
        self, releases: List[Sha1Git], ignore_displayname: bool = False
    ) -> List[Optional[Release]]:
        rows = self._cql_runner.release_get(releases)
        rels: Dict[Sha1Git, Release] = {}
        for row in rows:
            release = converters.release_from_db(row)
            rels[row.id] = release

        return [rels.get(rel_id) for rel_id in releases]

    def release_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Release]:
        return _get_paginated_sha1_partition(
            partition_id,
            nb_partitions,
            page_token,
            limit,
            self._cql_runner.release_get_token_range,
            lambda row: converters.release_from_db(row),
        )

    def release_get_random(self) -> Sha1Git:
        release = self._cql_runner.release_get_random()
        assert release, "Could not find any release"
        return release.id

    ##########################
    # Snapshot
    ##########################

    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict[str, int]:
        if not self._allow_overwrite:
            to_add = {s.id: s for s in snapshots}.values()
            missing = self._cql_runner.snapshot_missing([snp.id for snp in to_add])
            snapshots = [snp for snp in snapshots if snp.id in missing]

        for snapshot in snapshots:
            self.journal_writer.snapshot_add([snapshot])

            # Add branches
            for branch_name, branch in snapshot.branches.items():
                if branch is None:
                    target_type: Optional[str] = None
                    target: Optional[bytes] = None
                else:
                    target_type = branch.target_type.value
                    target = branch.target
                self._cql_runner.snapshot_branch_add_one(
                    SnapshotBranchRow(
                        snapshot_id=snapshot.id,
                        name=branch_name,
                        target_type=target_type,
                        target=target,
                    )
                )

            # Add the snapshot *after* adding all the branches, so someone
            # calling snapshot_get_branch in the meantime won't end up
            # with half the branches.
            self._cql_runner.snapshot_add_one(SnapshotRow(id=snapshot.id))

        return {"snapshot:add": len(snapshots)}

    def snapshot_missing(self, snapshots: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.snapshot_missing(snapshots)

    def snapshot_get(self, snapshot_id: Sha1Git) -> Optional[Dict[str, Any]]:
        d = self.snapshot_get_branches(snapshot_id)
        if d is None:
            return None
        return {
            "id": d["id"],
            "branches": {
                name: branch.to_dict() if branch else None
                for (name, branch) in d["branches"].items()
            },
            "next_branch": d["next_branch"],
        }

    def snapshot_get_id_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1Git]:
        return _get_paginated_sha1_partition(
            partition_id,
            nb_partitions,
            page_token,
            limit,
            self._cql_runner.snapshot_get_token_range,
            operator.attrgetter("id"),
        )

    def snapshot_count_branches(
        self,
        snapshot_id: Sha1Git,
        branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Optional[Dict[Optional[str], int]]:
        if self._cql_runner.snapshot_missing([snapshot_id]):
            # Makes sure we don't fetch branches for a snapshot that is
            # being added.
            return None

        return self._cql_runner.snapshot_count_branches(
            snapshot_id, branch_name_exclude_prefix
        )

    def snapshot_get_branches(
        self,
        snapshot_id: Sha1Git,
        branches_from: bytes = b"",
        branches_count: int = 1000,
        target_types: Optional[List[str]] = None,
        branch_name_include_substring: Optional[bytes] = None,
        branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Optional[PartialBranches]:
        if self._cql_runner.snapshot_missing([snapshot_id]):
            # Makes sure we don't fetch branches for a snapshot that is
            # being added.
            return None

        branches: List = []
        while len(branches) < branches_count + 1:
            new_branches = list(
                self._cql_runner.snapshot_branch_get(
                    snapshot_id,
                    branches_from,
                    branches_count + 1,
                    branch_name_exclude_prefix,
                )
            )

            if not new_branches:
                break

            branches_from = new_branches[-1].name

            if len(new_branches) > branches_count:
                new_branches_filtered = new_branches[:-1]
            else:
                new_branches_filtered = new_branches

            # Filter by target_type
            if target_types:
                new_branches_filtered = [
                    branch
                    for branch in new_branches_filtered
                    if branch.target is not None and branch.target_type in target_types
                ]

            # Filter by branches_name_pattern
            if branch_name_include_substring:
                new_branches_filtered = [
                    branch
                    for branch in new_branches_filtered
                    if branch.name is not None
                    and (
                        branch_name_include_substring is None
                        or branch_name_include_substring in branch.name
                    )
                ]

            branches.extend(new_branches_filtered)

            if len(new_branches) < branches_count + 1:
                break

        if len(branches) > branches_count:
            branches = branches[: branches_count + 1]
            last_branch = branches.pop(-1).name
        else:
            last_branch = None

        return PartialBranches(
            id=snapshot_id,
            branches={
                branch.name: (
                    None
                    if branch.target is None
                    else SnapshotBranch(
                        target=branch.target,
                        target_type=SnapshotTargetType(branch.target_type),
                    )
                )
                for branch in branches
            },
            next_branch=last_branch,
        )

    def snapshot_get_random(self) -> Sha1Git:
        snapshot = self._cql_runner.snapshot_get_random()
        assert snapshot, "Could not find any snapshot"
        return snapshot.id

    def snapshot_branch_get_by_name(
        self,
        snapshot_id: Sha1Git,
        branch_name: bytes,
        follow_alias_chain: bool = True,
        max_alias_chain_length: int = 100,
    ) -> Optional[SnapshotBranchByNameResponse]:
        if self._cql_runner.snapshot_missing([snapshot_id]):
            return None

        resolve_chain: List[bytes] = []
        while True:
            branches = list(
                self._cql_runner.snapshot_branch_get_from_name(
                    snapshot_id=snapshot_id, from_=branch_name, limit=1
                )
            )
            if len(branches) != 1 or branches[0].name != branch_name:
                # target branch is None, there could be items in aliases_followed
                target = None
                break
            branch = branches[0]
            resolve_chain.append(branch_name)
            if (
                branch.target_type != SnapshotTargetType.ALIAS.value
                or not follow_alias_chain
            ):
                # first non alias branch or the first branch when follow_alias_chain is False
                target = (
                    SnapshotBranch(
                        target=branch.target,
                        target_type=SnapshotTargetType(branch.target_type),
                    )
                    if branch.target
                    else None
                )
                break
            elif (
                # Circular reference
                resolve_chain.count(branch_name) > 1
                # Too many re-directs
                or len(resolve_chain) >= max_alias_chain_length
            ):
                target = None
                break
            # Branch has a non-None target with type alias
            assert branch.target is not None
            branch_name = branch.target

        return SnapshotBranchByNameResponse(
            # resolve_chian has items, brach_found must be True
            branch_found=bool(resolve_chain),
            target=target,
            aliases_followed=resolve_chain,
        )

    ##########################
    # Origin
    ##########################

    def origin_get(self, origins: List[str]) -> List[Optional[Origin]]:
        return [self.origin_get_one(origin) for origin in origins]

    def origin_get_one(self, origin_url: str) -> Optional[Origin]:
        """Given an origin url, return the origin if it exists, None otherwise"""
        rows = list(self._cql_runner.origin_get_by_url(origin_url))
        if rows:
            assert len(rows) == 1
            return Origin(url=rows[0].url)
        else:
            return None

    def origin_get_by_sha1(self, sha1s: List[bytes]) -> List[Optional[Dict[str, Any]]]:
        results = []
        for sha1 in sha1s:
            rows = list(self._cql_runner.origin_get_by_sha1(sha1))
            origin = {"url": rows[0].url} if rows else None
            results.append(origin)
        return results

    def origin_list(
        self, page_token: Optional[str] = None, limit: int = 100
    ) -> PagedResult[Origin]:
        # Compute what token to begin the listing from
        start_token = TOKEN_BEGIN
        if page_token:
            start_token = int(page_token)
            if not (TOKEN_BEGIN <= start_token <= TOKEN_END):
                raise StorageArgumentException("Invalid page_token.")
        next_page_token = None

        origins = []
        # Take one more origin so we can reuse it as the next page token if any
        for tok, row in self._cql_runner.origin_list(start_token, limit + 1):
            origins.append(Origin(url=row.url))
            # keep reference of the last id for pagination purposes
            last_id = tok

        if len(origins) > limit:
            # last origin id is the next page token
            next_page_token = str(last_id)
            # excluding that origin from the result to respect the limit size
            origins = origins[:limit]

        assert len(origins) <= limit

        return PagedResult(results=origins, next_page_token=next_page_token)

    def origin_search(
        self,
        url_pattern: str,
        page_token: Optional[str] = None,
        limit: int = 50,
        regexp: bool = False,
        with_visit: bool = False,
        visit_types: Optional[List[str]] = None,
    ) -> PagedResult[Origin]:
        # TODO: remove this endpoint, swh-search should be used instead.
        next_page_token = None
        offset = int(page_token) if page_token else 0

        origin_rows = [row for row in self._cql_runner.origin_iter_all()]
        if regexp:
            pat = re.compile(url_pattern)
            origin_rows = [row for row in origin_rows if pat.search(row.url)]
        else:
            origin_rows = [row for row in origin_rows if url_pattern in row.url]

        if with_visit:
            origin_rows = [row for row in origin_rows if row.next_visit_id > 1]

        if visit_types:

            def _has_visit_types(origin, visit_types):
                for origin_visit in stream_results(self.origin_visit_get, origin):
                    if origin_visit.type in visit_types:
                        return True
                return False

            origin_rows = [
                row for row in origin_rows if _has_visit_types(row.url, visit_types)
            ]

        origins = [Origin(url=row.url) for row in origin_rows]

        origins = origins[offset : offset + limit + 1]
        if len(origins) > limit:
            # next offset
            next_page_token = str(offset + limit)
            # excluding that origin from the result to respect the limit size
            origins = origins[:limit]

        assert len(origins) <= limit
        return PagedResult(results=origins, next_page_token=next_page_token)

    def origin_count(
        self, url_pattern: str, regexp: bool = False, with_visit: bool = False
    ) -> int:
        raise NotImplementedError(
            "The Cassandra backend does not implement origin_count"
        )

    def origin_snapshot_get_all(self, origin_url: str) -> List[Sha1Git]:
        return list(self._cql_runner.origin_snapshot_get_all(origin_url))

    def origin_add(self, origins: List[Origin]) -> Dict[str, int]:
        if not self._allow_overwrite:
            to_add = {o.url: o for o in origins}.values()
            origins = [ori for ori in to_add if self.origin_get_one(ori.url) is None]

        self.journal_writer.origin_add(origins)
        for origin in origins:
            self._cql_runner.origin_add_one(
                OriginRow(sha1=hash_url(origin.url), url=origin.url, next_visit_id=1)
            )
        return {"origin:add": len(origins)}

    ##########################
    # OriginVisit and OriginVisitStatus
    ##########################

    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get_one(visit.origin)
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        for visit in visits:
            if visit.visit:
                # Set origin.next_visit_id = max(origin.next_visit_id, visit.visit+1)
                # so the next loader run does not reuse the id.
                self._cql_runner.origin_bump_next_visit_id(visit.origin, visit.visit)
                add_status = False
            else:
                visit_id = self._cql_runner.origin_generate_unique_visit_id(
                    visit.origin
                )
                visit = attr.evolve(visit, visit=visit_id)
                add_status = True
            self.journal_writer.origin_visit_add([visit])
            self._cql_runner.origin_visit_add_one(OriginVisitRow(**visit.to_dict()))
            assert visit.visit is not None
            all_visits.append(visit)
            if add_status:
                self._origin_visit_status_add(
                    OriginVisitStatus(
                        origin=visit.origin,
                        visit=visit.visit,
                        date=visit.date,
                        type=visit.type,
                        status="created",
                        snapshot=None,
                    )
                )
        return all_visits

    def _origin_visit_status_add(self, visit_status: OriginVisitStatus) -> None:
        """Add an origin visit status"""
        if visit_status.type is None:
            visit_row = self._cql_runner.origin_visit_get_one(
                visit_status.origin, visit_status.visit
            )
            if visit_row is None:
                raise StorageArgumentException(
                    f"Unknown origin visit {visit_status.visit} "
                    f"of origin {visit_status.origin}"
                )
            visit_status = attr.evolve(visit_status, type=visit_row.type)

        self.journal_writer.origin_visit_status_add([visit_status])
        self._cql_runner.origin_visit_status_add_one(
            converters.visit_status_to_row(visit_status)
        )

    def origin_visit_status_add(
        self, visit_statuses: List[OriginVisitStatus]
    ) -> Dict[str, int]:
        # First round to check existence (fail early if any is ko)
        for visit_status in visit_statuses:
            origin_url = self.origin_get_one(visit_status.origin)
            if not origin_url:
                raise StorageArgumentException(f"Unknown origin {visit_status.origin}")

        for visit_status in visit_statuses:
            self._origin_visit_status_add(visit_status)
        return {"origin_visit_status:add": len(visit_statuses)}

    def _origin_visit_apply_status(
        self, visit: Dict[str, Any], visit_status: OriginVisitStatusRow
    ) -> Dict[str, Any]:
        """Retrieve the latest visit status information for the origin visit.
        Then merge it with the visit and return it.

        """
        return {
            # default to the values in visit
            **visit,
            # override with the last update
            **visit_status.to_dict(),
            # visit['origin'] is the URL (via a join), while
            # visit_status['origin'] is only an id.
            "origin": visit["origin"],
            # but keep the date of the creation of the origin visit
            "date": visit["date"],
            # We use the visit type from origin visit
            # if it's not present on the origin visit status
            "type": visit_status.type or visit["type"],
        }

    def _origin_visit_get_latest_status(self, visit: OriginVisit) -> OriginVisitStatus:
        """Retrieve the latest visit status information for the origin visit object."""
        assert visit.visit
        row = self._cql_runner.origin_visit_status_get_latest(visit.origin, visit.visit)
        assert row is not None
        visit_status = converters.row_to_visit_status(row)
        return attr.evolve(visit_status, origin=visit.origin)

    @staticmethod
    def _format_origin_visit_row(visit):
        return {
            **visit.to_dict(),
            "origin": visit.origin,
            "date": visit.date.replace(tzinfo=datetime.timezone.utc),
        }

    def origin_visit_get(
        self,
        origin: str,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisit]:
        if not isinstance(order, ListOrder):
            raise StorageArgumentException("order must be a ListOrder value")
        if page_token and not isinstance(page_token, str):
            raise StorageArgumentException("page_token must be a string.")

        next_page_token = None
        visit_from = None if page_token is None else int(page_token)
        visits: List[OriginVisit] = []
        extra_limit = limit + 1

        rows = self._cql_runner.origin_visit_get(origin, visit_from, extra_limit, order)
        for row in rows:
            visits.append(converters.row_to_visit(row))

        assert len(visits) <= extra_limit
        if len(visits) == extra_limit:
            visits = visits[:limit]
            next_page_token = str(visits[-1].visit)

        return PagedResult(results=visits, next_page_token=next_page_token)

    def origin_visit_get_with_statuses(
        self,
        origin: str,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisitWithStatuses]:
        next_page_token = None
        visit_from = None if page_token is None else int(page_token)
        extra_limit = limit + 1

        # First get visits (plus one so we can use it as the next page token if any)
        rows = self._cql_runner.origin_visit_get(origin, visit_from, extra_limit, order)
        visits: List[OriginVisit] = [converters.row_to_visit(row) for row in rows]

        if visits:
            assert visits[0].visit is not None
            assert visits[-1].visit is not None
            visit_from = min(visits[0].visit, visits[-1].visit)
            visit_to = max(visits[0].visit, visits[-1].visit)

            # Then, fetch all statuses associated to these visits
            statuses_rows = self._cql_runner.origin_visit_status_get_all_range(
                origin, visit_from, visit_to
            )
            visit_statuses: Dict[int, List[OriginVisitStatus]] = defaultdict(list)
            for status_row in statuses_rows:
                if allowed_statuses and status_row.status not in allowed_statuses:
                    continue
                if require_snapshot and status_row.snapshot is None:
                    continue
                visit_status = converters.row_to_visit_status(status_row)
                visit_statuses[visit_status.visit].append(visit_status)

            # Add pagination if there are more visits
            assert len(visits) <= extra_limit
            if len(visits) == extra_limit:
                # excluding that visit from the result to respect the limit size
                visits = visits[:limit]
                # last visit id is the next page token
                next_page_token = str(visits[-1].visit)

        results = [
            OriginVisitWithStatuses(visit=visit, statuses=visit_statuses[visit.visit])
            for visit in visits
            if visit.visit is not None
        ]

        return PagedResult(results=results, next_page_token=next_page_token)

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
            try:
                date_from = datetime.datetime.fromisoformat(page_token)
            except ValueError:
                raise StorageArgumentException(
                    "Invalid page_token argument to origin_visit_status_get."
                ) from None

        # Take one more visit status so we can reuse it as the next page token if any
        rows = self._cql_runner.origin_visit_status_get_range(
            origin, visit, date_from, limit + 1, order
        )
        visit_statuses = [converters.row_to_visit_status(row) for row in rows]
        if len(visit_statuses) > limit:
            # last visit status date is the next page token
            next_page_token = str(visit_statuses[-1].date)
            # excluding that visit status from the result to respect the limit size
            visit_statuses = visit_statuses[:limit]

        return PagedResult(results=visit_statuses, next_page_token=next_page_token)

    def origin_visit_find_by_date(
        self, origin: str, visit_date: datetime.datetime, type: Optional[str] = None
    ) -> Optional[OriginVisit]:
        # Iterator over all the visits of the origin
        # This should be ok for now, as there aren't too many visits
        # per origin.
        rows = [
            visit
            for visit in self._cql_runner.origin_visit_iter_all(origin)
            if type is None or visit.type == type
        ]

        def key(visit):
            dt = visit.date.replace(tzinfo=datetime.timezone.utc) - visit_date
            return (abs(dt), -visit.visit)

        if rows:
            return converters.row_to_visit(min(rows, key=key))
        return None

    def origin_visit_get_by(self, origin: str, visit: int) -> Optional[OriginVisit]:
        row = self._cql_runner.origin_visit_get_one(origin, visit)
        if row:
            return converters.row_to_visit(row)
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
        rows = self._cql_runner.origin_visit_iter_all(origin)
        for row in rows:
            visit = self._format_origin_visit_row(row)
            for status_row in self._cql_runner.origin_visit_status_get(
                origin, visit["visit"]
            ):
                updated_visit = self._origin_visit_apply_status(visit, status_row)
                if type is not None and updated_visit["type"] != type:
                    continue
                if allowed_statuses and updated_visit["status"] not in allowed_statuses:
                    continue
                if require_snapshot and updated_visit["snapshot"] is None:
                    continue

                return OriginVisit(
                    origin=visit["origin"],
                    visit=visit["visit"],
                    date=visit["date"],
                    type=visit["type"],
                )

        return None

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
        rows = list(self._cql_runner.origin_visit_status_get(origin_url, visit))
        # filtering is done python side as we cannot do it server side
        if allowed_statuses:
            rows = [row for row in rows if row.status in allowed_statuses]
        if require_snapshot:
            rows = [row for row in rows if row.snapshot is not None]
        if not rows:
            return None
        return converters.row_to_visit_status(rows[0])

    def origin_visit_status_get_random(self, type: str) -> Optional[OriginVisitStatus]:
        back_in_the_day = now() - datetime.timedelta(weeks=13)  # 3 months back

        # Random position to start iteration at
        start_token = random.randint(TOKEN_BEGIN, TOKEN_END)

        # Iterator over all visits, ordered by token(origins) then visit_id
        rows = self._cql_runner.origin_visit_iter(start_token)
        for row in rows:
            visit = converters.row_to_visit(row)
            visit_status = self._origin_visit_get_latest_status(visit)
            if visit.date > back_in_the_day and visit_status.status == "full":
                return visit_status
        return None

    ##########################
    # misc.
    ##########################

    def object_find_by_sha1_git(self, ids: List[Sha1Git]) -> Dict[Sha1Git, List[Dict]]:
        results: Dict[Sha1Git, List[Dict]] = {id_: [] for id_ in ids}
        missing_ids = set(ids)

        # Mind the order, revision is the most likely one for a given ID,
        # so we check revisions first.
        queries: List[Tuple[str, Callable[[List[Sha1Git]], Iterable[Sha1Git]]]] = [
            ("revision", self._cql_runner.revision_missing),
            ("release", self._cql_runner.release_missing),
            ("content", self.content_missing_per_sha1_git),
            ("directory", self._cql_runner.directory_missing),
        ]

        for object_type, query_fn in queries:
            found_ids = missing_ids - set(query_fn(list(missing_ids)))
            for sha1_git in found_ids:
                results[sha1_git].append(
                    {
                        "sha1_git": sha1_git,
                        "type": object_type,
                    }
                )
                missing_ids.remove(sha1_git)

            if not missing_ids:
                # We found everything, skipping the next queries.
                break

        return results

    def stat_counters(self):
        rows = self._cql_runner.stat_counters()
        keys = (
            "content",
            "directory",
            "origin",
            "origin_visit",
            "release",
            "revision",
            "skipped_content",
            "snapshot",
        )
        stats = {key: 0 for key in keys}
        stats.update({row.object_type: row.count for row in rows})
        return stats

    def refresh_stat_counters(self):
        pass

    ##########################
    # RawExtrinsicMetadata
    ##########################

    def raw_extrinsic_metadata_add(
        self, metadata: List[RawExtrinsicMetadata]
    ) -> Dict[str, int]:
        """Add extrinsic metadata on objects (contents, directories, ...)."""
        self.journal_writer.raw_extrinsic_metadata_add(metadata)
        counter = Counter[ExtendedObjectType]()
        for metadata_entry in metadata:
            if not self._cql_runner.metadata_authority_get(
                metadata_entry.authority.type.value, metadata_entry.authority.url
            ):
                raise UnknownMetadataAuthority(str(metadata_entry.authority))
            if not self._cql_runner.metadata_fetcher_get(
                metadata_entry.fetcher.name, metadata_entry.fetcher.version
            ):
                raise UnknownMetadataFetcher(str(metadata_entry.fetcher))

            try:
                row = RawExtrinsicMetadataRow(
                    id=metadata_entry.id,
                    type=metadata_entry.target.object_type.name.lower(),
                    target=str(metadata_entry.target),
                    authority_type=metadata_entry.authority.type.value,
                    authority_url=metadata_entry.authority.url,
                    discovery_date=metadata_entry.discovery_date,
                    fetcher_name=metadata_entry.fetcher.name,
                    fetcher_version=metadata_entry.fetcher.version,
                    format=metadata_entry.format,
                    metadata=metadata_entry.metadata,
                    # Cassandra handles null values for row properties as removals
                    # (tombstones), which are never cleaned up as the values were never
                    # set. To avoid this issue, we instead store a known invalid value
                    # of the proper type as a placeholder for null properties: for
                    # strings and bytes: the empty value; for visit ids (integers): 0.
                    # The inverse conversion is performed when reading the data back
                    # from Cassandra.
                    origin=metadata_entry.origin or "",
                    visit=metadata_entry.visit or 0,
                    snapshot=(
                        str(metadata_entry.snapshot) if metadata_entry.snapshot else ""
                    ),
                    release=(
                        str(metadata_entry.release) if metadata_entry.release else ""
                    ),
                    revision=(
                        str(metadata_entry.revision) if metadata_entry.revision else ""
                    ),
                    path=metadata_entry.path or b"",
                    directory=(
                        str(metadata_entry.directory)
                        if metadata_entry.directory
                        else ""
                    ),
                )

            except TypeError as e:
                raise StorageArgumentException(*e.args)

            # Add to the index first
            self._cql_runner.raw_extrinsic_metadata_by_id_add(
                RawExtrinsicMetadataByIdRow(
                    id=row.id,
                    target=row.target,
                    authority_type=row.authority_type,
                    authority_url=row.authority_url,
                )
            )

            # Then to the main table
            self._cql_runner.raw_extrinsic_metadata_add(row)
            counter[metadata_entry.target.object_type] += 1
        return {
            f"{type.value}_metadata:add": count for (type, count) in counter.items()
        }

    def raw_extrinsic_metadata_get(
        self,
        target: ExtendedSWHID,
        authority: MetadataAuthority,
        after: Optional[datetime.datetime] = None,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
    ) -> PagedResult[RawExtrinsicMetadata]:
        if page_token is not None:
            (after_date, id_) = msgpack_loads(base64.b64decode(page_token))
            if after and after_date < after:
                raise StorageArgumentException(
                    "page_token is inconsistent with the value of 'after'."
                )
            entries = self._cql_runner.raw_extrinsic_metadata_get_after_date_and_id(
                str(target),
                authority.type.value,
                authority.url,
                after_date,
                id_,
            )
        elif after is not None:
            entries = self._cql_runner.raw_extrinsic_metadata_get_after_date(
                str(target), authority.type.value, authority.url, after
            )
        else:
            entries = self._cql_runner.raw_extrinsic_metadata_get(
                str(target), authority.type.value, authority.url
            )

        if limit:
            entries = itertools.islice(entries, 0, limit + 1)

        results = []
        for entry in entries:
            assert str(target) == entry.target

            results.append(converters.row_to_raw_extrinsic_metadata(entry))

        if len(results) > limit:
            results.pop()
            assert len(results) == limit
            last_result = results[-1]
            next_page_token: Optional[str] = base64.b64encode(
                msgpack_dumps(
                    (
                        last_result.discovery_date,
                        last_result.id,
                    )
                )
            ).decode()
        else:
            next_page_token = None

        return PagedResult(
            next_page_token=next_page_token,
            results=results,
        )

    def raw_extrinsic_metadata_get_by_ids(
        self, ids: List[Sha1Git]
    ) -> List[RawExtrinsicMetadata]:
        keys = self._cql_runner.raw_extrinsic_metadata_get_by_ids(ids)

        results: Set[RawExtrinsicMetadata] = set()
        for key in keys:
            candidates = self._cql_runner.raw_extrinsic_metadata_get(
                key.target, key.authority_type, key.authority_url
            )
            candidates = [
                candidate for candidate in candidates if candidate.id == key.id
            ]
            if len(candidates) > 1:
                raise Exception(
                    "Found multiple RawExtrinsicMetadata objects with the same id: "
                    + hash_to_hex(key.id)
                )
            results.update(map(converters.row_to_raw_extrinsic_metadata, candidates))

        return list(results)

    ##########################
    # MetadataAuthority and MetadataFetcher
    ##########################

    def raw_extrinsic_metadata_get_authorities(
        self, target: ExtendedSWHID
    ) -> List[MetadataAuthority]:
        return [
            MetadataAuthority(
                type=MetadataAuthorityType(authority_type), url=authority_url
            )
            for (authority_type, authority_url) in set(
                self._cql_runner.raw_extrinsic_metadata_get_authorities(str(target))
            )
        ]

    def metadata_fetcher_add(self, fetchers: List[MetadataFetcher]) -> Dict[str, int]:
        self.journal_writer.metadata_fetcher_add(fetchers)
        for fetcher in fetchers:
            self._cql_runner.metadata_fetcher_add(
                MetadataFetcherRow(
                    name=fetcher.name,
                    version=fetcher.version,
                )
            )
        return {"metadata_fetcher:add": len(fetchers)}

    def metadata_fetcher_get(
        self, name: str, version: str
    ) -> Optional[MetadataFetcher]:
        fetcher = self._cql_runner.metadata_fetcher_get(name, version)
        if fetcher:
            return MetadataFetcher(
                name=fetcher.name,
                version=fetcher.version,
            )
        else:
            return None

    def metadata_authority_add(
        self, authorities: List[MetadataAuthority]
    ) -> Dict[str, int]:
        self.journal_writer.metadata_authority_add(authorities)
        for authority in authorities:
            self._cql_runner.metadata_authority_add(
                MetadataAuthorityRow(
                    url=authority.url,
                    type=authority.type.value,
                )
            )
        return {"metadata_authority:add": len(authorities)}

    def metadata_authority_get(
        self, type: MetadataAuthorityType, url: str
    ) -> Optional[MetadataAuthority]:
        authority = self._cql_runner.metadata_authority_get(type.value, url)
        if authority:
            return MetadataAuthority(
                type=MetadataAuthorityType(authority.type),
                url=authority.url,
            )
        else:
            return None

    ##########################
    # ExtID
    ##########################

    def extid_add(self, ids: List[ExtID]) -> Dict[str, int]:
        if not self._allow_overwrite:
            extids = [
                extid
                for extid in ids
                if not self._cql_runner.extid_get_from_pk(
                    extid_type=extid.extid_type,
                    extid_version=extid.extid_version,
                    extid=extid.extid,
                    target=extid.target,
                )
            ]
        else:
            extids = list(ids)

        self.journal_writer.extid_add(extids)

        inserted = 0
        for extid in extids:
            target_type = extid.target.object_type.value
            target = extid.target.object_id
            extid_version = extid.extid_version
            extid_type = extid.extid_type
            extidrow = ExtIDRow(
                extid_type=extid_type,
                extid_version=extid_version,
                extid=extid.extid,
                target_type=target_type,
                target=target,
            )
            (token, insertion_finalizer) = self._cql_runner.extid_add_prepare(extidrow)
            indexrow = ExtIDByTargetRow(
                target_type=target_type,
                target=target,
                target_token=token,
            )
            self._cql_runner.extid_index_add_one(indexrow)
            insertion_finalizer()
            inserted += 1
        return {"extid:add": inserted}

    def extid_get_from_extid(
        self, id_type: str, ids: List[bytes], version: Optional[int] = None
    ) -> List[ExtID]:
        result: List[ExtID] = []
        for extid in ids:
            if version is not None:
                extidrows = self._cql_runner.extid_get_from_extid_and_version(
                    id_type, extid, version
                )
            else:
                extidrows = self._cql_runner.extid_get_from_extid(id_type, extid)
            result.extend(
                ExtID(
                    extid_type=extidrow.extid_type,
                    extid_version=extidrow.extid_version,
                    extid=extidrow.extid,
                    target=CoreSWHID(
                        object_type=extidrow.target_type,
                        object_id=extidrow.target,
                    ),
                )
                for extidrow in extidrows
            )
        return result

    def extid_get_from_target(
        self,
        target_type: SwhidObjectType,
        ids: List[Sha1Git],
        extid_type: Optional[str] = None,
        extid_version: Optional[int] = None,
    ) -> List[ExtID]:
        if (extid_version is not None and extid_type is None) or (
            extid_version is None and extid_type is not None
        ):
            raise ValueError("You must provide both extid_type and extid_version")

        result: List[ExtID] = []
        for target in ids:
            extidrows = self._cql_runner.extid_get_from_target(
                target_type.value,
                target,
                extid_type=extid_type,
                extid_version=extid_version,
            )

            result.extend(
                ExtID(
                    extid_type=extidrow.extid_type,
                    extid_version=extidrow.extid_version,
                    extid=extidrow.extid,
                    target=CoreSWHID(
                        object_type=SwhidObjectType(extidrow.target_type),
                        object_id=extidrow.target,
                    ),
                )
                for extidrow in extidrows
            )
        return result

    #########################
    # 'object_references' table
    #########################

    def object_find_recent_references(
        self, target_swhid: ExtendedSWHID, limit: int
    ) -> List[ExtendedSWHID]:
        rows = self._cql_runner.object_reference_get(
            target=target_swhid.object_id,
            target_type=target_swhid.object_type.value,
            limit=limit,
        )
        return [converters.row_to_object_reference(row).source for row in rows]

    def object_references_add(
        self, references: List[ObjectReference]
    ) -> Dict[str, int]:
        to_add = list(
            {converters.object_reference_to_row(reference) for reference in references}
        )
        self._cql_runner.object_reference_add_concurrent(to_add)
        return {"object_reference:add": len(to_add)}

    #########################
    # Deletion
    #########################

    def _content_delete(self, object_ids: Set[Sha1Git]) -> Dict[str, int]:
        # We have take care of both Content and SkippedContent and
        # we don’t know where are each SWHID…
        # Sadly, we have to read before write which is considered an
        # anti-pattern for Cassandra. But we only have a SWHID, so a `sha1_git`.
        # But we need to use the right primary keys to perform a deletion. We
        # also cannot use `token()` in a WHERE clause of a DELETE… So we have to
        # perform two SELECTs (in `_content_get_from_hashes()``) to get enough
        # data to perform our DELETE.
        content_hashes = []
        for row in self._content_get_from_hashes("sha1_git", list(object_ids)):
            content_hashes.append(
                TotalHashDict(
                    sha1=row.sha1,
                    sha1_git=row.sha1_git,
                    sha256=row.sha256,
                    blake2s256=row.blake2s256,
                )
            )
        for content_hash in content_hashes:
            self._cql_runner.content_delete(content_hash)
        object_ids -= {h["sha1_git"] for h in content_hashes}
        skipped_content_hashes = []
        for object_id in object_ids:
            for token in self._cql_runner.skipped_content_get_tokens_from_single_hash(
                "sha1_git", object_id
            ):
                for row in self._cql_runner.skipped_content_get_from_token(token):
                    skipped_content_hashes.append(
                        TotalHashDict(
                            sha1=row.sha1,
                            sha1_git=row.sha1_git,
                            sha256=row.sha256,
                            blake2s256=row.blake2s256,
                        )
                    )
        for skipped_content_hash in skipped_content_hashes:
            self._cql_runner.skipped_content_delete(skipped_content_hash)
        return {
            "content:delete": len(content_hashes),
            "content:delete:bytes": 0,
            "skipped_content:delete": len(skipped_content_hashes),
        }

    def _directory_delete(self, object_ids: Set[Sha1Git]) -> Dict[str, int]:
        for object_id in object_ids:
            self._cql_runner.directory_delete(object_id)
            self._cql_runner.directory_entry_delete(object_id)
        # Cassandra makes it so we can’t get the number of affected rows
        return {"directory:delete": len(object_ids)}

    def _revision_delete(self, object_ids: Set[Sha1Git]) -> Dict[str, int]:
        for object_id in object_ids:
            self._cql_runner.revision_delete(object_id)
            self._cql_runner.revision_parent_delete(object_id)
        # Cassandra makes it so we can’t get the number of affected rows
        return {"revision:delete": len(object_ids)}

    def _release_delete(self, object_ids: Set[Sha1Git]) -> Dict[str, int]:
        for object_id in object_ids:
            self._cql_runner.release_delete(object_id)
        # Cassandra makes it so we can’t get the number of affected rows
        return {"release:delete": len(object_ids)}

    def _snapshot_delete(self, object_ids: Set[Sha1Git]) -> Dict[str, int]:
        for object_id in object_ids:
            self._cql_runner.snapshot_delete(object_id)
            self._cql_runner.snapshot_branch_delete(object_id)
        # Cassandra makes it so we can’t get the number of affected rows
        return {"snapshot:delete": len(object_ids)}

    def _origin_delete(self, object_ids: Set[Sha1Git]) -> Dict[str, int]:
        origin_count = 0
        origin_visit_count = 0
        origin_visit_status_count = 0
        for object_id in object_ids:
            for origin_row in self._cql_runner.origin_get_by_sha1(object_id):
                origin_count += 1
                self._cql_runner.origin_delete(origin_row.sha1)
                # XXX: We could avoid needless queries if we modify ObjectDeletionInterface
                for origin_visit_row in self._cql_runner.origin_visit_iter_all(
                    origin_row.url
                ):
                    origin_visit_count += 1
                    origin_visit_status_count += len(
                        list(
                            self._cql_runner.origin_visit_status_get_range(
                                origin_row.url,
                                origin_visit_row.visit,
                                None,
                                99999999,
                                ListOrder.ASC,
                            )
                        )
                    )
                self._cql_runner.origin_visit_delete(origin_row.url)
                self._cql_runner.origin_visit_status_delete(origin_row.url)
        return {
            "origin:delete": origin_count,
            "origin_visit:delete": origin_visit_count,
            "origin_visit_status:delete": origin_visit_status_count,
        }

    def _raw_extrinsic_metadata_delete(self, emd_ids: Set[Sha1Git]) -> Dict[str, int]:
        result: Counter[str] = Counter()
        for emd_by_id_row in self._cql_runner.raw_extrinsic_metadata_get_by_ids(
            emd_ids
        ):
            discovery_date = None
            # Sadly, the `raw_extrinsic_metadata_by_id` table does not contain
            # the `discovery_date` which is needed to remove a specific line in
            # the `raw_extrinsic_metadata` table. We have to get it by scanning
            # `raw_extrinsic_metadata` table, looking for the right line.
            for emd_row in self._cql_runner.raw_extrinsic_metadata_get(
                emd_by_id_row.target,
                emd_by_id_row.authority_type,
                emd_by_id_row.authority_url,
            ):
                if emd_row.id == emd_by_id_row.id:
                    discovery_date = emd_row.discovery_date
                    break
            if discovery_date is None:
                logger.warning(
                    f"Unable to find `swh:1:emd:{emd_by_id_row.id.hex()}` in "
                    "`raw_extrinsic_metadata` while it exists in "
                    "`raw_extrinsic_metadata_by_id`!"
                )
                continue
            self._cql_runner.raw_extrinsic_metadata_delete(
                emd_by_id_row.target,
                emd_by_id_row.authority_type,
                emd_by_id_row.authority_url,
                discovery_date,
                emd_by_id_row.id,
            )
            result[f"{emd_by_id_row.target[6:9]}_metadata:delete"] += 1
        for emd_id in emd_ids:
            self._cql_runner.raw_extrinsic_metadata_by_id_delete(emd_id)
        return dict(result)

    def object_delete(self, swhids: List[ExtendedSWHID]) -> Dict[str, int]:
        """Delete objects from the storage

        All skipped content objects matching the given SWHID will be removed,
        including those who have the same SWHID due to hash collisions.

        Origin objects are removed alongside their associated origin visit and
        origin visit status objects.

        Args:
            swhids: list of SWHID of the objects to remove

        Returns:
            dict: number of objects removed. Details of each key:

            content:delete
                Number of content objects removed

            content:delete:bytes
                Sum of the removed contents’ data length

            skipped_content:delete
                Number of skipped content objects removed

            directory:delete
                Number of directory objects removed

            revision:delete
                Number of revision objects removed

            release:delete
                Number of release objects removed

            snapshot:delete
                Number of snapshot objects removed

            origin:delete
                Number of origin objects removed

            origin_visit:delete
                Number of origin visit objects removed

            origin_visit_status:delete
                Number of origin visit status objects removed

            ori_metadata:delete
                Number of raw extrinsic metadata objects targeting
                an origin that have been removed

            snp_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a snapshot that have been removed

            rev_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a revision that have been removed

            rel_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a release that have been removed

            dir_metadata:delete
                Number ef raw extrinsic metadata objects targeting
                a directory that have been removed

            cnt_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a content that have been removed

            emd_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a raw extrinsic metadata object that have been removed
        """

        # groupby() splits consecutive groups, so we need to order the list first
        def key(swhid: ExtendedSWHID) -> int:
            return _DELETE_ORDERING[swhid.object_type]

        result = {}
        sorted_swhids = sorted(swhids, key=key)
        for object_type, grouped_swhids in itertools.groupby(
            sorted_swhids, key=operator.attrgetter("object_type")
        ):
            object_ids = {swhid.object_id for swhid in grouped_swhids}
            result.update(_DELETE_METHODS[object_type](self, object_ids))
        return result

    def extid_delete_for_target(self, target_swhids: List[CoreSWHID]) -> Dict[str, int]:
        """Delete ExtID objects from the storage

        Args:
            target_swhids: list of SWHIDs targeted by the ExtID objects to remove

        Returns:
            Summary dict with the following keys and associated values:

                extid:delete: Number of ExtID objects removed
        """
        deleted_extid_count = 0
        for target_swhid in target_swhids:
            target_type = target_swhid.object_type.value
            target = target_swhid.object_id

            extid_rows = list(
                self._cql_runner.extid_get_from_target(target_type, target)
            )
            self._cql_runner.extid_delete_from_by_target_table(target_type, target)
            for extid_row in extid_rows:
                self._cql_runner.extid_delete(**extid_row.to_dict())
                deleted_extid_count += 1
        return {"extid:delete": deleted_extid_count}

    ##########################
    # misc.
    ##########################

    def clear_buffers(self, object_types: Sequence[str] = ()) -> None:
        """Do nothing"""
        return None

    def flush(self, object_types: Sequence[str] = ()) -> Dict[str, int]:
        return {}


_DELETE_METHODS: Dict[
    ExtendedObjectType,
    Callable[[CassandraStorage, Set[Sha1Git]], Dict[str, int]],
] = {
    ExtendedObjectType.CONTENT: CassandraStorage._content_delete,
    ExtendedObjectType.DIRECTORY: CassandraStorage._directory_delete,
    ExtendedObjectType.REVISION: CassandraStorage._revision_delete,
    ExtendedObjectType.RELEASE: CassandraStorage._release_delete,
    ExtendedObjectType.SNAPSHOT: CassandraStorage._snapshot_delete,
    ExtendedObjectType.ORIGIN: CassandraStorage._origin_delete,
    ExtendedObjectType.RAW_EXTRINSIC_METADATA: CassandraStorage._raw_extrinsic_metadata_delete,
}

_DELETE_ORDERING: Dict[ExtendedObjectType, int] = {
    object_type: order for order, object_type in enumerate(_DELETE_METHODS.keys())
}
