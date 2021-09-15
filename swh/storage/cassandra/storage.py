# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import datetime
import itertools
import operator
import random
import re
from typing import (
    Any,
    Callable,
    Counter,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import attr

from swh.core.api.classes import stream_results
from swh.core.api.serializers import msgpack_dumps, msgpack_loads
from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_hex
from swh.model.identifiers import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.model.identifiers import ObjectType as SwhidObjectType
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
    TargetType,
)
from swh.storage.interface import (
    VISIT_STATUSES,
    ListOrder,
    PagedResult,
    PartialBranches,
    Sha1,
)
from swh.storage.metrics import process_metrics, send_metric, timed
from swh.storage.objstorage import ObjStorage
from swh.storage.utils import map_optional, now
from swh.storage.writer import JournalWriter

from . import converters
from ..exc import HashCollision, StorageArgumentException
from ..utils import remove_keys
from .common import TOKEN_BEGIN, TOKEN_END, hash_url
from .cql import CqlRunner
from .model import (
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
    SkippedContentRow,
    SnapshotBranchRow,
    SnapshotRow,
)
from .schema import HASH_ALGORITHMS

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000

DIRECTORY_ENTRIES_INSERT_ALGOS = ["one-by-one", "concurrent", "batch"]


class CassandraStorage:
    def __init__(
        self,
        hosts,
        keyspace,
        objstorage,
        port=9042,
        journal_writer=None,
        allow_overwrite=False,
        consistency_level="ONE",
        directory_entries_insert_algo="one-by-one",
    ):
        """
        A backend of swh-storage backed by Cassandra

        Args:
            hosts: Seed Cassandra nodes, to start connecting to the cluster
            keyspace: Name of the Cassandra database to use
            objstorage: Passed as argument to :class:`ObjStorage`
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
        """
        self._hosts = hosts
        self._keyspace = keyspace
        self._port = port
        self._consistency_level = consistency_level
        self._set_cql_runner()
        self.journal_writer: JournalWriter = JournalWriter(journal_writer)
        self.objstorage: ObjStorage = ObjStorage(objstorage)
        self._allow_overwrite = allow_overwrite

        if directory_entries_insert_algo not in DIRECTORY_ENTRIES_INSERT_ALGOS:
            raise ValueError(
                f"directory_entries_insert_algo must be one of: "
                f"{', '.join(DIRECTORY_ENTRIES_INSERT_ALGOS)}"
            )
        self._directory_entries_insert_algo = directory_entries_insert_algo

    def _set_cql_runner(self):
        """Used by tests when they need to reset the CqlRunner"""
        self._cql_runner: CqlRunner = CqlRunner(
            self._hosts, self._keyspace, self._port, self._consistency_level
        )

    @timed
    def check_config(self, *, check_write: bool) -> bool:
        self._cql_runner.check_read()

        return True

    def _content_get_from_hashes(self, algo, hashes: List[bytes]) -> Iterable:
        """From the name of a hash algorithm and a value of that hash,
        looks up the "hash -> token" secondary table (content_by_{algo})
        to get tokens.
        Then, looks up the main table (content) to get all contents with
        that token, and filters out contents whose hash doesn't match."""
        found_tokens = self._cql_runner.content_get_tokens_from_single_algo(
            algo, hashes
        )

        for token in found_tokens:
            assert isinstance(token, int), found_tokens
            # Query the main table ('content').
            res = self._cql_runner.content_get_from_token(token)

            for row in res:
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
                        collisions.append(content.hashes())
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

    @timed
    @process_metrics
    def content_add(self, content: List[Content]) -> Dict[str, int]:
        to_add = {
            (c.sha1, c.sha1_git, c.sha256, c.blake2s256): c for c in content
        }.values()
        contents = [attr.evolve(c, ctime=now()) for c in to_add]
        return self._content_add(list(contents), with_data=True)

    @timed
    def content_update(
        self, contents: List[Dict[str, Any]], keys: List[str] = []
    ) -> None:
        raise NotImplementedError(
            "content_update is not supported by the Cassandra backend"
        )

    @timed
    @process_metrics
    def content_add_metadata(self, content: List[Content]) -> Dict[str, int]:
        return self._content_add(content, with_data=False)

    @timed
    def content_get_data(self, content: Sha1) -> Optional[bytes]:
        # FIXME: Make this method support slicing the `data`
        return self.objstorage.content_get(content)

    @timed
    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Content]:
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

        rows = self._cql_runner.content_get_token_range(
            range_start, range_end, limit + 1
        )
        contents = []
        for counter, (tok, row) in enumerate(rows):
            if row.status == "absent":
                continue
            row_d = row.to_dict()
            if counter >= limit:
                next_page_token = str(tok)
                break
            row_d.pop("ctime")
            contents.append(Content(**row_d))

        assert len(contents) <= limit
        return PagedResult(results=contents, next_page_token=next_page_token)

    @timed
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

    @timed
    def content_find(self, content: Dict[str, Any]) -> List[Content]:
        return self._content_find_many([content])

    def _content_find_many(self, contents: List[Dict[str, Any]]) -> List[Content]:
        # Find an algorithm that is common to all the requested contents.
        # It will be used to do an initial filtering efficiently.
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
            common_algo, [content[common_algo] for content in contents]
        )
        for row in rows:
            # Re-check all the hashes, in case of collisions (either of the
            # hash of the partition key, or the hashes in it)
            for content in contents:
                for algo in HASH_ALGORITHMS:
                    if content.get(algo) and getattr(row, algo) != content[algo]:
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

    @timed
    def content_missing(
        self, contents: List[Dict[str, Any]], key_hash: str = "sha1"
    ) -> Iterable[bytes]:
        if key_hash not in DEFAULT_ALGORITHMS:
            raise StorageArgumentException(
                "key_hash should be one of {','.join(DEFAULT_ALGORITHMS)}"
            )

        contents_with_all_hashes = []
        contents_with_missing_hashes = []
        for content in contents:
            if DEFAULT_ALGORITHMS <= set(content):
                contents_with_all_hashes.append(content)
            else:
                contents_with_missing_hashes.append(content)

        # These contents can be queried efficiently directly in the main table
        for content in self._cql_runner.content_missing_from_hashes(
            contents_with_all_hashes
        ):
            yield content[key_hash]

        # For these, we need the expensive index lookups + main table.
        for content in contents_with_missing_hashes:
            res = self.content_find(content)
            if not res:
                yield content[key_hash]

    @timed
    def content_missing_per_sha1(self, contents: List[bytes]) -> Iterable[bytes]:
        return self.content_missing([{"sha1": c} for c in contents])

    @timed
    def content_missing_per_sha1_git(
        self, contents: List[Sha1Git]
    ) -> Iterable[Sha1Git]:
        return self.content_missing(
            [{"sha1_git": c} for c in contents], key_hash="sha1_git"
        )

    @timed
    def content_get_random(self) -> Sha1Git:
        content = self._cql_runner.content_get_random()
        assert content, "Could not find any content"
        return content.sha1_git

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

    @timed
    @process_metrics
    def skipped_content_add(self, content: List[SkippedContent]) -> Dict[str, int]:
        contents = [attr.evolve(c, ctime=now()) for c in content]
        return self._skipped_content_add(contents)

    @timed
    def skipped_content_missing(
        self, contents: List[Dict[str, Any]]
    ) -> Iterable[Dict[str, Any]]:
        for content in contents:
            if not self._cql_runner.skipped_content_get_from_pk(content):
                yield {algo: content[algo] for algo in DEFAULT_ALGORITHMS}

    @timed
    @process_metrics
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
            self._cql_runner.directory_add_one(DirectoryRow(id=directory.id))

        return {"directory:add": len(directories)}

    @timed
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
                tokens = list(
                    self._cql_runner.skipped_content_get_tokens_from_single_hash(
                        "sha1_git", ret["target"]
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

    @timed
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

    @timed
    def directory_ls(
        self, directory: Sha1Git, recursive: bool = False
    ) -> Iterable[Dict[str, Any]]:
        yield from self._directory_ls(directory, recursive)

    @timed
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

    @timed
    def directory_get_random(self) -> Sha1Git:
        directory = self._cql_runner.directory_get_random()
        assert directory, "Could not find any directory"
        return directory.id

    @timed
    @process_metrics
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
                for (rank, parent) in enumerate(revision.parents):
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

    @timed
    def revision_missing(self, revisions: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.revision_missing(revisions)

    @timed
    def revision_get(self, revision_ids: List[Sha1Git]) -> List[Optional[Revision]]:
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
        Iterable[Dict[str, Any]], Iterable[Tuple[Sha1Git, Tuple[Sha1Git, ...]]],
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

    @timed
    def revision_log(
        self, revisions: List[Sha1Git], limit: Optional[int] = None
    ) -> Iterable[Optional[Dict[str, Any]]]:
        seen: Set[Sha1Git] = set()
        yield from self._get_parent_revs(revisions, seen, limit, False)

    @timed
    def revision_shortlog(
        self, revisions: List[Sha1Git], limit: Optional[int] = None
    ) -> Iterable[Optional[Tuple[Sha1Git, Tuple[Sha1Git, ...]]]]:
        seen: Set[Sha1Git] = set()
        yield from self._get_parent_revs(revisions, seen, limit, True)

    @timed
    def revision_get_random(self) -> Sha1Git:
        revision = self._cql_runner.revision_get_random()
        assert revision, "Could not find any revision"
        return revision.id

    @timed
    @process_metrics
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

    @timed
    def release_missing(self, releases: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.release_missing(releases)

    @timed
    def release_get(self, releases: List[Sha1Git]) -> List[Optional[Release]]:
        rows = self._cql_runner.release_get(releases)
        rels: Dict[Sha1Git, Release] = {}
        for row in rows:
            release = converters.release_from_db(row)
            rels[row.id] = release

        return [rels.get(rel_id) for rel_id in releases]

    @timed
    def release_get_random(self) -> Sha1Git:
        release = self._cql_runner.release_get_random()
        assert release, "Could not find any release"
        return release.id

    @timed
    @process_metrics
    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict[str, int]:
        if not self._allow_overwrite:
            to_add = {s.id: s for s in snapshots}.values()
            missing = self._cql_runner.snapshot_missing([snp.id for snp in to_add])
            snapshots = [snp for snp in snapshots if snp.id in missing]

        for snapshot in snapshots:
            self.journal_writer.snapshot_add([snapshot])

            # Add branches
            for (branch_name, branch) in snapshot.branches.items():
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

    @timed
    def snapshot_missing(self, snapshots: List[Sha1Git]) -> Iterable[Sha1Git]:
        return self._cql_runner.snapshot_missing(snapshots)

    @timed
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

    @timed
    def snapshot_count_branches(
        self, snapshot_id: Sha1Git, branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Optional[Dict[Optional[str], int]]:
        if self._cql_runner.snapshot_missing([snapshot_id]):
            # Makes sure we don't fetch branches for a snapshot that is
            # being added.
            return None

        return self._cql_runner.snapshot_count_branches(
            snapshot_id, branch_name_exclude_prefix
        )

    @timed
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
            last_branch = branches.pop(-1).name
        else:
            last_branch = None

        return PartialBranches(
            id=snapshot_id,
            branches={
                branch.name: None
                if branch.target is None
                else SnapshotBranch(
                    target=branch.target, target_type=TargetType(branch.target_type)
                )
                for branch in branches
            },
            next_branch=last_branch,
        )

    @timed
    def snapshot_get_random(self) -> Sha1Git:
        snapshot = self._cql_runner.snapshot_get_random()
        assert snapshot, "Could not find any snapshot"
        return snapshot.id

    @timed
    def object_find_by_sha1_git(self, ids: List[Sha1Git]) -> Dict[Sha1Git, List[Dict]]:
        results: Dict[Sha1Git, List[Dict]] = {id_: [] for id_ in ids}
        missing_ids = set(ids)

        # Mind the order, revision is the most likely one for a given ID,
        # so we check revisions first.
        queries: List[Tuple[str, Callable[[List[Sha1Git]], List[Sha1Git]]]] = [
            ("revision", self._cql_runner.revision_missing),
            ("release", self._cql_runner.release_missing),
            ("content", self._cql_runner.content_missing_by_sha1_git),
            ("directory", self._cql_runner.directory_missing),
        ]

        for (object_type, query_fn) in queries:
            found_ids = missing_ids - set(query_fn(list(missing_ids)))
            for sha1_git in found_ids:
                results[sha1_git].append(
                    {"sha1_git": sha1_git, "type": object_type,}
                )
                missing_ids.remove(sha1_git)

            if not missing_ids:
                # We found everything, skipping the next queries.
                break

        return results

    @timed
    def origin_get(self, origins: List[str]) -> Iterable[Optional[Origin]]:
        return [self.origin_get_one(origin) for origin in origins]

    @timed
    def origin_get_one(self, origin_url: str) -> Optional[Origin]:
        """Given an origin url, return the origin if it exists, None otherwise

        """
        rows = list(self._cql_runner.origin_get_by_url(origin_url))
        if rows:
            assert len(rows) == 1
            return Origin(url=rows[0].url)
        else:
            return None

    @timed
    def origin_get_by_sha1(self, sha1s: List[bytes]) -> List[Optional[Dict[str, Any]]]:
        results = []
        for sha1 in sha1s:
            rows = list(self._cql_runner.origin_get_by_sha1(sha1))
            origin = {"url": rows[0].url} if rows else None
            results.append(origin)
        return results

    @timed
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
        for (tok, row) in self._cql_runner.origin_list(start_token, limit + 1):
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

    @timed
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

    @timed
    def origin_count(
        self, url_pattern: str, regexp: bool = False, with_visit: bool = False
    ) -> int:
        raise NotImplementedError(
            "The Cassandra backend does not implement origin_count"
        )

    @timed
    @process_metrics
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

    @timed
    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get_one(visit.origin)
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        nb_visits = 0
        for visit in visits:
            nb_visits += 1
            if visit.visit:
                # Set origin.next_visit_id = max(origin.next_visit_id, visit.visit+1)
                # so the next loader run does not reuse the id.
                self._cql_runner.origin_bump_next_visit_id(visit.origin, visit.visit)
            else:
                visit_id = self._cql_runner.origin_generate_unique_visit_id(
                    visit.origin
                )
                visit = attr.evolve(visit, visit=visit_id)
            self.journal_writer.origin_visit_add([visit])
            self._cql_runner.origin_visit_add_one(OriginVisitRow(**visit.to_dict()))
            assert visit.visit is not None
            all_visits.append(visit)
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
        send_metric("origin_visit:add", count=nb_visits, method_name="origin_visit")
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

    @timed
    @process_metrics
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
        """Retrieve the latest visit status information for the origin visit object.

        """
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

    @timed
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

    @timed
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

    @timed
    def origin_visit_find_by_date(
        self, origin: str, visit_date: datetime.datetime
    ) -> Optional[OriginVisit]:
        # Iterator over all the visits of the origin
        # This should be ok for now, as there aren't too many visits
        # per origin.
        rows = list(self._cql_runner.origin_visit_get_all(origin))

        def key(visit):
            dt = visit.date.replace(tzinfo=datetime.timezone.utc) - visit_date
            return (abs(dt), -visit.visit)

        if rows:
            return converters.row_to_visit(min(rows, key=key))
        return None

    @timed
    def origin_visit_get_by(self, origin: str, visit: int) -> Optional[OriginVisit]:
        row = self._cql_runner.origin_visit_get_one(origin, visit)
        if row:
            return converters.row_to_visit(row)
        return None

    @timed
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
        # TODO: Do not fetch all visits
        rows = self._cql_runner.origin_visit_get_all(origin)
        latest_visit = None
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

                # updated_visit is a candidate
                if latest_visit is not None:
                    if updated_visit["date"] < latest_visit["date"]:
                        continue
                    assert (
                        updated_visit["visit"] >= latest_visit["visit"]
                    ), "Cassandra returned visits not ordered by increasing visit id."

                latest_visit = updated_visit

        if latest_visit is None:
            return None
        return OriginVisit(
            origin=latest_visit["origin"],
            visit=latest_visit["visit"],
            date=latest_visit["date"],
            type=latest_visit["type"],
        )

    @timed
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

    @timed
    def origin_visit_status_get_random(self, type: str) -> Optional[OriginVisitStatus]:
        back_in_the_day = now() - datetime.timedelta(weeks=12)  # 3 months back

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

    @timed
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

    @timed
    def refresh_stat_counters(self):
        pass

    @timed
    @process_metrics
    def raw_extrinsic_metadata_add(
        self, metadata: List[RawExtrinsicMetadata]
    ) -> Dict[str, int]:
        self.journal_writer.raw_extrinsic_metadata_add(metadata)
        counter = Counter[ExtendedObjectType]()
        for metadata_entry in metadata:
            if not self._cql_runner.metadata_authority_get(
                metadata_entry.authority.type.value, metadata_entry.authority.url
            ):
                raise StorageArgumentException(
                    f"Unknown authority {metadata_entry.authority}"
                )
            if not self._cql_runner.metadata_fetcher_get(
                metadata_entry.fetcher.name, metadata_entry.fetcher.version
            ):
                raise StorageArgumentException(
                    f"Unknown fetcher {metadata_entry.fetcher}"
                )

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
                    origin=metadata_entry.origin,
                    visit=metadata_entry.visit,
                    snapshot=map_optional(str, metadata_entry.snapshot),
                    release=map_optional(str, metadata_entry.release),
                    revision=map_optional(str, metadata_entry.revision),
                    path=metadata_entry.path,
                    directory=map_optional(str, metadata_entry.directory),
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

    @timed
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
                str(target), authority.type.value, authority.url, after_date, id_,
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
                msgpack_dumps((last_result.discovery_date, last_result.id,))
            ).decode()
        else:
            next_page_token = None

        return PagedResult(next_page_token=next_page_token, results=results,)

    @timed
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

    @timed
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

    @timed
    @process_metrics
    def metadata_fetcher_add(self, fetchers: List[MetadataFetcher]) -> Dict[str, int]:
        self.journal_writer.metadata_fetcher_add(fetchers)
        for fetcher in fetchers:
            self._cql_runner.metadata_fetcher_add(
                MetadataFetcherRow(name=fetcher.name, version=fetcher.version,)
            )
        return {"metadata_fetcher:add": len(fetchers)}

    @timed
    def metadata_fetcher_get(
        self, name: str, version: str
    ) -> Optional[MetadataFetcher]:
        fetcher = self._cql_runner.metadata_fetcher_get(name, version)
        if fetcher:
            return MetadataFetcher(name=fetcher.name, version=fetcher.version,)
        else:
            return None

    @timed
    @process_metrics
    def metadata_authority_add(
        self, authorities: List[MetadataAuthority]
    ) -> Dict[str, int]:
        self.journal_writer.metadata_authority_add(authorities)
        for authority in authorities:
            self._cql_runner.metadata_authority_add(
                MetadataAuthorityRow(url=authority.url, type=authority.type.value,)
            )
        return {"metadata_authority:add": len(authorities)}

    @timed
    def metadata_authority_get(
        self, type: MetadataAuthorityType, url: str
    ) -> Optional[MetadataAuthority]:
        authority = self._cql_runner.metadata_authority_get(type.value, url)
        if authority:
            return MetadataAuthority(
                type=MetadataAuthorityType(authority.type), url=authority.url,
            )
        else:
            return None

    # ExtID tables
    @timed
    @process_metrics
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
                target_type=target_type, target=target, target_token=token,
            )
            self._cql_runner.extid_index_add_one(indexrow)
            insertion_finalizer()
            inserted += 1
        return {"extid:add": inserted}

    @timed
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
                        object_type=extidrow.target_type, object_id=extidrow.target,
                    ),
                )
                for extidrow in extidrows
            )
        return result

    @timed
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

    # Misc
    def clear_buffers(self, object_types: Sequence[str] = ()) -> None:
        """Do nothing

        """
        return None

    def flush(self, object_types: Sequence[str] = ()) -> Dict[str, int]:
        return {}
