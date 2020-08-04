# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import datetime
import itertools
import json
import random
import re
from typing import Any, Dict, List, Iterable, Optional, Tuple, Union

import attr

from swh.core.api.serializers import msgpack_loads, msgpack_dumps
from swh.model.identifiers import parse_swhid, SWHID
from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import (
    Revision,
    Release,
    Directory,
    DirectoryEntry,
    Content,
    SkippedContent,
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    Origin,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    RawExtrinsicMetadata,
    Sha1Git,
)
from swh.storage.interface import ListOrder, PagedResult, VISIT_STATUSES
from swh.storage.objstorage import ObjStorage
from swh.storage.writer import JournalWriter
from swh.storage.utils import map_optional, now

from ..exc import StorageArgumentException, HashCollision
from .common import TOKEN_BEGIN, TOKEN_END
from . import converters
from .cql import CqlRunner
from .schema import HASH_ALGORITHMS


# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


class CassandraStorage:
    def __init__(self, hosts, keyspace, objstorage, port=9042, journal_writer=None):
        self._cql_runner = CqlRunner(hosts, keyspace, port)
        self.journal_writer = JournalWriter(journal_writer)
        self.objstorage = ObjStorage(objstorage)

    def check_config(self, *, check_write):
        self._cql_runner.check_read()

        return True

    def _content_get_from_hash(self, algo, hash_) -> Iterable:
        """From the name of a hash algorithm and a value of that hash,
        looks up the "hash -> token" secondary table (content_by_{algo})
        to get tokens.
        Then, looks up the main table (content) to get all contents with
        that token, and filters out contents whose hash doesn't match."""
        found_tokens = self._cql_runner.content_get_tokens_from_single_hash(algo, hash_)

        for token in found_tokens:
            # Query the main table ('content').
            res = self._cql_runner.content_get_from_token(token)

            for row in res:
                # re-check the the hash (in case of murmur3 collision)
                if getattr(row, algo) == hash_:
                    yield row

    def _content_add(self, contents: List[Content], with_data: bool) -> Dict:
        # Filter-out content already in the database.
        contents = [
            c for c in contents if not self._cql_runner.content_get_from_pk(c.to_dict())
        ]

        self.journal_writer.content_add(contents)

        if with_data:
            # First insert to the objstorage, if the endpoint is
            # `content_add` (as opposed to `content_add_metadata`).
            # TODO: this should probably be done in concurrently to inserting
            # in index tables (but still before the main table; so an entry is
            # only added to the main table after everything else was
            # successfully inserted.
            summary = self.objstorage.content_add(
                c for c in contents if c.status != "absent"
            )
            content_add_bytes = summary["content:add:bytes"]

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
            for algo in {"sha1", "sha1_git"}:
                collisions = []
                # Get tokens of 'content' rows with the same value for
                # sha1/sha1_git
                rows = self._content_get_from_hash(algo, content.get_hash(algo))
                for row in rows:
                    if getattr(row, algo) != content.get_hash(algo):
                        # collision of token(partition key), ignore this
                        # row
                        continue

                    for algo in HASH_ALGORITHMS:
                        if getattr(row, algo) != content.get_hash(algo):
                            # This hash didn't match; discard the row.
                            collisions.append(
                                {algo: getattr(row, algo) for algo in HASH_ALGORITHMS}
                            )

                if collisions:
                    collisions.append(content.hashes())
                    raise HashCollision(algo, content.get_hash(algo), collisions)

            (token, insertion_finalizer) = self._cql_runner.content_add_prepare(content)

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

    def content_add(self, content: List[Content]) -> Dict:
        contents = [attr.evolve(c, ctime=now()) for c in content]
        return self._content_add(list(contents), with_data=True)

    def content_update(self, content, keys=[]):
        raise NotImplementedError(
            "content_update is not supported by the Cassandra backend"
        )

    def content_add_metadata(self, content: List[Content]) -> Dict:
        return self._content_add(content, with_data=False)

    def content_get(self, content):
        if len(content) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise StorageArgumentException(
                "Sending at most %s contents." % BULK_BLOCK_CONTENT_LEN_MAX
            )
        yield from self.objstorage.content_get(content)

    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        limit: int = 1000,
        page_token: str = None,
    ):
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

        # Get the first rows of the range
        rows = self._cql_runner.content_get_token_range(range_start, range_end, limit)
        rows = list(rows)

        if len(rows) == limit:
            next_page_token: Optional[str] = str(rows[-1].tok + 1)
        else:
            next_page_token = None

        return {
            "contents": [row._asdict() for row in rows if row.status != "absent"],
            "next_page_token": next_page_token,
        }

    def content_get_metadata(self, contents: List[bytes]) -> Dict[bytes, List[Dict]]:
        result: Dict[bytes, List[Dict]] = {sha1: [] for sha1 in contents}
        for sha1 in contents:
            # Get all (sha1, sha1_git, sha256, blake2s256) whose sha1
            # matches the argument, from the index table ('content_by_sha1')
            for row in self._content_get_from_hash("sha1", sha1):
                content_metadata = row._asdict()
                content_metadata.pop("ctime")
                result[content_metadata["sha1"]].append(content_metadata)
        return result

    def content_find(self, content: Dict[str, Any]) -> List[Content]:
        # Find an algorithm that is common to all the requested contents.
        # It will be used to do an initial filtering efficiently.
        filter_algos = list(set(content).intersection(HASH_ALGORITHMS))
        if not filter_algos:
            raise StorageArgumentException(
                "content keys must contain at least one "
                f"of: {', '.join(sorted(HASH_ALGORITHMS))}"
            )
        common_algo = filter_algos[0]

        results = []
        rows = self._content_get_from_hash(common_algo, content[common_algo])
        for row in rows:
            # Re-check all the hashes, in case of collisions (either of the
            # hash of the partition key, or the hashes in it)
            for algo in HASH_ALGORITHMS:
                if content.get(algo) and getattr(row, algo) != content[algo]:
                    # This hash didn't match; discard the row.
                    break
            else:
                # All hashes match, keep this row.
                row_d = row._asdict()
                row_d["ctime"] = row.ctime.replace(tzinfo=datetime.timezone.utc)
                results.append(Content(**row_d))
        return results

    def content_missing(self, content, key_hash="sha1"):
        for cont in content:
            res = self.content_find(cont)
            if not res:
                yield cont[key_hash]

    def content_missing_per_sha1(self, contents):
        return self.content_missing([{"sha1": c for c in contents}])

    def content_missing_per_sha1_git(self, contents):
        return self.content_missing(
            [{"sha1_git": c for c in contents}], key_hash="sha1_git"
        )

    def content_get_random(self) -> Sha1Git:
        return self._cql_runner.content_get_random().sha1_git

    def _skipped_content_get_from_hash(self, algo, hash_) -> Iterable:
        """From the name of a hash algorithm and a value of that hash,
        looks up the "hash -> token" secondary table
        (skipped_content_by_{algo}) to get tokens.
        Then, looks up the main table (content) to get all contents with
        that token, and filters out contents whose hash doesn't match."""
        found_tokens = self._cql_runner.skipped_content_get_tokens_from_single_hash(
            algo, hash_
        )

        for token in found_tokens:
            # Query the main table ('content').
            res = self._cql_runner.skipped_content_get_from_token(token)

            for row in res:
                # re-check the the hash (in case of murmur3 collision)
                if getattr(row, algo) == hash_:
                    yield row

    def _skipped_content_add(self, contents: List[SkippedContent]) -> Dict:
        # Filter-out content already in the database.
        contents = [
            c
            for c in contents
            if not self._cql_runner.skipped_content_get_from_pk(c.to_dict())
        ]

        self.journal_writer.skipped_content_add(contents)

        for content in contents:
            # Compute token of the row in the main table
            (token, insertion_finalizer) = self._cql_runner.skipped_content_add_prepare(
                content
            )

            # Then add to index tables
            for algo in HASH_ALGORITHMS:
                self._cql_runner.skipped_content_index_add_one(algo, content, token)

            # Then to the main table
            insertion_finalizer()

        return {"skipped_content:add": len(contents)}

    def skipped_content_add(self, content: List[SkippedContent]) -> Dict:
        contents = [attr.evolve(c, ctime=now()) for c in content]
        return self._skipped_content_add(contents)

    def skipped_content_missing(self, contents):
        for content in contents:
            if not self._cql_runner.skipped_content_get_from_pk(content):
                yield {algo: content[algo] for algo in DEFAULT_ALGORITHMS}

    def directory_add(self, directories: List[Directory]) -> Dict:
        # Filter out directories that are already inserted.
        missing = self.directory_missing([dir_.id for dir_ in directories])
        directories = [dir_ for dir_ in directories if dir_.id in missing]

        self.journal_writer.directory_add(directories)

        for directory in directories:
            # Add directory entries to the 'directory_entry' table
            for entry in directory.entries:
                self._cql_runner.directory_entry_add_one(
                    {**entry.to_dict(), "directory_id": directory.id}
                )

            # Add the directory *after* adding all the entries, so someone
            # calling snapshot_get_branch in the meantime won't end up
            # with half the entries.
            self._cql_runner.directory_add_one(directory.id)

        return {"directory:add": len(missing)}

    def directory_missing(self, directories):
        return self._cql_runner.directory_missing(directories)

    def _join_dentry_to_content(self, dentry: DirectoryEntry) -> Dict[str, Any]:
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
            contents = self.content_find({"sha1_git": ret["target"]})
            if contents:
                content = contents[0]
                for key in keys:
                    ret[key] = getattr(content, key)
        return ret

    def _directory_ls(self, directory_id, recursive, prefix=b""):
        if self.directory_missing([directory_id]):
            return
        rows = list(self._cql_runner.directory_entry_get([directory_id]))

        for row in rows:
            # Build and yield the directory entry dict
            entry = row._asdict()
            del entry["directory_id"]
            entry = DirectoryEntry.from_dict(entry)
            ret = self._join_dentry_to_content(entry)
            ret["name"] = prefix + ret["name"]
            ret["dir_id"] = directory_id
            yield ret

            if recursive and ret["type"] == "dir":
                yield from self._directory_ls(
                    ret["target"], True, prefix + ret["name"] + b"/"
                )

    def directory_entry_get_by_path(self, directory, paths):
        return self._directory_entry_get_by_path(directory, paths, b"")

    def _directory_entry_get_by_path(self, directory, paths, prefix):
        if not paths:
            return

        contents = list(self.directory_ls(directory))

        if not contents:
            return

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
            return

        return self._directory_entry_get_by_path(
            first_item["target"], paths[1:], prefix + paths[0] + b"/"
        )

    def directory_ls(self, directory, recursive=False):
        yield from self._directory_ls(directory, recursive)

    def directory_get_random(self) -> Sha1Git:
        return self._cql_runner.directory_get_random().id

    def revision_add(self, revisions: List[Revision]) -> Dict:
        # Filter-out revisions already in the database
        missing = self.revision_missing([rev.id for rev in revisions])
        revisions = [rev for rev in revisions if rev.id in missing]
        self.journal_writer.revision_add(revisions)

        for revision in revisions:
            revobject = converters.revision_to_db(revision)
            if revobject:
                # Add parents first
                for (rank, parent) in enumerate(revobject["parents"]):
                    self._cql_runner.revision_parent_add_one(
                        revobject["id"], rank, parent
                    )

                # Then write the main revision row.
                # Writing this after all parents were written ensures that
                # read endpoints don't return a partial view while writing
                # the parents
                self._cql_runner.revision_add_one(revobject)

        return {"revision:add": len(revisions)}

    def revision_missing(self, revisions):
        return self._cql_runner.revision_missing(revisions)

    def revision_get(self, revisions):
        rows = self._cql_runner.revision_get(revisions)
        revs = {}
        for row in rows:
            # TODO: use a single query to get all parents?
            # (it might have lower latency, but requires more code and more
            # bandwidth, because revision id would be part of each returned
            # row)
            parent_rows = self._cql_runner.revision_parent_get(row.id)
            # parent_rank is the clustering key, so results are already
            # sorted by rank.
            parents = tuple(row.parent_id for row in parent_rows)
            rev = converters.revision_from_db(row, parents=parents)
            revs[rev.id] = rev.to_dict()

        for rev_id in revisions:
            yield revs.get(rev_id)

    def _get_parent_revs(self, rev_ids, seen, limit, short):
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
            rows = self._cql_runner.revision_get_ids(rev_ids)
        else:
            rows = self._cql_runner.revision_get(rev_ids)

        for row in rows:
            # TODO: use a single query to get all parents?
            # (it might have less latency, but requires less code and more
            # bandwidth (because revision id would be part of each returned
            # row)
            parent_rows = self._cql_runner.revision_parent_get(row.id)

            # parent_rank is the clustering key, so results are already
            # sorted by rank.
            parents = tuple(row.parent_id for row in parent_rows)

            if short:
                yield (row.id, parents)
            else:
                rev = converters.revision_from_db(row, parents=parents)
                yield rev.to_dict()
            yield from self._get_parent_revs(parents, seen, limit, short)

    def revision_log(self, revisions, limit=None):
        seen = set()
        yield from self._get_parent_revs(revisions, seen, limit, False)

    def revision_shortlog(self, revisions, limit=None):
        seen = set()
        yield from self._get_parent_revs(revisions, seen, limit, True)

    def revision_get_random(self) -> Sha1Git:
        return self._cql_runner.revision_get_random().id

    def release_add(self, releases: List[Release]) -> Dict:
        to_add = []
        for rel in releases:
            if rel not in to_add:
                to_add.append(rel)
        missing = set(self.release_missing([rel.id for rel in to_add]))
        to_add = [rel for rel in to_add if rel.id in missing]

        self.journal_writer.release_add(to_add)

        for release in to_add:
            if release:
                self._cql_runner.release_add_one(converters.release_to_db(release))

        return {"release:add": len(to_add)}

    def release_missing(self, releases):
        return self._cql_runner.release_missing(releases)

    def release_get(self, releases):
        rows = self._cql_runner.release_get(releases)
        rels = {}
        for row in rows:
            release = converters.release_from_db(row)
            rels[row.id] = release.to_dict()

        for rel_id in releases:
            yield rels.get(rel_id)

    def release_get_random(self) -> Sha1Git:
        return self._cql_runner.release_get_random().id

    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict:
        missing = self._cql_runner.snapshot_missing([snp.id for snp in snapshots])
        snapshots = [snp for snp in snapshots if snp.id in missing]

        for snapshot in snapshots:
            self.journal_writer.snapshot_add([snapshot])

            # Add branches
            for (branch_name, branch) in snapshot.branches.items():
                if branch is None:
                    target_type = None
                    target = None
                else:
                    target_type = branch.target_type.value
                    target = branch.target
                self._cql_runner.snapshot_branch_add_one(
                    {
                        "snapshot_id": snapshot.id,
                        "name": branch_name,
                        "target_type": target_type,
                        "target": target,
                    }
                )

            # Add the snapshot *after* adding all the branches, so someone
            # calling snapshot_get_branch in the meantime won't end up
            # with half the branches.
            self._cql_runner.snapshot_add_one(snapshot.id)

        return {"snapshot:add": len(snapshots)}

    def snapshot_missing(self, snapshots):
        return self._cql_runner.snapshot_missing(snapshots)

    def snapshot_get(self, snapshot_id):
        return self.snapshot_get_branches(snapshot_id)

    def snapshot_get_by_origin_visit(self, origin, visit):
        visit_status = self.origin_visit_status_get_latest(
            origin, visit, require_snapshot=True
        )
        if not visit_status:
            return None
        return self.snapshot_get(visit_status.snapshot)

    def snapshot_count_branches(self, snapshot_id):
        if self._cql_runner.snapshot_missing([snapshot_id]):
            # Makes sure we don't fetch branches for a snapshot that is
            # being added.
            return None
        rows = list(self._cql_runner.snapshot_count_branches(snapshot_id))
        assert len(rows) == 1
        (nb_none, counts) = rows[0].counts
        counts = dict(counts)
        if nb_none:
            counts[None] = nb_none
        return counts

    def snapshot_get_branches(
        self, snapshot_id, branches_from=b"", branches_count=1000, target_types=None
    ):
        if self._cql_runner.snapshot_missing([snapshot_id]):
            # Makes sure we don't fetch branches for a snapshot that is
            # being added.
            return None

        branches = []
        while len(branches) < branches_count + 1:
            new_branches = list(
                self._cql_runner.snapshot_branch_get(
                    snapshot_id, branches_from, branches_count + 1
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

            branches.extend(new_branches_filtered)

            if len(new_branches) < branches_count + 1:
                break

        if len(branches) > branches_count:
            last_branch = branches.pop(-1).name
        else:
            last_branch = None

        branches = {
            branch.name: {"target": branch.target, "target_type": branch.target_type,}
            if branch.target
            else None
            for branch in branches
        }

        return {
            "id": snapshot_id,
            "branches": branches,
            "next_branch": last_branch,
        }

    def snapshot_get_random(self) -> Sha1Git:
        return self._cql_runner.snapshot_get_random().id

    def object_find_by_sha1_git(self, ids):
        results = {id_: [] for id_ in ids}
        missing_ids = set(ids)

        # Mind the order, revision is the most likely one for a given ID,
        # so we check revisions first.
        queries = [
            ("revision", self._cql_runner.revision_missing),
            ("release", self._cql_runner.release_missing),
            ("content", self._cql_runner.content_missing_by_sha1_git),
            ("directory", self._cql_runner.directory_missing),
        ]

        for (object_type, query_fn) in queries:
            found_ids = missing_ids - set(query_fn(missing_ids))
            for sha1_git in found_ids:
                results[sha1_git].append(
                    {"sha1_git": sha1_git, "type": object_type,}
                )
                missing_ids.remove(sha1_git)

            if not missing_ids:
                # We found everything, skipping the next queries.
                break

        return results

    def origin_get(self, origins: List[str]) -> Iterable[Optional[Origin]]:
        return [self.origin_get_one(origin) for origin in origins]

    def origin_get_one(self, origin_url: str) -> Optional[Origin]:
        """Given an origin url, return the origin if it exists, None otherwise

        """
        rows = list(self._cql_runner.origin_get_by_url(origin_url))
        if rows:
            assert len(rows) == 1
            return Origin(url=rows[0].url)
        else:
            return None

    def origin_get_by_sha1(self, sha1s):
        results = []
        for sha1 in sha1s:
            rows = self._cql_runner.origin_get_by_sha1(sha1)
            if rows:
                results.append({"url": rows.one().url})
            else:
                results.append(None)
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
        for row in self._cql_runner.origin_list(start_token, limit + 1):
            origins.append(Origin(url=row.url))
            # keep reference of the last id for pagination purposes
            last_id = row.tok

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
    ) -> PagedResult[Origin]:
        # TODO: remove this endpoint, swh-search should be used instead.
        next_page_token = None
        offset = int(page_token) if page_token else 0

        origins = self._cql_runner.origin_iter_all()
        if regexp:
            pat = re.compile(url_pattern)
            origins = [Origin(orig.url) for orig in origins if pat.search(orig.url)]
        else:
            origins = [Origin(orig.url) for orig in origins if url_pattern in orig.url]

        if with_visit:
            origins = [Origin(orig.url) for orig in origins if orig.next_visit_id > 1]

        origins = origins[offset : offset + limit + 1]
        if len(origins) > limit:
            # next offset
            next_page_token = str(offset + limit)
            # excluding that origin from the result to respect the limit size
            origins = origins[:limit]

        assert len(origins) <= limit
        return PagedResult(results=origins, next_page_token=next_page_token)

    def origin_add(self, origins: List[Origin]) -> Dict[str, int]:
        to_add = [ori for ori in origins if self.origin_get_one(ori.url) is None]
        self.journal_writer.origin_add(to_add)
        for origin in to_add:
            self._cql_runner.origin_add_one(origin)
        return {"origin:add": len(to_add)}

    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get_one(visit.origin)
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        nb_visits = 0
        for visit in visits:
            nb_visits += 1
            if not visit.visit:
                visit_id = self._cql_runner.origin_generate_unique_visit_id(
                    visit.origin
                )
                visit = attr.evolve(visit, visit=visit_id)
            self.journal_writer.origin_visit_add([visit])
            self._cql_runner.origin_visit_add_one(visit)
            assert visit.visit is not None
            all_visits.append(visit)
            self._origin_visit_status_add(
                OriginVisitStatus(
                    origin=visit.origin,
                    visit=visit.visit,
                    date=visit.date,
                    status="created",
                    snapshot=None,
                )
            )

        return all_visits

    def _origin_visit_status_add(self, visit_status: OriginVisitStatus) -> None:
        """Add an origin visit status"""
        self.journal_writer.origin_visit_status_add([visit_status])
        self._cql_runner.origin_visit_status_add_one(visit_status)

    def origin_visit_status_add(self, visit_statuses: List[OriginVisitStatus]) -> None:
        # First round to check existence (fail early if any is ko)
        for visit_status in visit_statuses:
            origin_url = self.origin_get_one(visit_status.origin)
            if not origin_url:
                raise StorageArgumentException(f"Unknown origin {visit_status.origin}")

        for visit_status in visit_statuses:
            self._origin_visit_status_add(visit_status)

    def _origin_visit_apply_last_status(self, visit: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve the latest visit status information for the origin visit.
        Then merge it with the visit and return it.

        """
        row = self._cql_runner.origin_visit_status_get_latest(
            visit["origin"], visit["visit"]
        )
        assert row is not None
        visit_status = converters.row_to_visit_status(row)
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
        }

    def _origin_visit_get_latest_status(self, visit: OriginVisit) -> OriginVisitStatus:
        """Retrieve the latest visit status information for the origin visit object.

        """
        row = self._cql_runner.origin_visit_status_get_latest(visit.origin, visit.visit)
        assert row is not None
        visit_status = converters.row_to_visit_status(row)
        return attr.evolve(visit_status, origin=visit.origin)

    @staticmethod
    def _format_origin_visit_row(visit):
        return {
            **visit._asdict(),
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
        visit_from = page_token and int(page_token)
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
        # TODO: Do not fetch all visits
        rows = self._cql_runner.origin_visit_get_all(origin)
        latest_visit = None
        for row in rows:
            visit = self._format_origin_visit_row(row)
            updated_visit = self._origin_visit_apply_last_status(visit)
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
                if updated_visit["visit"] < latest_visit["visit"]:
                    continue

            latest_visit = updated_visit

        if latest_visit is None:
            return None
        return OriginVisit(
            origin=latest_visit["origin"],
            visit=latest_visit["visit"],
            date=latest_visit["date"],
            type=latest_visit["type"],
        )

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
        rows = self._cql_runner.origin_visit_status_get(
            origin_url, visit, allowed_statuses, require_snapshot
        )
        # filtering is done python side as we cannot do it server side
        if allowed_statuses:
            rows = [row for row in rows if row.status in allowed_statuses]
        if require_snapshot:
            rows = [row for row in rows if row.snapshot is not None]
        if not rows:
            return None
        return converters.row_to_visit_status(rows[0])

    def origin_visit_status_get_random(
        self, type: str
    ) -> Optional[Tuple[OriginVisit, OriginVisitStatus]]:
        back_in_the_day = now() - datetime.timedelta(weeks=12)  # 3 months back

        # Random position to start iteration at
        start_token = random.randint(TOKEN_BEGIN, TOKEN_END)

        # Iterator over all visits, ordered by token(origins) then visit_id
        rows = self._cql_runner.origin_visit_iter(start_token)
        for row in rows:
            visit = converters.row_to_visit(row)
            visit_status = self._origin_visit_get_latest_status(visit)
            if visit.date > back_in_the_day and visit_status.status == "full":
                return visit, visit_status
        return None

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

    def raw_extrinsic_metadata_add(self, metadata: List[RawExtrinsicMetadata]) -> None:
        self.journal_writer.raw_extrinsic_metadata_add(metadata)
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
                self._cql_runner.raw_extrinsic_metadata_add(
                    type=metadata_entry.type.value,
                    id=str(metadata_entry.id),
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

    def raw_extrinsic_metadata_get(
        self,
        type: MetadataTargetType,
        id: Union[str, SWHID],
        authority: MetadataAuthority,
        after: Optional[datetime.datetime] = None,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
    ) -> PagedResult[RawExtrinsicMetadata]:
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
            (after_date, after_fetcher_name, after_fetcher_url) = msgpack_loads(
                base64.b64decode(page_token)
            )
            if after and after_date < after:
                raise StorageArgumentException(
                    "page_token is inconsistent with the value of 'after'."
                )
            entries = self._cql_runner.raw_extrinsic_metadata_get_after_date_and_fetcher(  # noqa
                str(id),
                authority.type.value,
                authority.url,
                after_date,
                after_fetcher_name,
                after_fetcher_url,
            )
        elif after is not None:
            entries = self._cql_runner.raw_extrinsic_metadata_get_after_date(
                str(id), authority.type.value, authority.url, after
            )
        else:
            entries = self._cql_runner.raw_extrinsic_metadata_get(
                str(id), authority.type.value, authority.url
            )

        if limit:
            entries = itertools.islice(entries, 0, limit + 1)

        results = []
        for entry in entries:
            discovery_date = entry.discovery_date.replace(tzinfo=datetime.timezone.utc)

            assert str(id) == entry.id

            result = RawExtrinsicMetadata(
                type=MetadataTargetType(entry.type),
                id=id,
                authority=MetadataAuthority(
                    type=MetadataAuthorityType(entry.authority_type),
                    url=entry.authority_url,
                ),
                fetcher=MetadataFetcher(
                    name=entry.fetcher_name, version=entry.fetcher_version,
                ),
                discovery_date=discovery_date,
                format=entry.format,
                metadata=entry.metadata,
                origin=entry.origin,
                visit=entry.visit,
                snapshot=map_optional(parse_swhid, entry.snapshot),
                release=map_optional(parse_swhid, entry.release),
                revision=map_optional(parse_swhid, entry.revision),
                path=entry.path,
                directory=map_optional(parse_swhid, entry.directory),
            )

            results.append(result)

        if len(results) > limit:
            results.pop()
            assert len(results) == limit
            last_result = results[-1]
            next_page_token: Optional[str] = base64.b64encode(
                msgpack_dumps(
                    (
                        last_result.discovery_date,
                        last_result.fetcher.name,
                        last_result.fetcher.version,
                    )
                )
            ).decode()
        else:
            next_page_token = None

        return PagedResult(next_page_token=next_page_token, results=results,)

    def metadata_fetcher_add(self, fetchers: List[MetadataFetcher]) -> None:
        self.journal_writer.metadata_fetcher_add(fetchers)
        for fetcher in fetchers:
            self._cql_runner.metadata_fetcher_add(
                fetcher.name,
                fetcher.version,
                json.dumps(map_optional(dict, fetcher.metadata)),
            )

    def metadata_fetcher_get(
        self, name: str, version: str
    ) -> Optional[MetadataFetcher]:
        fetcher = self._cql_runner.metadata_fetcher_get(name, version)
        if fetcher:
            return MetadataFetcher(
                name=fetcher.name,
                version=fetcher.version,
                metadata=json.loads(fetcher.metadata),
            )
        else:
            return None

    def metadata_authority_add(self, authorities: List[MetadataAuthority]) -> None:
        self.journal_writer.metadata_authority_add(authorities)
        for authority in authorities:
            self._cql_runner.metadata_authority_add(
                authority.url,
                authority.type.value,
                json.dumps(map_optional(dict, authority.metadata)),
            )

    def metadata_authority_get(
        self, type: MetadataAuthorityType, url: str
    ) -> Optional[MetadataAuthority]:
        authority = self._cql_runner.metadata_authority_get(type.value, url)
        if authority:
            return MetadataAuthority(
                type=MetadataAuthorityType(authority.type),
                url=authority.url,
                metadata=json.loads(authority.metadata),
            )
        else:
            return None

    def clear_buffers(self, object_types: Optional[List[str]] = None) -> None:
        """Do nothing

        """
        return None

    def flush(self, object_types: Optional[List[str]] = None) -> Dict:
        return {}
