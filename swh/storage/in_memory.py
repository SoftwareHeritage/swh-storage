# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import bisect
import collections
import copy
import datetime
import itertools
import random
import re

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
    Directory,
    Revision,
    Release,
    Snapshot,
    OriginVisit,
    OriginVisitStatus,
    Origin,
    SHA1_SIZE,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    RawExtrinsicMetadata,
    Sha1Git,
)
from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_bytes, hash_to_hex
from swh.storage.interface import ListOrder, PagedResult, VISIT_STATUSES
from swh.storage.objstorage import ObjStorage
from swh.storage.utils import now

from .converters import origin_url_to_sha1
from .exc import StorageArgumentException, HashCollision
from .utils import get_partition_bounds_bytes
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


class InMemoryStorage:
    def __init__(self, journal_writer=None):

        self.reset()
        self.journal_writer = JournalWriter(journal_writer)

    def reset(self):
        self._contents = {}
        self._content_indexes = defaultdict(lambda: defaultdict(set))
        self._skipped_contents = {}
        self._skipped_content_indexes = defaultdict(lambda: defaultdict(set))
        self._directories = {}
        self._revisions = {}
        self._releases = {}
        self._snapshots = {}
        self._origins = {}
        self._origins_by_sha1 = {}
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

    def check_config(self, *, check_write):
        return True

    def _content_add(self, contents: List[Content], with_data: bool) -> Dict:
        self.journal_writer.content_add(contents)

        content_add = 0
        if with_data:
            summary = self.objstorage.content_add(
                c for c in contents if c.status != "absent"
            )
            content_add_bytes = summary["content:add:bytes"]

        for content in contents:
            key = self._content_key(content)
            if key in self._contents:
                continue
            for algorithm in DEFAULT_ALGORITHMS:
                hash_ = content.get_hash(algorithm)
                if hash_ in self._content_indexes[algorithm] and (
                    algorithm not in {"blake2s256", "sha256"}
                ):
                    colliding_content_hashes = []
                    # Add the already stored contents
                    for content_hashes_set in self._content_indexes[algorithm][hash_]:
                        hashes = dict(content_hashes_set)
                        colliding_content_hashes.append(hashes)
                    # Add the new colliding content
                    colliding_content_hashes.append(content.hashes())
                    raise HashCollision(algorithm, hash_, colliding_content_hashes)
            for algorithm in DEFAULT_ALGORITHMS:
                hash_ = content.get_hash(algorithm)
                self._content_indexes[algorithm][hash_].add(key)
            self._objects[content.sha1_git].append(("content", content.sha1))
            self._contents[key] = content
            self._sorted_sha1s.add(content.sha1)
            self._contents[key] = attr.evolve(self._contents[key], data=None)
            content_add += 1

        summary = {
            "content:add": content_add,
        }
        if with_data:
            summary["content:add:bytes"] = content_add_bytes

        return summary

    def content_add(self, content: List[Content]) -> Dict:
        content = [attr.evolve(c, ctime=now()) for c in content]
        return self._content_add(content, with_data=True)

    def content_update(self, content, keys=[]):
        self.journal_writer.content_update(content)

        for cont_update in content:
            cont_update = cont_update.copy()
            sha1 = cont_update.pop("sha1")
            for old_key in self._content_indexes["sha1"][sha1]:
                old_cont = self._contents.pop(old_key)

                for algorithm in DEFAULT_ALGORITHMS:
                    hash_ = old_cont.get_hash(algorithm)
                    self._content_indexes[algorithm][hash_].remove(old_key)

                new_cont = attr.evolve(old_cont, **cont_update)
                new_key = self._content_key(new_cont)

                self._contents[new_key] = new_cont

                for algorithm in DEFAULT_ALGORITHMS:
                    hash_ = new_cont.get_hash(algorithm)
                    self._content_indexes[algorithm][hash_].add(new_key)

    def content_add_metadata(self, content: List[Content]) -> Dict:
        return self._content_add(content, with_data=False)

    def content_get(self, content):
        # FIXME: Make this method support slicing the `data`.
        if len(content) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise StorageArgumentException(
                "Sending at most %s contents." % BULK_BLOCK_CONTENT_LEN_MAX
            )
        yield from self.objstorage.content_get(content)

    def content_get_range(self, start, end, limit=1000):
        if limit is None:
            raise StorageArgumentException("limit should not be None")
        sha1s = (
            (sha1, content_key)
            for sha1 in self._sorted_sha1s.iter_from(start)
            for content_key in self._content_indexes["sha1"][sha1]
        )
        matched = []
        next_content = None
        for sha1, key in sha1s:
            if sha1 > end:
                break
            if len(matched) >= limit:
                next_content = sha1
                break
            matched.append(self._contents[key].to_dict())
        return {
            "contents": matched,
            "next": next_content,
        }

    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        limit: int = 1000,
        page_token: str = None,
    ):
        if limit is None:
            raise StorageArgumentException("limit should not be None")
        (start, end) = get_partition_bounds_bytes(
            partition_id, nb_partitions, SHA1_SIZE
        )
        if page_token:
            start = hash_to_bytes(page_token)
        if end is None:
            end = b"\xff" * SHA1_SIZE
        result = self.content_get_range(start, end, limit)
        result2 = {
            "contents": result["contents"],
            "next_page_token": None,
        }
        if result["next"]:
            result2["next_page_token"] = hash_to_hex(result["next"])
        return result2

    def content_get_metadata(self, contents: List[bytes]) -> Dict[bytes, List[Dict]]:
        result: Dict = {sha1: [] for sha1 in contents}
        for sha1 in contents:
            if sha1 in self._content_indexes["sha1"]:
                objs = self._content_indexes["sha1"][sha1]
                # only 1 element as content_add_metadata would have raised a
                # hash collision otherwise
                for key in objs:
                    d = self._contents[key].to_dict()
                    del d["ctime"]
                    if "data" in d:
                        del d["data"]
                    result[sha1].append(d)
        return result

    def content_find(self, content: Dict[str, Any]) -> List[Content]:
        if not set(content).intersection(DEFAULT_ALGORITHMS):
            raise StorageArgumentException(
                "content keys must contain at least one "
                f"of: {', '.join(sorted(DEFAULT_ALGORITHMS))}"
            )
        found = []
        for algo in DEFAULT_ALGORITHMS:
            hash = content.get(algo)
            if hash and hash in self._content_indexes[algo]:
                found.append(self._content_indexes[algo][hash])

        if not found:
            return []

        keys = list(set.intersection(*found))
        return [self._contents[key] for key in keys]

    def content_missing(self, content, key_hash="sha1"):
        for cont in content:
            for (algo, hash_) in cont.items():
                if algo not in DEFAULT_ALGORITHMS:
                    continue
                if hash_ not in self._content_indexes.get(algo, []):
                    yield cont[key_hash]
                    break

    def content_missing_per_sha1(self, contents):
        for content in contents:
            if content not in self._content_indexes["sha1"]:
                yield content

    def content_missing_per_sha1_git(self, contents):
        for content in contents:
            if content not in self._content_indexes["sha1_git"]:
                yield content

    def content_get_random(self) -> Sha1Git:
        return random.choice(list(self._content_indexes["sha1_git"]))

    def _skipped_content_add(self, contents: List[SkippedContent]) -> Dict:
        self.journal_writer.skipped_content_add(contents)

        summary = {"skipped_content:add": 0}

        missing_contents = self.skipped_content_missing([c.hashes() for c in contents])
        missing = {self._content_key(c) for c in missing_contents}
        contents = [c for c in contents if self._content_key(c) in missing]
        for content in contents:
            key = self._content_key(content)
            for algo in DEFAULT_ALGORITHMS:
                if content.get_hash(algo):
                    self._skipped_content_indexes[algo][content.get_hash(algo)].add(key)
            self._skipped_contents[key] = content
            summary["skipped_content:add"] += 1

        return summary

    def skipped_content_add(self, content: List[SkippedContent]) -> Dict:
        content = [attr.evolve(c, ctime=now()) for c in content]
        return self._skipped_content_add(content)

    def skipped_content_missing(self, contents):
        for content in contents:
            matches = list(self._skipped_contents.values())
            for (algorithm, key) in self._content_key(content):
                if algorithm == "blake2s256":
                    continue
                # Filter out skipped contents with the same hash
                matches = [
                    match for match in matches if match.get_hash(algorithm) == key
                ]
            # if none of the contents match
            if not matches:
                yield {algo: content[algo] for algo in DEFAULT_ALGORITHMS}

    def directory_add(self, directories: List[Directory]) -> Dict:
        directories = [dir_ for dir_ in directories if dir_.id not in self._directories]
        self.journal_writer.directory_add(directories)

        count = 0
        for directory in directories:
            count += 1
            self._directories[directory.id] = directory
            self._objects[directory.id].append(("directory", directory.id))

        return {"directory:add": count}

    def directory_missing(self, directories):
        for id in directories:
            if id not in self._directories:
                yield id

    def _join_dentry_to_content(self, dentry: Dict[str, Any]) -> Dict[str, Any]:
        keys = (
            "status",
            "sha1",
            "sha1_git",
            "sha256",
            "length",
        )
        ret = dict.fromkeys(keys)
        ret.update(dentry)
        if ret["type"] == "file":
            # TODO: Make it able to handle more than one content
            contents = self.content_find({"sha1_git": ret["target"]})
            if contents:
                content = contents[0]
                for key in keys:
                    ret[key] = getattr(content, key)
        return ret

    def _directory_ls(self, directory_id, recursive, prefix=b""):
        if directory_id in self._directories:
            for entry in self._directories[directory_id].entries:
                ret = self._join_dentry_to_content(entry.to_dict())
                ret["name"] = prefix + ret["name"]
                ret["dir_id"] = directory_id
                yield ret
                if recursive and ret["type"] == "dir":
                    yield from self._directory_ls(
                        ret["target"], True, prefix + ret["name"] + b"/"
                    )

    def directory_ls(self, directory, recursive=False):
        yield from self._directory_ls(directory, recursive)

    def directory_entry_get_by_path(self, directory, paths):
        return self._directory_entry_get_by_path(directory, paths, b"")

    def directory_get_random(self) -> Sha1Git:
        return random.choice(list(self._directories))

    def _directory_entry_get_by_path(self, directory, paths, prefix):
        if not paths:
            return

        contents = list(self.directory_ls(directory))

        if not contents:
            return

        def _get_entry(entries, name):
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

    def revision_add(self, revisions: List[Revision]) -> Dict:
        revisions = [rev for rev in revisions if rev.id not in self._revisions]
        self.journal_writer.revision_add(revisions)

        count = 0
        for revision in revisions:
            revision = attr.evolve(
                revision,
                committer=self._person_add(revision.committer),
                author=self._person_add(revision.author),
            )
            self._revisions[revision.id] = revision
            self._objects[revision.id].append(("revision", revision.id))
            count += 1

        return {"revision:add": count}

    def revision_missing(self, revisions):
        for id in revisions:
            if id not in self._revisions:
                yield id

    def revision_get(self, revisions):
        for id in revisions:
            if id in self._revisions:
                yield self._revisions.get(id).to_dict()
            else:
                yield None

    def _get_parent_revs(self, rev_id, seen, limit):
        if limit and len(seen) >= limit:
            return
        if rev_id in seen or rev_id not in self._revisions:
            return
        seen.add(rev_id)
        yield self._revisions[rev_id].to_dict()
        for parent in self._revisions[rev_id].parents:
            yield from self._get_parent_revs(parent, seen, limit)

    def revision_log(self, revisions, limit=None):
        seen = set()
        for rev_id in revisions:
            yield from self._get_parent_revs(rev_id, seen, limit)

    def revision_shortlog(self, revisions, limit=None):
        yield from (
            (rev["id"], rev["parents"]) for rev in self.revision_log(revisions, limit)
        )

    def revision_get_random(self) -> Sha1Git:
        return random.choice(list(self._revisions))

    def release_add(self, releases: List[Release]) -> Dict:
        to_add = []
        for rel in releases:
            if rel.id not in self._releases and rel not in to_add:
                to_add.append(rel)
        self.journal_writer.release_add(to_add)

        for rel in to_add:
            if rel.author:
                self._person_add(rel.author)
            self._objects[rel.id].append(("release", rel.id))
            self._releases[rel.id] = rel

        return {"release:add": len(to_add)}

    def release_missing(self, releases):
        yield from (rel for rel in releases if rel not in self._releases)

    def release_get(self, releases):
        for rel_id in releases:
            if rel_id in self._releases:
                yield self._releases[rel_id].to_dict()
            else:
                yield None

    def release_get_random(self) -> Sha1Git:
        return random.choice(list(self._releases))

    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict:
        count = 0
        snapshots = [snap for snap in snapshots if snap.id not in self._snapshots]
        for snapshot in snapshots:
            self.journal_writer.snapshot_add([snapshot])
            self._snapshots[snapshot.id] = snapshot
            self._objects[snapshot.id].append(("snapshot", snapshot.id))
            count += 1

        return {"snapshot:add": count}

    def snapshot_missing(self, snapshots):
        for id in snapshots:
            if id not in self._snapshots:
                yield id

    def snapshot_get(self, snapshot_id):
        return self.snapshot_get_branches(snapshot_id)

    def snapshot_get_by_origin_visit(self, origin, visit):
        origin_url = self._get_origin_url(origin)
        if not origin_url:
            return

        if origin_url not in self._origins or visit > len(
            self._origin_visits[origin_url]
        ):
            return None

        visit = self._origin_visit_get_updated(origin_url, visit)
        snapshot_id = visit["snapshot"]
        if snapshot_id:
            return self.snapshot_get(snapshot_id)
        else:
            return None

    def snapshot_count_branches(self, snapshot_id):
        snapshot = self._snapshots[snapshot_id]
        return collections.Counter(
            branch.target_type.value if branch else None
            for branch in snapshot.branches.values()
        )

    def snapshot_get_branches(
        self, snapshot_id, branches_from=b"", branches_count=1000, target_types=None
    ):
        snapshot = self._snapshots.get(snapshot_id)
        if snapshot is None:
            return None
        sorted_branches = sorted(snapshot.branches.items())
        sorted_branch_names = [k for (k, v) in sorted_branches]
        from_index = bisect.bisect_left(sorted_branch_names, branches_from)
        if target_types:
            next_branch = None
            branches = {}
            for (branch_name, branch) in sorted_branches:
                if branch_name in sorted_branch_names[from_index:]:
                    if branch and branch.target_type.value in target_types:
                        if len(branches) < branches_count:
                            branches[branch_name] = branch
                        else:
                            next_branch = branch_name
                            break
        else:
            # As there is no 'target_types', we can do that much faster
            to_index = from_index + branches_count
            returned_branch_names = frozenset(sorted_branch_names[from_index:to_index])
            branches = dict(
                (branch_name, branch)
                for (branch_name, branch) in snapshot.branches.items()
                if branch_name in returned_branch_names
            )
            if to_index >= len(sorted_branch_names):
                next_branch = None
            else:
                next_branch = sorted_branch_names[to_index]

        branches = {
            name: branch.to_dict() if branch else None
            for (name, branch) in branches.items()
        }

        return {
            "id": snapshot_id,
            "branches": branches,
            "next_branch": next_branch,
        }

    def snapshot_get_random(self) -> Sha1Git:
        return random.choice(list(self._snapshots))

    def object_find_by_sha1_git(self, ids):
        ret = {}
        for id_ in ids:
            objs = self._objects.get(id_, [])
            ret[id_] = [{"sha1_git": id_, "type": obj[0],} for obj in objs]
        return ret

    def _convert_origin(self, t):
        if t is None:
            return None

        return t.to_dict()

    def origin_get_one(self, origin_url: str) -> Optional[Origin]:
        return self._origins.get(origin_url)

    def origin_get(self, origins: List[str]) -> Iterable[Optional[Origin]]:
        return [self.origin_get_one(origin_url) for origin_url in origins]

    def origin_get_by_sha1(self, sha1s):
        return [self._convert_origin(self._origins_by_sha1.get(sha1)) for sha1 in sha1s]

    def origin_list(
        self, page_token: Optional[str] = None, limit: int = 100
    ) -> PagedResult[Origin]:
        origin_urls = sorted(self._origins)
        from_ = bisect.bisect_left(origin_urls, page_token) if page_token else 0
        next_page_token = None

        # Take one more origin so we can reuse it as the next page token if any
        origins = [Origin(url=url) for url in origin_urls[from_ : from_ + limit + 1]]

        if len(origins) > limit:
            # last origin id is the next page token
            next_page_token = str(origins[-1].url)
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
        next_page_token = None
        offset = int(page_token) if page_token else 0

        origins = self._origins.values()
        if regexp:
            pat = re.compile(url_pattern)
            origins = [orig for orig in origins if pat.search(orig.url)]
        else:
            origins = [orig for orig in origins if url_pattern in orig.url]

        if with_visit:
            filtered_origins = []
            for orig in origins:
                visits = (
                    self._origin_visit_get_updated(ov.origin, ov.visit)
                    for ov in self._origin_visits[orig.url]
                )
                for ov in visits:
                    snapshot = ov["snapshot"]
                    if snapshot and snapshot in self._snapshots:
                        filtered_origins.append(orig)
                        break
        else:
            filtered_origins = origins

        # Take one more origin so we can reuse it as the next page token if any
        origins = filtered_origins[offset : offset + limit + 1]
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
        actual_page = self.origin_search(
            url_pattern, regexp=regexp, with_visit=with_visit, limit=len(self._origins),
        )
        assert actual_page.next_page_token is None
        return len(actual_page.results)

    def origin_add(self, origins: List[Origin]) -> Dict[str, int]:
        added = 0
        for origin in origins:
            if origin.url not in self._origins:
                self.origin_add_one(origin)
                added += 1

        return {"origin:add": added}

    def origin_add_one(self, origin: Origin) -> str:
        if origin.url not in self._origins:
            self.journal_writer.origin_add([origin])
            self._origins[origin.url] = origin
            self._origins_by_sha1[origin_url_to_sha1(origin.url)] = origin
            self._origin_visits[origin.url] = []
            self._objects[origin.url].append(("origin", origin.url))

        return origin.url

    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get_one(visit.origin)
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        for visit in visits:
            origin_url = visit.origin
            if origin_url in self._origins:
                origin = self._origins[origin_url]
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

        ori = self._origins.get(origin)
        if not ori:
            return None

        visits = sorted(
            self._origin_visits[ori.url], key=lambda v: (v.date, v.visit), reverse=True,
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

        ori = self._origins.get(origin_url)
        if not ori:
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

    def stat_counters(self):
        keys = (
            "content",
            "directory",
            "origin",
            "origin_visit",
            "person",
            "release",
            "revision",
            "skipped_content",
            "snapshot",
        )
        stats = {key: 0 for key in keys}
        stats.update(
            collections.Counter(
                obj_type
                for (obj_type, obj_id) in itertools.chain(*self._objects.values())
            )
        )
        return stats

    def refresh_stat_counters(self):
        pass

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
