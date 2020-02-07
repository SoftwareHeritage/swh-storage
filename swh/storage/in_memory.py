# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
import bisect
import dateutil
import collections
import copy
import datetime
import itertools
import random

from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Union

import attr

from swh.model.model import (
    BaseContent, Content, SkippedContent, Directory, Revision, Release,
    Snapshot, OriginVisit, Origin, SHA1_SIZE)
from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_bytes, hash_to_hex
from swh.storage.objstorage import ObjStorage

from . import HashCollision
from .exc import StorageArgumentException

from .converters import origin_url_to_sha1
from .utils import get_partition_bounds_bytes
from .writer import JournalWriter

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class InMemoryStorage:
    def __init__(self, journal_writer=None):
        self._contents = {}
        self._content_indexes = defaultdict(lambda: defaultdict(set))
        self._skipped_contents = {}
        self._skipped_content_indexes = defaultdict(lambda: defaultdict(set))

        self.reset()
        self.journal_writer = JournalWriter(journal_writer)

    def reset(self):
        self._directories = {}
        self._revisions = {}
        self._releases = {}
        self._snapshots = {}
        self._origins = {}
        self._origins_by_id = []
        self._origins_by_sha1 = {}
        self._origin_visits = {}
        self._persons = []
        self._origin_metadata = defaultdict(list)
        self._tools = {}
        self._metadata_providers = {}
        self._objects = defaultdict(list)

        # ideally we would want a skip list for both fast inserts and searches
        self._sorted_sha1s = []

        self.objstorage = ObjStorage({'cls': 'memory', 'args': {}})

    def check_config(self, *, check_write):
        return True

    def _content_add(
            self, contents: Iterable[Content], with_data: bool) -> Dict:
        self.journal_writer.content_add(contents)

        content_add = 0
        content_add_bytes = 0
        if with_data:
            summary = self.objstorage.content_add(
                c for c in contents
                if c.status != 'absent')
            content_add_bytes = summary['content:add:bytes']

        for content in contents:
            key = self._content_key(content)
            if key in self._contents:
                continue
            for algorithm in DEFAULT_ALGORITHMS:
                hash_ = content.get_hash(algorithm)
                if hash_ in self._content_indexes[algorithm]\
                   and (algorithm not in {'blake2s256', 'sha256'}):
                    raise HashCollision(algorithm, hash_, key)
            for algorithm in DEFAULT_ALGORITHMS:
                hash_ = content.get_hash(algorithm)
                self._content_indexes[algorithm][hash_].add(key)
            self._objects[content.sha1_git].append(
                ('content', content.sha1))
            self._contents[key] = content
            bisect.insort(self._sorted_sha1s, content.sha1)
            self._contents[key] = attr.evolve(
                self._contents[key],
                data=None)
            content_add += 1

        summary = {
            'content:add': content_add,
        }
        if with_data:
            summary['content:add:bytes'] = content_add_bytes

        return summary

    def content_add(self, content: Iterable[Content]) -> Dict:
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        content = [attr.evolve(c, ctime=now) for c in content]
        return self._content_add(content, with_data=True)

    def content_update(self, content, keys=[]):
        self.journal_writer.content_update(content)

        for cont_update in content:
            cont_update = cont_update.copy()
            sha1 = cont_update.pop('sha1')
            for old_key in self._content_indexes['sha1'][sha1]:
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

    def content_add_metadata(self, content: Iterable[Content]) -> Dict:
        return self._content_add(content, with_data=False)

    def content_get(self, content):
        # FIXME: Make this method support slicing the `data`.
        if len(content) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise StorageArgumentException(
                "Sending at most %s contents." % BULK_BLOCK_CONTENT_LEN_MAX)
        yield from self.objstorage.content_get(content)

    def content_get_range(self, start, end, limit=1000):
        if limit is None:
            raise StorageArgumentException('limit should not be None')
        from_index = bisect.bisect_left(self._sorted_sha1s, start)
        sha1s = itertools.islice(self._sorted_sha1s, from_index, None)
        sha1s = ((sha1, content_key)
                 for sha1 in sha1s
                 for content_key in self._content_indexes['sha1'][sha1])
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
            'contents': matched,
            'next': next_content,
        }

    def content_get_partition(
            self, partition_id: int, nb_partitions: int, limit: int = 1000,
            page_token: str = None):
        if limit is None:
            raise StorageArgumentException('limit should not be None')
        (start, end) = get_partition_bounds_bytes(
            partition_id, nb_partitions, SHA1_SIZE)
        if page_token:
            start = hash_to_bytes(page_token)
        if end is None:
            end = b'\xff'*SHA1_SIZE
        result = self.content_get_range(start, end, limit)
        result2 = {
            'contents': result['contents'],
            'next_page_token': None,
        }
        if result['next']:
            result2['next_page_token'] = hash_to_hex(result['next'])
        return result2

    def content_get_metadata(
            self, contents: List[bytes]) -> Dict[bytes, List[Dict]]:
        result: Dict = {sha1: [] for sha1 in contents}
        for sha1 in contents:
            if sha1 in self._content_indexes['sha1']:
                objs = self._content_indexes['sha1'][sha1]
                # only 1 element as content_add_metadata would have raised a
                # hash collision otherwise
                for key in objs:
                    d = self._contents[key].to_dict()
                    del d['ctime']
                    if 'data' in d:
                        del d['data']
                    result[sha1].append(d)
        return result

    def content_find(self, content):
        if not set(content).intersection(DEFAULT_ALGORITHMS):
            raise StorageArgumentException(
                'content keys must contain at least one of: %s'
                % ', '.join(sorted(DEFAULT_ALGORITHMS)))
        found = []
        for algo in DEFAULT_ALGORITHMS:
            hash = content.get(algo)
            if hash and hash in self._content_indexes[algo]:
                found.append(self._content_indexes[algo][hash])

        if not found:
            return []

        keys = list(set.intersection(*found))
        return [self._contents[key].to_dict() for key in keys]

    def content_missing(self, content, key_hash='sha1'):
        for cont in content:
            for (algo, hash_) in cont.items():
                if algo not in DEFAULT_ALGORITHMS:
                    continue
                if hash_ not in self._content_indexes.get(algo, []):
                    yield cont[key_hash]
                    break
            else:
                for result in self.content_find(cont):
                    if result['status'] == 'missing':
                        yield cont[key_hash]

    def content_missing_per_sha1(self, contents):
        for content in contents:
            if content not in self._content_indexes['sha1']:
                yield content

    def content_missing_per_sha1_git(self, contents):
        for content in contents:
            if content not in self._content_indexes['sha1_git']:
                yield content

    def content_get_random(self):
        return random.choice(list(self._content_indexes['sha1_git']))

    def _skipped_content_add(self, contents: Iterable[SkippedContent]) -> Dict:
        self.journal_writer.skipped_content_add(contents)

        summary = {
            'skipped_content:add': 0
        }

        skipped_content_missing = self.skipped_content_missing(
            [c.to_dict() for c in contents])
        for content in skipped_content_missing:
            key = self._content_key(content, allow_missing=True)
            for algo in DEFAULT_ALGORITHMS:
                if content.get(algo):
                    self._skipped_content_indexes[algo][
                        content.get(algo)].add(key)
            self._skipped_contents[key] = content
            summary['skipped_content:add'] += 1

        return summary

    def skipped_content_missing(self, contents):
        for content in contents:
            for (key, algorithm) in self._content_key_algorithm(content):
                if algorithm == 'blake2s256':
                    continue
                if key not in self._skipped_content_indexes[algorithm]:
                    # index must contain hashes of algos except blake2s256
                    # else the content is considered skipped
                    yield {algo: content[algo]
                           for algo in DEFAULT_ALGORITHMS
                           if content[algo] is not None}
                    break

    def skipped_content_add(self, content: Iterable[SkippedContent]) -> Dict:
        content = list(content)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        content = [attr.evolve(c, ctime=now) for c in content]
        return self._skipped_content_add(content)

    def directory_add(self, directories: Iterable[Directory]) -> Dict:
        directories = [dir_ for dir_ in directories
                       if dir_.id not in self._directories]
        self.journal_writer.directory_add(directories)

        count = 0
        for directory in directories:
            count += 1
            self._directories[directory.id] = directory
            self._objects[directory.id].append(
                ('directory', directory.id))

        return {'directory:add': count}

    def directory_missing(self, directories):
        for id in directories:
            if id not in self._directories:
                yield id

    def _join_dentry_to_content(self, dentry):
        keys = (
            'status',
            'sha1',
            'sha1_git',
            'sha256',
            'length',
        )
        ret = dict.fromkeys(keys)
        ret.update(dentry)
        if ret['type'] == 'file':
            # TODO: Make it able to handle more than one content
            content = self.content_find({'sha1_git': ret['target']})
            if content:
                content = content[0]
                for key in keys:
                    ret[key] = content[key]
        return ret

    def _directory_ls(self, directory_id, recursive, prefix=b''):
        if directory_id in self._directories:
            for entry in self._directories[directory_id].entries:
                ret = self._join_dentry_to_content(entry.to_dict())
                ret['name'] = prefix + ret['name']
                ret['dir_id'] = directory_id
                yield ret
                if recursive and ret['type'] == 'dir':
                    yield from self._directory_ls(
                        ret['target'], True, prefix + ret['name'] + b'/')

    def directory_ls(self, directory, recursive=False):
        yield from self._directory_ls(directory, recursive)

    def directory_entry_get_by_path(self, directory, paths):
        return self._directory_entry_get_by_path(directory, paths, b'')

    def directory_get_random(self):
        if not self._directories:
            return None
        return random.choice(list(self._directories))

    def _directory_entry_get_by_path(self, directory, paths, prefix):
        if not paths:
            return

        contents = list(self.directory_ls(directory))

        if not contents:
            return

        def _get_entry(entries, name):
            for entry in entries:
                if entry['name'] == name:
                    entry = entry.copy()
                    entry['name'] = prefix + entry['name']
                    return entry

        first_item = _get_entry(contents, paths[0])

        if len(paths) == 1:
            return first_item

        if not first_item or first_item['type'] != 'dir':
            return

        return self._directory_entry_get_by_path(
                first_item['target'], paths[1:], prefix + paths[0] + b'/')

    def revision_add(self, revisions: Iterable[Revision]) -> Dict:
        revisions = [rev for rev in revisions
                     if rev.id not in self._revisions]
        self.journal_writer.revision_add(revisions)

        count = 0
        for revision in revisions:
            revision = attr.evolve(
                revision,
                committer=self._person_add(revision.committer),
                author=self._person_add(revision.author))
            self._revisions[revision.id] = revision
            self._objects[revision.id].append(
                ('revision', revision.id))
            count += 1

        return {'revision:add': count}

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
        yield from ((rev['id'], rev['parents'])
                    for rev in self.revision_log(revisions, limit))

    def revision_get_random(self):
        return random.choice(list(self._revisions))

    def release_add(self, releases: Iterable[Release]) -> Dict:
        releases = [rel for rel in releases
                    if rel.id not in self._releases]
        self.journal_writer.release_add(releases)

        count = 0
        for rel in releases:
            if rel.author:
                self._person_add(rel.author)
            self._objects[rel.id].append(
                ('release', rel.id))
            self._releases[rel.id] = rel
            count += 1

        return {'release:add': count}

    def release_missing(self, releases):
        yield from (rel for rel in releases if rel not in self._releases)

    def release_get(self, releases):
        for rel_id in releases:
            if rel_id in self._releases:
                yield self._releases[rel_id].to_dict()
            else:
                yield None

    def release_get_random(self):
        return random.choice(list(self._releases))

    def snapshot_add(self, snapshots: Iterable[Snapshot]) -> Dict:
        count = 0
        snapshots = (snap for snap in snapshots
                     if snap.id not in self._snapshots)
        for snapshot in snapshots:
            self.journal_writer.snapshot_add(snapshot)
            sorted_branch_names = sorted(snapshot.branches)
            self._snapshots[snapshot.id] = (snapshot, sorted_branch_names)
            self._objects[snapshot.id].append(('snapshot', snapshot.id))
            count += 1

        return {'snapshot:add': count}

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

        if origin_url not in self._origins or \
           visit > len(self._origin_visits[origin_url]):
            return None
        snapshot_id = self._origin_visits[origin_url][visit-1].snapshot
        if snapshot_id:
            return self.snapshot_get(snapshot_id)
        else:
            return None

    def snapshot_get_latest(self, origin, allowed_statuses=None):
        origin_url = self._get_origin_url(origin)
        if not origin_url:
            return

        visit = self.origin_visit_get_latest(
            origin_url,
            allowed_statuses=allowed_statuses,
            require_snapshot=True)
        if visit and visit['snapshot']:
            snapshot = self.snapshot_get(visit['snapshot'])
            if not snapshot:
                raise StorageArgumentException(
                    'last origin visit references an unknown snapshot')
            return snapshot

    def snapshot_count_branches(self, snapshot_id):
        (snapshot, _) = self._snapshots[snapshot_id]
        return collections.Counter(branch.target_type.value if branch else None
                                   for branch in snapshot.branches.values())

    def snapshot_get_branches(self, snapshot_id, branches_from=b'',
                              branches_count=1000, target_types=None):
        res = self._snapshots.get(snapshot_id)
        if res is None:
            return None
        (snapshot, sorted_branch_names) = res
        from_index = bisect.bisect_left(
                sorted_branch_names, branches_from)
        if target_types:
            next_branch = None
            branches = {}
            for branch_name in sorted_branch_names[from_index:]:
                branch = snapshot.branches[branch_name]
                if branch and branch.target_type.value in target_types:
                    if len(branches) < branches_count:
                        branches[branch_name] = branch
                    else:
                        next_branch = branch_name
                        break
        else:
            # As there is no 'target_types', we can do that much faster
            to_index = from_index + branches_count
            returned_branch_names = sorted_branch_names[from_index:to_index]
            branches = {branch_name: snapshot.branches[branch_name]
                        for branch_name in returned_branch_names}
            if to_index >= len(sorted_branch_names):
                next_branch = None
            else:
                next_branch = sorted_branch_names[to_index]

        branches = {name: branch.to_dict() if branch else None
                    for (name, branch) in branches.items()}

        return {
                'id': snapshot_id,
                'branches': branches,
                'next_branch': next_branch,
                }

    def snapshot_get_random(self):
        return random.choice(list(self._snapshots))

    def object_find_by_sha1_git(self, ids):
        ret = {}
        for id_ in ids:
            objs = self._objects.get(id_, [])
            ret[id_] = [{
                    'sha1_git': id_,
                    'type': obj[0],
                    } for obj in objs]
        return ret

    def _convert_origin(self, t):
        if t is None:
            return None

        return t.to_dict()

    def origin_get(self, origins):
        if isinstance(origins, dict):
            # Old API
            return_single = True
            origins = [origins]
        else:
            return_single = False

        # Sanity check to be error-compatible with the pgsql backend
        if any('id' in origin for origin in origins) \
                and not all('id' in origin for origin in origins):
            raise StorageArgumentException(
                'Either all origins or none at all should have an "id".')
        if any('url' in origin for origin in origins) \
                and not all('url' in origin for origin in origins):
            raise StorageArgumentException(
                'Either all origins or none at all should have '
                'an "url" key.')

        results = []
        for origin in origins:
            result = None
            if 'url' in origin:
                if origin['url'] in self._origins:
                    result = self._origins[origin['url']]
            else:
                raise StorageArgumentException(
                    'Origin must have an url.')
            results.append(self._convert_origin(result))

        if return_single:
            assert len(results) == 1
            return results[0]
        else:
            return results

    def origin_get_by_sha1(self, sha1s):
        return [
            self._convert_origin(self._origins_by_sha1.get(sha1))
            for sha1 in sha1s
        ]

    def origin_get_range(self, origin_from=1, origin_count=100):
        origin_from = max(origin_from, 1)
        if origin_from <= len(self._origins_by_id):
            max_idx = origin_from + origin_count - 1
            if max_idx > len(self._origins_by_id):
                max_idx = len(self._origins_by_id)
            for idx in range(origin_from-1, max_idx):
                origin = self._convert_origin(
                    self._origins[self._origins_by_id[idx]])
                yield {'id': idx+1, **origin}

    def origin_list(self, page_token: Optional[str] = None, limit: int = 100
                    ) -> dict:
        origin_urls = sorted(self._origins)
        if page_token:
            from_ = bisect.bisect_left(origin_urls, page_token)
        else:
            from_ = 0

        result = {
            'origins': [{'url': origin_url}
                        for origin_url in origin_urls[from_:from_+limit]]
        }

        if from_+limit < len(origin_urls):
            result['next_page_token'] = origin_urls[from_+limit]

        return result

    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, with_visit=False):
        origins = map(self._convert_origin, self._origins.values())
        if regexp:
            pat = re.compile(url_pattern)
            origins = [orig for orig in origins if pat.search(orig['url'])]
        else:
            origins = [orig for orig in origins if url_pattern in orig['url']]
        if with_visit:
            origins = [
                orig for orig in origins
                if len(self._origin_visits[orig['url']]) > 0 and
                set(ov.snapshot
                    for ov in self._origin_visits[orig['url']]
                    if ov.snapshot) &
                set(self._snapshots)]

        return origins[offset:offset+limit]

    def origin_count(self, url_pattern, regexp=False, with_visit=False):
        return len(self.origin_search(url_pattern, regexp=regexp,
                                      with_visit=with_visit,
                                      limit=len(self._origins)))

    def origin_add(self, origins: Iterable[Origin]) -> List[Dict]:
        origins = copy.deepcopy(list(origins))
        for origin in origins:
            self.origin_add_one(origin)
        return [origin.to_dict() for origin in origins]

    def origin_add_one(self, origin: Origin) -> str:
        if origin.url not in self._origins:
            self.journal_writer.origin_add_one(origin)
            # generate an origin_id because it is needed by origin_get_range.
            # TODO: remove this when we remove origin_get_range
            origin_id = len(self._origins) + 1
            self._origins_by_id.append(origin.url)
            assert len(self._origins_by_id) == origin_id

            self._origins[origin.url] = origin
            self._origins_by_sha1[origin_url_to_sha1(origin.url)] = origin
            self._origin_visits[origin.url] = []
            self._objects[origin.url].append(('origin', origin.url))

        return origin.url

    def origin_visit_add(
            self, origin, date, type) -> Optional[Dict[str, Union[str, int]]]:
        origin_url = origin
        if origin_url is None:
            raise StorageArgumentException('Unknown origin.')

        if isinstance(date, str):
            # FIXME: Converge on iso8601 at some point
            date = dateutil.parser.parse(date)
        elif not isinstance(date, datetime.datetime):
            raise StorageArgumentException(
                'date must be a datetime or a string.')

        visit_ret = None
        if origin_url in self._origins:
            origin = self._origins[origin_url]
            # visit ids are in the range [1, +inf[
            visit_id = len(self._origin_visits[origin_url]) + 1
            status = 'ongoing'
            visit = OriginVisit(
                origin=origin.url,
                date=date,
                type=type,
                status=status,
                snapshot=None,
                metadata=None,
                visit=visit_id,
            )
            self._origin_visits[origin_url].append(visit)
            visit_ret = {
                'origin': origin.url,
                'visit': visit_id,
            }

            self._objects[(origin_url, visit_id)].append(
                ('origin_visit', None))

            self.journal_writer.origin_visit_add(visit)

        return visit_ret

    def origin_visit_update(
            self, origin: str, visit_id: int, status: Optional[str] = None,
            metadata: Optional[Dict] = None, snapshot: Optional[bytes] = None):
        origin_url = self._get_origin_url(origin)
        if origin_url is None:
            raise StorageArgumentException('Unknown origin.')

        try:
            visit = self._origin_visits[origin_url][visit_id-1]
        except IndexError:
            raise StorageArgumentException(
                'Unknown visit_id for this origin') from None

        updates: Dict[str, Any] = {}
        if status:
            updates['status'] = status
        if metadata:
            updates['metadata'] = metadata
        if snapshot:
            updates['snapshot'] = snapshot

        try:
            visit = attr.evolve(visit, **updates)
        except (KeyError, TypeError, ValueError) as e:
            raise StorageArgumentException(*e.args)

        self.journal_writer.origin_visit_update(visit)

        self._origin_visits[origin_url][visit_id-1] = visit

    def origin_visit_upsert(self, visits):
        for visit in visits:
            if not isinstance(visit['origin'], str):
                raise TypeError("visit['origin'] must be a string, not %r"
                                % (visit['origin'],))
        try:
            visits = [OriginVisit.from_dict(d) for d in visits]
        except (KeyError, TypeError, ValueError) as e:
            raise StorageArgumentException(*e.args)

        self.journal_writer.origin_visit_upsert(visits)

        for visit in visits:
            visit_id = visit.visit
            origin_url = visit.origin

            try:
                visit = attr.evolve(visit, origin=origin_url)
            except (KeyError, TypeError, ValueError) as e:
                raise StorageArgumentException(*e.args)

            self._objects[(origin_url, visit_id)].append(
                ('origin_visit', None))

            while len(self._origin_visits[origin_url]) <= visit_id:
                self._origin_visits[origin_url].append(None)

            self._origin_visits[origin_url][visit_id-1] = visit

    def _convert_visit(self, visit):
        if visit is None:
            return

        visit = visit.to_dict()

        return visit

    def origin_visit_get(self, origin, last_visit=None, limit=None):
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits:
            visits = self._origin_visits[origin_url]
            if last_visit is not None:
                visits = visits[last_visit:]
            if limit is not None:
                visits = visits[:limit]
            for visit in visits:
                if not visit:
                    continue
                visit_id = visit.visit

                yield self._convert_visit(
                    self._origin_visits[origin_url][visit_id-1])

    def origin_visit_find_by_date(self, origin, visit_date):
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits:
            visits = self._origin_visits[origin_url]
            visit = min(
                visits,
                key=lambda v: (abs(v.date - visit_date), -v.visit))
            return self._convert_visit(visit)

    def origin_visit_get_by(self, origin, visit):
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits and \
           visit <= len(self._origin_visits[origin_url]):
            return self._convert_visit(
                self._origin_visits[origin_url][visit-1])

    def origin_visit_get_latest(
            self, origin, allowed_statuses=None, require_snapshot=False):
        origin = self._origins.get(origin)
        if not origin:
            return
        visits = self._origin_visits[origin.url]
        if allowed_statuses is not None:
            visits = [visit for visit in visits
                      if visit.status in allowed_statuses]
        if require_snapshot:
            visits = [visit for visit in visits
                      if visit.snapshot]

        visit = max(
            visits, key=lambda v: (v.date, v.visit), default=None)
        return self._convert_visit(visit)

    def _select_random_origin_visit_by_type(self, type: str) -> str:
        while True:
            url = random.choice(list(self._origin_visits.keys()))
            random_origin_visits = self._origin_visits[url]
            if random_origin_visits[0].type == type:
                return url

    def origin_visit_get_random(self, type: str) -> Optional[Dict[str, Any]]:
        url = self._select_random_origin_visit_by_type(type)
        random_origin_visits = copy.deepcopy(self._origin_visits[url])
        random_origin_visits.reverse()
        back_in_the_day = now() - timedelta(weeks=12)  # 3 months back
        # This should be enough for tests
        for visit in random_origin_visits:
            if visit.date > back_in_the_day and visit.status == 'full':
                return visit.to_dict()
        else:
            return None

    def stat_counters(self):
        keys = (
            'content',
            'directory',
            'origin',
            'origin_visit',
            'person',
            'release',
            'revision',
            'skipped_content',
            'snapshot'
            )
        stats = {key: 0 for key in keys}
        stats.update(collections.Counter(
            obj_type
            for (obj_type, obj_id)
            in itertools.chain(*self._objects.values())))
        return stats

    def refresh_stat_counters(self):
        pass

    def origin_metadata_add(self, origin_url, ts, provider, tool, metadata):
        if not isinstance(origin_url, str):
            raise TypeError('origin_id must be str, not %r' % (origin_url,))

        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        origin_metadata = {
                'origin_url': origin_url,
                'discovery_date': ts,
                'tool_id': tool,
                'metadata': metadata,
                'provider_id': provider,
                }
        self._origin_metadata[origin_url].append(origin_metadata)
        return None

    def origin_metadata_get_by(self, origin_url, provider_type=None):
        if not isinstance(origin_url, str):
            raise TypeError('origin_url must be str, not %r' % (origin_url,))
        metadata = []
        for item in self._origin_metadata[origin_url]:
            item = copy.deepcopy(item)
            provider = self.metadata_provider_get(item['provider_id'])
            for attr_name in ('name', 'type', 'url'):
                item['provider_' + attr_name] = \
                    provider['provider_' + attr_name]
            metadata.append(item)
        return metadata

    def tool_add(self, tools):
        inserted = []
        for tool in tools:
            key = self._tool_key(tool)
            assert 'id' not in tool
            record = copy.deepcopy(tool)
            record['id'] = key  # TODO: remove this
            if key not in self._tools:
                self._tools[key] = record
            inserted.append(copy.deepcopy(self._tools[key]))

        return inserted

    def tool_get(self, tool):
        return self._tools.get(self._tool_key(tool))

    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata):
        provider = {
                'provider_name': provider_name,
                'provider_type': provider_type,
                'provider_url': provider_url,
                'metadata': metadata,
                }
        key = self._metadata_provider_key(provider)
        provider['id'] = key
        self._metadata_providers[key] = provider
        return key

    def metadata_provider_get(self, provider_id):
        return self._metadata_providers.get(provider_id)

    def metadata_provider_get_by(self, provider):
        key = self._metadata_provider_key(provider)
        return self._metadata_providers.get(key)

    def _get_origin_url(self, origin):
        if isinstance(origin, str):
            return origin
        else:
            raise TypeError('origin must be a string.')

    def _person_add(self, person):
        key = ('person', person.fullname)
        if key not in self._objects:
            person_id = len(self._persons) + 1
            self._persons.append(person)
            self._objects[key].append(('person', person_id))
        else:
            person_id = self._objects[key][0][1]
            person = self._persons[person_id-1]
        return person

    @staticmethod
    def _content_key(content, allow_missing=False):
        """A stable key for a content"""
        return tuple(getattr(content, key, None)
                     for key in sorted(DEFAULT_ALGORITHMS))

    @staticmethod
    def _content_key_algorithm(content):
        """ A stable key and the algorithm for a content"""
        if isinstance(content, BaseContent):
            content = content.to_dict()
        return tuple((content.get(key), key)
                     for key in sorted(DEFAULT_ALGORITHMS))

    @staticmethod
    def _tool_key(tool):
        return '%r %r %r' % (tool['name'], tool['version'],
                             tuple(sorted(tool['configuration'].items())))

    @staticmethod
    def _metadata_provider_key(provider):
        return '%r %r' % (provider['provider_name'], provider['provider_url'])

    def diff_directories(self, from_dir, to_dir, track_renaming=False):
        raise NotImplementedError('InMemoryStorage.diff_directories')

    def diff_revisions(self, from_rev, to_rev, track_renaming=False):
        raise NotImplementedError('InMemoryStorage.diff_revisions')

    def diff_revision(self, revision, track_renaming=False):
        raise NotImplementedError('InMemoryStorage.diff_revision')
