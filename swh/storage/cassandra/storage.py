# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json
import random
import re
from typing import Any, Dict, List, Iterable, Optional, Union
import uuid

import attr
import dateutil

from swh.model.model import (
    Revision, Release, Directory, DirectoryEntry, Content, SkippedContent,
    OriginVisit, Snapshot, Origin
)
from swh.storage.objstorage import ObjStorage
from swh.storage.writer import JournalWriter

from .. import HashCollision
from ..exc import StorageArgumentException
from .common import TOKEN_BEGIN, TOKEN_END
from .converters import (
    revision_to_db, revision_from_db, release_to_db, release_from_db,
)
from .cql import CqlRunner
from .schema import HASH_ALGORITHMS


# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class CassandraStorage:
    def __init__(self, hosts, keyspace, objstorage,
                 port=9042, journal_writer=None):
        self._cql_runner = CqlRunner(hosts, keyspace, port)
        self.journal_writer = JournalWriter(journal_writer)
        self.objstorage = ObjStorage(objstorage)

    def check_config(self, *, check_write):
        self._cql_runner.check_read()

        return True

    def _content_add(self, contents: List[Content], with_data: bool) -> Dict:
        # Filter-out content already in the database.
        contents = [c for c in contents
                    if not self._cql_runner.content_get_from_pk(c.to_dict())]

        self.journal_writer.content_add(contents)

        if with_data:
            # First insert to the objstorage, if the endpoint is
            # `content_add` (as opposed to `content_add_metadata`).
            # TODO: this should probably be done in concurrently to inserting
            # in index tables (but still before the main table; so an entry is
            # only added to the main table after everything else was
            # successfully inserted.
            summary = self.objstorage.content_add(
                c for c in contents if c.status != 'absent')
            content_add_bytes = summary['content:add:bytes']

        content_add = 0
        for content in contents:
            content_add += 1

            # Then add to index tables
            for algo in HASH_ALGORITHMS:
                self._cql_runner.content_index_add_one(algo, content)

            # Then to the main table
            self._cql_runner.content_add_one(content)

            # Note that we check for collisions *after* inserting. This
            # differs significantly from the pgsql storage, but checking
            # before insertion does not provide any guarantee in case
            # another thread inserts the colliding hash at the same time.
            #
            # The proper way to do it would probably be a BATCH, but this
            # would be inefficient because of the number of partitions we
            # need to affect (len(HASH_ALGORITHMS)+1, which is currently 5)
            for algo in {'sha1', 'sha1_git'}:
                pks = self._cql_runner.content_get_pks_from_single_hash(
                    algo, content.get_hash(algo))
                if len(pks) > 1:
                    # There are more than the one we just inserted.
                    raise HashCollision(algo, content.get_hash(algo), pks)

        summary = {
            'content:add': content_add,
        }

        if with_data:
            summary['content:add:bytes'] = content_add_bytes

        return summary

    def content_add(self, content: Iterable[Content]) -> Dict:
        return self._content_add(list(content), with_data=True)

    def content_update(self, content, keys=[]):
        raise NotImplementedError(
            'content_update is not supported by the Cassandra backend')

    def content_add_metadata(self, content: Iterable[Content]) -> Dict:
        return self._content_add(list(content), with_data=False)

    def content_get(self, content):
        if len(content) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise StorageArgumentException(
                "Sending at most %s contents." % BULK_BLOCK_CONTENT_LEN_MAX)
        yield from self.objstorage.content_get(content)

    def content_get_partition(
            self, partition_id: int, nb_partitions: int, limit: int = 1000,
            page_token: str = None):
        if limit is None:
            raise StorageArgumentException('limit should not be None')

        # Compute start and end of the range of tokens covered by the
        # requested partition
        partition_size = (TOKEN_END-TOKEN_BEGIN)//nb_partitions
        range_start = TOKEN_BEGIN + partition_id*partition_size
        range_end = TOKEN_BEGIN + (partition_id+1)*partition_size

        # offset the range start according to the `page_token`.
        if page_token is not None:
            if not (range_start <= int(page_token) <= range_end):
                raise StorageArgumentException('Invalid page_token.')
            range_start = int(page_token)

        # Get the first rows of the range
        rows = self._cql_runner.content_get_token_range(
            range_start, range_end, limit)
        rows = list(rows)

        if len(rows) == limit:
            next_page_token: Optional[str] = str(rows[-1].tok+1)
        else:
            next_page_token = None

        return {
            'contents': [row._asdict() for row in rows
                         if row.status != 'absent'],
            'next_page_token': next_page_token,
        }

    def content_get_metadata(
            self, contents: List[bytes]) -> Dict[bytes, List[Dict]]:
        result: Dict[bytes, List[Dict]] = {sha1: [] for sha1 in contents}
        for sha1 in contents:
            # Get all (sha1, sha1_git, sha256, blake2s256) whose sha1
            # matches the argument, from the index table ('content_by_sha1')
            pks = self._cql_runner.content_get_pks_from_single_hash(
                'sha1', sha1)

            if pks:
                # TODO: what to do if there are more than one?
                pk = pks[0]

                # Query the main table ('content')
                res = self._cql_runner.content_get_from_pk(pk._asdict())

                # Rows in 'content' are inserted after corresponding
                # rows in 'content_by_*', so we might be missing it
                if res:
                    content_metadata = res._asdict()
                    content_metadata.pop('ctime')
                    result[content_metadata['sha1']].append(content_metadata)
        return result

    def content_find(self, content):
        # Find an algorithm that is common to all the requested contents.
        # It will be used to do an initial filtering efficiently.
        filter_algos = list(set(content).intersection(HASH_ALGORITHMS))
        if not filter_algos:
            raise StorageArgumentException(
                'content keys must contain at least one of: '
                '%s' % ', '.join(sorted(HASH_ALGORITHMS)))
        common_algo = filter_algos[0]

        # Find all contents whose common_algo matches at least one
        # of the requests.
        found_pks = self._cql_runner.content_get_pks_from_single_hash(
            common_algo, content[common_algo])
        found_pks = [pk._asdict() for pk in found_pks]

        # Filter with the other hash algorithms.
        for algo in filter_algos[1:]:
            found_pks = [pk for pk in found_pks if pk[algo] == content[algo]]

        results = []
        for pk in found_pks:
            # Query the main table ('content').
            res = self._cql_runner.content_get_from_pk(pk)

            # Rows in 'content' are inserted after corresponding
            # rows in 'content_by_*', so we might be missing it
            if res:
                results.append({
                    **res._asdict(),
                    'ctime': res.ctime.replace(tzinfo=datetime.timezone.utc)
                })
        return results

    def content_missing(self, content, key_hash='sha1'):
        for cont in content:
            res = self.content_find(cont)
            if not res:
                yield cont[key_hash]
            if any(c['status'] == 'missing' for c in res):
                yield cont[key_hash]

    def content_missing_per_sha1(self, contents):
        return self.content_missing([{'sha1': c for c in contents}])

    def content_missing_per_sha1_git(self, contents):
        return self.content_missing([{'sha1_git': c for c in contents}],
                                    key_hash='sha1_git')

    def content_get_random(self):
        return self._cql_runner.content_get_random().sha1_git

    def _skipped_content_add(self, contents: Iterable[SkippedContent]) -> Dict:
        # Filter-out content already in the database.
        contents = [
            c for c in contents
            if not self._cql_runner.skipped_content_get_from_pk(c.to_dict())]

        self.journal_writer.skipped_content_add(contents)

        for content in contents:
            # Add to index tables
            for algo in HASH_ALGORITHMS:
                if content.get_hash(algo) is not None:
                    self._cql_runner.skipped_content_index_add_one(
                        algo, content)

            # Then to the main table
            self._cql_runner.skipped_content_add_one(content)

        return {
            'skipped_content:add': len(contents)
        }

    def skipped_content_add(self, content: Iterable[SkippedContent]) -> Dict:
        return self._skipped_content_add(content)

    def skipped_content_missing(self, contents):
        for content in contents:
            if not self._cql_runner.skipped_content_get_from_pk(content):
                yield content

    def directory_add(self, directories: Iterable[Directory]) -> Dict:
        directories = list(directories)

        # Filter out directories that are already inserted.
        missing = self.directory_missing([dir_.id for dir_ in directories])
        directories = [dir_ for dir_ in directories if dir_.id in missing]

        self.journal_writer.directory_add(directories)

        for directory in directories:
            # Add directory entries to the 'directory_entry' table
            for entry in directory.entries:
                self._cql_runner.directory_entry_add_one({
                    **entry.to_dict(),
                    'directory_id': directory.id
                })

            # Add the directory *after* adding all the entries, so someone
            # calling snapshot_get_branch in the meantime won't end up
            # with half the entries.
            self._cql_runner.directory_add_one(directory.id)

        return {'directory:add': len(missing)}

    def directory_missing(self, directories):
        return self._cql_runner.directory_missing(directories)

    def _join_dentry_to_content(self, dentry):
        keys = (
            'status',
            'sha1',
            'sha1_git',
            'sha256',
            'length',
        )
        ret = dict.fromkeys(keys)
        ret.update(dentry.to_dict())
        if ret['type'] == 'file':
            content = self.content_find({'sha1_git': ret['target']})
            if content:
                content = content[0]
                for key in keys:
                    ret[key] = content[key]
        return ret

    def _directory_ls(self, directory_id, recursive, prefix=b''):
        if self.directory_missing([directory_id]):
            return
        rows = list(self._cql_runner.directory_entry_get([directory_id]))

        for row in rows:
            # Build and yield the directory entry dict
            entry = row._asdict()
            del entry['directory_id']
            entry = DirectoryEntry.from_dict(entry)
            ret = self._join_dentry_to_content(entry)
            ret['name'] = prefix + ret['name']
            ret['dir_id'] = directory_id
            yield ret

            if recursive and ret['type'] == 'dir':
                yield from self._directory_ls(
                    ret['target'], True, prefix + ret['name'] + b'/')

    def directory_entry_get_by_path(self, directory, paths):
        return self._directory_entry_get_by_path(directory, paths, b'')

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

    def directory_ls(self, directory, recursive=False):
        yield from self._directory_ls(directory, recursive)

    def directory_get_random(self):
        return self._cql_runner.directory_get_random().id

    def revision_add(self, revisions: Iterable[Revision]) -> Dict:
        revisions = list(revisions)

        # Filter-out revisions already in the database
        missing = self.revision_missing([rev.id for rev in revisions])
        revisions = [rev for rev in revisions if rev.id in missing]

        self.journal_writer.revision_add(revisions)

        for revision in revisions:
            revision = revision_to_db(revision)
            if revision:
                # Add parents first
                for (rank, parent) in enumerate(revision.parents):
                    self._cql_runner.revision_parent_add_one(
                        revision.id, rank, parent)

                # Then write the main revision row.
                # Writing this after all parents were written ensures that
                # read endpoints don't return a partial view while writing
                # the parents
                self._cql_runner.revision_add_one(revision)

        return {'revision:add': len(revisions)}

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
            parents = [row.parent_id for row in parent_rows]

            rev = Revision(**row._asdict(), parents=parents)

            rev = revision_from_db(rev)
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
            parents = [row.parent_id for row in parent_rows]

            if short:
                yield (row.id, parents)
            else:
                rev = revision_from_db(Revision(
                    **row._asdict(), parents=parents))
                yield rev.to_dict()
            yield from self._get_parent_revs(parents, seen, limit, short)

    def revision_log(self, revisions, limit=None):
        seen = set()
        yield from self._get_parent_revs(revisions, seen, limit, False)

    def revision_shortlog(self, revisions, limit=None):
        seen = set()
        yield from self._get_parent_revs(revisions, seen, limit, True)

    def revision_get_random(self):
        return self._cql_runner.revision_get_random().id

    def release_add(self, releases: Iterable[Release]) -> Dict:
        missing = self.release_missing([rel.id for rel in releases])
        releases = [rel for rel in releases if rel.id in missing]

        self.journal_writer.release_add(releases)

        for release in releases:
            if release:
                release = release_to_db(release)
                self._cql_runner.release_add_one(release)

        return {'release:add': len(missing)}

    def release_missing(self, releases):
        return self._cql_runner.release_missing(releases)

    def release_get(self, releases):
        rows = self._cql_runner.release_get(releases)
        rels = {}
        for row in rows:
            release = Release(**row._asdict())
            release = release_from_db(release)
            rels[row.id] = release.to_dict()

        for rel_id in releases:
            yield rels.get(rel_id)

    def release_get_random(self):
        return self._cql_runner.release_get_random().id

    def snapshot_add(self, snapshots: Iterable[Snapshot]) -> Dict:
        missing = self._cql_runner.snapshot_missing(
            [snp.id for snp in snapshots])
        snapshots = [snp for snp in snapshots if snp.id in missing]

        for snapshot in snapshots:
            self.journal_writer.snapshot_add(snapshot)

            # Add branches
            for (branch_name, branch) in snapshot.branches.items():
                if branch is None:
                    target_type = None
                    target = None
                else:
                    target_type = branch.target_type.value
                    target = branch.target
                self._cql_runner.snapshot_branch_add_one({
                    'snapshot_id': snapshot.id,
                    'name': branch_name,
                    'target_type': target_type,
                    'target': target,
                })

            # Add the snapshot *after* adding all the branches, so someone
            # calling snapshot_get_branch in the meantime won't end up
            # with half the branches.
            self._cql_runner.snapshot_add_one(snapshot.id)

        return {'snapshot:add': len(snapshots)}

    def snapshot_missing(self, snapshots):
        return self._cql_runner.snapshot_missing(snapshots)

    def snapshot_get(self, snapshot_id):
        return self.snapshot_get_branches(snapshot_id)

    def snapshot_get_by_origin_visit(self, origin, visit):
        try:
            visit = self._cql_runner.origin_visit_get_one(origin, visit)
        except IndexError:
            return None

        return self.snapshot_get(visit.snapshot)

    def snapshot_get_latest(self, origin, allowed_statuses=None):
        visit = self.origin_visit_get_latest(
            origin,
            allowed_statuses=allowed_statuses,
            require_snapshot=True)

        if visit:
            assert visit['snapshot']
            if self._cql_runner.snapshot_missing([visit['snapshot']]):
                raise StorageArgumentException(
                    'Visit references unknown snapshot')
            return self.snapshot_get_branches(visit['snapshot'])

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

    def snapshot_get_branches(self, snapshot_id, branches_from=b'',
                              branches_count=1000, target_types=None):
        if self._cql_runner.snapshot_missing([snapshot_id]):
            # Makes sure we don't fetch branches for a snapshot that is
            # being added.
            return None

        branches = []
        while len(branches) < branches_count+1:
            new_branches = list(self._cql_runner.snapshot_branch_get(
                snapshot_id, branches_from, branches_count+1))

            if not new_branches:
                break

            branches_from = new_branches[-1].name

            new_branches_filtered = new_branches

            # Filter by target_type
            if target_types:
                new_branches_filtered = [
                    branch for branch in new_branches_filtered
                    if branch.target is not None
                    and branch.target_type in target_types]

            branches.extend(new_branches_filtered)

            if len(new_branches) < branches_count+1:
                break

        if len(branches) > branches_count:
            last_branch = branches.pop(-1).name
        else:
            last_branch = None

        branches = {
            branch.name: {
                'target': branch.target,
                'target_type': branch.target_type,
            } if branch.target else None
            for branch in branches
        }

        return {
            'id': snapshot_id,
            'branches': branches,
            'next_branch': last_branch,
        }

    def snapshot_get_random(self):
        return self._cql_runner.snapshot_get_random().id

    def object_find_by_sha1_git(self, ids):
        results = {id_: [] for id_ in ids}
        missing_ids = set(ids)

        # Mind the order, revision is the most likely one for a given ID,
        # so we check revisions first.
        queries = [
            ('revision', self._cql_runner.revision_missing),
            ('release', self._cql_runner.release_missing),
            ('content', self._cql_runner.content_missing_by_sha1_git),
            ('directory', self._cql_runner.directory_missing),
        ]

        for (object_type, query_fn) in queries:
            found_ids = missing_ids - set(query_fn(missing_ids))
            for sha1_git in found_ids:
                results[sha1_git].append({
                    'sha1_git': sha1_git,
                    'type': object_type,
                })
                missing_ids.remove(sha1_git)

            if not missing_ids:
                # We found everything, skipping the next queries.
                break

        return results

    def origin_get(self, origins):
        if isinstance(origins, dict):
            # Old API
            return_single = True
            origins = [origins]
        else:
            return_single = False

        if any('id' in origin for origin in origins):
            raise StorageArgumentException('Origin ids are not supported.')

        results = [self.origin_get_one(origin) for origin in origins]

        if return_single:
            assert len(results) == 1
            return results[0]
        else:
            return results

    def origin_get_one(self, origin):
        if 'id' in origin:
            raise StorageArgumentException('Origin ids are not supported.')
        if 'url' not in origin:
            raise StorageArgumentException('Missing origin url')
        rows = self._cql_runner.origin_get_by_url(origin['url'])

        rows = list(rows)
        if rows:
            assert len(rows) == 1
            result = rows[0]._asdict()
            return {
                'url': result['url'],
            }
        else:
            return None

    def origin_get_by_sha1(self, sha1s):
        results = []
        for sha1 in sha1s:
            rows = self._cql_runner.origin_get_by_sha1(sha1)
            if rows:
                results.append({'url': rows.one().url})
            else:
                results.append(None)
        return results

    def origin_list(self, page_token: Optional[str] = None, limit: int = 100
                    ) -> dict:
        # Compute what token to begin the listing from
        start_token = TOKEN_BEGIN
        if page_token:
            start_token = int(page_token)
            if not (TOKEN_BEGIN <= start_token <= TOKEN_END):
                raise StorageArgumentException('Invalid page_token.')

        rows = self._cql_runner.origin_list(start_token, limit)
        rows = list(rows)

        if len(rows) == limit:
            next_page_token: Optional[str] = str(rows[-1].tok+1)
        else:
            next_page_token = None

        return {
            'origins': [{'url': row.url} for row in rows],
            'next_page_token': next_page_token,
        }

    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, with_visit=False):
        # TODO: remove this endpoint, swh-search should be used instead.
        origins = self._cql_runner.origin_iter_all()
        if regexp:
            pat = re.compile(url_pattern)
            origins = [orig for orig in origins if pat.search(orig.url)]
        else:
            origins = [orig for orig in origins if url_pattern in orig.url]

        if with_visit:
            origins = [orig for orig in origins
                       if orig.next_visit_id > 1]

        return [
            {
                'url': orig.url,
            }
            for orig in origins[offset:offset+limit]]

    def origin_add(self, origins: Iterable[Origin]) -> List[Dict]:
        results = []
        for origin in origins:
            self.origin_add_one(origin)
            results.append(origin.to_dict())
        return results

    def origin_add_one(self, origin: Origin) -> str:
        known_origin = self.origin_get_one(origin.to_dict())

        if known_origin:
            origin_url = known_origin['url']
        else:
            self.journal_writer.origin_add_one(origin)

            self._cql_runner.origin_add_one(origin)
            origin_url = origin.url

        return origin_url

    def origin_visit_add(
            self, origin, date, type) -> Optional[Dict[str, Union[str, int]]]:
        origin_url = origin  # TODO: rename the argument

        if isinstance(date, str):
            date = dateutil.parser.parse(date)

        origin = self.origin_get_one({'url': origin_url})

        if not origin:
            return None

        visit_id = self._cql_runner.origin_generate_unique_visit_id(origin_url)

        try:
            visit = OriginVisit.from_dict({
                'origin': origin_url,
                'date': date,
                'type': type,
                'status': 'ongoing',
                'snapshot': None,
                'metadata': None,
                'visit': visit_id
            })
        except (KeyError, TypeError, ValueError) as e:
            raise StorageArgumentException(*e.args)
        self.journal_writer.origin_visit_add(visit)

        self._cql_runner.origin_visit_add_one(visit)

        return {
                'origin': origin_url,
                'visit': visit_id,
            }

    def origin_visit_update(
            self, origin: str, visit_id: int, status: Optional[str] = None,
            metadata: Optional[Dict] = None, snapshot: Optional[bytes] = None):
        origin_url = origin  # TODO: rename the argument

        # Get the existing data of the visit
        row = self._cql_runner.origin_visit_get_one(origin_url, visit_id)
        if not row:
            raise StorageArgumentException('This origin visit does not exist.')
        try:
            visit = OriginVisit.from_dict(self._format_origin_visit_row(row))
        except (KeyError, TypeError, ValueError) as e:
            raise StorageArgumentException(*e.args)

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

        self._cql_runner.origin_visit_update(origin_url, visit_id, updates)

    def origin_visit_upsert(self, visits):
        visits = [visit.copy() for visit in visits]
        for visit in visits:
            if isinstance(visit['date'], str):
                visit['date'] = dateutil.parser.parse(visit['date'])

        self.journal_writer.origin_visit_upsert(visits)

        for visit in visits:
            visit = visit.copy()
            if visit.get('metadata'):
                visit['metadata'] = json.dumps(visit['metadata'])
            self._cql_runner.origin_visit_upsert(visit)

    @staticmethod
    def _format_origin_visit_row(visit):
        return {
            **visit._asdict(),
            'origin': visit.origin,
            'date': visit.date.replace(tzinfo=datetime.timezone.utc),
            'metadata': (json.loads(visit.metadata)
                         if visit.metadata else None),
        }

    def origin_visit_get(self, origin, last_visit=None, limit=None):
        rows = self._cql_runner.origin_visit_get(origin, last_visit, limit)

        yield from map(self._format_origin_visit_row, rows)

    def origin_visit_find_by_date(self, origin, visit_date):
        # Iterator over all the visits of the origin
        # This should be ok for now, as there aren't too many visits
        # per origin.
        visits = list(self._cql_runner.origin_visit_get_all(origin))

        def key(visit):
            dt = visit.date.replace(tzinfo=datetime.timezone.utc) - visit_date
            return (abs(dt), -visit.visit)

        if visits:
            visit = min(visits, key=key)
            return visit._asdict()

    def origin_visit_get_by(self, origin, visit):
        visit = self._cql_runner.origin_visit_get_one(origin, visit)
        if visit:
            return self._format_origin_visit_row(visit)

    def origin_visit_get_latest(
            self, origin, allowed_statuses=None, require_snapshot=False):
        visit = self._cql_runner.origin_visit_get_latest(
            origin,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot)
        if visit:
            return self._format_origin_visit_row(visit)

    def origin_visit_get_random(self, type: str) -> Optional[Dict[str, Any]]:
        back_in_the_day = now() - datetime.timedelta(weeks=12)  # 3 months back

        # Random position to start iteration at
        start_token = random.randint(TOKEN_BEGIN, TOKEN_END)

        # Iterator over all visits, ordered by token(origins) then visit_id
        rows = self._cql_runner.origin_visit_iter(start_token)
        for row in rows:
            visit = self._format_origin_visit_row(row)
            if visit['date'] > back_in_the_day \
                    and visit['status'] == 'full':
                return visit
        else:
            return None

    def tool_add(self, tools):
        inserted = []
        for tool in tools:
            tool = tool.copy()
            tool_json = tool.copy()
            tool_json['configuration'] = json.dumps(
                tool['configuration'], sort_keys=True).encode()
            id_ = self._cql_runner.tool_get_one_uuid(**tool_json)
            if not id_:
                id_ = uuid.uuid1()
                tool_json['id'] = id_
                self._cql_runner.tool_by_uuid_add_one(tool_json)
                self._cql_runner.tool_add_one(tool_json)
            tool['id'] = id_
            inserted.append(tool)
        return inserted

    def tool_get(self, tool):
        id_ = self._cql_runner.tool_get_one_uuid(
            tool['name'], tool['version'],
            json.dumps(tool['configuration'], sort_keys=True).encode())
        if id_:
            tool = tool.copy()
            tool['id'] = id_
            return tool
        else:
            return None

    def stat_counters(self):
        rows = self._cql_runner.stat_counters()
        keys = (
            'content', 'directory', 'origin', 'origin_visit',
            'release', 'revision', 'skipped_content', 'snapshot')
        stats = {key: 0 for key in keys}
        stats.update({row.object_type: row.count for row in rows})
        return stats

    def refresh_stat_counters(self):
        pass

    def origin_metadata_add(self, origin_url, ts, provider, tool, metadata):
        # TODO
        raise NotImplementedError('not yet supported for Cassandra')

    def origin_metadata_get_by(self, origin_url, provider_type=None):
        # TODO
        raise NotImplementedError('not yet supported for Cassandra')

    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata):
        # TODO
        raise NotImplementedError('not yet supported for Cassandra')

    def metadata_provider_get(self, provider_id):
        # TODO
        raise NotImplementedError('not yet supported for Cassandra')

    def metadata_provider_get_by(self, provider):
        # TODO
        raise NotImplementedError('not yet supported for Cassandra')
