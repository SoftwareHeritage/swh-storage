# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import itertools
import json

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import dateutil.parser
import psycopg2
import psycopg2.pool

from swh.model.model import SHA1_SIZE
from swh.model.hashutil import ALGORITHMS, hash_to_bytes, hash_to_hex
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
try:
    from swh.journal.writer import get_journal_writer
except ImportError:
    get_journal_writer = None  # type: ignore
    # mypy limitation, see https://github.com/python/mypy/issues/1153

from . import converters
from .common import db_transaction_generator, db_transaction
from .db import Db
from .exc import StorageDBError
from .algos import diff
from .metrics import timed, send_metric, process_metrics
from .utils import get_partition_bounds_bytes


# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000

EMPTY_SNAPSHOT_ID = hash_to_bytes('1a8893e6a86f444e8be8e7bda6cb34fb1735a00e')
"""Identifier for the empty snapshot"""


class Storage():
    """SWH storage proxy, encompassing DB and object storage

    """

    def __init__(self, db, objstorage, min_pool_conns=1, max_pool_conns=10,
                 journal_writer=None):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection
            obj_root: path to the root of the object storage

        """
        try:
            if isinstance(db, psycopg2.extensions.connection):
                self._pool = None
                self._db = Db(db)
            else:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    min_pool_conns, max_pool_conns, db
                )
                self._db = None
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)

        self.objstorage = get_objstorage(**objstorage)
        if journal_writer:
            if get_journal_writer is None:
                raise EnvironmentError(
                    'You need the swh.journal package to use the '
                    'journal_writer feature')
            self.journal_writer = get_journal_writer(**journal_writer)
        else:
            self.journal_writer = None

    def get_db(self):
        if self._db:
            return self._db
        else:
            return Db.from_pool(self._pool)

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()

    @contextmanager
    def db(self):
        db = None
        try:
            db = self.get_db()
            yield db
        finally:
            if db:
                self.put_db(db)

    @timed
    @db_transaction()
    def check_config(self, *, check_write, db=None, cur=None):

        if not self.objstorage.check_config(check_write=check_write):
            return False

        # Check permissions on one of the tables
        if check_write:
            check = 'INSERT'
        else:
            check = 'SELECT'

        cur.execute(
            "select has_table_privilege(current_user, 'content', %s)",
            (check,)
        )
        return cur.fetchone()[0]

    def _content_unique_key(self, hash, db):
        """Given a hash (tuple or dict), return a unique key from the
           aggregation of keys.

        """
        keys = db.content_hash_keys
        if isinstance(hash, tuple):
            return hash
        return tuple([hash[k] for k in keys])

    @staticmethod
    def _normalize_content(d):
        d = d.copy()

        if 'status' not in d:
            d['status'] = 'visible'

        if 'length' not in d:
            d['length'] = -1

        return d

    @staticmethod
    def _validate_content(d):
        """Sanity checks on status / reason / length, that postgresql
        doesn't enforce."""
        if d['status'] not in ('visible', 'absent', 'hidden'):
            raise ValueError('Invalid content status: {}'.format(d['status']))

        if d['status'] != 'absent' and d.get('reason') is not None:
            raise ValueError(
                'Must not provide a reason if content is not absent.')

        if d['length'] < -1:
            raise ValueError('Content length must be positive or -1.')

    def _filter_new_content(self, content, db=None, cur=None):
        """Sort contents into buckets 'with data' and 'without data',
        and filter out those already in the database."""
        content_by_status = defaultdict(list)
        for d in content:
            content_by_status[d['status']].append(d)

        content_with_data = content_by_status['visible'] \
            + content_by_status['hidden']
        content_without_data = content_by_status['absent']

        missing_content = set(self.content_missing(content_with_data,
                                                   db=db, cur=cur))
        missing_skipped = set(self._content_unique_key(hashes, db)
                              for hashes in self.skipped_content_missing(
                                  content_without_data, db=db, cur=cur))

        content_with_data = [
            cont for cont in content_with_data
            if cont['sha1'] in missing_content]
        content_without_data = [
            cont for cont in content_without_data
            if self._content_unique_key(cont, db) in missing_skipped]

        summary = {
            'content:add': len(missing_content),
            'skipped_content:add': len(missing_skipped),
        }

        return (content_with_data, content_without_data, summary)

    def _content_add_metadata(self, db, cur,
                              content_with_data, content_without_data):
        """Add content to the postgresql database but not the object storage.
        """
        if content_with_data:
            # create temporary table for metadata injection
            db.mktemp('content', cur)

            db.copy_to(content_with_data, 'tmp_content',
                       db.content_add_keys, cur)

            # move metadata in place
            try:
                db.content_add_from_temp(cur)
            except psycopg2.IntegrityError as e:
                from . import HashCollision
                if e.diag.sqlstate == '23505' and \
                        e.diag.table_name == 'content':
                    constraint_to_hash_name = {
                        'content_pkey': 'sha1',
                        'content_sha1_git_idx': 'sha1_git',
                        'content_sha256_idx': 'sha256',
                        }
                    colliding_hash_name = constraint_to_hash_name \
                        .get(e.diag.constraint_name)
                    raise HashCollision(colliding_hash_name) from None
                else:
                    raise

        if content_without_data:
            content_without_data = \
                [cont.copy() for cont in content_without_data]
            origin_ids = db.origin_id_get_by_url(
                [cont.get('origin') for cont in content_without_data],
                cur=cur)
            for (cont, origin_id) in zip(content_without_data, origin_ids):
                if 'origin' in cont:
                    cont['origin'] = origin_id
            db.mktemp('skipped_content', cur)
            db.copy_to(content_without_data, 'tmp_skipped_content',
                       db.skipped_content_keys, cur)

            # move metadata in place
            db.skipped_content_add_from_temp(cur)

    @timed
    @process_metrics
    @db_transaction()
    def content_add(self, content, db=None, cur=None):
        content = [dict(c.items()) for c in content]  # semi-shallow copy
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        for item in content:
            item['ctime'] = now

        content = [self._normalize_content(c) for c in content]
        for c in content:
            self._validate_content(c)

        (content_with_data, content_without_data, summary) = \
            self._filter_new_content(content, db, cur)

        if self.journal_writer:
            for item in content_with_data:
                if 'data' in item:
                    item = item.copy()
                    del item['data']
                self.journal_writer.write_addition('content', item)
            for item in content_without_data:
                self.journal_writer.write_addition('content', item)

        def add_to_objstorage():
            """Add to objstorage the new missing_content

            Returns:
                Sum of all the content's data length pushed to the
                objstorage. Content present twice is only sent once.

            """
            content_bytes_added = 0
            data = {}
            for cont in content_with_data:
                if cont['sha1'] not in data:
                    data[cont['sha1']] = cont['data']
                    content_bytes_added += max(0, cont['length'])

            # FIXME: Since we do the filtering anyway now, we might as
            # well make the objstorage's add_batch call return what we
            # want here (real bytes added)... that'd simplify this...
            self.objstorage.add_batch(data)
            return content_bytes_added

        with ThreadPoolExecutor(max_workers=1) as executor:
            added_to_objstorage = executor.submit(add_to_objstorage)

            self._content_add_metadata(
                db, cur, content_with_data, content_without_data)

            # Wait for objstorage addition before returning from the
            # transaction, bubbling up any exception
            content_bytes_added = added_to_objstorage.result()

        summary['content:add:bytes'] = content_bytes_added
        return summary

    @timed
    @db_transaction()
    def content_update(self, content, keys=[], db=None, cur=None):
        # TODO: Add a check on input keys. How to properly implement
        # this? We don't know yet the new columns.

        if self.journal_writer:
            raise NotImplementedError(
                'content_update is not yet supported with a journal_writer.')

        db.mktemp('content', cur)
        select_keys = list(set(db.content_get_metadata_keys).union(set(keys)))
        db.copy_to(content, 'tmp_content', select_keys, cur)
        db.content_update_from_temp(keys_to_update=keys,
                                    cur=cur)

    @timed
    @process_metrics
    @db_transaction()
    def content_add_metadata(self, content, db=None, cur=None):
        content = [self._normalize_content(c) for c in content]
        for c in content:
            self._validate_content(c)

        (content_with_data, content_without_data, summary) = \
            self._filter_new_content(content, db, cur)

        if self.journal_writer:
            for item in itertools.chain(content_with_data,
                                        content_without_data):
                assert 'data' not in content
                self.journal_writer.write_addition('content', item)

        self._content_add_metadata(
            db, cur, content_with_data, content_without_data)

        return summary

    @timed
    def content_get(self, content):
        # FIXME: Make this method support slicing the `data`.
        if len(content) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise ValueError(
                "Send at maximum %s contents." % BULK_BLOCK_CONTENT_LEN_MAX)

        for obj_id in content:
            try:
                data = self.objstorage.get(obj_id)
            except ObjNotFoundError:
                yield None
                continue

            yield {'sha1': obj_id, 'data': data}

    @timed
    @db_transaction()
    def content_get_range(self, start, end, limit=1000, db=None, cur=None):
        if limit is None:
            raise ValueError('Development error: limit should not be None')
        contents = []
        next_content = None
        for counter, content_row in enumerate(
                db.content_get_range(start, end, limit+1, cur)):
            content = dict(zip(db.content_get_metadata_keys, content_row))
            if counter >= limit:
                # take the last commit for the next page starting from this
                next_content = content['sha1']
                break
            contents.append(content)
        return {
            'contents': contents,
            'next': next_content,
        }

    @timed
    @db_transaction()
    def content_get_partition(
            self, partition_id: int, nb_partitions: int, limit: int = 1000,
            page_token: str = None, db=None, cur=None):
        if limit is None:
            raise ValueError('Development error: limit should not be None')
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

    @timed
    @db_transaction(statement_timeout=500)
    def content_get_metadata(
            self, contents: List[bytes],
            db=None, cur=None) -> Dict[bytes, List[Dict]]:
        result: Dict[bytes, List[Dict]] = {sha1: [] for sha1 in contents}
        for row in db.content_get_metadata_from_sha1s(contents, cur):
            content_meta = dict(zip(db.content_get_metadata_keys, row))
            result[content_meta['sha1']].append(content_meta)
        return result

    @timed
    @db_transaction_generator()
    def content_missing(self, content, key_hash='sha1', db=None, cur=None):
        keys = db.content_hash_keys

        if key_hash not in keys:
            raise ValueError("key_hash should be one of %s" % keys)

        key_hash_idx = keys.index(key_hash)

        if not content:
            return

        for obj in db.content_missing_from_list(content, cur):
            yield obj[key_hash_idx]

    @timed
    @db_transaction_generator()
    def content_missing_per_sha1(self, contents, db=None, cur=None):
        for obj in db.content_missing_per_sha1(contents, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def content_missing_per_sha1_git(self, contents, db=None, cur=None):
        for obj in db.content_missing_per_sha1_git(contents, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def skipped_content_missing(self, contents, db=None, cur=None):
        for content in db.skipped_content_missing(contents, cur):
            yield dict(zip(db.content_hash_keys, content))

    @timed
    @db_transaction()
    def content_find(self, content, db=None, cur=None):
        if not set(content).intersection(ALGORITHMS):
            raise ValueError('content keys must contain at least one of: '
                             'sha1, sha1_git, sha256, blake2s256')

        contents = db.content_find(sha1=content.get('sha1'),
                                   sha1_git=content.get('sha1_git'),
                                   sha256=content.get('sha256'),
                                   blake2s256=content.get('blake2s256'),
                                   cur=cur)
        return [dict(zip(db.content_find_cols, content))
                for content in contents]

    @timed
    @db_transaction()
    def content_get_random(self, db=None, cur=None):
        return db.content_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def directory_add(self, directories, db=None, cur=None):
        directories = list(directories)
        summary = {'directory:add': 0}

        dirs = set()
        dir_entries = {
            'file': defaultdict(list),
            'dir': defaultdict(list),
            'rev': defaultdict(list),
        }

        for cur_dir in directories:
            dir_id = cur_dir['id']
            dirs.add(dir_id)
            for src_entry in cur_dir['entries']:
                entry = src_entry.copy()
                entry['dir_id'] = dir_id
                if entry['type'] not in ('file', 'dir', 'rev'):
                    raise ValueError(
                        'Entry type must be file, dir, or rev; not %s'
                        % entry['type'])
                dir_entries[entry['type']][dir_id].append(entry)

        dirs_missing = set(self.directory_missing(dirs, db=db, cur=cur))
        if not dirs_missing:
            return summary

        if self.journal_writer:
            self.journal_writer.write_additions(
                'directory',
                (dir_ for dir_ in directories
                 if dir_['id'] in dirs_missing))

        # Copy directory ids
        dirs_missing_dict = ({'id': dir} for dir in dirs_missing)
        db.mktemp('directory', cur)
        db.copy_to(dirs_missing_dict, 'tmp_directory', ['id'], cur)

        # Copy entries
        for entry_type, entry_list in dir_entries.items():
            entries = itertools.chain.from_iterable(
                entries_for_dir
                for dir_id, entries_for_dir
                in entry_list.items()
                if dir_id in dirs_missing)

            db.mktemp_dir_entry(entry_type)

            db.copy_to(
                entries,
                'tmp_directory_entry_%s' % entry_type,
                ['target', 'name', 'perms', 'dir_id'],
                cur,
            )

        # Do the final copy
        db.directory_add_from_temp(cur)
        summary['directory:add'] = len(dirs_missing)

        return summary

    @timed
    @db_transaction_generator()
    def directory_missing(self, directories, db=None, cur=None):
        for obj in db.directory_missing_from_list(directories, cur):
            yield obj[0]

    @timed
    @db_transaction_generator(statement_timeout=20000)
    def directory_ls(self, directory, recursive=False, db=None, cur=None):
        if recursive:
            res_gen = db.directory_walk(directory, cur=cur)
        else:
            res_gen = db.directory_walk_one(directory, cur=cur)

        for line in res_gen:
            yield dict(zip(db.directory_ls_cols, line))

    @timed
    @db_transaction(statement_timeout=2000)
    def directory_entry_get_by_path(self, directory, paths, db=None, cur=None):
        res = db.directory_entry_get_by_path(directory, paths, cur)
        if res:
            return dict(zip(db.directory_ls_cols, res))

    @timed
    @db_transaction()
    def directory_get_random(self, db=None, cur=None):
        return db.directory_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def revision_add(self, revisions, db=None, cur=None):
        revisions = list(revisions)
        summary = {'revision:add': 0}

        revisions_missing = set(self.revision_missing(
            set(revision['id'] for revision in revisions),
            db=db, cur=cur))

        if not revisions_missing:
            return summary

        db.mktemp_revision(cur)

        revisions_filtered = [
            revision for revision in revisions
            if revision['id'] in revisions_missing]

        if self.journal_writer:
            self.journal_writer.write_additions('revision', revisions_filtered)

        revisions_filtered = map(converters.revision_to_db, revisions_filtered)

        parents_filtered = []

        db.copy_to(
            revisions_filtered, 'tmp_revision', db.revision_add_cols,
            cur,
            lambda rev: parents_filtered.extend(rev['parents']))

        db.revision_add_from_temp(cur)

        db.copy_to(parents_filtered, 'revision_history',
                   ['id', 'parent_id', 'parent_rank'], cur)

        return {'revision:add': len(revisions_missing)}

    @timed
    @db_transaction_generator()
    def revision_missing(self, revisions, db=None, cur=None):
        if not revisions:
            return

        for obj in db.revision_missing_from_list(revisions, cur):
            yield obj[0]

    @timed
    @db_transaction_generator(statement_timeout=1000)
    def revision_get(self, revisions, db=None, cur=None):
        for line in db.revision_get_from_list(revisions, cur):
            data = converters.db_to_revision(
                dict(zip(db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    @timed
    @db_transaction_generator(statement_timeout=2000)
    def revision_log(self, revisions, limit=None, db=None, cur=None):
        for line in db.revision_log(revisions, limit, cur):
            data = converters.db_to_revision(
                dict(zip(db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    @timed
    @db_transaction_generator(statement_timeout=2000)
    def revision_shortlog(self, revisions, limit=None, db=None, cur=None):

        yield from db.revision_shortlog(revisions, limit, cur)

    @timed
    @db_transaction()
    def revision_get_random(self, db=None, cur=None):
        return db.revision_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def release_add(self, releases, db=None, cur=None):
        releases = list(releases)
        summary = {'release:add': 0}

        release_ids = set(release['id'] for release in releases)
        releases_missing = set(self.release_missing(release_ids,
                                                    db=db, cur=cur))

        if not releases_missing:
            return summary

        db.mktemp_release(cur)

        releases_missing = list(releases_missing)

        releases_filtered = [
            release for release in releases
            if release['id'] in releases_missing
        ]

        if self.journal_writer:
            self.journal_writer.write_additions('release', releases_filtered)

        releases_filtered = map(converters.release_to_db, releases_filtered)

        db.copy_to(releases_filtered, 'tmp_release', db.release_add_cols,
                   cur)

        db.release_add_from_temp(cur)

        return {'release:add': len(releases_missing)}

    @timed
    @db_transaction_generator()
    def release_missing(self, releases, db=None, cur=None):
        if not releases:
            return

        for obj in db.release_missing_from_list(releases, cur):
            yield obj[0]

    @timed
    @db_transaction_generator(statement_timeout=500)
    def release_get(self, releases, db=None, cur=None):
        for release in db.release_get_from_list(releases, cur):
            data = converters.db_to_release(
                dict(zip(db.release_get_cols, release))
            )
            yield data if data['target_type'] else None

    @timed
    @db_transaction()
    def release_get_random(self, db=None, cur=None):
        return db.release_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def snapshot_add(self, snapshots, db=None, cur=None):
        created_temp_table = False

        count = 0
        for snapshot in snapshots:
            if not db.snapshot_exists(snapshot['id'], cur):
                if not created_temp_table:
                    db.mktemp_snapshot_branch(cur)
                    created_temp_table = True

                db.copy_to(
                    (
                        {
                            'name': name,
                            'target': info['target'] if info else None,
                            'target_type': (info['target_type']
                                            if info else None),
                        }
                        for name, info in snapshot['branches'].items()
                    ),
                    'tmp_snapshot_branch',
                    ['name', 'target', 'target_type'],
                    cur,
                )

                if self.journal_writer:
                    self.journal_writer.write_addition('snapshot', snapshot)

                db.snapshot_add(snapshot['id'], cur)
                count += 1

        return {'snapshot:add': count}

    @timed
    @db_transaction_generator()
    def snapshot_missing(self, snapshots, db=None, cur=None):
        for obj in db.snapshot_missing_from_list(snapshots, cur):
            yield obj[0]

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_get(self, snapshot_id, db=None, cur=None):

        return self.snapshot_get_branches(snapshot_id, db=db, cur=cur)

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_get_by_origin_visit(self, origin, visit, db=None, cur=None):
        snapshot_id = db.snapshot_get_by_origin_visit(origin, visit, cur)

        if snapshot_id:
            return self.snapshot_get(snapshot_id, db=db, cur=cur)

        return None

    @timed
    @db_transaction(statement_timeout=4000)
    def snapshot_get_latest(self, origin, allowed_statuses=None, db=None,
                            cur=None):
        if isinstance(origin, int):
            origin = self.origin_get({'id': origin}, db=db, cur=cur)
            if not origin:
                return
            origin = origin['url']

        origin_visit = self.origin_visit_get_latest(
            origin, allowed_statuses=allowed_statuses, require_snapshot=True,
            db=db, cur=cur)
        if origin_visit and origin_visit['snapshot']:
            snapshot = self.snapshot_get(
                    origin_visit['snapshot'], db=db, cur=cur)
            if not snapshot:
                raise ValueError(
                    'last origin visit references an unknown snapshot')
            return snapshot

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_count_branches(self, snapshot_id, db=None, cur=None):
        return dict([bc for bc in
                     db.snapshot_count_branches(snapshot_id, cur)])

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_get_branches(self, snapshot_id, branches_from=b'',
                              branches_count=1000, target_types=None,
                              db=None, cur=None):
        if snapshot_id == EMPTY_SNAPSHOT_ID:
            return {
                'id': snapshot_id,
                'branches': {},
                'next_branch': None,
            }

        branches = {}
        next_branch = None

        fetched_branches = list(db.snapshot_get_by_id(
            snapshot_id, branches_from=branches_from,
            branches_count=branches_count+1, target_types=target_types,
            cur=cur,
        ))
        for branch in fetched_branches[:branches_count]:
            branch = dict(zip(db.snapshot_get_cols, branch))
            del branch['snapshot_id']
            name = branch.pop('name')
            if branch == {'target': None, 'target_type': None}:
                branch = None
            branches[name] = branch

        if len(fetched_branches) > branches_count:
            branch = dict(zip(db.snapshot_get_cols, fetched_branches[-1]))
            next_branch = branch['name']

        if branches:
            return {
                'id': snapshot_id,
                'branches': branches,
                'next_branch': next_branch,
            }

        return None

    @timed
    @db_transaction()
    def snapshot_get_random(self, db=None, cur=None):
        return db.snapshot_get_random(cur)

    @timed
    @db_transaction()
    def origin_visit_add(self, origin, date, type,
                         db=None, cur=None):
        origin_url = origin

        if isinstance(date, str):
            # FIXME: Converge on iso8601 at some point
            date = dateutil.parser.parse(date)

        visit_id = db.origin_visit_add(origin_url, date, type, cur)

        if self.journal_writer:
            # We can write to the journal only after inserting to the
            # DB, because we want the id of the visit
            self.journal_writer.write_addition('origin_visit', {
                'origin': origin_url, 'date': date, 'type': type,
                'visit': visit_id,
                'status': 'ongoing', 'metadata': None, 'snapshot': None})

        send_metric('origin_visit:add', count=1, method_name='origin_visit')
        return {
            'origin': origin_url,
            'visit': visit_id,
        }

    @timed
    @db_transaction()
    def origin_visit_update(self, origin, visit_id, status=None,
                            metadata=None, snapshot=None,
                            db=None, cur=None):
        if not isinstance(origin, str):
            raise TypeError('origin must be a string, not %r' % (origin,))
        origin_url = origin
        visit = db.origin_visit_get(origin_url, visit_id, cur=cur)

        if not visit:
            raise ValueError('Invalid visit_id for this origin.')

        visit = dict(zip(db.origin_visit_get_cols, visit))

        updates = {}
        if status and status != visit['status']:
            updates['status'] = status
        if metadata and metadata != visit['metadata']:
            updates['metadata'] = metadata
        if snapshot and snapshot != visit['snapshot']:
            updates['snapshot'] = snapshot

        if updates:
            if self.journal_writer:
                self.journal_writer.write_update('origin_visit', {
                    **visit, **updates})

            db.origin_visit_update(origin_url, visit_id, updates, cur)

    @timed
    @db_transaction()
    def origin_visit_upsert(self, visits, db=None, cur=None):
        visits = copy.deepcopy(visits)
        for visit in visits:
            if isinstance(visit['date'], str):
                visit['date'] = dateutil.parser.parse(visit['date'])
            if not isinstance(visit['origin'], str):
                raise TypeError("visit['origin'] must be a string, not %r"
                                % (visit['origin'],))

        if self.journal_writer:
            for visit in visits:
                self.journal_writer.write_addition('origin_visit', visit)

        for visit in visits:
            # TODO: upsert them all in a single query
            db.origin_visit_upsert(**visit, cur=cur)

    @timed
    @db_transaction_generator(statement_timeout=500)
    def origin_visit_get(self, origin, last_visit=None, limit=None, db=None,
                         cur=None):
        for line in db.origin_visit_get_all(
                origin, last_visit=last_visit, limit=limit, cur=cur):
            data = dict(zip(db.origin_visit_get_cols, line))
            yield data

    @timed
    @db_transaction(statement_timeout=500)
    def origin_visit_find_by_date(self, origin, visit_date, db=None, cur=None):
        line = db.origin_visit_find_by_date(origin, visit_date, cur=cur)
        if line:
            return dict(zip(db.origin_visit_get_cols, line))

    @timed
    @db_transaction(statement_timeout=500)
    def origin_visit_get_by(self, origin, visit, db=None, cur=None):
        ori_visit = db.origin_visit_get(origin, visit, cur)
        if not ori_visit:
            return None

        return dict(zip(db.origin_visit_get_cols, ori_visit))

    @timed
    @db_transaction(statement_timeout=4000)
    def origin_visit_get_latest(
            self, origin, allowed_statuses=None, require_snapshot=False,
            db=None, cur=None):
        origin_visit = db.origin_visit_get_latest(
            origin, allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot, cur=cur)
        if origin_visit:
            return dict(zip(db.origin_visit_get_cols, origin_visit))

    @timed
    @db_transaction()
    def origin_visit_get_random(
            self, type: str, db=None, cur=None) -> Optional[Dict[str, Any]]:
        result = db.origin_visit_get_random(type, cur)
        if result:
            return dict(zip(db.origin_visit_get_cols, result))
        else:
            return None

    @timed
    @db_transaction(statement_timeout=2000)
    def object_find_by_sha1_git(self, ids, db=None, cur=None):
        ret = {id: [] for id in ids}

        for retval in db.object_find_by_sha1_git(ids, cur=cur):
            if retval[1]:
                ret[retval[0]].append(dict(zip(db.object_find_by_sha1_git_cols,
                                               retval)))

        return ret

    @timed
    @db_transaction(statement_timeout=500)
    def origin_get(self, origins, db=None, cur=None):
        if isinstance(origins, dict):
            # Old API
            return_single = True
            origins = [origins]
        elif len(origins) == 0:
            return []
        else:
            return_single = False

        origin_urls = [origin['url'] for origin in origins]
        results = db.origin_get_by_url(origin_urls, cur)

        results = [dict(zip(db.origin_cols, result))
                   for result in results]
        if return_single:
            assert len(results) == 1
            if results[0]['url'] is not None:
                return results[0]
            else:
                return None
        else:
            return [None if res['url'] is None else res for res in results]

    @timed
    @db_transaction_generator(statement_timeout=500)
    def origin_get_by_sha1(self, sha1s, db=None, cur=None):
        for line in db.origin_get_by_sha1(sha1s, cur):
            if line[0] is not None:
                yield dict(zip(db.origin_cols, line))
            else:
                yield None

    @timed
    @db_transaction_generator()
    def origin_get_range(self, origin_from=1, origin_count=100,
                         db=None, cur=None):
        for origin in db.origin_get_range(origin_from, origin_count, cur):
            yield dict(zip(db.origin_get_range_cols, origin))

    @timed
    @db_transaction()
    def origin_list(self, page_token: Optional[str] = None, limit: int = 100,
                    *, db=None, cur=None) -> dict:
        page_token = page_token or '0'
        if not isinstance(page_token, str):
            raise TypeError('page_token must be a string.')
        origin_from = int(page_token)
        result: Dict[str, Any] = {
            'origins': [
                dict(zip(db.origin_get_range_cols, origin))
                for origin in db.origin_get_range(origin_from, limit, cur)
            ],
        }

        assert len(result['origins']) <= limit
        if len(result['origins']) == limit:
            result['next_page_token'] = str(result['origins'][limit-1]['id']+1)

        for origin in result['origins']:
            del origin['id']

        return result

    @timed
    @db_transaction_generator()
    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, with_visit=False, db=None, cur=None):
        for origin in db.origin_search(url_pattern, offset, limit,
                                       regexp, with_visit, cur):
            yield dict(zip(db.origin_cols, origin))

    @timed
    @db_transaction()
    def origin_count(self, url_pattern, regexp=False,
                     with_visit=False, db=None, cur=None):
        return db.origin_count(url_pattern, regexp, with_visit, cur)

    @timed
    @db_transaction()
    def origin_add(self, origins, db=None, cur=None):
        origins = copy.deepcopy(list(origins))
        for origin in origins:
            self.origin_add_one(origin, db=db, cur=cur)

        send_metric('origin:add', count=len(origins), method_name='origin_add')
        return origins

    @timed
    @db_transaction()
    def origin_add_one(self, origin, db=None, cur=None):
        origin_row = list(db.origin_get_by_url([origin['url']], cur))[0]
        origin_url = dict(zip(db.origin_cols, origin_row))['url']
        if origin_url:
            return origin_url

        if self.journal_writer:
            self.journal_writer.write_addition('origin', origin)

        origins = db.origin_add(origin['url'], cur)
        send_metric('origin:add', count=len(origins), method_name='origin_add')
        return origins

    @db_transaction(statement_timeout=500)
    def stat_counters(self, db=None, cur=None):
        return {k: v for (k, v) in db.stat_counters()}

    @db_transaction()
    def refresh_stat_counters(self, db=None, cur=None):
        keys = [
            'content',
            'directory',
            'directory_entry_dir',
            'directory_entry_file',
            'directory_entry_rev',
            'origin',
            'origin_visit',
            'person',
            'release',
            'revision',
            'revision_history',
            'skipped_content',
            'snapshot']

        for key in keys:
            cur.execute('select * from swh_update_counter(%s)', (key,))

    @timed
    @db_transaction()
    def origin_metadata_add(self, origin_url, ts, provider, tool, metadata,
                            db=None, cur=None):
        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        db.origin_metadata_add(origin_url, ts, provider, tool,
                               metadata, cur)
        send_metric(
            'origin_metadata:add', count=1, method_name='origin_metadata_add')

    @timed
    @db_transaction_generator(statement_timeout=500)
    def origin_metadata_get_by(self, origin_url, provider_type=None, db=None,
                               cur=None):
        for line in db.origin_metadata_get_by(origin_url, provider_type, cur):
            yield dict(zip(db.origin_metadata_get_cols, line))

    @timed
    @db_transaction()
    def tool_add(self, tools, db=None, cur=None):
        db.mktemp_tool(cur)
        db.copy_to(tools, 'tmp_tool',
                   ['name', 'version', 'configuration'],
                   cur)

        tools = db.tool_add_from_temp(cur)
        results = [dict(zip(db.tool_cols, line)) for line in tools]
        send_metric('tool:add', count=len(results), method_name='tool_add')
        return results

    @timed
    @db_transaction(statement_timeout=500)
    def tool_get(self, tool, db=None, cur=None):
        tool_conf = tool['configuration']
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)

        idx = db.tool_get(tool['name'],
                          tool['version'],
                          tool_conf)
        if not idx:
            return None
        return dict(zip(db.tool_cols, idx))

    @timed
    @db_transaction()
    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata, db=None, cur=None):
        result = db.metadata_provider_add(provider_name, provider_type,
                                          provider_url, metadata, cur)
        send_metric(
            'metadata_provider:add', count=1, method_name='metadata_provider')
        return result

    @timed
    @db_transaction()
    def metadata_provider_get(self, provider_id, db=None, cur=None):
        result = db.metadata_provider_get(provider_id)
        if not result:
            return None
        return dict(zip(db.metadata_provider_cols, result))

    @timed
    @db_transaction()
    def metadata_provider_get_by(self, provider, db=None, cur=None):
        result = db.metadata_provider_get_by(provider['provider_name'],
                                             provider['provider_url'])
        if not result:
            return None
        return dict(zip(db.metadata_provider_cols, result))

    @timed
    def diff_directories(self, from_dir, to_dir, track_renaming=False):
        return diff.diff_directories(self, from_dir, to_dir, track_renaming)

    @timed
    def diff_revisions(self, from_rev, to_rev, track_renaming=False):
        return diff.diff_revisions(self, from_rev, to_rev, track_renaming)

    @timed
    def diff_revision(self, revision, track_renaming=False):
        return diff.diff_revision(self, revision, track_renaming)
