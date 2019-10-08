# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from collections import defaultdict
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import datetime
import itertools
import json

import dateutil.parser
import psycopg2
import psycopg2.pool

from . import converters
from .common import db_transaction_generator, db_transaction
from .db import Db
from .exc import StorageDBError
from .algos import diff

from swh.model.hashutil import ALGORITHMS, hash_to_bytes
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
try:
    from swh.journal.writer import get_journal_writer
except ImportError:
    get_journal_writer = None  # type: ignore
    # mypy limitation, see https://github.com/python/mypy/issues/1153


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

    @db_transaction()
    def check_config(self, *, check_write, db, cur):
        """Check that the storage is configured and ready to go."""

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

    def _filter_new_content(self, content, db, cur):
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
            origins = db.origin_get_by_url(
                [cont.get('origin') for cont in content_without_data],
                cur=cur)
            for (cont, origin) in zip(content_without_data, origins):
                origin = dict(zip(db.origin_cols, origin))
                if 'origin' in cont:
                    cont['origin'] = origin['id']
            db.mktemp('skipped_content', cur)
            db.copy_to(content_without_data, 'tmp_skipped_content',
                       db.skipped_content_keys, cur)

            # move metadata in place
            db.skipped_content_add_from_temp(cur)

    @db_transaction()
    def content_add(self, content, db, cur):
        """Add content blobs to the storage

        Note: in case of DB errors, objects might have already been added to
        the object storage and will not be removed. Since addition to the
        object storage is idempotent, that should not be a problem.

        Args:
            contents (iterable): iterable of dictionaries representing
                individual pieces of content to add. Each dictionary has the
                following keys:

                - data (bytes): the actual content
                - length (int): content length (default: -1)
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.ALGORITHMS`, mapped to the
                  corresponding checksum
                - status (str): one of visible, hidden, absent
                - reason (str): if status = absent, the reason why
                - origin (int): if status = absent, the origin we saw the
                  content in

        Raises:

            In case of errors, nothing is stored in the db (in the
            objstorage, it could though). The following exceptions can
            occur:

            - HashCollision in case of collision
            - Any other exceptions raise by the db

        Returns:
            Summary dict with the following key and associated values:

                content:add: New contents added
                content:add:bytes: Sum of the contents' length data
                skipped_content:add: New skipped contents (no data) added
        """
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

    @db_transaction()
    def content_update(self, content, keys=[], db=None, cur=None):
        """Update content blobs to the storage. Does nothing for unknown
        contents or skipped ones.

        Args:
            content (iterable): iterable of dictionaries representing
                individual pieces of content to update. Each dictionary has the
                following keys:

                - data (bytes): the actual content
                - length (int): content length (default: -1)
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.ALGORITHMS`, mapped to the
                  corresponding checksum
                - status (str): one of visible, hidden, absent

            keys (list): List of keys (str) whose values needs an update, e.g.,
                new hash column

        """
        # TODO: Add a check on input keys. How to properly implement
        # this? We don't know yet the new columns.

        if self.journal_writer:
            raise NotImplementedError(
                'content_update is not yet support with a journal_writer.')

        db.mktemp('content', cur)
        select_keys = list(set(db.content_get_metadata_keys).union(set(keys)))
        db.copy_to(content, 'tmp_content', select_keys, cur)
        db.content_update_from_temp(keys_to_update=keys,
                                    cur=cur)

    @db_transaction()
    def content_add_metadata(self, content, db, cur):
        """Add content metadata to the storage (like `content_add`, but
        without inserting to the objstorage).

        Args:
            content (iterable): iterable of dictionaries representing
                individual pieces of content to add. Each dictionary has the
                following keys:

                - length (int): content length (default: -1)
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.ALGORITHMS`, mapped to the
                  corresponding checksum
                - status (str): one of visible, hidden, absent
                - reason (str): if status = absent, the reason why
                - origin (int): if status = absent, the origin we saw the
                  content in
                - ctime (datetime): time of insertion in the archive

        Returns:
            Summary dict with the following key and associated values:

                content:add: New contents added
                skipped_content:add: New skipped contents (no data) added
        """

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

    def content_get(self, content):
        """Retrieve in bulk contents and their data.

        This generator yields exactly as many items than provided sha1
        identifiers, but callers should not assume this will always be true.

        It may also yield `None` values in case an object was not found.

        Args:
            content: iterables of sha1

        Yields:
            Dict[str, bytes]: Generates streams of contents as dict with their
                raw data:

                - sha1 (bytes): content id
                - data (bytes): content's raw data

        Raises:
            ValueError in case of too much contents are required.
            cf. BULK_BLOCK_CONTENT_LEN_MAX

        """
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

    @db_transaction()
    def content_get_range(self, start, end, limit=1000, db=None, cur=None):
        """Retrieve contents within range [start, end] bound by limit.

        Note that this function may return more than one blob per hash. The
        limit is enforced with multiplicity (ie. two blobs with the same hash
        will count twice toward the limit).

        Args:
            **start** (bytes): Starting identifier range (expected smaller
                           than end)
            **end** (bytes): Ending identifier range (expected larger
                             than start)
            **limit** (int): Limit result (default to 1000)

        Returns:
            a dict with keys:
            - contents [dict]: iterable of contents in between the range.
            - next (bytes): There remains content in the range
              starting from this next sha1

        """
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

    @db_transaction_generator(statement_timeout=500)
    def content_get_metadata(self, content, db=None, cur=None):
        """Retrieve content metadata in bulk

        Args:
            content: iterable of content identifiers (sha1)

        Returns:
            an iterable with content metadata corresponding to the given ids
        """
        for metadata in db.content_get_metadata_from_sha1s(content, cur):
            yield dict(zip(db.content_get_metadata_keys, metadata))

    @db_transaction_generator()
    def content_missing(self, content, key_hash='sha1', db=None, cur=None):
        """List content missing from storage

        Args:
            content ([dict]): iterable of dictionaries whose keys are
                              either 'length' or an item of
                              :data:`swh.model.hashutil.ALGORITHMS`;
                              mapped to the corresponding checksum
                              (or length).

            key_hash (str): name of the column to use as hash id
                            result (default: 'sha1')

        Returns:
            iterable ([bytes]): missing content ids (as per the
            key_hash column)

        Raises:
            TODO: an exception when we get a hash collision.

        """
        keys = db.content_hash_keys

        if key_hash not in keys:
            raise ValueError("key_hash should be one of %s" % keys)

        key_hash_idx = keys.index(key_hash)

        if not content:
            return

        for obj in db.content_missing_from_list(content, cur):
            yield obj[key_hash_idx]

    @db_transaction_generator()
    def content_missing_per_sha1(self, contents, db=None, cur=None):
        """List content missing from storage based only on sha1.

        Args:
            contents: Iterable of sha1 to check for absence.

        Returns:
            iterable: missing ids

        Raises:
            TODO: an exception when we get a hash collision.

        """
        for obj in db.content_missing_per_sha1(contents, cur):
            yield obj[0]

    @db_transaction_generator()
    def skipped_content_missing(self, contents, db=None, cur=None):
        """List skipped_content missing from storage

        Args:
            content: iterable of dictionaries containing the data for each
                checksum algorithm.

        Returns:
            iterable: missing signatures

        """
        for content in db.skipped_content_missing(contents, cur):
            yield dict(zip(db.content_hash_keys, content))

    @db_transaction()
    def content_find(self, content, db=None, cur=None):
        """Find a content hash in db.

        Args:
            content: a dictionary representing one content hash, mapping
                checksum algorithm names (see swh.model.hashutil.ALGORITHMS) to
                checksum values

        Returns:
            a triplet (sha1, sha1_git, sha256) if the content exist
            or None otherwise.

        Raises:
            ValueError: in case the key of the dictionary is not sha1, sha1_git
                nor sha256.

        """
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

    @db_transaction()
    def directory_add(self, directories, db, cur):
        """Add directories to the storage

        Args:
            directories (iterable): iterable of dictionaries representing the
                individual directories to add. Each dict has the following
                keys:

                - id (sha1_git): the id of the directory to add
                - entries (list): list of dicts for each entry in the
                      directory.  Each dict has the following keys:

                      - name (bytes)
                      - type (one of 'file', 'dir', 'rev'): type of the
                        directory entry (file, directory, revision)
                      - target (sha1_git): id of the object pointed at by the
                        directory entry
                      - perms (int): entry permissions

        Returns:
            Summary dict of keys with associated count as values:

                directory:add: Number of directories actually added

        """
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

    @db_transaction_generator()
    def directory_missing(self, directories, db=None, cur=None):
        """List directories missing from storage

        Args:
            directories (iterable): an iterable of directory ids

        Yields:
            missing directory ids

        """
        for obj in db.directory_missing_from_list(directories, cur):
            yield obj[0]

    @db_transaction_generator(statement_timeout=20000)
    def directory_ls(self, directory, recursive=False, db=None, cur=None):
        """Get entries for one directory.

        Args:
            - directory: the directory to list entries from.
            - recursive: if flag on, this list recursively from this directory.

        Returns:
            List of entries for such directory.

        If `recursive=True`, names in the path of a dir/file not at the
        root are concatenated with a slash (`/`).

        """
        if recursive:
            res_gen = db.directory_walk(directory, cur=cur)
        else:
            res_gen = db.directory_walk_one(directory, cur=cur)

        for line in res_gen:
            yield dict(zip(db.directory_ls_cols, line))

    @db_transaction(statement_timeout=2000)
    def directory_entry_get_by_path(self, directory, paths, db=None, cur=None):
        """Get the directory entry (either file or dir) from directory with path.

        Args:
            - directory: sha1 of the top level directory
            - paths: path to lookup from the top level directory. From left
              (top) to right (bottom).

        Returns:
            The corresponding directory entry if found, None otherwise.

        """
        res = db.directory_entry_get_by_path(directory, paths, cur)
        if res:
            return dict(zip(db.directory_ls_cols, res))

    @db_transaction()
    def revision_add(self, revisions, db, cur):
        """Add revisions to the storage

        Args:
            revisions (Iterable[dict]): iterable of dictionaries representing
                the individual revisions to add. Each dict has the following
                keys:

                - **id** (:class:`sha1_git`): id of the revision to add
                - **date** (:class:`dict`): date the revision was written
                - **committer_date** (:class:`dict`): date the revision got
                  added to the origin
                - **type** (one of 'git', 'tar'): type of the
                  revision added
                - **directory** (:class:`sha1_git`): the directory the
                  revision points at
                - **message** (:class:`bytes`): the message associated with
                  the revision
                - **author** (:class:`Dict[str, bytes]`): dictionary with
                  keys: name, fullname, email
                - **committer** (:class:`Dict[str, bytes]`): dictionary with
                  keys: name, fullname, email
                - **metadata** (:class:`jsonb`): extra information as
                  dictionary
                - **synthetic** (:class:`bool`): revision's nature (tarball,
                  directory creates synthetic revision`)
                - **parents** (:class:`list[sha1_git]`): the parents of
                  this revision

        date dictionaries have the form defined in :mod:`swh.model`.

        Returns:
            Summary dict of keys with associated count as values

                revision:add: New objects actually stored in db

        """
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

    @db_transaction_generator()
    def revision_missing(self, revisions, db=None, cur=None):
        """List revisions missing from storage

        Args:
            revisions (iterable): revision ids

        Yields:
            missing revision ids

        """
        if not revisions:
            return

        for obj in db.revision_missing_from_list(revisions, cur):
            yield obj[0]

    @db_transaction_generator(statement_timeout=1000)
    def revision_get(self, revisions, db=None, cur=None):
        """Get all revisions from storage

        Args:
            revisions: an iterable of revision ids

        Returns:
            iterable: an iterable of revisions as dictionaries (or None if the
                revision doesn't exist)

        """
        for line in db.revision_get_from_list(revisions, cur):
            data = converters.db_to_revision(
                dict(zip(db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    @db_transaction_generator(statement_timeout=2000)
    def revision_log(self, revisions, limit=None, db=None, cur=None):
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revision to lookup
            limit: limitation on the output result. Default to None.

        Yields:
            List of revision log from such revisions root.

        """
        for line in db.revision_log(revisions, limit, cur):
            data = converters.db_to_revision(
                dict(zip(db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    @db_transaction_generator(statement_timeout=2000)
    def revision_shortlog(self, revisions, limit=None, db=None, cur=None):
        """Fetch the shortlog for the given revisions

        Args:
            revisions: list of root revisions to lookup
            limit: depth limitation for the output

        Yields:
            a list of (id, parents) tuples.

        """

        yield from db.revision_shortlog(revisions, limit, cur)

    @db_transaction()
    def release_add(self, releases, db, cur):
        """Add releases to the storage

        Args:
            releases (Iterable[dict]): iterable of dictionaries representing
                the individual releases to add. Each dict has the following
                keys:

                - **id** (:class:`sha1_git`): id of the release to add
                - **revision** (:class:`sha1_git`): id of the revision the
                  release points to
                - **date** (:class:`dict`): the date the release was made
                - **name** (:class:`bytes`): the name of the release
                - **comment** (:class:`bytes`): the comment associated with
                  the release
                - **author** (:class:`Dict[str, bytes]`): dictionary with
                  keys: name, fullname, email

        the date dictionary has the form defined in :mod:`swh.model`.

        Returns:
            Summary dict of keys with associated count as values

                release:add: New objects contents actually stored in db

        """
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

    @db_transaction_generator()
    def release_missing(self, releases, db=None, cur=None):
        """List releases missing from storage

        Args:
            releases: an iterable of release ids

        Returns:
            a list of missing release ids

        """
        if not releases:
            return

        for obj in db.release_missing_from_list(releases, cur):
            yield obj[0]

    @db_transaction_generator(statement_timeout=500)
    def release_get(self, releases, db=None, cur=None):
        """Given a list of sha1, return the releases's information

        Args:
            releases: list of sha1s

        Yields:
            dicts with the same keys as those given to `release_add`
            (or ``None`` if a release does not exist)

        """
        for release in db.release_get_from_list(releases, cur):
            data = converters.db_to_release(
                dict(zip(db.release_get_cols, release))
            )
            yield data if data['target_type'] else None

    @db_transaction()
    def snapshot_add(self, snapshots, db=None, cur=None):
        """Add snapshots to the storage.

        Args:
            snapshot ([dict]): the snapshots to add, containing the
              following keys:

              - **id** (:class:`bytes`): id of the snapshot
              - **branches** (:class:`dict`): branches the snapshot contains,
                mapping the branch name (:class:`bytes`) to the branch target,
                itself a :class:`dict` (or ``None`` if the branch points to an
                unknown object)

                - **target_type** (:class:`str`): one of ``content``,
                  ``directory``, ``revision``, ``release``,
                  ``snapshot``, ``alias``
                - **target** (:class:`bytes`): identifier of the target
                  (currently a ``sha1_git`` for all object kinds, or the name
                  of the target branch for aliases)

        Raises:
            ValueError: if the origin or visit id does not exist.

        Returns:

            Summary dict of keys with associated count as values

                snapshot:add: Count of object actually stored in db

        """
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

    @db_transaction(statement_timeout=2000)
    def snapshot_get(self, snapshot_id, db=None, cur=None):
        """Get the content, possibly partial, of a snapshot with the given id

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        .. warning:: At most 1000 branches contained in the snapshot will be
            returned for performance reasons. In order to browse the whole
            set of branches, the method :meth:`snapshot_get_branches`
            should be used instead.

        Args:
            snapshot_id (bytes): identifier of the snapshot
        Returns:
            dict: a dict with three keys:
                * **id**: identifier of the snapshot
                * **branches**: a dict of branches contained in the snapshot
                  whose keys are the branches' names.
                * **next_branch**: the name of the first branch not returned
                  or :const:`None` if the snapshot has less than 1000
                  branches.
        """

        return self.snapshot_get_branches(snapshot_id, db=db, cur=cur)

    @db_transaction(statement_timeout=2000)
    def snapshot_get_by_origin_visit(self, origin, visit, db=None, cur=None):
        """Get the content, possibly partial, of a snapshot for the given origin visit

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        .. warning:: At most 1000 branches contained in the snapshot will be
            returned for performance reasons. In order to browse the whole
            set of branches, the method :meth:`snapshot_get_branches`
            should be used instead.

        Args:
            origin (int): the origin identifier
            visit (int): the visit identifier
        Returns:
            dict: None if the snapshot does not exist;
              a dict with three keys otherwise:
                * **id**: identifier of the snapshot
                * **branches**: a dict of branches contained in the snapshot
                  whose keys are the branches' names.
                * **next_branch**: the name of the first branch not returned
                  or :const:`None` if the snapshot has less than 1000
                  branches.

        """
        snapshot_id = db.snapshot_get_by_origin_visit(origin, visit, cur)

        if snapshot_id:
            return self.snapshot_get(snapshot_id, db=db, cur=cur)

        return None

    @db_transaction(statement_timeout=4000)
    def snapshot_get_latest(self, origin, allowed_statuses=None, db=None,
                            cur=None):
        """Get the content, possibly partial, of the latest snapshot for the
        given origin, optionally only from visits that have one of the given
        allowed_statuses

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        .. warning:: At most 1000 branches contained in the snapshot will be
            returned for performance reasons. In order to browse the whole
            set of branches, the method :meth:`snapshot_get_branches`
            should be used instead.

        Args:
            origin (Union[str,int]): the origin's URL or identifier
            allowed_statuses (list of str): list of visit statuses considered
                to find the latest snapshot for the visit. For instance,
                ``allowed_statuses=['full']`` will only consider visits that
                have successfully run to completion.
        Returns:
            dict: a dict with three keys:
                * **id**: identifier of the snapshot
                * **branches**: a dict of branches contained in the snapshot
                  whose keys are the branches' names.
                * **next_branch**: the name of the first branch not returned
                  or :const:`None` if the snapshot has less than 1000
                  branches.
        """
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

    @db_transaction(statement_timeout=2000)
    def snapshot_count_branches(self, snapshot_id, db=None, cur=None):
        """Count the number of branches in the snapshot with the given id

        Args:
            snapshot_id (bytes): identifier of the snapshot

        Returns:
            dict: A dict whose keys are the target types of branches and
            values their corresponding amount
        """
        return dict([bc for bc in
                     db.snapshot_count_branches(snapshot_id, cur)])

    @db_transaction(statement_timeout=2000)
    def snapshot_get_branches(self, snapshot_id, branches_from=b'',
                              branches_count=1000, target_types=None,
                              db=None, cur=None):
        """Get the content, possibly partial, of a snapshot with the given id

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        Args:
            snapshot_id (bytes): identifier of the snapshot
            branches_from (bytes): optional parameter used to skip branches
                whose name is lesser than it before returning them
            branches_count (int): optional parameter used to restrain
                the amount of returned branches
            target_types (list): optional parameter used to filter the
                target types of branch to return (possible values that can be
                contained in that list are `'content', 'directory',
                'revision', 'release', 'snapshot', 'alias'`)
        Returns:
            dict: None if the snapshot does not exist;
              a dict with three keys otherwise:
                * **id**: identifier of the snapshot
                * **branches**: a dict of branches contained in the snapshot
                  whose keys are the branches' names.
                * **next_branch**: the name of the first branch not returned
                  or :const:`None` if the snapshot has less than
                  `branches_count` branches after `branches_from` included.
        """
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

    @db_transaction()
    def origin_visit_add(self, origin, date, type=None,
                         db=None, cur=None):
        """Add an origin_visit for the origin at ts with status 'ongoing'.

        For backward compatibility, `type` is optional and defaults to
        the origin's type.

        Args:
            origin (Union[int,str]): visited origin's identifier or URL
            date (Union[str,datetime]): timestamp of such visit
            type (str): the type of loader used for the visit (hg, git, ...)

        Returns:
            dict: dictionary with keys origin and visit where:

            - origin: origin identifier
            - visit: the visit identifier for the new visit occurrence

        """
        if isinstance(origin, str):
            origin = self.origin_get({'url': origin}, db=db, cur=cur)
            origin_id = origin['id']
        else:
            origin = self.origin_get({'id': origin}, db=db, cur=cur)
            origin_id = origin['id']

        if isinstance(date, str):
            # FIXME: Converge on iso8601 at some point
            date = dateutil.parser.parse(date)

        if type is None:
            type = origin['type']

        visit_id = db.origin_visit_add(origin_id, date, type, cur)

        if self.journal_writer:
            # We can write to the journal only after inserting to the
            # DB, because we want the id of the visit
            del origin['id']
            self.journal_writer.write_addition('origin_visit', {
                'origin': origin, 'date': date, 'type': type,
                'visit': visit_id,
                'status': 'ongoing', 'metadata': None, 'snapshot': None})

        return {
            'origin': origin_id,
            'visit': visit_id,
        }

    @db_transaction()
    def origin_visit_update(self, origin, visit_id, status=None,
                            metadata=None, snapshot=None,
                            db=None, cur=None):
        """Update an origin_visit's status.

        Args:
            origin (Union[int,str]): visited origin's identifier or URL
            visit_id: Visit's id
            status: Visit's new status
            metadata: Data associated to the visit
            snapshot (sha1_git): identifier of the snapshot to add to
                the visit

        Returns:
            None

        """
        if isinstance(origin, str):
            origin_id = self.origin_get({'url': origin}, db=db, cur=cur)['id']
        else:
            origin_id = origin

        visit = db.origin_visit_get(origin_id, visit_id, cur=cur)

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
                origin = self.origin_get(
                    [{'id': origin_id}], db=db, cur=cur)[0]
                del origin['id']
                self.journal_writer.write_update('origin_visit', {
                    **visit, **updates, 'origin': origin})

            db.origin_visit_update(origin_id, visit_id, updates, cur)

    @db_transaction()
    def origin_visit_upsert(self, visits, db=None, cur=None):
        """Add a origin_visits with a specific id and with all its data.
        If there is already an origin_visit with the same
        `(origin_id, visit_id)`, overwrites it.

        Args:
            visits: iterable of dicts with keys:

                origin: dict with keys either `id` or `url`
                visit: origin visit id
                date: timestamp of such visit
                status: Visit's new status
                metadata: Data associated to the visit
                snapshot (sha1_git): identifier of the snapshot to add to
                    the visit
        """
        visits = copy.deepcopy(visits)
        for visit in visits:
            if isinstance(visit['date'], str):
                visit['date'] = dateutil.parser.parse(visit['date'])
            visit['origin'] = \
                self.origin_get([visit['origin']], db=db, cur=cur)[0]

        if self.journal_writer:
            for visit in visits:
                visit = copy.deepcopy(visit)
                if visit.get('type') is None:
                    visit['type'] = visit['origin']['type']
                del visit['origin']['id']
                self.journal_writer.write_addition('origin_visit', visit)

        for visit in visits:
            visit['origin'] = visit['origin']['id']
            # TODO: upsert them all in a single query
            db.origin_visit_upsert(**visit, cur=cur)

    @db_transaction_generator(statement_timeout=500)
    def origin_visit_get(self, origin, last_visit=None, limit=None, db=None,
                         cur=None):
        """Retrieve all the origin's visit's information.

        Args:
            origin (Union[int,str]): The occurrence's origin (identifier/URL).
            last_visit: Starting point from which listing the next visits
                Default to None
            limit (int): Number of results to return from the last visit.
                Default to None

        Yields:
            List of visits.

        """
        if isinstance(origin, str):
            origin = self.origin_get([{'url': origin}], db=db, cur=cur)[0]
            if not origin:
                return
            origin = origin['id']
        for line in db.origin_visit_get_all(
                origin, last_visit=last_visit, limit=limit, cur=cur):
            data = dict(zip(db.origin_visit_get_cols, line))
            yield data

    @db_transaction(statement_timeout=500)
    def origin_visit_find_by_date(self, origin, visit_date, db=None, cur=None):
        """Retrieves the origin visit whose date is closest to the provided
        timestamp.
        In case of a tie, the visit with largest id is selected.

        Args:
            origin (str): The occurrence's origin (URL).
            target (datetime): target timestamp

        Returns:
            A visit.

        """
        origin = self.origin_get([{'url': origin}], db=db, cur=cur)[0]
        if not origin:
            return
        origin = origin['id']
        line = db.origin_visit_find_by_date(origin, visit_date, cur=cur)
        if line:
            return dict(zip(db.origin_visit_get_cols, line))

    @db_transaction(statement_timeout=500)
    def origin_visit_get_by(self, origin, visit, db=None, cur=None):
        """Retrieve origin visit's information.

        Args:
            origin: The occurrence's origin (identifier).

        Returns:
            The information on that particular (origin, visit) or None if
            it does not exist

        """
        if isinstance(origin, str):
            origin = self.origin_get({'url': origin}, db=db, cur=cur)
            if not origin:
                return
            origin = origin['id']
        ori_visit = db.origin_visit_get(origin, visit, cur)
        if not ori_visit:
            return None

        return dict(zip(db.origin_visit_get_cols, ori_visit))

    @db_transaction(statement_timeout=4000)
    def origin_visit_get_latest(
            self, origin, allowed_statuses=None, require_snapshot=False,
            db=None, cur=None):
        """Get the latest origin visit for the given origin, optionally
        looking only for those with one of the given allowed_statuses
        or for those with a known snapshot.

        Args:
            origin (str): the origin's URL
            allowed_statuses (list of str): list of visit statuses considered
                to find the latest visit. For instance,
                ``allowed_statuses=['full']`` will only consider visits that
                have successfully run to completion.
            require_snapshot (bool): If True, only a visit with a snapshot
                will be returned.
        Returns:
            dict: a dict with the following keys:

                origin: the URL of the origin
                visit: origin visit id
                type: type of loader used for the visit
                date: timestamp of such visit
                status: Visit's new status
                metadata: Data associated to the visit
                snapshot (Optional[sha1_git]): identifier of the snapshot
                    associated to the visit
        """
        origin = self.origin_get({'url': origin}, db=db, cur=cur)
        if not origin:
            return
        origin = origin['id']

        origin_visit = db.origin_visit_get_latest(
            origin, allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot, cur=cur)
        if origin_visit:
            return dict(zip(db.origin_visit_get_cols, origin_visit))

    @db_transaction(statement_timeout=2000)
    def object_find_by_sha1_git(self, ids, db=None, cur=None):
        """Return the objects found with the given ids.

        Args:
            ids: a generator of sha1_gits

        Returns:
            dict: a mapping from id to the list of objects found. Each object
            found is itself a dict with keys:

            - sha1_git: the input id
            - type: the type of object found
            - id: the id of the object found
            - object_id: the numeric id of the object found.

        """
        ret = {id: [] for id in ids}

        for retval in db.object_find_by_sha1_git(ids, cur=cur):
            if retval[1]:
                ret[retval[0]].append(dict(zip(db.object_find_by_sha1_git_cols,
                                               retval)))

        return ret

    origin_keys = ['id', 'type', 'url']

    @db_transaction(statement_timeout=500)
    def origin_get(self, origins, db=None, cur=None):
        """Return origins, either all identified by their ids or all
        identified by tuples (type, url).

        If the url is given and the type is omitted, one of the origins with
        that url is returned.

        Args:
            origin: a list of dictionaries representing the individual
                origins to find.
                These dicts have either the key url (and optionally type):

                - type (FIXME: enum TBD): the origin type ('git', 'wget', ...)
                - url (bytes): the url the origin points to

                or the id:

                - id: the origin id

        Returns:
            dict: the origin dictionary with the keys:

            - id: origin's id
            - type: origin's type
            - url: origin's url

        Raises:
            ValueError: if the keys does not match (url and type) nor id.

        """
        if isinstance(origins, dict):
            # Old API
            return_single = True
            origins = [origins]
        elif len(origins) == 0:
            return []
        else:
            return_single = False

        origin_ids = [origin.get('id') for origin in origins]
        origin_urls = [origin.get('url') for origin in origins]
        if any(origin_ids):
            # Lookup per ID
            if all(origin_ids):
                results = db.origin_get_by_id(origin_ids, cur)
            else:
                raise ValueError(
                    'Either all origins or none at all should have an "id".')
        elif any(origin_urls):
            # Lookup per type + URL
            if all(origin_urls):
                results = db.origin_get_by_url(origin_urls, cur)
            else:
                raise ValueError(
                    'Either all origins or none at all should have '
                    'an "url" key.')
        else:  # unsupported lookup
            raise ValueError('Origin must have either id or url.')

        results = [dict(zip(self.origin_keys, result))
                   for result in results]
        if return_single:
            assert len(results) == 1
            if results[0]['id'] is not None:
                return results[0]
            else:
                return None
        else:
            return [None if res['id'] is None else res for res in results]

    @db_transaction_generator()
    def origin_get_range(self, origin_from=1, origin_count=100,
                         db=None, cur=None):
        """Retrieve ``origin_count`` origins whose ids are greater
        or equal than ``origin_from``.

        Origins are sorted by id before retrieving them.

        Args:
            origin_from (int): the minimum id of origins to retrieve
            origin_count (int): the maximum number of origins to retrieve

        Yields:
            dicts containing origin information as returned
            by :meth:`swh.storage.storage.Storage.origin_get`.
        """
        for origin in db.origin_get_range(origin_from, origin_count, cur):
            yield dict(zip(self.origin_keys, origin))

    @db_transaction_generator()
    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, with_visit=False, db=None, cur=None):
        """Search for origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The search is performed in a case insensitive way.

        Args:
            url_pattern (str): the string pattern to search for in origin urls
            offset (int): number of found origins to skip before returning
                results
            limit (int): the maximum number of found origins to return
            regexp (bool): if True, consider the provided pattern as a regular
                expression and return origins whose urls match it
            with_visit (bool): if True, filter out origins with no visit

        Yields:
            dicts containing origin information as returned
            by :meth:`swh.storage.storage.Storage.origin_get`.
        """
        for origin in db.origin_search(url_pattern, offset, limit,
                                       regexp, with_visit, cur):
            yield dict(zip(self.origin_keys, origin))

    @db_transaction()
    def origin_count(self, url_pattern, regexp=False,
                     with_visit=False, db=None, cur=None):
        """Count origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The pattern search in origin urls is performed in a case insensitive
        way.

        Args:
            url_pattern (str): the string pattern to search for in origin urls
            regexp (bool): if True, consider the provided pattern as a regular
                expression and return origins whose urls match it
            with_visit (bool): if True, filter out origins with no visit

        Returns:
            int: The number of origins matching the search criterion.
        """
        return db.origin_count(url_pattern, regexp, with_visit, cur)

    @db_transaction()
    def origin_add(self, origins, db=None, cur=None):
        """Add origins to the storage

        Args:
            origins: list of dictionaries representing the individual origins,
                with the following keys:

                - type: the origin type ('git', 'svn', 'deb', ...)
                - url (bytes): the url the origin points to

        Returns:
            list: given origins as dict updated with their id

        """
        origins = copy.deepcopy(origins)
        for origin in origins:
            origin['id'] = self.origin_add_one(origin, db=db, cur=cur)

        return origins

    @db_transaction()
    def origin_add_one(self, origin, db=None, cur=None):
        """Add origin to the storage

        Args:
            origin: dictionary representing the individual origin to add. This
                dict has the following keys:

                - type (FIXME: enum TBD): the origin type ('git', 'wget', ...)
                - url (bytes): the url the origin points to

        Returns:
            the id of the added origin, or of the identical one that already
            exists.

        """
        origin_id = list(db.origin_get_by_url(
            [origin['url']], cur))[0][0]
        if origin_id:
            return origin_id

        if self.journal_writer:
            self.journal_writer.write_addition('origin', origin)

        return db.origin_add(origin['type'], origin['url'], cur)

    @db_transaction()
    def fetch_history_start(self, origin_id, db=None, cur=None):
        """Add an entry for origin origin_id in fetch_history. Returns the id
        of the added fetch_history entry
        """
        if isinstance(origin_id, str):
            origin = \
                self.origin_get([{'url': origin_id}], db=db, cur=cur)
            if not origin:
                return
            origin_id = origin[0]['id']
        fetch_history = {
            'origin': origin_id,
            'date': datetime.datetime.now(tz=datetime.timezone.utc),
        }

        return db.create_fetch_history(fetch_history, cur)

    @db_transaction()
    def fetch_history_end(self, fetch_history_id, data, db=None, cur=None):
        """Close the fetch_history entry with id `fetch_history_id`, replacing
           its data with `data`.
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        fetch_history = db.get_fetch_history(fetch_history_id, cur)

        if not fetch_history:
            raise ValueError('No fetch_history with id %d' % fetch_history_id)

        fetch_history['duration'] = now - fetch_history['date']

        fetch_history.update(data)

        db.update_fetch_history(fetch_history, cur)

    @db_transaction()
    def fetch_history_get(self, fetch_history_id, db=None, cur=None):
        """Get the fetch_history entry with id `fetch_history_id`.
        """
        return db.get_fetch_history(fetch_history_id, cur)

    @db_transaction(statement_timeout=500)
    def stat_counters(self, db=None, cur=None):
        """compute statistics about the number of tuples in various tables

        Returns:
            dict: a dictionary mapping textual labels (e.g., content) to
            integer values (e.g., the number of tuples in table content)

        """
        return {k: v for (k, v) in db.stat_counters()}

    @db_transaction()
    def refresh_stat_counters(self, db=None, cur=None):
        """Recomputes the statistics for `stat_counters`."""
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

    @db_transaction()
    def origin_metadata_add(self, origin_id, ts, provider, tool, metadata,
                            db=None, cur=None):
        """ Add an origin_metadata for the origin at ts with provenance and
        metadata.

        Args:
            origin_id (int): the origin's id for which the metadata is added
            ts (datetime): timestamp of the found metadata
            provider (int): the provider of metadata (ex:'hal')
            tool (int): tool used to extract metadata
            metadata (jsonb): the metadata retrieved at the time and location

        Returns:
            id (int): the origin_metadata unique id
        """
        if isinstance(origin_id, str):
            origin = self.origin_get({'url': origin_id}, db=db, cur=cur)
            if not origin:
                return
            origin_id = origin['id']

        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        return db.origin_metadata_add(origin_id, ts, provider, tool,
                                      metadata, cur)

    @db_transaction_generator(statement_timeout=500)
    def origin_metadata_get_by(self, origin_id, provider_type=None, db=None,
                               cur=None):
        """Retrieve list of all origin_metadata entries for the origin_id

        Args:
            origin_id (int): the unique origin identifier
            provider_type (str): (optional) type of provider

        Returns:
            list of dicts: the origin_metadata dictionary with the keys:

            - origin_id (int): origin's id
            - discovery_date (datetime): timestamp of discovery
            - tool_id (int): metadata's extracting tool
            - metadata (jsonb)
            - provider_id (int): metadata's provider
            - provider_name (str)
            - provider_type (str)
            - provider_url (str)

        """
        if isinstance(origin_id, str):
            origin = self.origin_get({'url': origin_id}, db=db, cur=cur)
            if not origin:
                return
            origin_id = origin['id']

        for line in db.origin_metadata_get_by(origin_id, provider_type, cur):
            yield dict(zip(db.origin_metadata_get_cols, line))

    @db_transaction()
    def tool_add(self, tools, db=None, cur=None):
        """Add new tools to the storage.

        Args:
            tools (iterable of :class:`dict`): Tool information to add to
              storage. Each tool is a :class:`dict` with the following keys:

              - name (:class:`str`): name of the tool
              - version (:class:`str`): version of the tool
              - configuration (:class:`dict`): configuration of the tool,
                must be json-encodable

        Returns:
            :class:`dict`: All the tools inserted in storage
            (including the internal ``id``). The order of the list is not
            guaranteed to match the order of the initial list.

        """
        db.mktemp_tool(cur)
        db.copy_to(tools, 'tmp_tool',
                   ['name', 'version', 'configuration'],
                   cur)

        tools = db.tool_add_from_temp(cur)
        return [dict(zip(db.tool_cols, line)) for line in tools]

    @db_transaction(statement_timeout=500)
    def tool_get(self, tool, db=None, cur=None):
        """Retrieve tool information.

        Args:
            tool (dict): Tool information we want to retrieve from storage.
              The dicts have the same keys as those used in :func:`tool_add`.

        Returns:
            dict: The full tool information if it exists (``id`` included),
            None otherwise.

        """
        tool_conf = tool['configuration']
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)

        idx = db.tool_get(tool['name'],
                          tool['version'],
                          tool_conf)
        if not idx:
            return None
        return dict(zip(db.tool_cols, idx))

    @db_transaction()
    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata, db=None, cur=None):
        """Add a metadata provider.

        Args:
            provider_name (str): Its name
            provider_type (str): Its type (eg. `'deposit-client'`)
            provider_url (str): Its URL
            metadata: JSON-encodable object

        Returns:
            int: an identifier of the provider
        """
        return db.metadata_provider_add(provider_name, provider_type,
                                        provider_url, metadata, cur)

    @db_transaction()
    def metadata_provider_get(self, provider_id, db=None, cur=None):
        """Get a metadata provider

        Args:
            provider_id: Its identifier, as given by `metadata_provider_add`.

        Returns:
            dict: same as `metadata_provider_add`;
                  or None if it does not exist.
        """
        result = db.metadata_provider_get(provider_id)
        if not result:
            return None
        return dict(zip(db.metadata_provider_cols, result))

    @db_transaction()
    def metadata_provider_get_by(self, provider, db=None, cur=None):
        """Get a metadata provider

        Args:
            provider (dict): A dictionary with keys:
                * provider_name: Its name
                * provider_url: Its URL

        Returns:
            dict: same as `metadata_provider_add`;
                  or None if it does not exist.
        """
        result = db.metadata_provider_get_by(provider['provider_name'],
                                             provider['provider_url'])
        if not result:
            return None
        return dict(zip(db.metadata_provider_cols, result))

    def diff_directories(self, from_dir, to_dir, track_renaming=False):
        """Compute the list of file changes introduced between two arbitrary
        directories (insertion / deletion / modification / renaming of files).

        Args:
            from_dir (bytes): identifier of the directory to compare from
            to_dir (bytes): identifier of the directory to compare to
            track_renaming (bool): whether or not to track files renaming

        Returns:
            A list of dict describing the introduced file changes
            (see :func:`swh.storage.algos.diff.diff_directories`
            for more details).
        """
        return diff.diff_directories(self, from_dir, to_dir, track_renaming)

    def diff_revisions(self, from_rev, to_rev, track_renaming=False):
        """Compute the list of file changes introduced between two arbitrary
        revisions (insertion / deletion / modification / renaming of files).

        Args:
            from_rev (bytes): identifier of the revision to compare from
            to_rev (bytes): identifier of the revision to compare to
            track_renaming (bool): whether or not to track files renaming

        Returns:
            A list of dict describing the introduced file changes
            (see :func:`swh.storage.algos.diff.diff_directories`
            for more details).
        """
        return diff.diff_revisions(self, from_rev, to_rev, track_renaming)

    def diff_revision(self, revision, track_renaming=False):
        """Compute the list of file changes introduced by a specific revision
        (insertion / deletion / modification / renaming of files) by comparing
        it against its first parent.

        Args:
            revision (bytes): identifier of the revision from which to
                compute the list of files changes
            track_renaming (bool): whether or not to track files renaming

        Returns:
            A list of dict describing the introduced file changes
            (see :func:`swh.storage.algos.diff.diff_directories`
            for more details).
        """
        return diff.diff_revision(self, revision, track_renaming)
