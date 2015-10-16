# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from collections import defaultdict
import datetime
import functools
import itertools

import dateutil.parser
import psycopg2
from psycopg2.extras import DateTimeTZRange

from .db import Db
from .objstorage import ObjStorage

from swh.core.hashutil import ALGORITHMS


def db_transaction(meth):
    """decorator to execute Storage methods within DB transactions

    The decorated method must accept a `cur` keyword argument
    """
    @functools.wraps(meth)
    def _meth(self, *args, **kwargs):
        with self.db.transaction() as cur:
            return meth(self, *args, cur=cur, **kwargs)
    return _meth


def db_transaction_generator(meth):
    """decorator to execute Storage methods within DB transactions, while
    returning a generator

    The decorated method must accept a `cur` keyword argument

    """
    @functools.wraps(meth)
    def _meth(self, *args, **kwargs):
        with self.db.transaction() as cur:
            yield from meth(self, *args, cur=cur, **kwargs)
    return _meth


class Storage():
    """SWH storage proxy, encompassing DB and object storage

    """

    def __init__(self, db_conn, obj_root):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection
            obj_root: path to the root of the object storage

        """
        if isinstance(db_conn, psycopg2.extensions.connection):
            self.db = Db(db_conn)
        else:
            self.db = Db.connect(db_conn)

        self.objstorage = ObjStorage(obj_root)

    def content_add(self, content):
        """Add content blobs to the storage

        Note: in case of DB errors, objects might have already been added to
        the object storage and will not be removed. Since addition to the
        object storage is idempotent, that should not be a problem.

        Args:
            content: iterable of dictionaries representing individual pieces of
                content to add. Each dictionary has the following keys:
                - data (bytes): the actual content
                - length (int): content length (default: -1)
                - one key for each checksum algorithm in
                  swh.core.hashutil.ALGORITHMS, mapped to the corresponding
                  checksum
                - status (str): one of visible, hidden, absent
                - reason (str): if status = absent, the reason why
                - origin (int): if status = absent, the origin we saw the
                  content in

        """
        db = self.db

        content_by_status = defaultdict(list)
        for d in content:
            if 'status' not in d:
                d['status'] = 'visible'
            if 'length' not in d:
                d['length'] = -1
            content_by_status[d['status']].append(d)

        content_with_data = content_by_status['visible']
        content_without_data = content_by_status['absent']

        missing_content = set(self.content_missing(content_with_data))
        missing_skipped = set(
            sha1_git for sha1, sha1_git, sha256
            in self.skipped_content_missing(content_without_data))

        with db.transaction() as cur:
            if missing_content:
                # create temporary table for metadata injection
                db.mktemp('content', cur)

                def add_to_objstorage(cont):
                    self.objstorage.add_bytes(cont['data'],
                                              obj_id=cont['sha1'])

                content_filtered = (cont for cont in content_with_data
                                    if cont['sha1'] in missing_content)

                db.copy_to(content_filtered, 'tmp_content',
                           ['sha1', 'sha1_git', 'sha256', 'length', 'status'],
                           cur, item_cb=add_to_objstorage)

                # move metadata in place
                db.content_add_from_temp(cur)

            if missing_skipped:
                missing_filtered = (cont for cont in content_without_data
                                    if cont['sha1_git'] in missing_skipped)
                db.mktemp('skipped_content', cur)
                db.copy_to(missing_filtered, 'tmp_skipped_content',
                           ['sha1', 'sha1_git', 'sha256', 'length',
                            'reason', 'status', 'origin'], cur)

                # move metadata in place
                db.skipped_content_add_from_temp(cur)

    @db_transaction_generator
    def content_missing(self, content, key_hash='sha1', cur=None):
        """List content missing from storage

        Args:
            content: iterable of dictionaries containing one key for each
                checksum algorithm in swh.core.hashutil.ALGORITHMS, mapped to
                the corresponding checksum, and a length key mapped to the
                content length.
            key_hash: the name of the hash used as key (default: 'sha1')

        Returns:
            an iterable of `key_hash`es missing from the storage

        Raises:
            TODO: an exception when we get a hash collision.

        """
        db = self.db

        keys = ['sha1', 'sha1_git', 'sha256']

        if key_hash not in keys:
            raise ValueError("key_hash should be one of %s" % keys)

        key_hash_idx = keys.index(key_hash)

        # Create temporary table for metadata injection
        db.mktemp('content', cur)

        db.copy_to(content, 'tmp_content', keys + ['length'], cur)

        for obj in db.content_missing_from_temp(cur):
            yield obj[key_hash_idx]

    @db_transaction_generator
    def skipped_content_missing(self, content, cur=None):
        """List skipped_content missing from storage

        Args:
            content: iterable of dictionaries containing the data for each
                checksum algorithm.

        Returns:
            an iterable of signatures missing from the storage
        """
        keys = ['sha1', 'sha1_git', 'sha256']

        db = self.db

        db.mktemp('skipped_content', cur)
        db.copy_to(content, 'tmp_skipped_content',
                   keys + ['length', 'reason'], cur)

        yield from db.skipped_content_missing_from_temp(cur)

    @db_transaction
    def content_find(self, content, cur=None):
        """Find a content hash in db.

        Args:
            content: a dictionary representing one content hash, mapping
                checksum algorithm names (see swh.core.hashutil.ALGORITHMS) to
                checksum values

        Returns:
            a triplet (sha1, sha1_git, sha256) if the content exist
            or None otherwise.

        Raises:
            ValueError in case the key of the dictionary is not sha1, sha1_git
            nor sha256.

        """
        db = self.db

        if not set(content).intersection(ALGORITHMS):
            raise ValueError('content keys must contain at least one of: '
                             'sha1, sha1_git, sha256')

        # format the output
        found_hash = db.content_find(sha1=content.get('sha1'),
                                     sha1_git=content.get('sha1_git'),
                                     sha256=content.get('sha256'),
                                     cur=cur)

        return found_hash

    @db_transaction
    def content_exist(self, content, cur=None):
        """Predicate to check the presence of a content's hashes.

        Args:
            content: a dictionary entry representing one content hash.
            The dictionary key is one of swh.core.hashutil.ALGORITHMS.
            The value mapped to the corresponding checksum.

        Returns:
            a boolean indicator of presence

        Raises:
            ValueError in case the key of the dictionary is not sha1, sha1_git
            nor sha256.

        """
        return self.content_find(content) is not None

    @db_transaction
    def content_find_occurrence(self, content, cur=None):
        """Find the content's occurrence.

        Args:
            content: a dictionary entry representing one content hash.
            The dictionary key is one of swh.core.hashutil.ALGORITHMS.
            The value mapped to the corresponding checksum.

        Returns:
            The occurrence of the content.

        Raises:
            ValueError in case the key of the dictionary is not sha1, sha1_git
            nor sha256.

        """
        db = self.db

        c = self.content_find(content)

        if c is None:
            return None

        sha1, _, _ = c

        found_occ = db.content_find_occurrence(sha1, cur=cur)

        if found_occ is None:
            return None
        return {'origin_type': found_occ[0],
                'origin_url': found_occ[1],
                'branch': found_occ[2],
                'revision': found_occ[3],
                'path': found_occ[4]}

    def directory_add(self, directories):
        """Add directories to the storage

        Args:
            directories: iterable of dictionaries representing the individual
                directories to add. Each dict has the following keys:
                - id (sha1_git): the id of the directory to add
                - entries (list): list of dicts for each entry in the
                    directory.  Each dict has the following keys:
                    - name (bytes)
                    - type (one of 'file', 'dir', 'rev'):
                        type of the directory entry (file, directory, revision)
                    - target (sha1_git): id of the object pointed at by the
                          directory entry
                    - perms (int): entry permissions
        """
        dirs = set()
        dir_entries = {
            'file': defaultdict(list),
            'dir': defaultdict(list),
            'rev': defaultdict(list),
        }

        for cur_dir in directories:
            dir_id = cur_dir['id']
            dirs.add(dir_id)
            for entry in cur_dir['entries']:
                entry['dir_id'] = dir_id
                dir_entries[entry['type']][dir_id].append(entry)

        dirs_missing = set(self.directory_missing(dirs))
        if not dirs_missing:
            return

        db = self.db
        with db.transaction() as cur:
            dirs_missing_dict = ({'id': dir} for dir in dirs_missing)
            db.copy_to(dirs_missing_dict, 'directory', ['id'], cur)
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

                cur.execute('SELECT swh_directory_entry_add(%s)',
                            (entry_type,))

    @db_transaction_generator
    def directory_missing(self, directories, cur):
        """List directories missing from storage

        Args: an iterable of directory ids
        Returns: a list of missing directory ids
        """
        db = self.db

        # Create temporary table for metadata injection
        db.mktemp('directory', cur)

        directories_dicts = ({'id': dir} for dir in directories)

        db.copy_to(directories_dicts, 'tmp_directory', ['id'], cur)

        for obj in db.directory_missing_from_temp(cur):
            yield obj[0]

    def directory_get(self, directory):
        """Get the entries for one directory"""
        yield from self.db.directory_walk_one(directory)

    def revision_add(self, revisions):
        """Add revisions to the storage

        Args:
            revisions: iterable of dictionaries representing the individual
                revisions to add. Each dict has the following keys:
                - id (sha1_git): id of the revision to add
                - date (datetime.DateTime): date the revision was written
                - date_offset (int): offset from UTC in minutes the revision
                    was written
                - committer_date (datetime.DateTime): date the revision got
                    added to the origin
                - committer_date_offset (int): offset from UTC in minutes the
                    revision was added to the origin
                - type (one of 'git', 'tar'): type of the revision added
                - directory (sha1_git): the directory the revision points at
                - message (bytes): the message associated with the revision
                - author_name (bytes): the name of the revision author
                - author_email (bytes): the email of the revision author
                - committer_name (bytes): the name of the revision committer
                - committer_email (bytes): the email of the revision committer
                - parents (list of sha1_git): the parents of this revision
        """
        db = self.db

        parents = {}

        for revision in revisions:
            id = revision['id']
            cur_parents = enumerate(revision.get('parents', []))
            parents[id] = [{
                'id': id,
                'parent_id': parent,
                'parent_rank': i
            } for i, parent in cur_parents]

        revisions_missing = list(self.revision_missing(parents.keys()))

        if not revisions_missing:
            return

        with db.transaction() as cur:
            db.mktemp_revision(cur)

            revisions_filtered = (revision for revision in revisions
                                  if revision['id'] in revisions_missing)

            db.copy_to(revisions_filtered, 'tmp_revision',
                       ['id', 'date', 'date_offset', 'committer_date',
                        'committer_date_offset', 'type', 'directory',
                        'message', 'author_name', 'author_email',
                        'committer_name', 'committer_email',
                        'synthetic'],
                       cur)

            db.revision_add_from_temp(cur)

            parents_filtered = itertools.chain.from_iterable(
                parents[id] for id in revisions_missing)

            db.copy_to(parents_filtered, 'revision_history',
                       ['id', 'parent_id', 'parent_rank'], cur)

    @db_transaction_generator
    def revision_missing(self, revisions, cur):
        """List revisions missing from storage

        Args: an iterable of revision ids
        Returns: a list of missing revision ids
        """
        db = self.db

        # Create temporary table for metadata injection
        db.mktemp('revision', cur)

        revisions_dicts = ({'id': dir, 'type': 'git'} for dir in revisions)

        db.copy_to(revisions_dicts, 'tmp_revision', ['id', 'type'], cur)

        for obj in db.revision_missing_from_temp(cur):
            yield obj[0]

    @db_transaction_generator
    def revision_get(self, revisions, cur):
        """Get all revisions from storage
           Args: an iterable of revision ids
           Returns: an iterable of revisions as dictionaries
                    (or None if the revision doesn't exist)
        """

        keys = ('id', 'date', 'date_offset', 'committer_date',
                'committer_date_offset', 'type', 'directory',
                'message', 'author_name', 'author_email',
                'committer_name', 'committer_email', 'synthetic',
                'parents')

        db = self.db

        # Create temporary table for metadata injection
        db.mktemp('revision', cur)

        revisions_dicts = ({'id': rev, 'type': 'git'} for rev in revisions)

        db.copy_to(revisions_dicts, 'tmp_revision', ['id', 'type'], cur)

        for line in self.db.revision_get_from_temp(cur):
            data = dict(zip(keys, line))
            if not data['type']:
                yield None
                continue

            yield data

    def release_add(self, releases):
        """Add releases to the storage

        Args:
            releases: iterable of dictionaries representing the individual
                releases to add. Each dict has the following keys:
                - id (sha1_git): id of the release to add
                - revision (sha1_git): id of the revision the release points
                    to
                - date (datetime.DateTime): the date the release was made
                - date_offset (int): offset from UTC in minutes the release was
                    made
                - name (bytes): the name of the release
                - comment (bytes): the comment associated with the release
                - author_name (bytes): the name of the release author
                - author_email (bytes): the email of the release author
        """
        db = self.db

        release_ids = set(release['id'] for release in releases)
        releases_missing = list(self.release_missing(release_ids))

        if not releases_missing:
            return

        with db.transaction() as cur:
            db.mktemp_release(cur)

            releases_filtered = (release for release in releases
                                 if release['id'] in releases_missing)

            db.copy_to(releases_filtered, 'tmp_release',
                       ['id', 'revision', 'date', 'date_offset', 'name',
                        'comment', 'author_name', 'author_email',
                        'synthetic'],
                       cur)

            db.release_add_from_temp(cur)

    @db_transaction_generator
    def release_missing(self, releases, cur=None):
        """List releases missing from storage

        Args: an iterable of release ids
        Returns: a list of missing release ids
        """
        db = self.db

        # Create temporary table for metadata injection
        db.mktemp('release', cur)

        releases_dicts = ({'id': rel} for rel in releases)

        db.copy_to(releases_dicts, 'tmp_release', ['id'], cur)

        for obj in db.release_missing_from_temp(cur):
            yield obj[0]

    @db_transaction
    def occurrence_add(self, occurrences, cur=None):
        """Add occurrences to the storage

        Args:
            occurrences: iterable of dictionaries representing the individual
                occurrences to add. Each dict has the following keys:
                - origin (int): id of the origin corresponding to the
                    occurrence
                - branch (str): the reference name of the occurrence
                - revision (sha1_git): the id of the revision pointed to by
                    the occurrence
                - authority (int): id of the authority giving the validity
                - validity (datetime.DateTime): the validity date for the given
                    occurrence
        """
        db = self.db

        processed = []
        for occurrence in occurrences:
            validity = occurrence['validity']
            if isinstance(validity, str):
                validity = dateutil.parser.parse(validity)
            if isinstance(validity, datetime.datetime):
                occurrence = occurrence.copy()
                occurrence['validity'] = DateTimeTZRange(lower=validity)

            processed.append(occurrence)

        db.mktemp('occurrence_history', cur)
        db.copy_to(processed, 'tmp_occurrence_history',
                   ['origin', 'branch', 'revision', 'authority', 'validity'],
                   cur)

        db.occurrence_history_add_from_temp(cur)

    @db_transaction
    def origin_get(self, origin, cur=None):
        """Return the id of the given origin

        Args:
            origin: dictionary representing the individual
                origin to find. This dict has the following keys:
                - type (FIXME: enum TBD): the origin type ('git', 'wget', ...)
                - url (bytes): the url the origin points to

        Returns:
            the id of the queried origin
        """
        query = "select id from origin where type=%s and url=%s"

        cur.execute(query, (origin['type'], origin['url']))

        data = cur.fetchone()
        if not data:
            return None
        else:
            return data[0]

    @db_transaction
    def origin_add_one(self, origin, cur=None):
        """Add origin to the storage

        Args:
            origin: dictionary representing the individual
                origin to add. This dict has the following keys:
                - type (FIXME: enum TBD): the origin type ('git', 'wget', ...)
                - url (bytes): the url the origin points to

        Returns:
            the id of the added origin, or of the identical one that already
            exists.
        """
        query = "select id from origin where type=%s and url=%s"

        cur.execute(query, (origin['type'], origin['url']))

        data = cur.fetchone()
        if data:
            return data[0]

        insert = """insert into origin (type, url) values (%s, %s)
                    returning id"""

        cur.execute(insert, (origin['type'], origin['url']))
        return cur.fetchone()[0]

    @db_transaction
    def stat_counters(self, cur=None):
        """compute statistics about the number of tuples in various tables

        Returns:
            a dictionary mapping textual labels (e.g., content) to integer
            values (e.g., the number of tuples in table content)

        """
        return {k: v for (k, v) in self.db.stat_counters()}
