# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import functools
import itertools
import psycopg2

from collections import defaultdict

from .db import Db
from .objstorage import ObjStorage


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

    @db_transaction
    def content_add(self, content, cur):
        """Add content blobs to the storage

        Note: in case of DB errors, objects might have already been added to
        the object storage and will not be removed. Since addition to the
        object storage is idempotent, that should not be a problem.

        Args:
            content: iterable of dictionaries representing individual pieces of
                content to add. Each dictionary has the following keys:
                - data (bytes): the actual content
                - length (int): content length
                - one key for each checksum algorithm in
                  swh.core.hashutil.ALGORITHMS, mapped to the corresponding
                  checksum

        """
        db = self.db

        # create temporary table for metadata injection
        db.mktemp('content', cur)

        def add_to_objstorage(cont):
            self.objstorage.add_bytes(cont['data'], obj_id=cont['sha1'])

        db.copy_to(content, 'tmp_content',
                   ['sha1', 'sha1_git', 'sha256', 'length'],
                   cur, item_cb=add_to_objstorage)

        # move metadata in place
        db.content_add_from_temp(cur)
        db.conn.commit()

    @db_transaction_generator
    def content_missing(self, content, cur):
        """List content missing from storage

        Args:
            content: iterable of dictionaries containing one key for each
                checksum algorithm in swh.core.hashutil.ALGORITHMS, mapped to
                the corresponding checksum, and a length key mapped to the
                content length.

        Returns:
            an iterable of sha1s missing from the storage

        Raises:
            TODO: an exception when we get a hash collision.

        """
        db = self.db

        # Create temporary table for metadata injection
        db.mktemp('content', cur)

        db.copy_to(content, 'tmp_content',
                   ['sha1', 'sha1_git', 'sha256', 'length'],
                   cur)

        for obj in db.content_missing_from_temp(cur):
            yield obj[0]

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
                    - atime (datetime.DateTime): entry access time
                    - ctime (datetime.DateTime): entry creation time
                    - mtime (datetime.DateTime): entry modification time
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
                    ['target', 'name', 'perms', 'atime',
                     'mtime', 'ctime', 'dir_id'],
                    cur,
                )

                cur.execute('SELECT swh_directory_entry_%s_add()' % entry_type)

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
                        'committer_name', 'committer_email'],
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

    def release_add(self, releases):
        """Add releases to the storage

        Args:
            releases: iterable of dictionaries representing the individual
                releases to add. Each dict has the following keys:
                - id (sha1_git): id of the release to add
                - revision (sha1_git): id of the revision the release points
                    to
                - date (datetime.DateTime): the date the release was made
                - name (bytes): the name of the release
                - comment (bytes): the comment associated with the release
                - author_name (bytes): the name of the release author
                - author_email (bytes): the email of the release author
        """
        pass

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


    def occurrence_add(self, occurrences):
        """Add occurrences to the storage

        Args:
            occurrences: iterable of dictionaries representing the individual
                occurrences to add. Each dict has the following keys:
                - origin (int): id of the origin corresponding to the
                    occurrence
                - reference (bytes): the reference name of the occurrence
                - revision (sha1_git): the id of the revision pointed to by
                    the occurrence
                - date (datetime.DateTime): the validity date for the given
                    occurrence
        """
        pass

    def origin_add(self, origins):
        """Add origins to the storage

        Args:
            origins: iterable of dictionaries representing the individual
                origins to add. Each dict has the following keys:
                - type (FIXME: enum TBD): the origin type ('git', 'wget', ...)
                - url (bytes): the url the origin points to
        """
        pass
