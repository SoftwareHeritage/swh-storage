# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from collections import defaultdict
import datetime
import itertools
import json
import dateutil.parser
import psycopg2

from . import converters
from .common import db_transaction_generator, db_transaction
from .db import Db
from .exc import StorageDBError

from swh.model.hashutil import ALGORITHMS
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


CONTENT_HASH_KEYS = ['sha1', 'sha1_git', 'sha256', 'blake2s256']


class Storage():
    """SWH storage proxy, encompassing DB and object storage

    """

    def __init__(self, db, objstorage):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection
            obj_root: path to the root of the object storage

        """
        try:
            if isinstance(db, psycopg2.extensions.connection):
                self.db = Db(db)
            else:
                self.db = Db.connect(db)
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)

        self.objstorage = get_objstorage(**objstorage)

    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""

        if not self.objstorage.check_config(check_write=check_write):
            return False

        # Check permissions on one of the tables
        with self.db.transaction() as cur:
            if check_write:
                check = 'INSERT'
            else:
                check = 'SELECT'

            cur.execute(
                "select has_table_privilege(current_user, 'content', %s)",
                (check,)
            )
            return cur.fetchone()[0]

        return True

    def content_add(self, content):
        """Add content blobs to the storage

        Note: in case of DB errors, objects might have already been added to
        the object storage and will not be removed. Since addition to the
        object storage is idempotent, that should not be a problem.

        Args:
            content (iterable): iterable of dictionaries representing
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

        """
        db = self.db

        def _unique_key(hash, keys=CONTENT_HASH_KEYS):
            """Given a hash (tuple or dict), return a unique key from the
               aggregation of keys.

            """
            if isinstance(hash, tuple):
                return hash
            return tuple([hash[k] for k in keys])

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
        missing_skipped = set(_unique_key(hashes) for hashes
                              in self.skipped_content_missing(
                                  content_without_data))

        with db.transaction() as cur:
            if missing_content:
                # create temporary table for metadata injection
                db.mktemp('content', cur)

                def add_to_objstorage(cont):
                    self.objstorage.add(cont['data'],
                                        obj_id=cont['sha1'])

                content_filtered = (cont for cont in content_with_data
                                    if cont['sha1'] in missing_content)

                db.copy_to(content_filtered, 'tmp_content',
                           db.content_get_metadata_keys,
                           cur, item_cb=add_to_objstorage)

                # move metadata in place
                db.content_add_from_temp(cur)

            if missing_skipped:
                missing_filtered = (cont for cont in content_without_data
                                    if _unique_key(cont) in missing_skipped)

                db.mktemp('skipped_content', cur)
                db.copy_to(missing_filtered, 'tmp_skipped_content',
                           db.skipped_content_keys, cur)

                # move metadata in place
                db.skipped_content_add_from_temp(cur)

    @db_transaction
    def content_update(self, content, keys=[], cur=None):
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
        db = self.db

        # TODO: Add a check on input keys. How to properly implement
        # this? We don't know yet the new columns.

        db.mktemp('content')
        select_keys = list(set(db.content_get_metadata_keys).union(set(keys)))
        db.copy_to(content, 'tmp_content', select_keys, cur)
        db.content_update_from_temp(keys_to_update=keys,
                                    cur=cur)

    def content_get(self, content):
        """Retrieve in bulk contents and their data.

        Args:
            content: iterables of sha1

        Yields:
            dict: Generates streams of contents as dict with their raw data:

                - sha1: sha1's content
                - data: bytes data of the content

        Raises:
            ValueError in case of too much contents are required.
            cf. BULK_BLOCK_CONTENT_LEN_MAX

        """
        # FIXME: Improve on server module to slice the result
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

    @db_transaction_generator
    def content_get_metadata(self, content, cur=None):
        """Retrieve content metadata in bulk

        Args:
            content: iterable of content identifiers (sha1)

        Returns:
            an iterable with content metadata corresponding to the given ids
        """
        db = self.db

        db.store_tmp_bytea(content, cur)

        for content_metadata in db.content_get_metadata_from_temp(cur):
            yield dict(zip(db.content_get_metadata_keys, content_metadata))

    @db_transaction_generator
    def content_missing(self, content, key_hash='sha1', cur=None):
        """List content missing from storage

        Args:
            content ([dict]): iterable of dictionaries containing one
                              key for each checksum algorithm in
                              :data:`swh.model.hashutil.ALGORITHMS`,
                              mapped to the corresponding checksum,
                              and a length key mapped to the content
                              length.

            key_hash (str): name of the column to use as hash id
                            result (default: 'sha1')

        Returns:
            iterable ([bytes]): missing content ids (as per the
            key_hash column)

        Raises:
            TODO: an exception when we get a hash collision.

        """
        db = self.db

        keys = CONTENT_HASH_KEYS

        if key_hash not in CONTENT_HASH_KEYS:
            raise ValueError("key_hash should be one of %s" % keys)

        key_hash_idx = keys.index(key_hash)

        # Create temporary table for metadata injection
        db.mktemp('content', cur)

        db.copy_to(content, 'tmp_content', keys + ['length'], cur)

        for obj in db.content_missing_from_temp(cur):
            yield obj[key_hash_idx]

    @db_transaction_generator
    def content_missing_per_sha1(self, contents, cur=None):
        """List content missing from storage based only on sha1.

        Args:
            contents: Iterable of sha1 to check for absence.

        Returns:
            iterable: missing ids

        Raises:
            TODO: an exception when we get a hash collision.

        """
        db = self.db

        db.store_tmp_bytea(contents, cur)
        for obj in db.content_missing_per_sha1_from_temp(cur):
            yield obj[0]

    @db_transaction_generator
    def skipped_content_missing(self, content, cur=None):
        """List skipped_content missing from storage

        Args:
            content: iterable of dictionaries containing the data for each
                checksum algorithm.

        Returns:
            iterable: missing signatures

        """
        keys = CONTENT_HASH_KEYS

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
                checksum algorithm names (see swh.model.hashutil.ALGORITHMS) to
                checksum values

        Returns:
            a triplet (sha1, sha1_git, sha256) if the content exist
            or None otherwise.

        Raises:
            ValueError: in case the key of the dictionary is not sha1, sha1_git
                nor sha256.

        """
        db = self.db

        if not set(content).intersection(ALGORITHMS):
            raise ValueError('content keys must contain at least one of: '
                             'sha1, sha1_git, sha256, blake2s256')

        c = db.content_find(sha1=content.get('sha1'),
                            sha1_git=content.get('sha1_git'),
                            sha256=content.get('sha256'),
                            blake2s256=content.get('blake2s256'),
                            cur=cur)
        if c:
            return dict(zip(db.content_find_cols, c))
        return None

    def directory_add(self, directories):
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
            for src_entry in cur_dir['entries']:
                entry = src_entry.copy()
                entry['dir_id'] = dir_id
                dir_entries[entry['type']][dir_id].append(entry)

        dirs_missing = set(self.directory_missing(dirs))
        if not dirs_missing:
            return

        db = self.db
        with db.transaction() as cur:
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

    @db_transaction_generator
    def directory_missing(self, directories, cur):
        """List directories missing from storage

        Args:
            directories (iterable): an iterable of directory ids

        Yields:
            missing directory ids

        """
        db = self.db

        # Create temporary table for metadata injection
        db.mktemp('directory', cur)

        directories_dicts = ({'id': dir} for dir in directories)

        db.copy_to(directories_dicts, 'tmp_directory', ['id'], cur)

        for obj in db.directory_missing_from_temp(cur):
            yield obj[0]

    @db_transaction_generator
    def directory_get(self,
                      directories,
                      cur=None):
        """Get information on directories.

        Args:
            - directories: an iterable of directory ids

        Returns:
            List of directories as dict with keys and associated values.

        """
        db = self.db
        keys = ('id', 'dir_entries', 'file_entries', 'rev_entries')

        db.mktemp('directory', cur)
        db.copy_to(({'id': dir_id} for dir_id in directories),
                   'tmp_directory', ['id'], cur)

        dirs = db.directory_get_from_temp(cur)
        for line in dirs:
            yield dict(zip(keys, line))

    @db_transaction_generator
    def directory_ls(self, directory, recursive=False, cur=None):
        """Get entries for one directory.

        Args:
            - directory: the directory to list entries from.
            - recursive: if flag on, this list recursively from this directory.

        Returns:
            List of entries for such directory.

        """
        db = self.db

        if recursive:
            res_gen = db.directory_walk(directory, cur=cur)
        else:
            res_gen = db.directory_walk_one(directory, cur=cur)

        for line in res_gen:
            yield dict(zip(db.directory_ls_cols, line))

    @db_transaction
    def directory_entry_get_by_path(self, directory, paths, cur=None):
        """Get the directory entry (either file or dir) from directory with path.

        Args:
            - directory: sha1 of the top level directory
            - paths: path to lookup from the top level directory. From left
              (top) to right (bottom).

        Returns:
            The corresponding directory entry if found, None otherwise.

        """
        db = self.db

        res = db.directory_entry_get_by_path(directory, paths, cur)
        if res:
            return dict(zip(db.directory_ls_cols, res))

    def revision_add(self, revisions):
        """Add revisions to the storage

        Args:
            revisions (iterable): iterable of dictionaries representing the
                individual revisions to add. Each dict has the following keys:

                - id (sha1_git): id of the revision to add
                - date (datetime.DateTime): date the revision was written
                - date_offset (int): offset from UTC in minutes the revision
                  was written
                - date_neg_utc_offset (boolean): whether a null date_offset
                  represents a negative UTC offset
                - committer_date (datetime.DateTime): date the revision got
                  added to the origin
                - committer_date_offset (int): offset from UTC in minutes the
                  revision was added to the origin
                - committer_date_neg_utc_offset (boolean): whether a null
                  committer_date_offset represents a negative UTC offset
                - type (one of 'git', 'tar'): type of the revision added
                - directory (sha1_git): the directory the revision points at
                - message (bytes): the message associated with the revision
                - author_name (bytes): the name of the revision author
                - author_email (bytes): the email of the revision author
                - committer_name (bytes): the name of the revision committer
                - committer_email (bytes): the email of the revision committer
                - metadata (jsonb): extra information as dictionary
                - synthetic (bool): revision's nature (tarball, directory
                  creates synthetic revision)
                - parents (list of sha1_git): the parents of this revision

        """
        db = self.db

        revisions_missing = set(self.revision_missing(
            set(revision['id'] for revision in revisions)))

        if not revisions_missing:
            return

        with db.transaction() as cur:
            db.mktemp_revision(cur)

            revisions_filtered = (
                converters.revision_to_db(revision) for revision in revisions
                if revision['id'] in revisions_missing)

            parents_filtered = []

            db.copy_to(
                revisions_filtered, 'tmp_revision', db.revision_add_cols,
                cur,
                lambda rev: parents_filtered.extend(rev['parents']))

            db.revision_add_from_temp(cur)

            db.copy_to(parents_filtered, 'revision_history',
                       ['id', 'parent_id', 'parent_rank'], cur)

    @db_transaction_generator
    def revision_missing(self, revisions, cur=None):
        """List revisions missing from storage

        Args:
            revisions (iterable): revision ids

        Yields:
            missing revision ids

        """
        db = self.db

        db.store_tmp_bytea(revisions, cur)

        for obj in db.revision_missing_from_temp(cur):
            yield obj[0]

    @db_transaction_generator
    def revision_get(self, revisions, cur):
        """Get all revisions from storage

        Args:
            revisions: an iterable of revision ids

        Returns:
            iterable: an iterable of revisions as dictionaries (or None if the
                revision doesn't exist)

        """

        db = self.db

        db.store_tmp_bytea(revisions, cur)

        for line in self.db.revision_get_from_temp(cur):
            data = converters.db_to_revision(
                dict(zip(db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    @db_transaction_generator
    def revision_log(self, revisions, limit=None, cur=None):
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revision to lookup
            limit: limitation on the output result. Default to None.

        Yields:
            List of revision log from such revisions root.

        """
        db = self.db

        for line in db.revision_log(revisions, limit, cur):
            data = converters.db_to_revision(
                dict(zip(db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    @db_transaction_generator
    def revision_shortlog(self, revisions, limit=None, cur=None):
        """Fetch the shortlog for the given revisions

        Args:
            revisions: list of root revisions to lookup
            limit: depth limitation for the output

        Yields:
            a list of (id, parents) tuples.

        """

        db = self.db

        yield from db.revision_shortlog(revisions, limit, cur)

    @db_transaction_generator
    def revision_log_by(self, origin_id, branch_name=None, timestamp=None,
                        limit=None, cur=None):
        """Fetch revision entry from the actual origin_id's latest revision.

        Args:
            origin_id: the origin id from which deriving the revision
            branch_name: (optional) occurrence's branch name
            timestamp: (optional) occurrence's time
            limit: (optional) depth limitation for the
                output. Default to None.

        Yields:
            The revision log starting from the revision derived from
            the (origin, branch_name, timestamp) combination if any.

        Returns:
            None if no revision matching this combination is found.

        """
        db = self.db

        # Retrieve the revision by criterion
        revisions = list(db.revision_get_by(
            origin_id, branch_name, timestamp, limit=1))

        if not revisions:
            return None

        revision_id = revisions[0][0]
        # otherwise, retrieve the revision log from that revision
        yield from self.revision_log([revision_id], limit)

    def release_add(self, releases):
        """Add releases to the storage

        Args:
            releases (iterable): iterable of dictionaries representing the
                individual releases to add. Each dict has the following keys:

                - id (sha1_git): id of the release to add
                - revision (sha1_git): id of the revision the release points to
                - date (datetime.DateTime): the date the release was made
                - date_offset (int): offset from UTC in minutes the release was
                  made
                - date_neg_utc_offset (boolean): whether a null date_offset
                  represents a negative UTC offset
                - name (bytes): the name of the release
                - comment (bytes): the comment associated with the release
                - author_name (bytes): the name of the release author
                - author_email (bytes): the email of the release author

        """
        db = self.db

        release_ids = set(release['id'] for release in releases)
        releases_missing = set(self.release_missing(release_ids))

        if not releases_missing:
            return

        with db.transaction() as cur:
            db.mktemp_release(cur)

            releases_filtered = (
                converters.release_to_db(release) for release in releases
                if release['id'] in releases_missing
            )

            db.copy_to(releases_filtered, 'tmp_release', db.release_add_cols,
                       cur)

            db.release_add_from_temp(cur)

    @db_transaction_generator
    def release_missing(self, releases, cur=None):
        """List releases missing from storage

        Args:
            releases: an iterable of release ids

        Returns:
            a list of missing release ids

        """
        db = self.db

        # Create temporary table for metadata injection
        db.store_tmp_bytea(releases, cur)

        for obj in db.release_missing_from_temp(cur):
            yield obj[0]

    @db_transaction_generator
    def release_get(self, releases, cur=None):
        """Given a list of sha1, return the releases's information

        Args:
            releases: list of sha1s

        Yields:
            releases: list of releases as dicts with the following keys:

            - id: origin's id
            - revision: origin's type
            - url: origin's url
            - lister: lister's uuid
            - project: project's uuid (FIXME, retrieve this information)

        Raises:
            ValueError: if the keys does not match (url and type) nor id.

        """
        db = self.db

        # Create temporary table for metadata injection
        db.store_tmp_bytea(releases, cur)

        for release in db.release_get_from_temp(cur):
            yield converters.db_to_release(
                dict(zip(db.release_get_cols, release))
            )

    @db_transaction
    def snapshot_add(self, origin, visit, snapshot, back_compat=False,
                     cur=None):
        """Add a snapshot for the given origin/visit couple

        Args:
            origin (int): id of the origin
            visit (int): id of the visit
            snapshot (dict): the snapshot to add to the visit, containing the
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
            back_compat (bool): whether to add the occurrences for
              backwards-compatibility
        """
        db = self.db

        if not db.snapshot_exists(snapshot['id'], cur):
            db.mktemp_snapshot_branch(cur)
            db.copy_to(
                (
                    {
                        'name': name,
                        'target': info['target'] if info else None,
                        'target_type': info['target_type'] if info else None,
                    }
                    for name, info in snapshot['branches'].items()
                ),
                'tmp_snapshot_branch',
                ['name', 'target', 'target_type'],
                cur,
            )

        db.snapshot_add(origin, visit, snapshot['id'], cur)

        if not back_compat:
            return

        # TODO: drop this compat feature
        occurrences = []
        for name, info in snapshot['branches'].items():
            if not info:
                target = b'\x00' * 20
                target_type = 'revision'
            elif info['target_type'] == 'alias':
                continue
            else:
                target = info['target']
                target_type = info['target_type']

            occurrences.append({
                'origin': origin,
                'visit': visit,
                'branch': name,
                'target': target,
                'target_type': target_type,
            })

        self.occurrence_add(occurrences)

    @db_transaction
    def snapshot_get(self, snapshot_id, cur=None):
        """Get the snapshot with the given id

        Args:
           snapshot_id (bytes): id of the snapshot
        Returns:
           dict: a snapshot with two keys:
             id:: identifier for the snapshot
             branches:: a list of branches contained by the snapshot

        """
        db = self.db

        branches = {}
        for branch in db.snapshot_get_by_id(snapshot_id, cur):
            branch = dict(zip(db.snapshot_get_cols, branch))
            del branch['snapshot_id']
            name = branch.pop('name')
            if branch == {'target': None, 'target_type': None}:
                branch = None
            branches[name] = branch

        if branches:
            return {'id': snapshot_id, 'branches': branches}

        if db.snapshot_exists(snapshot_id, cur):
            # empty snapshot
            return {'id': snapshot_id, 'branches': {}}

        return None

    @db_transaction
    def snapshot_get_by_origin_visit(self, origin, visit, cur=None):
        """Get the snapshot for the given origin visit

        Args:
           origin (int): the origin identifier
           visit (int): the visit identifier
        Returns:
           dict: a snapshot with two keys:
             id:: identifier for the snapshot
             branches:: a dictionary containing the snapshot branch information

        """
        db = self.db

        snapshot_id = db.snapshot_get_by_origin_visit(origin, visit, cur)

        if snapshot_id:
            return self.snapshot_get(snapshot_id, cur=cur)
        else:
            # compatibility code during the snapshot migration
            origin_visit_info = self.origin_visit_get_by(origin, visit,
                                                         cur=cur)
            if origin_visit_info is None:
                return None
            ret = {'id': None}
            ret['branches'] = origin_visit_info['occurrences']
            return ret

        return None

    @db_transaction
    def snapshot_get_latest(self, origin, allowed_statuses=None, cur=None):
        """Get the latest snapshot for the given origin, optionally only from visits
        that have one of the given allowed_statuses.

        Args:
            origin (int): the origin identifier
            allowed_statuses (list of str): list of visit statuses considered
              to find the latest snapshot for the visit. For instance,
              ``allowed_statuses=['full']`` will only consider visits that
              have successfully run to completion.

        Returns:
           dict: a snapshot with two keys:
             id:: identifier for the snapshot
             branches:: a dictionary containing the snapshot branch information
        """
        db = self.db

        origin_visit = db.origin_visit_get_latest_snapshot(
            origin, allowed_statuses=allowed_statuses, cur=cur)
        if origin_visit:
            origin_visit = dict(zip(db.origin_visit_get_cols, origin_visit))
            return self.snapshot_get(origin_visit['snapshot'], cur=cur)

    @db_transaction
    def occurrence_add(self, occurrences, cur=None):
        """Add occurrences to the storage

        Args:
            occurrences: iterable of dictionaries representing the individual
                occurrences to add. Each dict has the following keys:

                - origin (int): id of the origin corresponding to the
                  occurrence
                - visit (int): id of the visit corresponding to the
                  occurrence
                - branch (str): the reference name of the occurrence
                - target (sha1_git): the id of the object pointed to by
                  the occurrence
                - target_type (str): the type of object pointed to by the
                  occurrence

        """
        db = self.db

        db.mktemp_occurrence_history(cur)
        db.copy_to(occurrences, 'tmp_occurrence_history',
                   ['origin', 'branch', 'target', 'target_type', 'visit'], cur)

        db.occurrence_history_add_from_temp(cur)

    @db_transaction_generator
    def occurrence_get(self, origin_id, cur=None):
        """Retrieve occurrence information per origin_id.

        Args:
            origin_id: The occurrence's origin.

        Yields:
            List of occurrences matching criterion.

        """
        db = self.db
        for line in db.occurrence_get(origin_id, cur):
            yield {
                'origin': line[0],
                'branch': line[1],
                'target': line[2],
                'target_type': line[3],
            }

    @db_transaction
    def origin_visit_add(self, origin, ts, cur=None):
        """Add an origin_visit for the origin at ts with status 'ongoing'.

        Args:
            origin: Visited Origin id
            ts: timestamp of such visit

        Returns:
            dict: dictionary with keys origin and visit where:

            - origin: origin identifier
            - visit: the visit identifier for the new visit occurrence
            - ts (datetime.DateTime): the visit date

        """
        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        return {
            'origin': origin,
            'visit': self.db.origin_visit_add(origin, ts, cur)
        }

    @db_transaction
    def origin_visit_update(self, origin, visit_id, status, metadata=None,
                            cur=None):
        """Update an origin_visit's status.

        Args:
            origin: Visited Origin id
            visit_id: Visit's id
            status: Visit's new status
            metadata: Data associated to the visit

        Returns:
            None

        """
        return self.db.origin_visit_update(origin, visit_id, status, metadata,
                                           cur)

    @db_transaction_generator
    def origin_visit_get(self, origin, last_visit=None, limit=None, cur=None):
        """Retrieve all the origin's visit's information.

        Args:
            origin (int): The occurrence's origin (identifier).
            last_visit (int): Starting point from which listing the next visits
                Default to None
            limit (int): Number of results to return from the last visit.
                Default to None

        Yields:
            List of visits.

        """
        db = self.db
        for line in db.origin_visit_get_all(
                origin, last_visit=last_visit, limit=limit, cur=cur):
            data = dict(zip(self.db.origin_visit_get_cols, line))
            yield data

    @db_transaction
    def origin_visit_get_by(self, origin, visit, cur=None):
        """Retrieve origin visit's information.

        Args:
            origin: The occurrence's origin (identifier).

        Returns:
            The information on that particular (origin, visit)

        """
        db = self.db

        ori_visit = db.origin_visit_get(origin, visit, cur)
        if not ori_visit:
            return None

        ori_visit = dict(zip(self.db.origin_visit_get_cols, ori_visit))

        if ori_visit['snapshot']:
            ori_visit['occurrences'] = self.snapshot_get(ori_visit['snapshot'],
                                                         cur=cur)['branches']
            return ori_visit

        # TODO: remove Backwards compatibility after snapshot migration
        occs = {}
        for occ in db.occurrence_by_origin_visit(origin, visit):
            _, branch_name, target, target_type = occ
            occs[branch_name] = {
                'target': target,
                'target_type': target_type
            }

        ori_visit['occurrences'] = occs

        return ori_visit

    @db_transaction_generator
    def revision_get_by(self,
                        origin_id,
                        branch_name=None,
                        timestamp=None,
                        limit=None,
                        cur=None):
        """Given an origin_id, retrieve occurrences' list per given criterions.

        Args:
            origin_id: The origin to filter on.
            branch_name: (optional) branch name.
            timestamp: (optional) time.
            limit: (optional) limit

        Yields:
            List of occurrences matching the criterions or None if nothing is
            found.

        """
        for line in self.db.revision_get_by(origin_id,
                                            branch_name,
                                            timestamp,
                                            limit=limit,
                                            cur=cur):
            data = converters.db_to_revision(
                dict(zip(self.db.revision_get_cols, line))
            )
            if not data['type']:
                yield None
                continue
            yield data

    def release_get_by(self, origin_id, limit=None):
        """Given an origin id, return all the tag objects pointing to heads of
        origin_id.

        Args:
            origin_id: the origin to filter on.
            limit: None by default

        Yields:
            List of releases matching the criterions or None if nothing is
            found.

        """

        for line in self.db.release_get_by(origin_id, limit=limit):
            data = converters.db_to_release(
                dict(zip(self.db.release_get_cols, line))
            )
            yield data

    @db_transaction
    def object_find_by_sha1_git(self, ids, cur=None):
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
        db = self.db

        ret = {id: [] for id in ids}

        for retval in db.object_find_by_sha1_git(ids):
            if retval[1]:
                ret[retval[0]].append(dict(zip(db.object_find_by_sha1_git_cols,
                                               retval)))

        return ret

    origin_keys = ['id', 'type', 'url', 'lister', 'project']

    @db_transaction
    def origin_get(self, origin, cur=None):
        """Return the origin either identified by its id or its tuple
        (type, url).

        Args:
            origin: dictionary representing the individual origin to find.
                This dict has either the keys type and url:

                - type (FIXME: enum TBD): the origin type ('git', 'wget', ...)
                - url (bytes): the url the origin points to

                or the id:

                - id: the origin id

        Returns:
            dict: the origin dictionary with the keys:

            - id: origin's id
            - type: origin's type
            - url: origin's url
            - lister: lister's uuid
            - project: project's uuid (FIXME, retrieve this information)

        Raises:
            ValueError: if the keys does not match (url and type) nor id.

        """
        db = self.db

        origin_id = origin.get('id')
        if origin_id:  # check lookup per id first
            ori = db.origin_get(origin_id, cur)
        elif 'type' in origin and 'url' in origin:  # or lookup per type, url
            ori = db.origin_get_with(origin['type'], origin['url'], cur)
        else:  # unsupported lookup
            raise ValueError('Origin must have either id or (type and url).')

        if ori:
            return dict(zip(self.origin_keys, ori))
        return None

    @db_transaction_generator
    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, cur=None):
        """Search for origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The search is performed in a case insensitive way.

        Args:
            url_pattern: the string pattern to search for in origin urls
            offset: number of found origins to skip before returning results
            limit: the maximum number of found origins to return
            regexp: if True, consider the provided pattern as a regular
                expression and return origins whose urls match it

        Returns:
            An iterable of dict containing origin information as returned
            by :meth:`swh.storage.storage.Storage.origin_get`.
        """
        db = self.db

        for origin in db.origin_search(url_pattern, offset, limit,
                                       regexp, cur):
            yield dict(zip(self.origin_keys, origin))

    @db_transaction
    def _person_add(self, person, cur=None):
        """Add a person in storage.

        Note: Internal function for now, do not use outside of this module.

        Do not do anything fancy in case a person already exists.
        Please adapt code if more checks are needed.

        Args:
            person: dictionary with keys name and email.

        Returns:
            Id of the new person.

        """
        db = self.db

        return db.person_add(person)

    @db_transaction_generator
    def person_get(self, person, cur=None):
        """Return the persons identified by their ids.

        Args:
            person: array of ids.

        Returns:
            The array of persons corresponding of the ids.

        """
        db = self.db

        for person in db.person_get(person):
            yield dict(zip(db.person_get_cols, person))

    @db_transaction
    def origin_add(self, origins, cur=None):
        """Add origins to the storage

        Args:
            origins: list of dictionaries representing the individual origins,
                with the following keys:

                - type: the origin type ('git', 'svn', 'deb', ...)
                - url (bytes): the url the origin points to

        Returns:
            list: ids corresponding to the given origins

        """

        ret = []
        for origin in origins:
            ret.append(self.origin_add_one(origin, cur=cur))

        return ret

    @db_transaction
    def origin_add_one(self, origin, cur=None):
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
        db = self.db

        data = db.origin_get_with(origin['type'], origin['url'], cur)
        if data:
            return data[0]

        return db.origin_add(origin['type'], origin['url'], cur)

    @db_transaction
    def fetch_history_start(self, origin_id, cur=None):
        """Add an entry for origin origin_id in fetch_history. Returns the id
        of the added fetch_history entry
        """
        fetch_history = {
            'origin': origin_id,
            'date': datetime.datetime.now(tz=datetime.timezone.utc),
        }

        return self.db.create_fetch_history(fetch_history, cur)

    @db_transaction
    def fetch_history_end(self, fetch_history_id, data, cur=None):
        """Close the fetch_history entry with id `fetch_history_id`, replacing
           its data with `data`.
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        fetch_history = self.db.get_fetch_history(fetch_history_id, cur)

        if not fetch_history:
            raise ValueError('No fetch_history with id %d' % fetch_history_id)

        fetch_history['duration'] = now - fetch_history['date']

        fetch_history.update(data)

        self.db.update_fetch_history(fetch_history, cur)

    @db_transaction
    def fetch_history_get(self, fetch_history_id, cur=None):
        """Get the fetch_history entry with id `fetch_history_id`.
        """
        return self.db.get_fetch_history(fetch_history_id, cur)

    @db_transaction
    def entity_add(self, entities, cur=None):
        """Add the given entitites to the database (in entity_history).

        Args:
            entities (iterable): iterable of dictionaries with the following
                keys:

                - uuid (uuid): id of the entity
                - parent (uuid): id of the parent entity
                - name (str): name of the entity
                - type (str): type of entity (one of 'organization',
                  'group_of_entities', 'hosting', 'group_of_persons', 'person',
                  'project')
                - description (str, optional): description of the entity
                - homepage (str): url of the entity's homepage
                - active (bool): whether the entity is active
                - generated (bool): whether the entity was generated
                - lister_metadata (dict): lister-specific entity metadata
                - metadata (dict): other metadata for the entity
                - validity (datetime.DateTime array): timestamps at which we
                  listed the entity.

        """
        db = self.db

        cols = list(db.entity_history_cols)
        cols.remove('id')

        db.mktemp_entity_history()
        db.copy_to(entities, 'tmp_entity_history', cols, cur)
        db.entity_history_add_from_temp()

    @db_transaction_generator
    def entity_get_from_lister_metadata(self, entities, cur=None):
        """Fetch entities from the database, matching with the lister and
           associated metadata.

        Args:
            entities (iterable): dictionaries containing the lister metadata to
               look for. Useful keys are 'lister', 'type', 'id', ...

        Yields:
            fetched entities with all their attributes. If no match was found,
            the returned entity is None.

        """

        db = self.db

        db.mktemp_entity_lister(cur)

        mapped_entities = []
        for i, entity in enumerate(entities):
            mapped_entity = {
                'id': i,
                'lister_metadata': entity,
            }
            mapped_entities.append(mapped_entity)

        db.copy_to(mapped_entities, 'tmp_entity_lister',
                   ['id', 'lister_metadata'], cur)

        cur.execute('''select id, %s
                       from swh_entity_from_tmp_entity_lister()
                       order by id''' %
                    ','.join(db.entity_cols))

        for id, *entity_vals in cur:
            fetched_entity = dict(zip(db.entity_cols, entity_vals))
            if fetched_entity['uuid']:
                yield fetched_entity
            else:
                yield {
                    'uuid': None,
                    'lister_metadata': entities[i],
                }

    @db_transaction_generator
    def entity_get(self, uuid, cur=None):
        """Returns the list of entity per its uuid identifier and also its
        parent hierarchy.

        Args:
            uuid: entity's identifier

        Returns:
            List of entities starting with entity with uuid and the parent
            hierarchy from such entity.

        """
        db = self.db
        for entity in db.entity_get(uuid, cur):
            yield dict(zip(db.entity_cols, entity))

    @db_transaction
    def entity_get_one(self, uuid, cur=None):
        """Returns one entity using its uuid identifier.

        Args:
            uuid: entity's identifier

        Returns:
            the object corresponding to the given entity

        """
        db = self.db
        entity = db.entity_get_one(uuid, cur)
        if entity:
            return dict(zip(db.entity_cols, entity))
        else:
            return None

    @db_transaction
    def stat_counters(self, cur=None):
        """compute statistics about the number of tuples in various tables

        Returns:
            dict: a dictionary mapping textual labels (e.g., content) to
            integer values (e.g., the number of tuples in table content)

        """
        return {k: v for (k, v) in self.db.stat_counters()}

    @db_transaction
    def origin_metadata_add(self, origin_id, ts, provider, tool, metadata,
                            cur=None):
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
        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        return self.db.origin_metadata_add(origin_id, ts, provider, tool,
                                           metadata, cur)

    @db_transaction_generator
    def origin_metadata_get_by(self, origin_id, provider_type=None, cur=None):
        """Retrieve list of all origin_metadata entries for the origin_id

        Args:
            origin_id (int): the unique origin identifier
            provider_type (str): (optional) type of provider

        Returns:
            list of dicts: the origin_metadata dictionary with the keys:

            - id (int): origin_metadata's id
            - origin_id (int): origin's id
            - discovery_date (datetime): timestamp of discovery
            - tool_id (int): metadata's extracting tool
            - metadata (jsonb)
            - provider_id (int): metadata's provider
            - provider_name (str)
            - provider_type (str)
            - provider_url (str)

        """
        db = self.db
        for line in db.origin_metadata_get_by(origin_id, provider_type, cur):
            yield dict(zip(db.origin_metadata_get_cols, line))

    @db_transaction_generator
    def tool_add(self, tools, cur=None):
        """Add new tools to the storage.

        Args:
            tools (iterable of :class:`dict`): Tool information to add to
              storage. Each tool is a :class:`dict` with the following keys:

              - name (:class:`str`): name of the tool
              - version (:class:`str`): version of the tool
              - configuration (:class:`dict`): configuration of the tool,
                must be json-encodable

        Returns:
            `iterable` of :class:`dict`: All the tools inserted in storage
            (including the internal ``id``). The order of the list is not
            guaranteed to match the order of the initial list.

        """
        db = self.db
        db.mktemp_tool(cur)
        db.copy_to(tools, 'tmp_tool',
                   ['name', 'version', 'configuration'],
                   cur)

        tools = db.tool_add_from_temp(cur)
        for line in tools:
            yield dict(zip(db.tool_cols, line))

    @db_transaction
    def tool_get(self, tool, cur=None):
        """Retrieve tool information.

        Args:
            tool (dict): Tool information we want to retrieve from storage.
              The dicts have the same keys as those used in :func:`tool_add`.

        Returns:
            dict: The full tool information if it exists (``id`` included),
            None otherwise.

        """
        db = self.db
        tool_conf = tool['configuration']
        if isinstance(tool_conf, dict):
            tool_conf = json.dumps(tool_conf)

        idx = db.tool_get(tool['name'],
                          tool['version'],
                          tool_conf)
        if not idx:
            return None
        return dict(zip(self.db.tool_cols, idx))

    @db_transaction
    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata, cur=None):
        db = self.db
        return db.metadata_provider_add(provider_name, provider_type,
                                        provider_url, metadata, cur)

    @db_transaction
    def metadata_provider_get(self, provider_id, cur=None):
        db = self.db
        result = db.metadata_provider_get(provider_id)
        if not result:
            return None
        return dict(zip(self.db.metadata_provider_cols, result))

    @db_transaction
    def metadata_provider_get_by(self, provider, cur=None):
        db = self.db
        result = db.metadata_provider_get_by(provider['provider_name'],
                                             provider['provider_url'])
        if not result:
            return None
        return dict(zip(self.db.metadata_provider_cols, result))
