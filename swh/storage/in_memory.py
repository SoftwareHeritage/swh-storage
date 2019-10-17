# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import re
import bisect
import dateutil
import collections
from collections import defaultdict
import copy
import datetime
import itertools
import random

import attr

from swh.model.model import \
    Content, Directory, Revision, Release, Snapshot, OriginVisit, Origin
from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError

from .storage import get_journal_writer

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


ENABLE_ORIGIN_IDS = \
    os.environ.get('SWH_STORAGE_IN_MEMORY_ENABLE_ORIGIN_IDS', 'true') == 'true'


class Storage:
    def __init__(self, journal_writer=None):
        self._contents = {}
        self._content_indexes = defaultdict(lambda: defaultdict(set))
        self._skipped_contents = {}
        self._skipped_content_indexes = defaultdict(lambda: defaultdict(set))

        self.reset()

        if journal_writer:
            self.journal_writer = get_journal_writer(**journal_writer)
        else:
            self.journal_writer = None

    def reset(self):
        self._directories = {}
        self._revisions = {}
        self._releases = {}
        self._snapshots = {}
        self._origins = {}
        self._origins_by_id = []
        self._origin_visits = {}
        self._persons = []
        self._origin_metadata = defaultdict(list)
        self._tools = {}
        self._metadata_providers = {}
        self._objects = defaultdict(list)

        # ideally we would want a skip list for both fast inserts and searches
        self._sorted_sha1s = []

        self.objstorage = get_objstorage('memory', {})

    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""
        return True

    def _content_add(self, contents, with_data):
        content_with_data = []
        content_without_data = []
        for content in contents:
            if content.status is None:
                content.status = 'visible'
            if content.length is None:
                content.length = -1
            if content.status != 'absent':
                if self._content_key(content) not in self._contents:
                    content_with_data.append(content)
            else:
                if self._content_key(content) not in self._skipped_contents:
                    content_without_data.append(content)

        if self.journal_writer:
            for content in content_with_data:
                content = attr.evolve(content, data=None)
                self.journal_writer.write_addition('content', content)
            for content in content_without_data:
                self.journal_writer.write_addition('content', content)

        count_content_added, count_content_bytes_added = \
            self._content_add_present(content_with_data, with_data)

        count_skipped_content_added = self._content_add_absent(
            content_without_data
        )

        summary = {
            'content:add': count_content_added,
            'skipped_content:add': count_skipped_content_added,
        }

        if with_data:
            summary['content:add:bytes'] = count_content_bytes_added

        return summary

    def _content_add_present(self, contents, with_data):
        count_content_added = 0
        count_content_bytes_added = 0
        for content in contents:
            key = self._content_key(content)
            if key in self._contents:
                continue
            for algorithm in DEFAULT_ALGORITHMS:
                hash_ = content.get_hash(algorithm)
                if hash_ in self._content_indexes[algorithm]\
                   and (algorithm not in {'blake2s256', 'sha256'}):
                    from . import HashCollision
                    raise HashCollision(algorithm, hash_, key)
            for algorithm in DEFAULT_ALGORITHMS:
                hash_ = content.get_hash(algorithm)
                self._content_indexes[algorithm][hash_].add(key)
            self._objects[content.sha1_git].append(
                ('content', content.sha1))
            self._contents[key] = content
            bisect.insort(self._sorted_sha1s, content.sha1)
            count_content_added += 1
            if with_data:
                content_data = self._contents[key].data
                self._contents[key].data = None
                count_content_bytes_added += len(content_data)
                self.objstorage.add(content_data, content.sha1)

        return (count_content_added, count_content_bytes_added)

    def _content_add_absent(self, contents):
        count = 0
        skipped_content_missing = self.skipped_content_missing(contents)
        for content in skipped_content_missing:
            key = self._content_key(content)
            for algo in DEFAULT_ALGORITHMS:
                self._skipped_content_indexes[algo][content.get_hash(algo)] \
                    .add(key)
            self._skipped_contents[key] = content
            count += 1

        return count

    def _content_to_model(self, contents):
        """Takes a list of content dicts, optionally with an extra 'origin'
        key, and yields tuples (model.Content, origin)."""
        for content in contents:
            content = content.copy()
            content.pop('origin', None)
            yield Content.from_dict(content)

    def content_add(self, content):
        """Add content blobs to the storage

        Args:
            content (iterable): iterable of dictionaries representing
                individual pieces of content to add. Each dictionary has the
                following keys:

                - data (bytes): the actual content
                - length (int): content length (default: -1)
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.DEFAULT_ALGORITHMS`, mapped to the
                  corresponding checksum
                - status (str): one of visible, hidden, absent
                - reason (str): if status = absent, the reason why
                - origin (int): if status = absent, the origin we saw the
                  content in

        Raises:
            HashCollision in case of collision

        Returns:
            Summary dict with the following key and associated values:

                content:add: New contents added
                content_bytes:add: Sum of the contents' length data
                skipped_content:add: New skipped contents (no data) added

        """
        content = list(self._content_to_model(content))
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        for item in content:
            item.ctime = now
        return self._content_add(content, with_data=True)

    def content_add_metadata(self, content):
        """Add content metadata to the storage (like `content_add`, but
        without inserting to the objstorage).

        Args:
            content (iterable): iterable of dictionaries representing
                individual pieces of content to add. Each dictionary has the
                following keys:

                - length (int): content length (default: -1)
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.DEFAULT_ALGORITHMS`, mapped to the
                  corresponding checksum
                - status (str): one of visible, hidden, absent
                - reason (str): if status = absent, the reason why
                - origin (int): if status = absent, the origin we saw the
                  content in
                - ctime (datetime): time of insertion in the archive

        Raises:
            HashCollision in case of collision

        Returns:
            Summary dict with the following key and associated values:

                content:add: New contents added
                skipped_content:add: New skipped contents (no data) added

        """
        content = list(self._content_to_model(content))
        return self._content_add(content, with_data=False)

    def content_get(self, content):
        """Retrieve in bulk contents and their data.

        This function may yield more blobs than provided sha1 identifiers,
        in case they collide.

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
                "Sending at most %s contents." % BULK_BLOCK_CONTENT_LEN_MAX)
        for obj_id in content:
            try:
                data = self.objstorage.get(obj_id)
            except ObjNotFoundError:
                yield None
                continue

            yield {'sha1': obj_id, 'data': data}

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

    def content_get_metadata(self, content):
        """Retrieve content metadata in bulk

        Args:
            content: iterable of content identifiers (sha1)

        Returns:
            an iterable with content metadata corresponding to the given ids
        """
        # FIXME: the return value should be a mapping from search key to found
        # content*s*
        for sha1 in content:
            if sha1 in self._content_indexes['sha1']:
                objs = self._content_indexes['sha1'][sha1]
                # FIXME: rather than selecting one of the objects with that
                # hash, we should return all of them. See:
                # https://forge.softwareheritage.org/D645?id=1994#inline-3389
                key = random.sample(objs, 1)[0]
                d = self._contents[key].to_dict()
                del d['ctime']
                yield d
            else:
                # FIXME: should really be None
                yield {
                    'sha1': sha1,
                    'sha1_git': None,
                    'sha256': None,
                    'blake2s256': None,
                    'length': None,
                    'status': None,
                }

    def content_find(self, content):
        if not set(content).intersection(DEFAULT_ALGORITHMS):
            raise ValueError('content keys must contain at least one of: '
                             '%s' % ', '.join(sorted(DEFAULT_ALGORITHMS)))
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
        """List content missing from storage

        Args:
            contents ([dict]): iterable of dictionaries whose keys are
                               either 'length' or an item of
                               :data:`swh.model.hashutil.ALGORITHMS`;
                               mapped to the corresponding checksum
                               (or length).

            key_hash (str): name of the column to use as hash id
                            result (default: 'sha1')

        Returns:
            iterable ([bytes]): missing content ids (as per the
            key_hash column)
        """
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
        """List content missing from storage based only on sha1.

        Args:
            contents: Iterable of sha1 to check for absence.

        Returns:
            iterable: missing ids

        Raises:
            TODO: an exception when we get a hash collision.

        """
        for content in contents:
            if content not in self._content_indexes['sha1']:
                yield content

    def skipped_content_missing(self, contents):
        """List all skipped_content missing from storage

        Args:
            contents: Iterable of sha1 to check for skipped content entry

        Returns:
            iterable: dict of skipped content entry
        """

        for content in contents:
            for (key, algorithm) in self._content_key_algorithm(content):
                if algorithm == 'blake2s256':
                    continue
                if key not in self._skipped_content_indexes[algorithm]:
                    # index must contain hashes of algos except blake2s256
                    # else the content is considered skipped
                    yield content
                    break

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
        Returns:
            Summary dict of keys with associated count as values:

                directory:add: Number of directories actually added

        """
        if self.journal_writer:
            self.journal_writer.write_additions(
                'directory',
                (dir_ for dir_ in directories
                 if dir_['id'] not in self._directories))

        directories = [Directory.from_dict(d) for d in directories]

        count = 0
        for directory in directories:
            if directory.id not in self._directories:
                count += 1
                self._directories[directory.id] = directory
                self._objects[directory.id].append(
                    ('directory', directory.id))

        return {'directory:add': count}

    def directory_missing(self, directories):
        """List directories missing from storage

        Args:
            directories (iterable): an iterable of directory ids

        Yields:
            missing directory ids

        """
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
        """Get entries for one directory.

        Args:
            - directory: the directory to list entries from.
            - recursive: if flag on, this list recursively from this directory.

        Returns:
            List of entries for such directory.

        If `recursive=True`, names in the path of a dir/file not at the
        root are concatenated with a slash (`/`).
        """
        yield from self._directory_ls(directory, recursive)

    def directory_entry_get_by_path(self, directory, paths):
        """Get the directory entry (either file or dir) from directory with path.

        Args:
            - directory: sha1 of the top level directory
            - paths: path to lookup from the top level directory. From left
              (top) to right (bottom).

        Returns:
            The corresponding directory entry if found, None otherwise.

        """
        return self._directory_entry_get_by_path(directory, paths, b'')

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

    def revision_add(self, revisions):
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

                revision_added: New objects actually stored in db

        """
        if self.journal_writer:
            self.journal_writer.write_additions(
                'revision',
                (rev for rev in revisions
                 if rev['id'] not in self._revisions))

        revisions = [Revision.from_dict(rev) for rev in revisions]

        count = 0
        for revision in revisions:
            if revision.id not in self._revisions:
                revision.committer = self._person_add(revision.committer)
                revision.author = self._person_add(revision.author)
                self._revisions[revision.id] = revision
                self._objects[revision.id].append(
                    ('revision', revision.id))
                count += 1

        return {'revision:add': count}

    def revision_missing(self, revisions):
        """List revisions missing from storage

        Args:
            revisions (iterable): revision ids

        Yields:
            missing revision ids

        """
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
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revision to lookup
            limit: limitation on the output result. Default to None.

        Yields:
            List of revision log from such revisions root.

        """
        seen = set()
        for rev_id in revisions:
            yield from self._get_parent_revs(rev_id, seen, limit)

    def revision_shortlog(self, revisions, limit=None):
        """Fetch the shortlog for the given revisions

        Args:
            revisions: list of root revisions to lookup
            limit: depth limitation for the output

        Yields:
            a list of (id, parents) tuples.

        """
        yield from ((rev['id'], rev['parents'])
                    for rev in self.revision_log(revisions, limit))

    def release_add(self, releases):
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
        if self.journal_writer:
            self.journal_writer.write_additions(
                'release',
                (rel for rel in releases
                 if rel['id'] not in self._releases))

        releases = [Release.from_dict(rel) for rel in releases]

        count = 0
        for rel in releases:
            if rel.id not in self._releases:
                if rel.author:
                    self._person_add(rel.author)
                self._objects[rel.id].append(
                    ('release', rel.id))
                self._releases[rel.id] = rel
                count += 1

        return {'release:add': count}

    def release_missing(self, releases):
        """List releases missing from storage

        Args:
            releases: an iterable of release ids

        Returns:
            a list of missing release ids

        """
        yield from (rel for rel in releases if rel not in self._releases)

    def release_get(self, releases):
        """Given a list of sha1, return the releases's information

        Args:
            releases: list of sha1s

        Yields:
            dicts with the same keys as those given to `release_add`
            (or ``None`` if a release does not exist)

        """
        for rel_id in releases:
            if rel_id in self._releases:
                yield self._releases[rel_id].to_dict()
            else:
                yield None

    def snapshot_add(self, snapshots):
        """Add a snapshot to the storage

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
            ValueError: if the origin's or visit's identifier does not exist.

        Returns:
            Summary dict of keys with associated count as values

                snapshot_added: Count of object actually stored in db

        """
        count = 0
        snapshots = (Snapshot.from_dict(d) for d in snapshots)
        snapshots = (snap for snap in snapshots
                     if snap.id not in self._snapshots)
        for snapshot in snapshots:
            if self.journal_writer:
                self.journal_writer.write_addition('snapshot', snapshot)

            sorted_branch_names = sorted(snapshot.branches)
            self._snapshots[snapshot.id] = (snapshot, sorted_branch_names)
            self._objects[snapshot.id].append(('snapshot', snapshot.id))
            count += 1

        return {'snapshot:add': count}

    def snapshot_get(self, snapshot_id):
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
        return self.snapshot_get_branches(snapshot_id)

    def snapshot_get_by_origin_visit(self, origin, visit):
        """Get the content, possibly partial, of a snapshot for the given origin visit

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        .. warning:: At most 1000 branches contained in the snapshot will be
            returned for performance reasons. In order to browse the whole
            set of branches, the method :meth:`snapshot_get_branches`
            should be used instead.

        Args:
            origin (int): the origin's identifier
            visit (int): the visit's identifier
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
        """Get the content, possibly partial, of the latest snapshot for the
        given origin, optionally only from visits that have one of the given
        allowed_statuses

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        .. warning:: At most 1000 branches contained in the snapshot will be
            returned for performance reasons. In order to browse the whole
            set of branches, the methods :meth:`origin_visit_get_latest`
            and :meth:`snapshot_get_branches` should be used instead.

        Args:
            origin (Union[str,int]): the origin's URL or identifier
            allowed_statuses (list of str): list of visit statuses considered
                to find the latest snapshot for the origin. For instance,
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
                raise ValueError(
                    'last origin visit references an unknown snapshot')
            return snapshot

    def snapshot_count_branches(self, snapshot_id, db=None, cur=None):
        """Count the number of branches in the snapshot with the given id

        Args:
            snapshot_id (bytes): identifier of the snapshot

        Returns:
            dict: A dict whose keys are the target types of branches and
            values their corresponding amount
        """
        (snapshot, _) = self._snapshots[snapshot_id]
        return collections.Counter(branch.target_type.value if branch else None
                                   for branch in snapshot.branches.values())

    def snapshot_get_branches(self, snapshot_id, branches_from=b'',
                              branches_count=1000, target_types=None):
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
        ret = {}
        for id_ in ids:
            objs = self._objects.get(id_, [])
            ret[id_] = [{
                    'sha1_git': id_,
                    'type': obj[0],
                    'id': obj[1],
                    'object_id': id_,
                    } for obj in objs]
        return ret

    def _convert_origin(self, t):
        if t is None:
            return None
        (origin_id, origin) = t
        origin = origin.to_dict()
        if ENABLE_ORIGIN_IDS:
            origin['id'] = origin_id
        return origin

    def origin_get(self, origins):
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

                - id (int): the origin's identifier

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
        else:
            return_single = False

        # Sanity check to be error-compatible with the pgsql backend
        if any('id' in origin for origin in origins) \
                and not all('id' in origin for origin in origins):
            raise ValueError(
                'Either all origins or none at all should have an "id".')
        if any('url' in origin for origin in origins) \
                and not all('url' in origin for origin in origins):
            raise ValueError(
                'Either all origins or none at all should have '
                'an "url" key.')

        results = []
        for origin in origins:
            result = None
            if 'id' in origin:
                assert ENABLE_ORIGIN_IDS, 'origin ids are disabled'
                if origin['id'] <= len(self._origins_by_id):
                    result = self._origins[self._origins_by_id[origin['id']-1]]
            elif 'url' in origin:
                if origin['url'] in self._origins:
                    result = self._origins[origin['url']]
            else:
                raise ValueError(
                    'Origin must have either id or url.')
            results.append(self._convert_origin(result))

        if return_single:
            assert len(results) == 1
            return results[0]
        else:
            return results

    def origin_get_range(self, origin_from=1, origin_count=100):
        """Retrieve ``origin_count`` origins whose ids are greater
        or equal than ``origin_from``.

        Origins are sorted by id before retrieving them.

        Args:
            origin_from (int): the minimum id of origins to retrieve
            origin_count (int): the maximum number of origins to retrieve

        Yields:
            dicts containing origin information as returned
            by :meth:`swh.storage.in_memory.Storage.origin_get`.
        """
        origin_from = max(origin_from, 1)
        if origin_from <= len(self._origins_by_id):
            max_idx = origin_from + origin_count - 1
            if max_idx > len(self._origins_by_id):
                max_idx = len(self._origins_by_id)
            for idx in range(origin_from-1, max_idx):
                yield self._convert_origin(
                    self._origins[self._origins_by_id[idx]])

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

        Returns:
            An iterable of dict containing origin information as returned
            by :meth:`swh.storage.storage.Storage.origin_get`.
        """
        origins = map(self._convert_origin, self._origins.values())
        if regexp:
            pat = re.compile(url_pattern)
            origins = [orig for orig in origins if pat.search(orig['url'])]
        else:
            origins = [orig for orig in origins if url_pattern in orig['url']]
        if with_visit:
            origins = [orig for orig in origins
                       if len(self._origin_visits[orig['url']]) > 0]

        if ENABLE_ORIGIN_IDS:
            origins.sort(key=lambda origin: origin['id'])

        return origins[offset:offset+limit]

    def origin_count(self, url_pattern, regexp=False, with_visit=False,
                     db=None, cur=None):
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
        return len(self.origin_search(url_pattern, regexp=regexp,
                                      with_visit=with_visit,
                                      limit=len(self._origins)))

    def origin_add(self, origins):
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
            if ENABLE_ORIGIN_IDS:
                origin['id'] = self.origin_add_one(origin)
            else:
                self.origin_add_one(origin)
        return origins

    def origin_add_one(self, origin):
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
        origin = Origin.from_dict(origin)
        if origin.url in self._origins:
            if ENABLE_ORIGIN_IDS:
                (origin_id, _) = self._origins[origin.url]
        else:
            if self.journal_writer:
                self.journal_writer.write_addition('origin', origin)
            if ENABLE_ORIGIN_IDS:
                # origin ids are in the range [1, +inf[
                origin_id = len(self._origins) + 1
                self._origins_by_id.append(origin.url)
                assert len(self._origins_by_id) == origin_id
            else:
                origin_id = None
            self._origins[origin.url] = (origin_id, origin)
            self._origin_visits[origin.url] = []
            self._objects[origin.url].append(('origin', origin.url))

        if ENABLE_ORIGIN_IDS:
            return origin_id
        else:
            return origin.url

    def fetch_history_start(self, origin_id):
        """Add an entry for origin origin_id in fetch_history. Returns the id
        of the added fetch_history entry
        """
        assert not ENABLE_ORIGIN_IDS, 'origin ids are disabled'
        pass

    def fetch_history_end(self, fetch_history_id, data):
        """Close the fetch_history entry with id `fetch_history_id`, replacing
           its data with `data`.
        """
        pass

    def fetch_history_get(self, fetch_history_id):
        """Get the fetch_history entry with id `fetch_history_id`.
        """
        raise NotImplementedError('fetch_history_get is deprecated, use '
                                  'origin_visit_get instead.')

    def origin_visit_add(self, origin, date, type=None):
        """Add an origin_visit for the origin at date with status 'ongoing'.

        For backward compatibility, `type` is optional and defaults to
        the origin's type.

        Args:
            origin (Union[int,str]): visited origin's identifier or URL
            date (Union[str,datetime]): timestamp of such visit
            type (str): the type of loader used for the visit (hg, git, ...)

        Returns:
            dict: dictionary with keys origin and visit where:

            - origin: origin's identifier
            - visit: the visit's identifier for the new visit occurrence

        """
        origin_url = self._get_origin_url(origin)
        if origin_url is None:
            raise ValueError('Unknown origin.')

        if isinstance(date, str):
            # FIXME: Converge on iso8601 at some point
            date = dateutil.parser.parse(date)
        elif not isinstance(date, datetime.datetime):
            raise TypeError('date must be a datetime or a string.')

        visit_ret = None
        if origin_url in self._origins:
            (origin_id, origin) = self._origins[origin_url]
            # visit ids are in the range [1, +inf[
            visit_id = len(self._origin_visits[origin_url]) + 1
            status = 'ongoing'
            visit = OriginVisit(
                origin=origin,
                date=date,
                type=type or origin.type,
                status=status,
                snapshot=None,
                metadata=None,
                visit=visit_id,
            )
            self._origin_visits[origin_url].append(visit)
            visit_ret = {
                'origin': origin_id if ENABLE_ORIGIN_IDS else origin.url,
                'visit': visit_id,
            }

            self._objects[(origin_url, visit_id)].append(
                ('origin_visit', None))

            if self.journal_writer:
                self.journal_writer.write_addition('origin_visit', visit)

        return visit_ret

    def origin_visit_update(self, origin, visit_id, status=None,
                            metadata=None, snapshot=None):
        """Update an origin_visit's status.

        Args:
            origin (Union[int,str]): visited origin's identifier or URL
            visit_id (int): visit's identifier
            status: visit's new status
            metadata: data associated to the visit
            snapshot (sha1_git): identifier of the snapshot to add to
                the visit

        Returns:
            None

        """
        origin_url = self._get_origin_url(origin)
        if origin_url is None:
            raise ValueError('Unknown origin.')

        try:
            visit = self._origin_visits[origin_url][visit_id-1]
        except IndexError:
            raise ValueError('Unknown visit_id for this origin') \
                from None

        updates = {}
        if status:
            updates['status'] = status
        if metadata:
            updates['metadata'] = metadata
        if snapshot:
            updates['snapshot'] = snapshot

        visit = attr.evolve(visit, **updates)

        if self.journal_writer:
            (_, origin) = self._origins[origin_url]
            self.journal_writer.write_update('origin_visit', visit)

        self._origin_visits[origin_url][visit_id-1] = visit

        if origin_url not in self._origin_visits or \
                visit_id > len(self._origin_visits[origin_url]):
            return

    def origin_visit_upsert(self, visits):
        """Add a origin_visits with a specific id and with all its data.
        If there is already an origin_visit with the same
        `(origin_url, visit_id)`, updates it instead of inserting a new one.

        Args:
            visits: iterable of dicts with keys:

                origin: dict with keys either `id` or `url`
                visit: origin visit id
                type: type of loader used for the visit
                date: timestamp of such visit
                status: Visit's new status
                metadata: Data associated to the visit
                snapshot (sha1_git): identifier of the snapshot to add to
                    the visit
        """
        visits = [OriginVisit.from_dict(d) for d in visits]

        if self.journal_writer:
            for visit in visits:
                (_, visit.origin) = self._origins[visit.origin.url]
                self.journal_writer.write_addition('origin_visit', visit)

        for visit in visits:
            visit_id = visit.visit
            origin_url = visit.origin.url

            self._objects[(origin_url, visit_id)].append(
                ('origin_visit', None))

            while len(self._origin_visits[origin_url]) <= visit_id:
                self._origin_visits[origin_url].append(None)

            self._origin_visits[origin_url][visit_id-1] = visit

    def _convert_visit(self, visit):
        if visit is None:
            return

        (origin_id, origin) = self._origins[visit.origin.url]
        visit = visit.to_dict()
        if ENABLE_ORIGIN_IDS:
            visit['origin'] = origin_id
        else:
            visit['origin'] = origin.url

        return visit

    def origin_visit_get(self, origin, last_visit=None, limit=None):
        """Retrieve all the origin's visit's information.

        Args:
            origin (int): the origin's identifier
            last_visit (int): visit's id from which listing the next ones,
                default to None
            limit (int): maximum number of results to return,
                default to None

        Yields:
            List of visits.

        """
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
        """Retrieves the origin visit whose date is closest to the provided
        timestamp.
        In case of a tie, the visit with largest id is selected.

        Args:
            origin (str): The occurrence's origin (URL).
            target (datetime): target timestamp

        Returns:
            A visit.

        """
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits:
            visits = self._origin_visits[origin_url]
            visit = min(
                visits,
                key=lambda v: (abs(v.date - visit_date), -v.visit))
            return self._convert_visit(visit)

    def origin_visit_get_by(self, origin, visit):
        """Retrieve origin visit's information.

        Args:
            origin (int): the origin's identifier

        Returns:
            The information on that particular (origin, visit) or None if
            it does not exist

        """
        origin_url = self._get_origin_url(origin)
        if origin_url in self._origin_visits and \
           visit <= len(self._origin_visits[origin_url]):
            return self._convert_visit(
                self._origin_visits[origin_url][visit-1])

    def origin_visit_get_latest(
            self, origin, allowed_statuses=None, require_snapshot=False):
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
        res = self._origins.get(origin)
        if not res:
            return
        (_, origin) = res
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

    def stat_counters(self):
        """compute statistics about the number of tuples in various tables

        Returns:
            dict: a dictionary mapping textual labels (e.g., content) to
            integer values (e.g., the number of tuples in table content)

        """
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
        """Recomputes the statistics for `stat_counters`."""
        pass

    def origin_metadata_add(self, origin_id, ts, provider, tool, metadata,
                            db=None, cur=None):
        """ Add an origin_metadata for the origin at ts with provenance and
        metadata.

        Args:
            origin_id (int): the origin's id for which the metadata is added
            ts (datetime): timestamp of the found metadata
            provider: id of the provider of metadata (ex:'hal')
            tool: id of the tool used to extract metadata
            metadata (jsonb): the metadata retrieved at the time and location
        """
        if isinstance(origin_id, str):
            origin = self.origin_get({'url': origin_id})
            if not origin:
                return
            origin_id = origin['id']

        if isinstance(ts, str):
            ts = dateutil.parser.parse(ts)

        origin_metadata = {
                'origin_id': origin_id,
                'discovery_date': ts,
                'tool_id': tool,
                'metadata': metadata,
                'provider_id': provider,
                }
        self._origin_metadata[origin_id].append(origin_metadata)
        return None

    def origin_metadata_get_by(self, origin_id, provider_type=None, db=None,
                               cur=None):
        """Retrieve list of all origin_metadata entries for the origin_id

        Args:
            origin_id (int): the unique origin's identifier
            provider_type (str): (optional) type of provider

        Returns:
            list of dicts: the origin_metadata dictionary with the keys:

            - origin_id (int): origin's identifier
            - discovery_date (datetime): timestamp of discovery
            - tool_id (int): metadata's extracting tool
            - metadata (jsonb)
            - provider_id (int): metadata's provider
            - provider_name (str)
            - provider_type (str)
            - provider_url (str)

        """
        if isinstance(origin_id, str):
            origin = self.origin_get({'url': origin_id})
            if not origin:
                return
            origin_id = origin['id']

        metadata = []
        for item in self._origin_metadata[origin_id]:
            item = copy.deepcopy(item)
            provider = self.metadata_provider_get(item['provider_id'])
            for attr_name in ('name', 'type', 'url'):
                item['provider_' + attr_name] = \
                    provider['provider_' + attr_name]
            metadata.append(item)
        return metadata

    def tool_add(self, tools):
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
        """Retrieve tool information.

        Args:
            tool (dict): Tool information we want to retrieve from storage.
              The dicts have the same keys as those used in :func:`tool_add`.

        Returns:
            dict: The full tool information if it exists (``id`` included),
            None otherwise.

        """
        return self._tools.get(self._tool_key(tool))

    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata):
        """Add a metadata provider.

        Args:
            provider_name (str): Its name
            provider_type (str): Its type
            provider_url (str): Its URL
            metadata: JSON-encodable object

        Returns:
            an identifier of the provider
        """
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

    def metadata_provider_get(self, provider_id, db=None, cur=None):
        """Get a metadata provider

        Args:
            provider_id: Its identifier, as given by `metadata_provider_add`.

        Returns:
            dict: same as `metadata_provider_add`;
                  or None if it does not exist.
        """
        return self._metadata_providers.get(provider_id)

    def metadata_provider_get_by(self, provider, db=None, cur=None):
        """Get a metadata provider

        Args:
            provider_name: Its name
            provider_url: Its URL

        Returns:
            dict: same as `metadata_provider_add`;
                  or None if it does not exist.
        """
        key = self._metadata_provider_key(provider)
        return self._metadata_providers.get(key)

    def _get_origin_url(self, origin):
        if isinstance(origin, str):
            return origin
        elif isinstance(origin, int):
            if origin <= len(self._origins_by_id):
                return self._origins_by_id[origin-1]
            else:
                return None
        else:
            raise TypeError('origin must be a string or an integer.')

    def _person_add(self, person):
        """Add a person in storage.

        Note: Private method, do not use outside of this class.

        Args:
            person: dictionary with keys fullname, name and email.

        """
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
    def _content_key(content):
        """A stable key for a content"""
        return tuple(getattr(content, key)
                     for key in sorted(DEFAULT_ALGORITHMS))

    @staticmethod
    def _content_key_algorithm(content):
        """ A stable key and the algorithm for a content"""
        if isinstance(content, Content):
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
