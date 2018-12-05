# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
import bisect
import dateutil
import collections
from collections import defaultdict
import copy
import datetime
import itertools
import random
import warnings

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.identifiers import normalize_timestamp

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class Storage:
    def __init__(self):
        self._contents = {}
        self._contents_data = {}
        self._content_indexes = defaultdict(lambda: defaultdict(set))

        self._directories = {}
        self._revisions = {}
        self._releases = {}
        self._snapshots = {}
        self._origins = []
        self._origin_visits = []
        self._origin_metadata = defaultdict(list)
        self._tools = {}
        self._metadata_providers = {}
        self._objects = defaultdict(list)

        # ideally we would want a skip list for both fast inserts and searches
        self._sorted_sha1s = []

    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""
        return True

    def content_add(self, contents):
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

        """
        for content in contents:
            key = self._content_key(content)
            if key in self._contents:
                continue
            for algorithm in DEFAULT_ALGORITHMS:
                if content[algorithm] in self._content_indexes[algorithm]:
                    from . import HashCollision
                    raise HashCollision(algorithm, content[algorithm], key)
            for algorithm in DEFAULT_ALGORITHMS:
                self._content_indexes[algorithm][content[algorithm]].add(key)
            self._objects[content['sha1_git']].append(
                ('content', content['sha1']))
            self._contents[key] = copy.deepcopy(content)
            self._contents[key]['ctime'] = now()
            bisect.insort(self._sorted_sha1s, content['sha1'])
            if self._contents[key]['status'] == 'visible':
                self._contents_data[key] = self._contents[key].pop('data')

    def content_get(self, ids):
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
        if len(ids) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise ValueError(
                "Sending at most %s contents." % BULK_BLOCK_CONTENT_LEN_MAX)
        for id_ in ids:
            for key in self._content_indexes['sha1'][id_]:
                yield {
                    'sha1': id_,
                    'data': self._contents_data[key],
                }

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
        for sha1, key in sha1s:
            if sha1 > end:
                break
            if len(matched) >= limit:
                return {
                    'contents': matched,
                    'next': sha1,
                }
            matched.append({
                'data': self._contents_data[key],
                **self._contents[key],
                })
        return {
            'contents': matched,
            'next': None,
            }

    def content_get_metadata(self, sha1s):
        """Retrieve content metadata in bulk

        Args:
            content: iterable of content identifiers (sha1)

        Returns:
            an iterable with content metadata corresponding to the given ids
        """
        # FIXME: the return value should be a mapping from search key to found
        # content*s*
        for sha1 in sha1s:
            if sha1 in self._content_indexes['sha1']:
                objs = self._content_indexes['sha1'][sha1]
                # FIXME: rather than selecting one of the objects with that
                # hash, we should return all of them. See:
                # https://forge.softwareheritage.org/D645?id=1994#inline-3389
                key = random.sample(objs, 1)[0]
                data = copy.deepcopy(self._contents[key])
                data.pop('ctime')
                yield data
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
            return
        keys = list(set.intersection(*found))

        # FIXME: should really be a list of all the objects found
        return copy.deepcopy(self._contents[keys[0]])

    def content_missing(self, contents, key_hash='sha1'):
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
        for content in contents:
            for (algo, hash_) in content.items():
                if algo not in DEFAULT_ALGORITHMS:
                    continue
                if hash_ not in self._content_indexes.get(algo, []):
                    yield content[key_hash]
                    break
            else:
                # content_find cannot return None here, because we checked
                # above that there is a content with matching hashes.
                if self.content_find(content)['status'] == 'missing':
                    yield content[key_hash]

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
        for directory in directories:
            if directory['id'] not in self._directories:
                self._directories[directory['id']] = copy.deepcopy(directory)
                self._objects[directory['id']].append(
                    ('directory', directory['id']))

    def directory_missing(self, directory_ids):
        """List directories missing from storage

        Args:
            directories (iterable): an iterable of directory ids

        Yields:
            missing directory ids

        """
        for id in directory_ids:
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
            content = self.content_find({'sha1_git': ret['target']})
            if content:
                for key in keys:
                    ret[key] = content[key]
        return ret

    def _directory_ls(self, directory_id, recursive, prefix=b''):
        if directory_id in self._directories:
            for entry in self._directories[directory_id]['entries']:
                ret = self._join_dentry_to_content(entry)
                ret['name'] = prefix + ret['name']
                ret['dir_id'] = directory_id
                yield ret
                if recursive and ret['type'] == 'dir':
                    yield from self._directory_ls(
                        ret['target'], True, prefix + ret['name'] + b'/')

    def directory_ls(self, directory_id, recursive=False):
        """Get entries for one directory.

        Args:
            - directory: the directory to list entries from.
            - recursive: if flag on, this list recursively from this directory.

        Returns:
            List of entries for such directory.

        If `recursive=True`, names in the path of a dir/file not at the
        root are concatenated with a slash (`/`).
        """
        yield from self._directory_ls(directory_id, recursive)

    def directory_entry_get_by_path(self, directory, paths):
        """Get the directory entry (either file or dir) from directory with path.

        Args:
            - directory: sha1 of the top level directory
            - paths: path to lookup from the top level directory. From left
              (top) to right (bottom).

        Returns:
            The corresponding directory entry if found, None otherwise.

        """
        if not paths:
            return

        contents = list(self.directory_ls(directory))

        if not contents:
            return

        def _get_entry(entries, name):
            for entry in entries:
                if entry['name'] == name:
                    return entry

        first_item = _get_entry(contents, paths[0])

        if len(paths) == 1:
            return first_item

        if not first_item or first_item['type'] != 'dir':
            return

        return self.directory_entry_get_by_path(
                first_item['target'], paths[1:])

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
        """
        for revision in revisions:
            if revision['id'] not in self._revisions:
                self._revisions[revision['id']] = rev = copy.deepcopy(revision)
                rev['date'] = normalize_timestamp(rev.get('date'))
                rev['committer_date'] = normalize_timestamp(
                        rev.get('committer_date'))
                self._objects[revision['id']].append(
                    ('revision', revision['id']))

    def revision_missing(self, revision_ids):
        """List revisions missing from storage

        Args:
            revisions (iterable): revision ids

        Yields:
            missing revision ids

        """
        for id in revision_ids:
            if id not in self._revisions:
                yield id

    def revision_get(self, revision_ids):
        for id in revision_ids:
            yield copy.deepcopy(self._revisions.get(id))

    def _get_parent_revs(self, rev_id, seen, limit):
        if limit and len(seen) >= limit:
            return
        if rev_id in seen:
            return
        seen.add(rev_id)
        yield self._revisions[rev_id]
        for parent in self._revisions[rev_id]['parents']:
            yield from self._get_parent_revs(parent, seen, limit)

    def revision_log(self, revision_ids, limit=None):
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revision to lookup
            limit: limitation on the output result. Default to None.

        Yields:
            List of revision log from such revisions root.

        """
        seen = set()
        for rev_id in revision_ids:
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
        """
        for rel in releases:
            rel['date'] = normalize_timestamp(rel['date'])
            self._objects[rel['id']].append(
                ('release', rel['id']))
        self._releases.update((rel['id'], rel) for rel in releases)

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
            yield copy.deepcopy(self._releases.get(rel_id))

    def snapshot_add(self, origin, visit, snapshot):
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

        Raises:
            ValueError: if the origin's or visit's identifier does not exist.
        """
        snapshot_id = snapshot['id']
        if snapshot_id not in self._snapshots:
            self._snapshots[snapshot_id] = {
                'origin': origin,
                'visit': visit,
                'id': snapshot_id,
                'branches': copy.deepcopy(snapshot['branches']),
                '_sorted_branch_names': sorted(snapshot['branches'])
                }
            self._objects[snapshot_id].append(('snapshot', snapshot_id))
        if origin <= len(self._origin_visits) and \
           visit <= len(self._origin_visits[origin-1]):
            self._origin_visits[origin-1][visit-1]['snapshot'] = snapshot_id
        else:
            raise ValueError('Origin with id %s does not exist or has no visit'
                             ' with id %s' % (origin, visit))

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
        if origin > len(self._origins) or \
           visit > len(self._origin_visits[origin-1]):
            return None
        snapshot_id = self._origin_visits[origin-1][visit-1]['snapshot']
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
            set of branches, the method :meth:`snapshot_get_branches`
            should be used instead.

        Args:
            origin (int): the origin's identifier
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
        visits = self._origin_visits[origin-1]
        if allowed_statuses is not None:
            visits = [visit for visit in visits
                      if visit['status'] in allowed_statuses]
        snapshot = None
        for visit in sorted(visits, key=lambda v: (v['date'], v['visit']),
                            reverse=True):
            snapshot_id = visit['snapshot']
            snapshot = self.snapshot_get(snapshot_id)
            if snapshot:
                break

        return snapshot

    def snapshot_count_branches(self, snapshot_id, db=None, cur=None):
        """Count the number of branches in the snapshot with the given id

        Args:
            snapshot_id (bytes): identifier of the snapshot

        Returns:
            dict: A dict whose keys are the target types of branches and
            values their corresponding amount
        """
        branches = list(self._snapshots[snapshot_id]['branches'].values())
        return collections.Counter(branch['target_type'] if branch else None
                                   for branch in branches)

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
        snapshot = self._snapshots.get(snapshot_id)
        if snapshot is None:
            return None
        sorted_branch_names = snapshot['_sorted_branch_names']
        from_index = bisect.bisect_left(
                sorted_branch_names, branches_from)
        if target_types:
            next_branch = None
            branches = {}
            for branch_name in sorted_branch_names[from_index:]:
                branch = snapshot['branches'][branch_name]
                if branch and branch['target_type'] in target_types:
                    if len(branches) < branches_count:
                        branches[branch_name] = branch
                    else:
                        next_branch = branch_name
                        break
        else:
            # As there is no 'target_types', we can do that much faster
            to_index = from_index + branches_count
            returned_branch_names = sorted_branch_names[from_index:to_index]
            branches = {branch_name: snapshot['branches'][branch_name]
                        for branch_name in returned_branch_names}
            if to_index >= len(sorted_branch_names):
                next_branch = None
            else:
                next_branch = sorted_branch_names[to_index]
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

    def origin_get(self, origin):
        """Return the origin either identified by its id or its tuple
        (type, url).

        Args:
            origin: dictionary representing the individual origin to find.
                This dict has either the keys type and url:

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
        if 'id' in origin:
            origin_id = origin['id']
        elif 'type' in origin and 'url' in origin:
            origin_id = self._origin_id(origin)
        else:
            raise ValueError('Origin must have either id or (type and url).')
        origin = None
        # self._origin_id can return None
        if origin_id is not None:
            origin = copy.deepcopy(self._origins[origin_id-1])
            origin['id'] = origin_id
        return origin

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
        origins = self._origins
        if regexp:
            pat = re.compile(url_pattern)
            origins = [orig for orig in origins if pat.match(orig['url'])]
        else:
            origins = [orig for orig in origins if url_pattern in orig['url']]
        if with_visit:
            origins = [orig for orig in origins
                       if len(self._origin_visits[orig['id']-1]) > 0]
        origins = copy.deepcopy(origins[offset:offset+limit])
        return origins

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
            origin['id'] = self.origin_add_one(origin)
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
        origin = copy.deepcopy(origin)
        assert 'id' not in origin
        origin_id = self._origin_id(origin)
        if origin_id is None:
            # origin ids are in the range [1, +inf[
            origin_id = len(self._origins) + 1
            origin['id'] = origin_id
            self._origins.append(origin)
            self._origin_visits.append([])
            key = (origin['type'], origin['url'])
            self._objects[key].append(('origin', origin_id))
        return origin_id

    def fetch_history_start(self, origin_id):
        """Add an entry for origin origin_id in fetch_history. Returns the id
        of the added fetch_history entry
        """
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

    def origin_visit_add(self, origin, date=None, *, ts=None):
        """Add an origin_visit for the origin at date with status 'ongoing'.

        Args:
            origin (int): visited origin's identifier
            date: timestamp of such visit

        Returns:
            dict: dictionary with keys origin and visit where:

            - origin: origin's identifier
            - visit: the visit's identifier for the new visit occurrence

        """
        if ts is None:
            if date is None:
                raise TypeError('origin_visit_add expected 2 arguments.')
        else:
            assert date is None
            warnings.warn("argument 'ts' of origin_visit_add was renamed "
                          "to 'date' in v0.0.109.",
                          DeprecationWarning)
            date = ts

        if isinstance(date, str):
            date = dateutil.parser.parse(date)

        visit_ret = None
        if origin <= len(self._origin_visits):
            # visit ids are in the range [1, +inf[
            visit_id = len(self._origin_visits[origin-1]) + 1
            status = 'ongoing'
            visit = {
                'origin': origin,
                'date': date,
                'status': status,
                'snapshot': None,
                'metadata': None,
                'visit': visit_id
            }
            self._origin_visits[origin-1].append(copy.deepcopy(visit))
            visit_ret = {
                'origin': origin,
                'visit': visit_id,
            }

        return visit_ret

    def origin_visit_update(self, origin, visit_id, status, metadata=None):
        """Update an origin_visit's status.

        Args:
            origin (int): visited origin's identifier
            visit_id (int): visit's identifier
            status: visit's new status
            metadata: data associated to the visit

        Returns:
            None

        """
        if origin > len(self._origin_visits) or \
           visit_id > len(self._origin_visits[origin-1]):
            return
        self._origin_visits[origin-1][visit_id-1].update({
            'status': status,
            'metadata': metadata})

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
        visits = self._origin_visits[origin-1]
        if last_visit is not None:
            visits = visits[last_visit:]
        if limit is not None:
            visits = visits[:limit]
        for visit in visits:
            visit_id = visit['visit']
            yield self._origin_visits[origin-1][visit_id-1]

    def origin_visit_get_by(self, origin, visit):
        """Retrieve origin visit's information.

        Args:
            origin (int): the origin's identifier

        Returns:
            The information on that particular (origin, visit) or None if
            it does not exist

        """
        origin_visit = None
        if origin <= len(self._origin_visits) and \
           visit <= len(self._origin_visits[origin-1]):
            origin_visit = self._origin_visits[origin-1][visit-1]
        return origin_visit

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
        metadata = []
        for item in self._origin_metadata[origin_id]:
            item = copy.deepcopy(item)
            provider = self.metadata_provider_get(item['provider_id'])
            for attr in ('name', 'type', 'url'):
                item['provider_' + attr] = provider[attr]
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

        Yields:
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

        yield from inserted

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
                'name': provider_name,
                'type': provider_type,
                'url': provider_url,
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
        key = self._metadata_provider_key({
            'name': provider['provider_name'],
            'url': provider['provider_url']})
        return self._metadata_providers.get(key)

    def _origin_id(self, origin):
        origin_id = None
        for stored_origin in self._origins:
            if stored_origin['type'] == origin['type'] and \
               stored_origin['url'] == origin['url']:
                origin_id = stored_origin['id']
                break
        return origin_id

    @staticmethod
    def _content_key(content):
        """A stable key for a content"""
        return tuple(content.get(key) for key in sorted(DEFAULT_ALGORITHMS))

    @staticmethod
    def _tool_key(tool):
        return (tool['name'], tool['version'],
                tuple(sorted(tool['configuration'].items())))

    @staticmethod
    def _metadata_provider_key(provider):
        return (provider['name'], provider['url'])
