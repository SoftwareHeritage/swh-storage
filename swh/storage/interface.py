# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, List, Optional, Union

from swh.core.api import remote_api_endpoint
from swh.model.model import (
    SkippedContent, Content, Directory, Revision, Release,
    Snapshot, Origin
)


def deprecated(f):
    f.deprecated_endpoint = True
    return f


class StorageInterface:
    @remote_api_endpoint('check_config')
    def check_config(self, *, check_write):
        """Check that the storage is configured and ready to go."""
        ...

    @remote_api_endpoint('content/add')
    def content_add(self, content: Iterable[Content]) -> Dict:
        """Add content blobs to the storage

        Args:
            contents (iterable): iterable of dictionaries representing
                individual pieces of content to add. Each dictionary has the
                following keys:

                - data (bytes): the actual content
                - length (int): content length
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.ALGORITHMS`, mapped to the
                  corresponding checksum
                - status (str): one of visible, hidden

        Raises:

            The following exceptions can occur:

            - HashCollision in case of collision
            - Any other exceptions raise by the db

            In case of errors, some of the content may have been stored in
            the DB and in the objstorage.
            Since additions to both idempotent, that should not be a problem.

        Returns:
            Summary dict with the following keys and associated values:

                content:add: New contents added
                content:add:bytes: Sum of the contents' length data
        """
        ...

    @remote_api_endpoint('content/update')
    def content_update(self, content, keys=[]):
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
        ...

    @remote_api_endpoint('content/add_metadata')
    def content_add_metadata(self, content: Iterable[Content]) -> Dict:
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
        ...

    @remote_api_endpoint('content/data')
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
        ...

    @deprecated
    @remote_api_endpoint('content/range')
    def content_get_range(self, start, end, limit=1000):
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
        ...

    @remote_api_endpoint('content/partition')
    def content_get_partition(
            self, partition_id: int, nb_partitions: int, limit: int = 1000,
            page_token: str = None):
        """Splits contents into nb_partitions, and returns one of these based on
        partition_id (which must be in [0, nb_partitions-1])

        There is no guarantee on how the partitioning is done, or the
        result order.

        Args:
            partition_id (int): index of the partition to fetch
            nb_partitions (int): total number of partitions to split into
            limit (int): Limit result (default to 1000)
            page_token (Optional[str]): opaque token used for pagination.

        Returns:
            a dict with keys:
              - contents (List[dict]): iterable of contents in the partition.
              - **next_page_token** (Optional[str]): opaque token to be used as
                `page_token` for retrieving the next page. if absent, there is
                no more pages to gather.
        """
        ...

    @remote_api_endpoint('content/metadata')
    def content_get_metadata(
            self, contents: List[bytes]) -> Dict[bytes, List[Dict]]:
        """Retrieve content metadata in bulk

        Args:
            content: iterable of content identifiers (sha1)

        Returns:
            a dict with keys the content's sha1 and the associated value
            either the existing content's metadata or None if the content does
            not exist.

        """
        ...

    @remote_api_endpoint('content/missing')
    def content_missing(self, content, key_hash='sha1'):
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
        ...

    @remote_api_endpoint('content/missing/sha1')
    def content_missing_per_sha1(self, contents):
        """List content missing from storage based only on sha1.

        Args:
            contents: Iterable of sha1 to check for absence.

        Returns:
            iterable: missing ids

        Raises:
            TODO: an exception when we get a hash collision.

        """
        ...

    @remote_api_endpoint('content/missing/sha1_git')
    def content_missing_per_sha1_git(self, contents):
        """List content missing from storage based only on sha1_git.

        Args:
            contents (Iterable): An iterable of content id (sha1_git)

        Yields:
            missing contents sha1_git
        """
        ...

    @remote_api_endpoint('content/present')
    def content_find(self, content):
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
        ...

    @remote_api_endpoint('content/get_random')
    def content_get_random(self):
        """Finds a random content id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint('content/skipped/add')
    def skipped_content_add(self, content: Iterable[SkippedContent]) -> Dict:
        """Add contents to the skipped_content list, which contains
        (partial) information about content missing from the archive.

        Args:
            contents (iterable): iterable of dictionaries representing
                individual pieces of content to add. Each dictionary has the
                following keys:

                - length (Optional[int]): content length (default: -1)
                - one key for each checksum algorithm in
                  :data:`swh.model.hashutil.ALGORITHMS`, mapped to the
                  corresponding checksum; each is optional
                - status (str): must be "absent"
                - reason (str): the reason why the content is absent
                - origin (int): if status = absent, the origin we saw the
                  content in

        Raises:

            The following exceptions can occur:

            - HashCollision in case of collision
            - Any other exceptions raise by the backend

            In case of errors, some content may have been stored in
            the DB and in the objstorage.
            Since additions to both idempotent, that should not be a problem.

        Returns:
            Summary dict with the following key and associated values:

                skipped_content:add: New skipped contents (no data) added
        """
        ...

    @remote_api_endpoint('content/skipped/missing')
    def skipped_content_missing(self, contents):
        """List skipped_content missing from storage

        Args:
            content: iterable of dictionaries containing the data for each
                checksum algorithm.

        Returns:
            iterable: missing signatures

        """
        ...

    @remote_api_endpoint('directory/add')
    def directory_add(self, directories: Iterable[Directory]) -> Dict:
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
        ...

    @remote_api_endpoint('directory/missing')
    def directory_missing(self, directories):
        """List directories missing from storage

        Args:
            directories (iterable): an iterable of directory ids

        Yields:
            missing directory ids

        """
        ...

    @remote_api_endpoint('directory/ls')
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
        ...

    @remote_api_endpoint('directory/path')
    def directory_entry_get_by_path(self, directory, paths):
        """Get the directory entry (either file or dir) from directory with path.

        Args:
            - directory: sha1 of the top level directory
            - paths: path to lookup from the top level directory. From left
              (top) to right (bottom).

        Returns:
            The corresponding directory entry if found, None otherwise.

        """
        ...

    @remote_api_endpoint('directory/get_random')
    def directory_get_random(self):
        """Finds a random directory id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint('revision/add')
    def revision_add(self, revisions: Iterable[Revision]) -> Dict:
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
        ...

    @remote_api_endpoint('revision/missing')
    def revision_missing(self, revisions):
        """List revisions missing from storage

        Args:
            revisions (iterable): revision ids

        Yields:
            missing revision ids

        """
        ...

    @remote_api_endpoint('revision')
    def revision_get(self, revisions):
        """Get all revisions from storage

        Args:
            revisions: an iterable of revision ids

        Returns:
            iterable: an iterable of revisions as dictionaries (or None if the
                revision doesn't exist)

        """
        ...

    @remote_api_endpoint('revision/log')
    def revision_log(self, revisions, limit=None):
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revision to lookup
            limit: limitation on the output result. Default to None.

        Yields:
            List of revision log from such revisions root.

        """
        ...

    @remote_api_endpoint('revision/shortlog')
    def revision_shortlog(self, revisions, limit=None):
        """Fetch the shortlog for the given revisions

        Args:
            revisions: list of root revisions to lookup
            limit: depth limitation for the output

        Yields:
            a list of (id, parents) tuples.

        """
        ...

    @remote_api_endpoint('revision/get_random')
    def revision_get_random(self):
        """Finds a random revision id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint('release/add')
    def release_add(self, releases: Iterable[Release]) -> Dict:
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
        ...

    @remote_api_endpoint('release/missing')
    def release_missing(self, releases):
        """List releases missing from storage

        Args:
            releases: an iterable of release ids

        Returns:
            a list of missing release ids

        """
        ...

    @remote_api_endpoint('release')
    def release_get(self, releases):
        """Given a list of sha1, return the releases's information

        Args:
            releases: list of sha1s

        Yields:
            dicts with the same keys as those given to `release_add`
            (or ``None`` if a release does not exist)

        """
        ...

    @remote_api_endpoint('release/get_random')
    def release_get_random(self):
        """Finds a random release id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint('snapshot/add')
    def snapshot_add(self, snapshots: Iterable[Snapshot]) -> Dict:
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
        ...

    @remote_api_endpoint('snapshot/missing')
    def snapshot_missing(self, snapshots):
        """List snapshots missing from storage

        Args:
            snapshots (iterable): an iterable of snapshot ids

        Yields:
            missing snapshot ids

        """
        ...

    @remote_api_endpoint('snapshot')
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
        ...

    @remote_api_endpoint('snapshot/by_origin_visit')
    def snapshot_get_by_origin_visit(self, origin, visit):
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
        ...

    @remote_api_endpoint('snapshot/latest')
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
            origin (str): the origin's URL
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
        ...

    @remote_api_endpoint('snapshot/count_branches')
    def snapshot_count_branches(self, snapshot_id):
        """Count the number of branches in the snapshot with the given id

        Args:
            snapshot_id (bytes): identifier of the snapshot

        Returns:
            dict: A dict whose keys are the target types of branches and
            values their corresponding amount
        """
        ...

    @remote_api_endpoint('snapshot/get_branches')
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
        ...

    @remote_api_endpoint('snapshot/get_random')
    def snapshot_get_random(self):
        """Finds a random snapshot id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint('origin/visit/add')
    def origin_visit_add(
            self, origin, date, type) -> Optional[Dict[str, Union[str, int]]]:
        """Add an origin_visit for the origin at ts with status 'ongoing'.

        Args:
            origin (str): visited origin's identifier or URL
            date (Union[str,datetime]): timestamp of such visit
            type (str): the type of loader used for the visit (hg, git, ...)

        Returns:
            dict: dictionary with keys origin and visit where:

            - origin: origin identifier
            - visit: the visit identifier for the new visit occurrence

        """
        ...

    @remote_api_endpoint('origin/visit/update')
    def origin_visit_update(
            self, origin: str, visit_id: int, status: Optional[str] = None,
            metadata: Optional[Dict] = None, snapshot: Optional[bytes] = None):
        """Update an origin_visit's status.

        Args:
            origin (str): visited origin's URL
            visit_id: Visit's id
            status: Visit's new status
            metadata: Data associated to the visit
            snapshot (sha1_git): identifier of the snapshot to add to
                the visit

        Returns:
            None

        """
        ...

    @remote_api_endpoint('origin/visit/upsert')
    def origin_visit_upsert(self, visits):
        """Add a origin_visits with a specific id and with all its data.
        If there is already an origin_visit with the same
        `(origin_id, visit_id)`, overwrites it.

        Args:
            visits: iterable of dicts with keys:

                - **origin**: dict with keys either `id` or `url`
                - **visit**: origin visit id
                - **date**: timestamp of such visit
                - **status**: Visit's new status
                - **metadata**: Data associated to the visit
                - **snapshot**: identifier of the snapshot to add to
                    the visit
        """
        ...

    @remote_api_endpoint('origin/visit/get')
    def origin_visit_get(self, origin, last_visit=None, limit=None):
        """Retrieve all the origin's visit's information.

        Args:
            origin (str): The visited origin
            last_visit: Starting point from which listing the next visits
                Default to None
            limit (int): Number of results to return from the last visit.
                Default to None

        Yields:
            List of visits.

        """
        ...

    @remote_api_endpoint('origin/visit/find_by_date')
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
        ...

    @remote_api_endpoint('origin/visit/getby')
    def origin_visit_get_by(self, origin, visit):
        """Retrieve origin visit's information.

        Args:
            origin: The occurrence's origin (identifier).

        Returns:
            The information on that particular (origin, visit) or None if
            it does not exist

        """
        ...

    @remote_api_endpoint('origin/visit/get_latest')
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

                - **origin**: the URL of the origin
                - **visit**: origin visit id
                - **type**: type of loader used for the visit
                - **date**: timestamp of such visit
                - **status**: Visit's new status
                - **metadata**: Data associated to the visit
                - **snapshot** (Optional[sha1_git]): identifier of the snapshot
                    associated to the visit
        """
        ...

    @remote_api_endpoint('origin/visit/get_random')
    def origin_visit_get_random(
            self, type: str) -> Optional[Dict[str, Any]]:
        """Randomly select one successful origin visit with <type>
        made in the last 3 months.

        Returns:
            dict representing an origin visit, in the same format as
            :py:meth:`origin_visit_get`.

        """
        ...

    @remote_api_endpoint('object/find_by_sha1_git')
    def object_find_by_sha1_git(self, ids):
        """Return the objects found with the given ids.

        Args:
            ids: a generator of sha1_gits

        Returns:
            dict: a mapping from id to the list of objects found. Each object
            found is itself a dict with keys:

            - sha1_git: the input id
            - type: the type of object found

        """
        ...

    @remote_api_endpoint('origin/get')
    def origin_get(self, origins):
        """Return origins, either all identified by their ids or all
        identified by tuples (type, url).

        If the url is given and the type is omitted, one of the origins with
        that url is returned.

        Args:
            origin: a list of dictionaries representing the individual
                origins to find.
                These dicts have the key url:

                - url (bytes): the url the origin points to

        Returns:
            dict: the origin dictionary with the keys:

            - id: origin's id
            - url: origin's url

        Raises:
            ValueError: if the url or the id don't exist.

        """
        ...

    @remote_api_endpoint('origin/get_sha1')
    def origin_get_by_sha1(self, sha1s):
        """Return origins, identified by the sha1 of their URLs.

        Args:
            sha1s (list[bytes]): a list of sha1s

        Yields:
            dicts containing origin information as returned
            by :meth:`swh.storage.storage.Storage.origin_get`, or None if an
            origin matching the sha1 is not found.

        """
        ...

    @deprecated
    @remote_api_endpoint('origin/get_range')
    def origin_get_range(self, origin_from=1, origin_count=100):
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
        ...

    @remote_api_endpoint('origin/list')
    def origin_list(
            self, page_token: Optional[str] = None, limit: int = 100) -> dict:
        """Returns the list of origins

        Args:
            page_token: opaque token used for pagination.
            limit: the maximum number of results to return

        Returns:
            dict: dict with the following keys:
              - **next_page_token** (str, optional): opaque token to be used as
                `page_token` for retrieving the next page. if absent, there is
                no more pages to gather.
              - **origins** (List[dict]): list of origins, as returned by
                `origin_get`.
        """
        ...

    @remote_api_endpoint('origin/search')
    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, with_visit=False):
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
        ...

    @deprecated
    @remote_api_endpoint('origin/count')
    def origin_count(self, url_pattern, regexp=False,
                     with_visit=False):
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
        ...

    @remote_api_endpoint('origin/add_multi')
    def origin_add(self, origins: Iterable[Origin]) -> List[Dict]:
        """Add origins to the storage

        Args:
            origins: list of dictionaries representing the individual origins,
                with the following keys:

                - type: the origin type ('git', 'svn', 'deb', ...)
                - url (bytes): the url the origin points to

        Returns:
            list: given origins as dict updated with their id

        """
        ...

    @remote_api_endpoint('origin/add')
    def origin_add_one(self, origin: Origin) -> str:
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
        ...

    def stat_counters(self):
        """compute statistics about the number of tuples in various tables

        Returns:
            dict: a dictionary mapping textual labels (e.g., content) to
            integer values (e.g., the number of tuples in table content)

        """
        ...

    def refresh_stat_counters(self):
        """Recomputes the statistics for `stat_counters`."""
        ...

    @remote_api_endpoint('origin/metadata/add')
    def origin_metadata_add(self, origin_url, ts, provider, tool, metadata):
        """ Add an origin_metadata for the origin at ts with provenance and
        metadata.

        Args:
            origin_url (str): the origin url for which the metadata is added
            ts (datetime): timestamp of the found metadata
            provider (int): the provider of metadata (ex:'hal')
            tool (int): tool used to extract metadata
            metadata (jsonb): the metadata retrieved at the time and location
        """
        ...

    @remote_api_endpoint('origin/metadata/get')
    def origin_metadata_get_by(self, origin_url, provider_type=None):
        """Retrieve list of all origin_metadata entries for the origin_id

        Args:
            origin_url (str): the origin's URL
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
        ...

    @remote_api_endpoint('tool/add')
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
        ...

    @remote_api_endpoint('tool/data')
    def tool_get(self, tool):
        """Retrieve tool information.

        Args:
            tool (dict): Tool information we want to retrieve from storage.
              The dicts have the same keys as those used in :func:`tool_add`.

        Returns:
            dict: The full tool information if it exists (``id`` included),
            None otherwise.

        """
        ...

    @remote_api_endpoint('provider/add')
    def metadata_provider_add(self, provider_name, provider_type, provider_url,
                              metadata):
        """Add a metadata provider.

        Args:
            provider_name (str): Its name
            provider_type (str): Its type (eg. `'deposit-client'`)
            provider_url (str): Its URL
            metadata: JSON-encodable object

        Returns:
            int: an identifier of the provider
        """
        ...

    @remote_api_endpoint('provider/get')
    def metadata_provider_get(self, provider_id):
        """Get a metadata provider

        Args:
            provider_id: Its identifier, as given by `metadata_provider_add`.

        Returns:
            dict: same as `metadata_provider_add`;
                  or None if it does not exist.
        """
        ...

    @remote_api_endpoint('provider/getby')
    def metadata_provider_get_by(self, provider):
        """Get a metadata provider

        Args:
            provider (dict): A dictionary with keys:
                * provider_name: Its name
                * provider_url: Its URL

        Returns:
            dict: same as `metadata_provider_add`;
                  or None if it does not exist.
        """
        ...

    @deprecated
    @remote_api_endpoint('algos/diff_directories')
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
        ...

    @deprecated
    @remote_api_endpoint('algos/diff_revisions')
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
        ...

    @deprecated
    @remote_api_endpoint('algos/diff_revision')
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
        ...
