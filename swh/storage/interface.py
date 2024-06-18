# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar, Union

import attr
from typing_extensions import Protocol, TypedDict, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.core.api.classes import PagedResult as CorePagedResult
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ExtID,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Release,
    Revision,
    Sha1,
    Sha1Git,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
)
from swh.model.swhids import CoreSWHID, ExtendedSWHID, ObjectType


class ListOrder(Enum):
    """Specifies the order for paginated endpoints returning sorted results."""

    ASC = "asc"
    DESC = "desc"


class PartialBranches(TypedDict):
    """Type of the dictionary returned by snapshot_get_branches"""

    id: Sha1Git
    """Identifier of the snapshot"""
    branches: Dict[bytes, Optional[SnapshotBranch]]
    """A dict of branches contained in the snapshot
    whose keys are the branches' names"""
    next_branch: Optional[bytes]
    """The name of the first branch not returned or :const:`None` if
    the snapshot has less than the request number of branches."""


@attr.s
class SnapshotBranchByNameResponse:
    """Object returned by snapshot_branch_get_by_name"""

    branch_found = attr.ib(type=bool)
    """
    Branch with the name exists, with or without a target.
    """
    target = attr.ib(type=Optional[SnapshotBranch])
    """
    Branch target, will be None in case of a dangling branch.
    """
    aliases_followed = attr.ib(type=List[bytes])
    """
    List of alias names until (including) the target.
    This will be of length one for all non alias branches.
    """


class HashDict(TypedDict, total=False):
    sha1: bytes
    sha1_git: bytes
    sha256: bytes
    blake2s256: bytes


class TotalHashDict(HashDict, total=True):
    pass


@attr.s
class OriginVisitWithStatuses:
    visit = attr.ib(type=OriginVisit)
    statuses = attr.ib(type=List[OriginVisitStatus])


@attr.s
class ObjectReference:
    """Record that the object with SWHID ``source`` references the object with SWHID
    ``target``, meaning that the ``target`` needs to exist for the ``source`` object
    to be consistent within the archive."""

    source = attr.ib(type=ExtendedSWHID)
    target = attr.ib(type=ExtendedSWHID)


TResult = TypeVar("TResult")
PagedResult = CorePagedResult[TResult, str]


# TODO: Make it an enum (too much impact)
VISIT_STATUSES = ["created", "ongoing", "full", "partial"]


def deprecated(f):
    f.deprecated_endpoint = True
    return f


@runtime_checkable
class StorageInterface(Protocol):
    @remote_api_endpoint("check_config")
    def check_config(self, *, check_write: bool) -> bool:
        """Check that the storage is configured and ready to go."""
        ...

    ##########################
    # Content
    ##########################

    @remote_api_endpoint("content/add")
    def content_add(self, content: List[Content]) -> Dict[str, int]:
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

    @remote_api_endpoint("content/update")
    def content_update(
        self, contents: List[Dict[str, Any]], keys: List[str] = []
    ) -> None:
        """Update content blobs to the storage. Does nothing for unknown
        contents or skipped ones.

        Args:
            content: iterable of dictionaries representing
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

    @remote_api_endpoint("content/add_metadata")
    def content_add_metadata(self, content: List[Content]) -> Dict[str, int]:
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

    @remote_api_endpoint("content/data")
    def content_get_data(self, content: Union[HashDict, Sha1]) -> Optional[bytes]:
        """Given a content identifier, returns its associated data if any.

        Args:
            content: dict of hashes (or just sha1 identifier)

        Returns:
             raw content data (bytes)

        """
        ...

    @remote_api_endpoint("content/partition")
    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Content]:
        """Splits contents into nb_partitions, and returns one of these based on
        partition_id (which must be in [0, nb_partitions-1])

        There is no guarantee on how the partitioning is done, or the
        result order.

        Args:
            partition_id: index of the partition to fetch
            nb_partitions: total number of partitions to split into
            page_token: opaque token used for pagination.
            limit: Limit result (default to 1000)

        Returns:
            PagedResult of Content model objects within the partition. If
            next_page_token is None, there is no longer data to retrieve.

        """
        ...

    @remote_api_endpoint("content/metadata")
    def content_get(
        self, contents: List[bytes], algo: str = "sha1"
    ) -> List[Optional[Content]]:
        """Retrieve content metadata in bulk

        Args:
            content: List of content identifiers
            algo: one of the checksum algorithm in
              :data:`swh.model.hashutil.DEFAULT_ALGORITHMS`

        Returns:
            List of contents model objects when they exist, None otherwise.

        """
        ...

    @remote_api_endpoint("content/missing")
    def content_missing(
        self, contents: List[HashDict], key_hash: str = "sha1"
    ) -> Iterable[bytes]:
        """List content missing from storage

        Args:
            content: iterable of dictionaries whose keys are either 'length' or an item
                of :data:`swh.model.hashutil.ALGORITHMS`; mapped to the
                corresponding checksum (or length).
            key_hash: name of the column to use as hash id result (default: 'sha1')

        Raises:
            StorageArgumentException when key_hash is unknown.
            TODO: an exception when we get a hash collision.

        Returns:
            iterable of missing content ids (as per the `key_hash` column)

        """
        ...

    @remote_api_endpoint("content/missing/sha1")
    def content_missing_per_sha1(self, contents: List[bytes]) -> Iterable[bytes]:
        """List content missing from storage based only on sha1.

        Args:
            contents: List of sha1 to check for absence.

        Raises:
            TODO: an exception when we get a hash collision.

        Returns:
            Iterable of missing content ids (sha1)

        """
        ...

    @remote_api_endpoint("content/missing/sha1_git")
    def content_missing_per_sha1_git(
        self, contents: List[Sha1Git]
    ) -> Iterable[Sha1Git]:
        """List content missing from storage based only on sha1_git.

        Args:
            contents (List): An iterable of content id (sha1_git)

        Yields:
            missing contents sha1_git

        """
        ...

    @remote_api_endpoint("content/present")
    def content_find(self, content: HashDict) -> List[Content]:
        """Find a content hash in db.

        Args:
            content: a dictionary representing one content hash, mapping
                checksum algorithm names (see swh.model.hashutil.ALGORITHMS) to
                checksum values

        Raises:
            ValueError: in case the key of the dictionary is not sha1, sha1_git
                nor sha256.

        Returns:
            an iterable of Content objects matching the search criteria if the
            content exist. Empty iterable otherwise.

        """
        ...

    @remote_api_endpoint("content/get_random")
    def content_get_random(self) -> Sha1Git:
        """Finds a random content id.

        Returns:
            a sha1_git
        """
        ...

    ##########################
    # SkippedContent
    ##########################

    @remote_api_endpoint("content/skipped/add")
    def skipped_content_add(self, content: List[SkippedContent]) -> Dict[str, int]:
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

    @remote_api_endpoint("content/skipped/find")
    def skipped_content_find(self, content: HashDict) -> List[SkippedContent]:
        """Find skipped content for the given hashes

        Args:
            content: a dictionary representing one content hash, mapping
                checksum algorithm names (see swh.model.hashutil.ALGORITHMS) to
                checksum values

        Raises:
            ValueError: in case the key of the dictionary is not sha1, sha1_git
                nor sha256.

        Returns:
            a list of SkippedContent objects matching the search criteria if the
            skipped content exists. Empty list otherwise.
        """
        ...

    @remote_api_endpoint("content/skipped/missing")
    def skipped_content_missing(
        self, contents: List[Dict[str, Any]]
    ) -> Iterable[Dict[str, Any]]:
        """List skipped contents missing from storage.

        Args:
            contents: iterable of dictionaries containing the data for each
                checksum algorithm.

        Returns:
            Iterable of missing skipped contents as dict

        """
        ...

    ##########################
    # Directory
    ##########################

    @remote_api_endpoint("directory/add")
    def directory_add(self, directories: List[Directory]) -> Dict[str, int]:
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

    @remote_api_endpoint("directory/missing")
    def directory_missing(self, directories: List[Sha1Git]) -> Iterable[Sha1Git]:
        """List directories missing from storage.

        Args:
            directories: list of directory ids

        Yields:
            missing directory ids

        """
        ...

    @remote_api_endpoint("directory/ls")
    def directory_ls(
        self, directory: Sha1Git, recursive: bool = False
    ) -> Iterable[Dict[str, Any]]:
        """List entries for one directory.

        If `recursive=True`, names in the path of a dir/file not at the
        root are concatenated with a slash (`/`).

        Args:
            directory: the directory to list entries from.
            recursive: if flag on, this list recursively from this directory.

        Yields:
            directory entries for such directory.

        """
        ...

    @remote_api_endpoint("directory/path")
    def directory_entry_get_by_path(
        self, directory: Sha1Git, paths: List[bytes]
    ) -> Optional[Dict[str, Any]]:
        """Get the directory entry (either file or dir) from directory with path.

        Args:
            directory: directory id
            paths: path to lookup from the top level directory. From left
              (top) to right (bottom).

        Returns:
            The corresponding directory entry as dict if found, None otherwise.

        """
        ...

    @remote_api_endpoint("directory/get_entries")
    def directory_get_entries(
        self,
        directory_id: Sha1Git,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
    ) -> Optional[PagedResult[DirectoryEntry]]:
        """Get the content, possibly partial, of a directory with the given id

        The entries of the directory are not guaranteed to be returned in any
        particular order.

        The number of results is not guaranteed to be lower than the ``limit``.

        Args:
            directory_id: identifier of the directory
            page_token: opaque string used to get the next results of a search
            limit: Number of entries to return

        Returns:
            None if the directory does not exist; a page of DirectoryEntry
              objects otherwise.

        See Also:
            :py:func:`swh.storage.algos.directories.directory_get` will get all
            entries for a given directory.
            :py:func:`swh.storage.algos.directories.directory_get_many` will do
            the same for a set of directories.
        """
        ...

    @remote_api_endpoint("directory/get_raw_manifest")
    def directory_get_raw_manifest(
        self, directory_ids: List[Sha1Git]
    ) -> Dict[Sha1Git, Optional[bytes]]:
        """Returns the raw manifest of directories that do not fit the SWH data model,
        or None if they do.
        Directories missing from the archive are not returned at all.

        Args:
            directory_ids: List of directory ids to query
        """
        ...

    @remote_api_endpoint("directory/get_random")
    def directory_get_random(self) -> Sha1Git:
        """Finds a random directory id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint("directory/partition/id")
    def directory_get_id_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1Git]:
        """Splits directories into nb_partitions, and returns all the ids and
        raw manifests in one of these based on partition_id (which must be in
        [0, nb_partitions-1]).
        This does not return directory entries themselves; they should be
        retrieved using :meth:`directory_get_entries` and
        :meth:`directory_get_raw_manifest` instead.

        There is no guarantee on how the partitioning is done, or the
        result order.

        Args:
            partition_id: index of the partition to fetch
            nb_partitions: total number of partitions to split into

        Returns:
            Page of the directories' sha1_git hashes.
        """
        ...

    ##########################
    # Revision
    ##########################

    @remote_api_endpoint("revision/add")
    def revision_add(self, revisions: List[Revision]) -> Dict[str, int]:
        """Add revisions to the storage

        Args:
            revisions (List[dict]): iterable of dictionaries representing
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

    @remote_api_endpoint("revision/missing")
    def revision_missing(self, revisions: List[Sha1Git]) -> Iterable[Sha1Git]:
        """List revisions missing from storage

        Args:
            revisions: revision ids

        Yields:
            missing revision ids

        """
        ...

    @remote_api_endpoint("revision/partition")
    def revision_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Revision]:
        """Splits revisions into nb_partitions, and returns one of these based on
        partition_id (which must be in [0, nb_partitions-1])

        There is no guarantee on how the partitioning is done, or the
        result order.

        Args:
            partition_id: index of the partition to fetch
            nb_partitions: total number of partitions to split into

        Returns:
            Page of Revision model objects within the partition.

        """
        ...

    @remote_api_endpoint("revision")
    def revision_get(
        self, revision_ids: List[Sha1Git], ignore_displayname: bool = False
    ) -> List[Optional[Revision]]:
        """Get revisions from storage

        Args:
            revisions: revision ids
            ignore_displayname: return the original author/committer's full name even if
              it's masked by a displayname.

        Returns:
            list of revision object (if the revision exists or None otherwise)

        """
        ...

    @remote_api_endpoint("revision/log")
    def revision_log(
        self,
        revisions: List[Sha1Git],
        ignore_displayname: bool = False,
        limit: Optional[int] = None,
    ) -> Iterable[Optional[Dict[str, Any]]]:
        """Fetch revision entry from the given root revisions.

        Args:
            revisions: array of root revisions to lookup
            ignore_displayname: return the original author/committer's full name even if
              it's masked by a displayname.
            limit: limitation on the output result. Default to None.

        Yields:
            revision entries log from the given root root revisions

        """
        ...

    @remote_api_endpoint("revision/shortlog")
    def revision_shortlog(
        self, revisions: List[Sha1Git], limit: Optional[int] = None
    ) -> Iterable[Optional[Tuple[Sha1Git, Tuple[Sha1Git, ...]]]]:
        """Fetch the shortlog for the given revisions

        Args:
            revisions: list of root revisions to lookup
            limit: depth limitation for the output

        Yields:
            a list of (id, parents) tuples

        """
        ...

    @remote_api_endpoint("revision/get_random")
    def revision_get_random(self) -> Sha1Git:
        """Finds a random revision id.

        Returns:
            a sha1_git
        """
        ...

    ##########################
    # ExtID
    ##########################

    @remote_api_endpoint("extid/from_extid")
    def extid_get_from_extid(
        self, id_type: str, ids: List[bytes], version: Optional[int] = None
    ) -> List[ExtID]:
        """Get ExtID objects from external IDs

        Args:
            id_type: type of the given external identifiers (e.g. 'mercurial')
            ids: list of external IDs
            version: (Optional) version to use as filter

        Returns:
            list of ExtID objects

        """
        ...

    @remote_api_endpoint("extid/from_target")
    def extid_get_from_target(
        self,
        target_type: ObjectType,
        ids: List[Sha1Git],
        extid_type: Optional[str] = None,
        extid_version: Optional[int] = None,
    ) -> List[ExtID]:
        """Get ExtID objects from target IDs and target_type

        Args:
            target_type: type the SWH object
            ids: list of target IDs
            extid_type: (Optional) extid_type to use as filter. This cannot be empty if
              extid_version is provided.
            extid_version: (Optional) version to use as filter. This cannot be empty if
              extid_type is provided.

        Raises:
            ValueError if extid_version is provided without extid_type and vice versa.

        Returns:
            list of ExtID objects

        """
        ...

    @remote_api_endpoint("extid/add")
    def extid_add(self, ids: List[ExtID]) -> Dict[str, int]:
        """Add a series of ExtID objects

        Args:
            ids: list of ExtID objects

        Returns:
            Summary dict of keys with associated count as values

                extid:add: New ExtID objects actually stored in db
        """
        ...

    ##########################
    # Release
    ##########################

    @remote_api_endpoint("release/add")
    def release_add(self, releases: List[Release]) -> Dict[str, int]:
        """Add releases to the storage

        Args:
            releases (List[dict]): iterable of dictionaries representing
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

    @remote_api_endpoint("release/missing")
    def release_missing(self, releases: List[Sha1Git]) -> Iterable[Sha1Git]:
        """List missing release ids from storage

        Args:
            releases: release ids

        Yields:
            a list of missing release ids

        """
        ...

    @remote_api_endpoint("release")
    def release_get(
        self, releases: List[Sha1Git], ignore_displayname: bool = False
    ) -> List[Optional[Release]]:
        """Given a list of sha1, return the releases's information

        Args:
            releases: list of sha1s
            ignore_displayname: return the original author's full name even if it's
              masked by a displayname.

        Returns:
            List of releases matching the identifiers or None if the release does
            not exist.

        """
        ...

    @remote_api_endpoint("release/get_random")
    def release_get_random(self) -> Sha1Git:
        """Finds a random release id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint("release/partition")
    def release_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Release]:
        """Splits releases into nb_partitions, and returns one of these based on
        partition_id (which must be in [0, nb_partitions-1])

        There is no guarantee on how the partitioning is done, or the
        result order.

        Args:
            partition_id: index of the partition to fetch
            nb_partitions: total number of partitions to split into

        Returns:
            Page of Release model objects within the partition.

        """
        ...

    ##########################
    # Snapshot
    ##########################

    @remote_api_endpoint("snapshot/add")
    def snapshot_add(self, snapshots: List[Snapshot]) -> Dict[str, int]:
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

    @remote_api_endpoint("snapshot/missing")
    def snapshot_missing(self, snapshots: List[Sha1Git]) -> Iterable[Sha1Git]:
        """List snapshots missing from storage

        Args:
            snapshots: snapshot ids

        Yields:
            missing snapshot ids

        """
        ...

    @remote_api_endpoint("snapshot")
    def snapshot_get(self, snapshot_id: Sha1Git) -> Optional[Dict[str, Any]]:
        """Get the content, possibly partial, of a snapshot with the given id

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        .. warning:: At most 1000 branches contained in the snapshot will be
            returned for performance reasons. In order to browse the whole
            set of branches, the method :meth:`snapshot_get_branches`
            should be used instead.

        Args:
            snapshot_id: snapshot identifier

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

    @remote_api_endpoint("snapshot/count_branches")
    def snapshot_count_branches(
        self,
        snapshot_id: Sha1Git,
        branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Optional[Dict[Optional[str], int]]:
        """Count the number of branches in the snapshot with the given id

        Args:
            snapshot_id: snapshot identifier
            branch_name_exclude_prefix: if provided, do not count branches whose name
                starts with given prefix

        Returns:
            A dict whose keys are the target types of branches and values their
            corresponding amount

        """
        ...

    @remote_api_endpoint("snapshot/get_branches")
    def snapshot_get_branches(
        self,
        snapshot_id: Sha1Git,
        branches_from: bytes = b"",
        branches_count: int = 1000,
        target_types: Optional[List[str]] = None,
        branch_name_include_substring: Optional[bytes] = None,
        branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Optional[PartialBranches]:
        """Get the content, possibly partial, of a snapshot with the given id

        The branches of the snapshot are iterated in the lexicographical
        order of their names.

        Args:
            snapshot_id: identifier of the snapshot
            branches_from: optional parameter used to skip branches
                whose name is lesser than it before returning them
            branches_count: optional parameter used to restrain
                the amount of returned branches
            target_types: optional parameter used to filter the
                target types of branch to return (possible values that can be
                contained in that list are `'content', 'directory',
                'revision', 'release', 'snapshot', 'alias'`)
            branch_name_include_substring: if provided, only return branches whose name
                contains given substring
            branch_name_exclude_prefix: if provided, do not return branches whose name
                contains given prefix

        Returns:
            a PartialBranches object listing a limited amount of branches
            matching the given criteria or None if the snapshot does not exist.

        See Also:
            :py:func:`swh.storage.algos.snapshot.snapshot_get_all_branches` will get
            all branches for a given snapshot.
        """
        ...

    @remote_api_endpoint("snapshot/get_random")
    def snapshot_get_random(self) -> Sha1Git:
        """Finds a random snapshot id.

        Returns:
            a sha1_git
        """
        ...

    @remote_api_endpoint("snapshot/branches/get_by_name")
    def snapshot_branch_get_by_name(
        self,
        snapshot_id: Sha1Git,
        branch_name: bytes,
        follow_alias_chain: bool = True,
        max_alias_chain_length: int = 100,
    ) -> Optional[SnapshotBranchByNameResponse]:
        """Get a snapshot branch by its name

        Args:
            snapshot_id: Snapshot identifier
            branch_name: Branch name to look for
            follow_alias_chain: If True, find the first non alias branch.
                Return the first branch (alias or non alias) otherwise
            max_alias_chain_length: Maximum number of alias chains to be
                followed before treating the branch as dangling. This has
                no significance when follow_alias_chain is False.

        Returns:
            A SnapshotBranchByNameResponse object
        """
        ...

    @remote_api_endpoint("snapshot/partition/id")
    def snapshot_get_id_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
    ) -> PagedResult[Sha1Git]:
        """Splits directories into nb_partitions, and returns all the ids and
        raw manifests in one of these based on partition_id (which must be in
        [0, nb_partitions-1]).
        This does not return directory entries themselves; they should be
        retrieved using :meth:`snapshot_get_branches` instead.

        There is no guarantee on how the partitioning is done, or the
        result order.

        Args:
            partition_id: index of the partition to fetch
            nb_partitions: total number of partitions to split into

        Returns:
            Page of the snapshots' sha1_git hashes
        """
        ...

    ##########################
    # OriginVisit and OriginVisitStatus
    ##########################

    @remote_api_endpoint("origin/visit/add")
    def origin_visit_add(self, visits: List[OriginVisit]) -> Iterable[OriginVisit]:
        """Add visits to storage. If the visits have no id, they will be created and assigned
        one. The resulted visits are visits with their visit id set.

        Args:
            visits: List of OriginVisit objects to add

        Raises:
            StorageArgumentException if some origin visit reference unknown origins

        Returns:
            List[OriginVisit] stored

        """
        ...

    @remote_api_endpoint("origin/visit_status/add")
    def origin_visit_status_add(
        self,
        visit_statuses: List[OriginVisitStatus],
    ) -> Dict[str, int]:
        """Add origin visit statuses.

        If there is already a status for the same origin and visit id at the same
        date, the new one will be either dropped or will replace the existing one
        (it is unspecified which one of these two behaviors happens).

        Args:
            visit_statuses: origin visit statuses to add

        Raises: StorageArgumentException if the origin of the visit status is unknown

        """
        ...

    @remote_api_endpoint("origin/visit/get")
    def origin_visit_get(
        self,
        origin: str,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisit]:
        """Retrieve page of OriginVisit information.

        Args:
            origin: The visited origin
            page_token: opaque string used to get the next results of a search
            order: Order on visit id fields to list origin visits (default to asc)
            limit: Number of visits to return

        Raises:
            StorageArgumentException if the order is wrong or the page_token type is
            mistyped.

        Returns: Page of OriginVisit data model objects. if next_page_token is None,
            there is no longer data to retrieve.

        See Also:
            :py:func:`swh.storage.algos.origin.iter_origin_visits` will iterate
            over all OriginVisits for a given origin.
        """
        ...

    @remote_api_endpoint("origin/visit/find_by_date")
    def origin_visit_find_by_date(
        self, origin: str, visit_date: datetime.datetime, type: Optional[str] = None
    ) -> Optional[OriginVisit]:
        """Retrieves the origin visit whose date is closest to the provided
        timestamp.
        In case of a tie, the visit with largest id is selected.

        Args:
            origin: origin (URL)
            visit_date: expected visit date
            type: filter on a specific visit type if provided

        Returns:
            A visit if found, None otherwise

        """
        ...

    @remote_api_endpoint("origin/visit/getby")
    def origin_visit_get_by(self, origin: str, visit: int) -> Optional[OriginVisit]:
        """Retrieve origin visit's information.

        Args:
            origin: origin (URL)
            visit: visit id

        Returns:
            The information on that particular OriginVisit or None if
            it does not exist

        """
        ...

    @remote_api_endpoint("origin/visit/get_latest")
    def origin_visit_get_latest(
        self,
        origin: str,
        type: Optional[str] = None,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
    ) -> Optional[OriginVisit]:
        """Get the latest origin visit for the given origin, optionally
        looking only for those with one of the given allowed_statuses
        or for those with a snapshot.

        Args:
            origin: origin URL
            type: Optional visit type to filter on (e.g git, tar, dsc, svn,
            hg, npm, pypi, ...)
            allowed_statuses: list of visit statuses considered
                to find the latest visit. For instance,
                ``allowed_statuses=['full']`` will only consider visits that
                have successfully run to completion.
            require_snapshot: If True, only a visit with a snapshot
                will be returned.

        Raises:
            StorageArgumentException if values for the allowed_statuses parameters
            are unknown

        Returns:
            OriginVisit matching the criteria if found, None otherwise. Note that as
            OriginVisit no longer held reference on the visit status or snapshot, you
            may want to use origin_visit_status_get_latest for those information.

        """
        ...

    @remote_api_endpoint("origin/visit_status/get")
    def origin_visit_status_get(
        self,
        origin: str,
        visit: int,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisitStatus]:
        """Retrieve page of OriginVisitStatus information.

        Args:
            origin: The visited origin
            visit: The visit identifier
            page_token: opaque string used to get the next results of a search
            order: Order on visit status objects to list (default to asc)
            limit: Number of visit statuses to return

        Returns: Page of OriginVisitStatus data model objects. if next_page_token is
            None, there is no longer data to retrieve.

        See Also:
            :py:func:`swh.storage.algos.origin.iter_origin_visit_statuses` will iterate
            over all OriginVisitStatus objects for a given origin and visit.
        """
        ...

    @remote_api_endpoint("origin/visit_status/get_latest")
    def origin_visit_status_get_latest(
        self,
        origin_url: str,
        visit: int,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
    ) -> Optional[OriginVisitStatus]:
        """Get the latest origin visit status for the given origin visit, optionally
        looking only for those with one of the given allowed_statuses or with a
        snapshot.

        Args:
            origin: origin URL

            allowed_statuses: list of visit statuses considered to find the latest
                visit. Possible values are {created, ongoing, partial, full}. For
                instance, ``allowed_statuses=['full']`` will only consider visits that
                have successfully run to completion.
            require_snapshot: If True, only a visit with a snapshot
                will be returned.

        Raises:
            StorageArgumentException if values for the allowed_statuses parameters
            are unknown

        Returns:
            The OriginVisitStatus matching the criteria

        """
        ...

    @remote_api_endpoint("origin/visit/get_with_statuses")
    def origin_visit_get_with_statuses(
        self,
        origin: str,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
    ) -> PagedResult[OriginVisitWithStatuses]:
        """Retrieve page of origin visits and all their statuses.

        Origin visit statuses are always sorted in ascending order of their dates.

        Args:
            origin: The visited origin URL
            allowed_statuses: Only visit statuses matching that list will be returned.
                If empty, all visit statuses will be returned. Possible status values
                are ``created``, ``not_found``, ``ongoing``, ``failed``, ``partial``
                and ``full``.
            require_snapshot: If :const:`True`, only visit statuses with a snapshot
                will be returned.
            page_token: opaque string used to get the next results
            order: Order on visit objects to list (default to asc)
            limit: Number of visits with their statuses to return

        Returns: Page of OriginVisitWithStatuses objects. if next_page_token is
            None, there is no longer data to retrieve.
        """
        ...

    @remote_api_endpoint("origin/visit_status/get_random")
    def origin_visit_status_get_random(self, type: str) -> Optional[OriginVisitStatus]:
        """Randomly select one successful origin visit with <type>
        made in the last 3 months.

        Returns:
            One random OriginVisitStatus matching the selection criteria

        """
        ...

    ##########################
    # Origin
    ##########################

    @remote_api_endpoint("origin/get")
    def origin_get(self, origins: List[str]) -> List[Optional[Origin]]:
        """Return origins.

        Args:
            origin: a list of urls to find

        Returns:
            the list of associated existing origin model objects. The unknown origins
            will be returned as None at the same index as the input.

        """
        ...

    @remote_api_endpoint("origin/get_sha1")
    def origin_get_by_sha1(self, sha1s: List[bytes]) -> List[Optional[Dict[str, Any]]]:
        """Return origins, identified by the sha1 of their URLs.

        Args:
            sha1s: a list of sha1s

        Returns:
            List of origins dict whose sha1 of their url match, None otherwise.

        """
        ...

    @remote_api_endpoint("origin/list")
    def origin_list(
        self, page_token: Optional[str] = None, limit: int = 100
    ) -> PagedResult[Origin]:
        """Returns the list of origins

        Args:
            page_token: opaque token used for pagination.
            limit: the maximum number of results to return

        Returns:
            Page of Origin data model objects. if next_page_token is None, there is
            no longer data to retrieve.

        """
        ...

    @remote_api_endpoint("origin/search")
    def origin_search(
        self,
        url_pattern: str,
        page_token: Optional[str] = None,
        limit: int = 50,
        regexp: bool = False,
        with_visit: bool = False,
        visit_types: Optional[List[str]] = None,
    ) -> PagedResult[Origin]:
        """Search for origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The search is performed in a case insensitive way.

        Args:
            url_pattern: the string pattern to search for in origin urls
            page_token: opaque token used for pagination
            limit: the maximum number of found origins to return
            regexp: if True, consider the provided pattern as a regular
                expression and return origins whose urls match it
            with_visit: if True, filter out origins with no visit
            visit_types: Only origins having any of the provided visit types
                (e.g. git, svn, pypi) will be returned

        Yields:
            PagedResult of Origin

        """
        ...

    @deprecated
    @remote_api_endpoint("origin/count")
    def origin_count(
        self, url_pattern: str, regexp: bool = False, with_visit: bool = False
    ) -> int:
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

    @remote_api_endpoint("origin/snapshot/get")
    def origin_snapshot_get_all(self, origin_url: str) -> List[Sha1Git]:
        """Return all unique snapshot identifiers resulting from origin visits.

        Args:
            origin_url: origin URL

        Returns:
            list of sha1s

        """
        ...

    @remote_api_endpoint("origin/add_multi")
    def origin_add(self, origins: List[Origin]) -> Dict[str, int]:
        """Add origins to the storage

        Args:
            origins: list of dictionaries representing the individual origins,
                with the following keys:

                - type: the origin type ('git', 'svn', 'deb', ...)
                - url (bytes): the url the origin points to

        Returns:
            Summary dict of keys with associated count as values

                origin:add: Count of object actually stored in db

        """
        ...

    ##########################
    # reverse index for object references
    ##########################

    @remote_api_endpoint("object/find_references")
    def object_find_recent_references(
        self, target_swhid: ExtendedSWHID, limit: int
    ) -> List[ExtendedSWHID]:
        """Return the SWHIDs of objects that are known to reference the object
        ``target_swhid``.

        Args:
            target_swhid: the SWHID of the object targeted by the returned objects
            limit: the maximum number of SWHIDs to return

        Note:
            The data returned by this function is by essence limited to objects that
            were recently added to the archive, and is pruned regularly. For
            completeness, one must also query :mod:`swh.graph` for backwards edges
            targeting the requested object.
        """
        ...

    @remote_api_endpoint("object/references_add")
    def object_references_add(
        self, references: List[ObjectReference]
    ) -> Dict[str, int]:
        """For each object reference ``(source, target)``, record that the ``source``
        object references the ``target`` object (meaning that the ``target`` needs to
        exist for the ``source`` object to be consistent within the archive).

        This function will only be called internally by a reference recording proxy,
        through one of :func:`directory_add`, :func:`revision_add`, :func:`release_add`,
        :func:`snapshot_add`, or :func:`origin_visit_status_add`. External users of
        :mod:`swh.storage` should not need to use this function directly.

        Note:
            these records are inserted in time-based partitions that can be pruned when
            the objects are known in an up-to-date :mod:`swh.graph` instance.

        Args:
            references: a list of ``(source, target)`` SWHID tuples

        Returns:
            A summary dict with the following keys:

              object_reference:add: the number of object references added

        """
        ...

    ##########################
    # misc.
    ##########################

    @remote_api_endpoint("object/find_by_sha1_git")
    def object_find_by_sha1_git(self, ids: List[Sha1Git]) -> Dict[Sha1Git, List[Dict]]:
        """Return the objects found with the given ids.

        Args:
            ids: a generator of sha1_gits

        Returns:
            A dict from id to the list of objects found for that id. Each object
            found is itself a dict with keys:

            - sha1_git: the input id
            - type: the type of object found

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

    ##########################
    # RawExtrinsicMetadata
    ##########################

    @remote_api_endpoint("raw_extrinsic_metadata/add")
    def raw_extrinsic_metadata_add(
        self,
        metadata: List[RawExtrinsicMetadata],
    ) -> Dict[str, int]:
        """Add extrinsic metadata on objects (contents, directories, ...).

        The authority and fetcher must be known to the storage before
        using this endpoint.

        If there is already metadata for the same object, authority,
        fetcher, and at the same date; the new one will be either dropped or
        will replace the existing one
        (it is unspecified which one of these two behaviors happens).

        Args:
            metadata: iterable of RawExtrinsicMetadata objects to be inserted.
        """
        ...

    @remote_api_endpoint("raw_extrinsic_metadata/get")
    def raw_extrinsic_metadata_get(
        self,
        target: ExtendedSWHID,
        authority: MetadataAuthority,
        after: Optional[datetime.datetime] = None,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
    ) -> PagedResult[RawExtrinsicMetadata]:
        """Retrieve list of all raw_extrinsic_metadata entries targeting the id

        Args:
            target: the SWHID of the objects to find metadata on
            authority: a dict containing keys `type` and `url`.
            after: minimum discovery_date for a result to be returned
            page_token: opaque token, used to get the next page of results
            limit: maximum number of results to be returned

        Returns:
            PagedResult of RawExtrinsicMetadata

        Raises:
            UnknownMetadataAuthority: if the metadata authority does not exist
                at all
        """
        ...

    @remote_api_endpoint("raw_extrinsic_metadata/get_by_ids")
    def raw_extrinsic_metadata_get_by_ids(
        self, ids: List[Sha1Git]
    ) -> List[RawExtrinsicMetadata]:
        """Retrieve list of raw_extrinsic_metadata entries of the given id
        (unlike raw_extrinsic_metadata_get, which returns metadata entries
        **targeting** the id)

        Args:
            ids: list of hashes of RawExtrinsicMetadata objects

        """
        ...

    @remote_api_endpoint("raw_extrinsic_metadata/get_authorities")
    def raw_extrinsic_metadata_get_authorities(
        self, target: ExtendedSWHID
    ) -> List[MetadataAuthority]:
        """Returns all authorities that provided metadata on the given object."""
        ...

    ##########################
    # MetadataFetcher and MetadataAuthority
    ##########################

    @remote_api_endpoint("metadata_fetcher/add")
    def metadata_fetcher_add(
        self,
        fetchers: List[MetadataFetcher],
    ) -> Dict[str, int]:
        """Add new metadata fetchers to the storage.

        Their `name` and `version` together are unique identifiers of this
        fetcher; and `metadata` is an arbitrary dict of JSONable data
        with information about this fetcher, which must not be `None`
        (but may be empty).

        Args:
            fetchers: iterable of MetadataFetcher to be inserted

        """
        ...

    @remote_api_endpoint("metadata_fetcher/get")
    def metadata_fetcher_get(
        self, name: str, version: str
    ) -> Optional[MetadataFetcher]:
        """Retrieve information about a fetcher

        Args:
            name: the name of the fetcher
            version: version of the fetcher

        Returns:
            a MetadataFetcher object (with a non-None metadata field) if it is known,
            else None.

        """
        ...

    @remote_api_endpoint("metadata_authority/add")
    def metadata_authority_add(
        self, authorities: List[MetadataAuthority]
    ) -> Dict[str, int]:
        """Add new metadata authorities to the storage.

        Their `type` and `url` together are unique identifiers of this
        authority; and `metadata` is an arbitrary dict of JSONable data
        with information about this authority, which must not be `None`
        (but may be empty).

        Args:
            authorities: iterable of MetadataAuthority to be inserted
        """
        ...

    @remote_api_endpoint("metadata_authority/get")
    def metadata_authority_get(
        self, type: MetadataAuthorityType, url: str
    ) -> Optional[MetadataAuthority]:
        """Retrieve information about an authority

        Args:
            type: one of "deposit_client", "forge", or "registry"
            url: unique URI identifying the authority

        Returns:
            a MetadataAuthority object (with a non-None metadata field) if it is known,
            else None.
        """
        ...

    @remote_api_endpoint("clear/buffer")
    def clear_buffers(self, object_types: Sequence[str] = ()) -> None:
        """For backend storages (pg, storage, in-memory), this is a noop operation. For proxy
        storages (especially filter, buffer), this is an operation which cleans internal
        state.

        """

    @remote_api_endpoint("flush")
    def flush(self, object_types: Sequence[str] = ()) -> Dict[str, int]:
        """For backend storages (pg, storage, in-memory), this is expected to be a noop
        operation. For proxy storages (especially buffer), this is expected to trigger
        actual writes to the backend.
        """
        ...


class ObjectDeletionInterface(Protocol):
    def object_delete(self, swhids: List[ExtendedSWHID]) -> Dict[str, int]:
        """Delete objects from the storage

        All skipped content objects matching the given SWHID will be removed,
        including those who have the same SWHID due to hash collisions.

        Origin objects are removed alongside their associated origin visit and
        origin visit status objects.

        Only objects from this facility will be removed. The same method
        should be called on other storage, objstorage, or journal instances
        where the specified objects need to be removed.

        Args:
            swhids: list of SWHID of the objects to remove

        Returns:
            dict: number of objects removed. Details of each key:

            content:delete
                Number of content objects removed

            content:delete:bytes
                Sum of the removed contents’ data length

            skipped_content:delete
                Number of skipped content objects removed

            directory:delete
                Number of directory objects removed

            revision:delete
                Number of revision objects removed

            release:delete
                Number of release objects removed

            snapshot:delete
                Number of snapshot objects removed

            origin:delete
                Number of origin objects removed

            origin_visit:delete
                Number of origin visit objects removed

            origin_visit_status:delete
                Number of origin visit status objects removed

            ori_metadata:delete
                Number of raw extrinsic metadata objects targeting
                an origin that have been removed

            snp_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a snapshot that have been removed

            rev_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a revision that have been removed

            rel_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a release that have been removed

            dir_metadata:delete
                Number ef raw extrinsic metadata objects targeting
                a directory that have been removed

            cnt_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a content that have been removed

            emd_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a raw extrinsic metadata object that have been removed"""
        ...

    def extid_delete_for_target(self, target_swhids: List[CoreSWHID]) -> Dict[str, int]:
        """Delete ExtID objects from the storage

        Args:
            target_swhids: list of SWHIDs targeted by the ExtID objects to remove

        Returns:
            Summary dict with the following keys and associated values:

                extid:delete: Number of ExtID objects removed
        """
        ...
