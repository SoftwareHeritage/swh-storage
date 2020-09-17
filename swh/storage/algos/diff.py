# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# Utility module to efficiently compute the list of changed files
# between two directory trees.
# The implementation is inspired from the work of Alberto Cortés
# for the go-git project. For more details, you can refer to:
#   - this blog post: https://blog.sourced.tech/post/difftree/
#   - the reference implementation in go:
#     https://github.com/src-d/go-git/tree/master/utils/merkletrie


import collections
from typing import Any, Dict

from swh.model.hashutil import hash_to_bytes
from swh.model.identifiers import directory_identifier
from swh.storage.interface import StorageInterface

from .dir_iterators import DirectoryIterator, DoubleDirectoryIterator, Remaining

# get the hash identifier for an empty directory
_empty_dir_hash = hash_to_bytes(directory_identifier({"entries": []}))


def _get_rev(storage: StorageInterface, rev_id: bytes) -> Dict[str, Any]:
    """
    Return revision data from swh storage.
    """
    revision = storage.revision_get([rev_id])[0]
    assert revision is not None
    return revision.to_dict()


class _RevisionChangesList(object):
    """
    Helper class to track the changes between two
    revision directories.
    """

    def __init__(self, storage, track_renaming):
        """
        Args:
            storage: instance of swh storage
            track_renaming (bool): whether to track or not files renaming
        """
        self.storage = storage
        self.track_renaming = track_renaming
        self.result = []
        # dicts used to track file renaming based on hash value
        # we use a list instead of a single entry to handle the corner
        # case when a repository contains multiple instance of
        # the same file in different directories and a commit
        # renames all of them
        self.inserted_hash_idx = collections.defaultdict(list)
        self.deleted_hash_idx = collections.defaultdict(list)

    def add_insert(self, it_to):
        """
        Add a file insertion in the to directory.

        Args:
            it_to (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on the to directory
        """
        to_hash = it_to.current_hash()
        # if the current file hash has been previously marked as deleted,
        # the file has been renamed
        if self.track_renaming and self.deleted_hash_idx[to_hash]:
            # pop the delete change index in the same order it was inserted
            change = self.result[self.deleted_hash_idx[to_hash].pop(0)]
            # change the delete change as a rename one
            change["type"] = "rename"
            change["to"] = it_to.current()
            change["to_path"] = it_to.current_path()
        else:
            # add the insert change in the list
            self.result.append(
                {
                    "type": "insert",
                    "from": None,
                    "from_path": None,
                    "to": it_to.current(),
                    "to_path": it_to.current_path(),
                }
            )
            # if rename tracking is activated, add the change index in
            # the inserted_hash_idx dict
            if self.track_renaming:
                self.inserted_hash_idx[to_hash].append(len(self.result) - 1)

    def add_delete(self, it_from):
        """
        Add a file deletion in the from directory.

        Args:
            it_from (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on the from directory
        """
        from_hash = it_from.current_hash()
        # if the current file has been previously marked as inserted,
        # the file has been renamed
        if self.track_renaming and self.inserted_hash_idx[from_hash]:
            # pop the insert change index in the same order it was inserted
            change = self.result[self.inserted_hash_idx[from_hash].pop(0)]
            # change the insert change as a rename one
            change["type"] = "rename"
            change["from"] = it_from.current()
            change["from_path"] = it_from.current_path()
        else:
            # add the delete change in the list
            self.result.append(
                {
                    "type": "delete",
                    "from": it_from.current(),
                    "from_path": it_from.current_path(),
                    "to": None,
                    "to_path": None,
                }
            )
            # if rename tracking is activated, add the change index in
            # the deleted_hash_idx dict
            if self.track_renaming:
                self.deleted_hash_idx[from_hash].append(len(self.result) - 1)

    def add_modify(self, it_from, it_to):
        """
        Add a file modification in the to directory.

        Args:
            it_from (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on the from directory
            it_to (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on the to directory
        """
        self.result.append(
            {
                "type": "modify",
                "from": it_from.current(),
                "from_path": it_from.current_path(),
                "to": it_to.current(),
                "to_path": it_to.current_path(),
            }
        )

    def add_recursive(self, it, insert):
        """
        Recursively add changes from a directory.

        Args:
            it (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on a directory
            insert (bool): the type of changes to add (insertion
                or deletion)
        """
        # current iterated element is a regular file,
        # simply add adequate change in the list
        if not it.current_is_dir():
            if insert:
                self.add_insert(it)
            else:
                self.add_delete(it)
            return
        # current iterated element is a directory,
        dir_id = it.current_hash()
        # handle empty dir insertion/deletion as the swh model allow
        # to have such object compared to git
        if dir_id == _empty_dir_hash:
            if insert:
                self.add_insert(it)
            else:
                self.add_delete(it)
        # iterate on files reachable from it and add
        # adequate changes in the list
        else:
            sub_it = DirectoryIterator(self.storage, dir_id, it.current_path() + b"/")
            sub_it_current = sub_it.step()
            while sub_it_current:
                if not sub_it.current_is_dir():
                    if insert:
                        self.add_insert(sub_it)
                    else:
                        self.add_delete(sub_it)
                sub_it_current = sub_it.step()

    def add_recursive_insert(self, it_to):
        """
        Recursively add files insertion from a to directory.

        Args:
            it_to (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on a to directory
        """
        self.add_recursive(it_to, True)

    def add_recursive_delete(self, it_from):
        """
        Recursively add files deletion from a from directory.

        Args:
            it_from (swh.storage.algos.dir_iterators.DirectoryIterator):
                iterator on a from directory
        """
        self.add_recursive(it_from, False)


def _diff_elts_same_name(changes, it):
    """"
    Compare two directory entries with the same name and add adequate
    changes if any.

    Args:
        changes (_RevisionChangesList): the list of changes between
            two revisions
        it (swh.storage.algos.dir_iterators.DoubleDirectoryIterator):
            the iterator traversing two revision directories at the same time
    """
    # compare the two current directory elements of the iterator
    status = it.compare()
    # elements have same hash and same permissions:
    # no changes to add and call next on the two iterators
    if status["same_hash"] and status["same_perms"]:
        it.next_both()
    # elements are regular files and have been modified:
    # insert the modification change in the list and
    # call next on the two iterators
    elif status["both_are_files"]:
        changes.add_modify(it.it_from, it.it_to)
        it.next_both()
    # one element is a regular file, the other a directory:
    # recursively add delete/insert changes and call next
    # on the two iterators
    elif status["file_and_dir"]:
        changes.add_recursive_delete(it.it_from)
        changes.add_recursive_insert(it.it_to)
        it.next_both()
    # both elements are directories:
    elif status["both_are_dirs"]:
        # from directory is empty:
        # recursively add insert changes in the to directory
        # and call next on the two iterators
        if status["from_is_empty_dir"]:
            changes.add_recursive_insert(it.it_to)
            it.next_both()
        # to directory is empty:
        # recursively add delete changes in the from directory
        # and call next on the two iterators
        elif status["to_is_empty_dir"]:
            changes.add_recursive_delete(it.it_from)
            it.next_both()
        # both directories are not empty:
        # call step on the two iterators to descend further in
        # the directory trees.
        elif not status["from_is_empty_dir"] and not status["to_is_empty_dir"]:
            it.step_both()


def _compare_paths(path1, path2):
    """
    Compare paths in lexicographic depth-first order.
    For instance, it returns:

        - "a" < "b"
        - "b/c/d" < "b"
        - "c/foo.txt" < "c.txt"
    """
    path1_parts = path1.split(b"/")
    path2_parts = path2.split(b"/")
    i = 0
    while True:
        if len(path1_parts) == len(path2_parts) and i == len(path1_parts):
            return 0
        elif len(path2_parts) == i:
            return 1
        elif len(path1_parts) == i:
            return -1
        else:
            if path2_parts[i] > path1_parts[i]:
                return -1
            elif path2_parts[i] < path1_parts[i]:
                return 1
        i = i + 1


def _diff_elts(changes, it):
    """
    Compare two directory entries.

    Args:
        changes (_RevisionChangesList): the list of changes between
            two revisions
        it (swh.storage.algos.dir_iterators.DoubleDirectoryIterator):
            the iterator traversing two revision directories at the same time
    """
    # compare current to and from path in depth-first lexicographic order
    c = _compare_paths(it.it_from.current_path(), it.it_to.current_path())
    # current from path is lower than the current to path:
    # the from path has been deleted
    if c < 0:
        changes.add_recursive_delete(it.it_from)
        it.next_from()
    # current from path is greater than the current to path:
    # the to path has been inserted
    elif c > 0:
        changes.add_recursive_insert(it.it_to)
        it.next_to()
    # paths are the same and need more processing
    else:
        _diff_elts_same_name(changes, it)


def diff_directories(storage, from_dir, to_dir, track_renaming=False):
    """
    Compute the differential between two directories, i.e. the list of
    file changes (insertion / deletion / modification / renaming)
    between them.

    Args:
        storage (swh.storage.interface.StorageInterface): instance of a swh
            storage (either local or remote, for optimal performance
            the use of a local storage is recommended)
        from_dir (bytes): the swh identifier of the directory to compare from
        to_dir (bytes): the swh identifier of the directory to compare to
        track_renaming (bool): whether or not to track files renaming

    Returns:
        list: A list of dict representing the changes between the two
        revisions. Each dict contains the following entries:

            - *type*: a string describing the type of change
              ('insert' / 'delete' / 'modify' / 'rename')

            - *from*: a dict containing the directory entry metadata in the
              from revision (None in case of an insertion)

            - *from_path*: bytes string corresponding to the absolute path
              of the from revision entry (None in case of an insertion)

            - *to*: a dict containing the directory entry metadata in the
              to revision (None in case of a deletion)

            - *to_path*: bytes string corresponding to the absolute path
              of the to revision entry (None in case of a deletion)

        The returned list is sorted in lexicographic depth-first order
        according to the value of the *to_path* field.

    """
    changes = _RevisionChangesList(storage, track_renaming)
    it = DoubleDirectoryIterator(storage, from_dir, to_dir)
    while True:
        r = it.remaining()
        if r == Remaining.NoMoreFiles:
            break
        elif r == Remaining.OnlyFromFilesRemain:
            changes.add_recursive_delete(it.it_from)
            it.next_from()
        elif r == Remaining.OnlyToFilesRemain:
            changes.add_recursive_insert(it.it_to)
            it.next_to()
        else:
            _diff_elts(changes, it)
    return changes.result


def diff_revisions(storage, from_rev, to_rev, track_renaming=False):
    """
    Compute the differential between two revisions,
    i.e. the list of file changes between the two associated directories.

    Args:
        storage (swh.storage.interface.StorageInterface): instance of a swh
            storage (either local or remote, for optimal performance
            the use of a local storage is recommended)
        from_rev (bytes): the identifier of the revision to compare from
        to_rev (bytes): the identifier of the revision to compare to
        track_renaming (bool): whether or not to track files renaming

    Returns:
        list: A list of dict describing the introduced file changes
        (see :func:`swh.storage.algos.diff.diff_directories`).

    """
    from_dir = None
    if from_rev:
        from_dir = _get_rev(storage, from_rev)["directory"]
    to_dir = _get_rev(storage, to_rev)["directory"]
    return diff_directories(storage, from_dir, to_dir, track_renaming)


def diff_revision(storage, revision, track_renaming=False):
    """
    Computes the differential between a revision and its first parent.
    If the revision has no parents, the directory to compare from
    is considered as empty.
    In other words, it computes the file changes introduced in a
    specific revision.

    Args:
        storage (swh.storage.interface.StorageInterface): instance of a swh
            storage (either local or remote, for optimal performance
            the use of a local storage is recommended)
        revision (bytes): the identifier of the revision from which to
            compute the introduced changes.
        track_renaming (bool): whether or not to track files renaming

    Returns:
        list: A list of dict describing the introduced file changes
        (see :func:`swh.storage.algos.diff.diff_directories`).
    """
    rev_data = _get_rev(storage, revision)
    parent = None
    if rev_data["parents"]:
        parent = rev_data["parents"][0]
    return diff_revisions(storage, parent, revision, track_renaming)
