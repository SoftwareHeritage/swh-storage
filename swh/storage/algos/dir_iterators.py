# Copyright (C) 2018-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# Utility module to iterate on directory trees.
# The implementation is inspired from the work of Alberto Cortés
# for the go-git project. For more details, you can refer to:
#   - this blog post: https://blog.sourced.tech/post/difftree/
#   - the reference implementation in go:
#     https://github.com/src-d/go-git/tree/master/utils/merkletrie

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from swh.model.model import Directory
from swh.storage.interface import StorageInterface

# get the hash identifier for an empty directory
_empty_dir_hash = Directory(entries=()).id


def _get_dir(
    storage: StorageInterface, dir_id: Optional[bytes]
) -> List[Dict[str, Any]]:
    """
    Return directory data from swh storage.
    """
    return list(storage.directory_ls(dir_id)) if dir_id else []


class DirectoryIterator:
    """
    Helper class used to iterate on a directory tree in a depth-first search
    way with some additional features:

    - sibling nodes are iterated in lexicographic order by name
    - it is possible to skip the visit of sub-directories nodes
      for efficiency reasons when comparing two trees (no need to
      go deeper if two directories have the same hash)
    """

    def __init__(
        self, storage: StorageInterface, dir_id: Optional[bytes], base_path: bytes = b""
    ):
        """
        Args:
            storage (swh.storage.interface.StorageInterface): instance of
                swh storage (either local or remote)
            dir_id (bytes): identifier of a root directory
            base_path (bytes): optional base path used when traversing
                a sub-directory
        """
        self.storage = storage
        self.root_dir_id = dir_id
        self.base_path = base_path
        self.restart()

    def restart(self) -> None:
        """
        Restart the iteration at the beginning.
        """
        # stack of frames representing currently visited directories:
        # the root directory is at the bottom while the current one
        # is at the top
        self.frames: List[List[Dict[str, Any]]] = []
        self._push_dir_frame(self.root_dir_id)
        self.has_started = False

    def _push_dir_frame(self, dir_id: Optional[bytes]) -> None:
        """
        Visit a sub-directory by pushing a new frame to the stack.
        Each frame is itself a stack of directory entries.

        Args:
            dir_id: identifier of a root directory
        """
        # get directory entries
        dir_data = _get_dir(self.storage, dir_id)
        # sort them in lexicographical order and reverse the ordering
        # in order to unstack the "smallest" entry each time the
        # iterator advances
        dir_data = sorted(dir_data, key=lambda e: e["name"], reverse=True)
        # push the directory frame to the main stack
        self.frames.append(dir_data)

    def top(self) -> Optional[List[Dict[str, Any]]]:
        """
        Returns:
            The top frame of the main directories stack
        """
        if not self.frames:
            return None
        return self.frames[-1]

    def current(self) -> Optional[Dict[str, Any]]:
        """
        Returns:
            dict: The current visited directory entry, i.e. the
            top element from the top frame
        """
        top_frame = self.top()
        if not top_frame:
            return None
        return top_frame[-1]

    def current_hash(self) -> bytes:
        """
        Returns:
            The hash value of the currently visited directory entry
        """
        current = self.current()
        assert current is not None
        return current["target"]

    def current_perms(self) -> int:
        """
        Returns:
            The permissions value of the currently visited directory entry
        """
        current = self.current()
        assert current is not None
        return current["perms"]

    def current_path(self) -> Optional[bytes]:
        """
        Returns:
            The absolute path from the root directory of the currently visited
            directory entry
        """
        top_frame = self.top()
        if not top_frame:
            return None
        path = []
        for frame in self.frames:
            path.append(frame[-1]["name"])
        return self.base_path + b"/".join(path)

    def current_is_dir(self) -> bool:
        """
        Returns:
            If the currently visited directory entry is a directory
        """
        current = self.current()
        assert current is not None
        return current["type"] == "dir"

    def _advance(self, descend: bool) -> Optional[Dict[str, Any]]:
        """
        Advance in the tree iteration.

        Args:
            descend: whether or not to push a new frame if the currently
            visited element is a sub-directory

        Returns:
            The description of the newly visited directory entry
        """
        current = self.current()
        if not self.has_started or not current:
            self.has_started = True
            return current

        if descend and self.current_is_dir() and current["target"] != _empty_dir_hash:
            self._push_dir_frame(current["target"])
        else:
            self.drop()

        return self.current()

    def next(self) -> Optional[Dict[str, Any]]:
        """
        Advance the tree iteration by dropping the current visited
        directory entry from the top frame. If the top frame ends up empty,
        the operation is recursively applied to remove all empty frames
        as the tree is climbed up towards its root.

        Returns:
            The description of the newly visited directory entry
        """
        return self._advance(False)

    def step(self) -> Optional[Dict[str, Any]]:
        """
        Advance the tree iteration like the next operation with the
        difference that if the current visited element is a sub-directory
        a new frame representing its content is pushed to the main stack.

        Returns:
            The description of the newly visited directory entry
        """
        return self._advance(True)

    def drop(self) -> None:
        """
        Drop the current visited element from the top frame.
        If the frame ends up empty, the operation is recursively
        applied.
        """
        frame = self.top()
        if not frame:
            return
        frame.pop()
        if not frame:
            self.frames.pop()
            self.drop()

    def __next__(self) -> Dict[str, Any]:
        entry = self.step()
        if not entry:
            raise StopIteration
        entry["path"] = self.current_path()
        return entry

    def __iter__(self) -> DirectoryIterator:
        return DirectoryIterator(self.storage, self.root_dir_id, self.base_path)


def dir_iterator(storage: StorageInterface, dir_id: bytes) -> DirectoryIterator:
    """
    Return an iterator for recursively visiting a directory and
    its sub-directories. The associated paths are visited in
    lexicographic depth-first search order.

    Args:
        storage: an instance of a swh storage
        dir_id: a directory identifier

    Returns:
        an iterator returning a dict at each iteration step describing
        a directory entry. A ``path`` field is added in that dict to store
        the absolute path of the entry.
    """
    return DirectoryIterator(storage, dir_id)


class Remaining(Enum):
    """
    Enum to represent the current state when iterating
    on both directory trees at the same time.
    """

    NoMoreFiles = 0
    OnlyToFilesRemain = 1
    OnlyFromFilesRemain = 2
    BothHaveFiles = 3


class DoubleDirectoryIterator:
    """
    Helper class to traverse two directory trees at the same
    time and compare their contents to detect changes between them.
    """

    def __init__(
        self, storage: StorageInterface, dir_from: Optional[bytes], dir_to: bytes
    ):
        """
        Args:
            storage: instance of swh storage
            dir_from: hash identifier of the from directory
            dir_to: hash identifier of the to directory
        """
        self.storage = storage
        self.dir_from = dir_from
        self.dir_to = dir_to
        self.restart()

    def restart(self) -> None:
        """
        Restart the double iteration at the beginning.
        """
        # initialize custom dfs iterators for the two directories
        self.it_from = DirectoryIterator(self.storage, self.dir_from)
        self.it_to = DirectoryIterator(self.storage, self.dir_to)
        # grab the first element of each iterator
        self.it_from.next()
        self.it_to.next()

    def next_from(self) -> None:
        """
        Apply the next operation on the from iterator.
        """
        self.it_from.next()

    def next_to(self) -> None:
        """
        Apply the next operation on the to iterator.
        """
        self.it_to.next()

    def next_both(self) -> None:
        """
        Apply the next operation on both iterators.
        """
        self.next_from()
        self.next_to()

    def step_from(self) -> None:
        """
        Apply the step operation on the from iterator.
        """
        self.it_from.step()

    def step_to(self) -> None:
        """
        Apply the step operation on the from iterator.
        """
        self.it_to.step()

    def step_both(self) -> None:
        """
        Apply the step operation on the both iterators.
        """
        self.step_from()
        self.step_to()

    def remaining(self) -> Remaining:
        """
        Returns:
            Remaining: the current state of the double iteration
        """
        from_current = self.it_from.current()
        to_current = self.it_to.current()
        # no more files to iterate in both iterators
        if not from_current and not to_current:
            return Remaining.NoMoreFiles
        # still some files to iterate in the to iterator
        elif not from_current and to_current:
            return Remaining.OnlyToFilesRemain
        # still some files to iterate in the from iterator
        elif from_current and not to_current:
            return Remaining.OnlyFromFilesRemain
        # still files to iterate in the both iterators
        else:
            return Remaining.BothHaveFiles

    def compare(self) -> Dict[str, Any]:
        """
        Compare the current iterated directory entries in both iterators
        and return the comparison status.

        Returns:
            The status of the comparison with the following bool values:
                * ``same_hash``: indicates if the two entries have the same hash
                * ``same_perms``: indicates if the two entries have the same
                  permissions
                * ``both_are_dirs``: indicates if the two entries are directories
                * ``both_are_files``: indicates if the two entries are regular
                  files
                * ``file_and_dir``: indicates if one of the entry is a directory
                  and the other a regular file
                * ``from_is_empty_dir``: indicates if the from entry is the
                  empty directory
                * ``to_is_empty_dir``: indicates if the to entry is the
                  empty directory
        """
        from_current_hash = self.it_from.current_hash()
        to_current_hash = self.it_to.current_hash()
        from_current_perms = self.it_from.current_perms()
        to_current_perms = self.it_to.current_perms()
        from_is_dir = self.it_from.current_is_dir()
        to_is_dir = self.it_to.current_is_dir()
        status = {}
        # compare hash
        status["same_hash"] = from_current_hash == to_current_hash
        # compare permissions
        status["same_perms"] = from_current_perms == to_current_perms
        # check if both elements are directories
        status["both_are_dirs"] = from_is_dir and to_is_dir
        # check if both elements are regular files
        status["both_are_files"] = not from_is_dir and not to_is_dir
        # check if one element is a directory, the other a regular file
        status["file_and_dir"] = (
            not status["both_are_dirs"] and not status["both_are_files"]
        )
        # check if the from element is the empty directory
        status["from_is_empty_dir"] = (
            from_is_dir and from_current_hash == _empty_dir_hash
        )
        # check if the to element is the empty directory
        status["to_is_empty_dir"] = to_is_dir and to_current_hash == _empty_dir_hash
        return status
