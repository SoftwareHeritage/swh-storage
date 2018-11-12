# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import heapq

from abc import ABCMeta, abstractmethod
from collections import deque

_revs_walker_classes = {}


class _RevisionsWalkerMetaClass(ABCMeta):
    def __new__(cls, clsname, bases, attrs):
        newclass = super().__new__(cls, clsname, bases, attrs)
        if 'rw_type' in attrs:
            _revs_walker_classes[attrs['rw_type']] = newclass
        return newclass


class RevisionsWalker(metaclass=_RevisionsWalkerMetaClass):
    """
    Abstract base class encapsulating the logic to walk across
    a revisions history starting from a given one.

    It defines an iterator returning the revisions according
    to a specific ordering implemented in derived classes.

    The iteration step performs the following operations:

        1) Check if the iteration is finished by calling method
           :meth:`is_finished` and raises :exc:`StopIteration` if it
           it is the case

        2) Get the next unseen revision by calling method
           :meth:`get_next_rev_id`

        3) Process parents of that revision by calling method
           :meth:`process_parent_revs` for the next iteration
           steps

        4) Check if the revision should be returned by calling
           method :meth:`should_return` and returns it if
           it is the case

    In order to easily instantiate a specific type of revisions
    walker, it is recommended to use the factory function
    :func:`get_revisions_walker`.

    Args:
        storage (swh.storage.storage.Storage): instance of swh storage
            (either local or remote)
        rev_start (bytes): a revision identifier
        max_revs (Optional[int]): maximum number of revisions to return
        state (Optional[dict]): previous state of that revisions walker
    """

    def __init__(self, storage, rev_start, max_revs=None, state=None):
        self._revs_to_visit = []
        self._done = set()
        self._revs = {}
        self._last_rev = None
        self._num_revs = 0
        self._max_revs = max_revs
        if state:
            self._revs_to_visit = state['revs_to_visit']
            self._done = state['done']
            self._last_rev = state['last_rev']
            self._num_revs = state['num_revs']
        self.storage = storage
        self.process_rev(rev_start)

    @abstractmethod
    def process_rev(self, rev_id):
        """
        Abstract method whose purpose is to process a newly visited
        revision during the walk.
        Derived classes must implement it according to the desired
        method to walk across the revisions history (for instance
        through a dfs on the revisions DAG).

        Args:
            rev_id (bytes): the newly visited revision identifier
        """
        pass

    @abstractmethod
    def get_next_rev_id(self):
        """
        Abstract method whose purpose is to return the next revision
        during the iteration.
        Derived classes must implement it according to the desired
        method to walk across the revisions history.

        Returns:
            dict: A dict describing a revision as returned by
            :meth:`swh.storage.storage.Storage.revision_get`
        """
        pass

    def process_parent_revs(self, rev):
        """
        Process the parents of a revision when it is iterated.
        The default implementation simply calls :meth:`process_rev`
        for each parent revision in the order they are declared.

        Args:
            rev (dict): A dict describing a revision as returned by
                :meth:`swh.storage.storage.Storage.revision_get`
        """
        for parent_id in rev['parents']:
            self.process_rev(parent_id)

    def should_return(self, rev):
        """
        Filter out a revision to return if needed.
        Default implementation returns all iterated revisions.

        Args:
            rev (dict): A dict describing a revision as returned by
                :meth:`swh.storage.storage.Storage.revision_get`

        Returns:
            bool: Whether to return the revision in the iteration
        """
        return True

    def is_finished(self):
        """
        Determine if the iteration is finished.
        This method is called at the beginning of each iteration loop.

        Returns:
            bool: Whether the iteration is finished
        """
        if self._max_revs is not None and self._num_revs >= self._max_revs:
            return True
        if not self._revs_to_visit:
            return True
        return False

    def _get_rev(self, rev_id):
        rev = self._revs.get(rev_id, None)
        if not rev:
            # cache some revisions in advance to avoid sending too much
            # requests to storage and thus speedup the revisions walk
            for rev in self.storage.revision_log([rev_id], limit=100):
                self._revs[rev['id']] = rev
        return self._revs[rev_id]

    def export_state(self):
        """
        Export the internal state of that revision walker to a dict.
        Its purpose is to continue the iteration in a pagination context.

        Returns:
            dict: A dict containing the internal state of that revisions walker
        """
        return {
            'revs_to_visit': self._revs_to_visit,
            'done': self._done,
            'last_rev': self._last_rev,
            'num_revs': self._num_revs
        }

    def __next__(self):
        if self.is_finished():
            raise StopIteration
        while self._revs_to_visit:
            rev_id = self.get_next_rev_id()
            if rev_id in self._done:
                continue
            self._done.add(rev_id)
            rev = self._get_rev(rev_id)
            self.process_parent_revs(rev)
            if self.should_return(rev):
                self._num_revs += 1
                self._last_rev = rev
                return rev
        raise StopIteration

    def __iter__(self):
        return self


class CommitterDateRevisionsWalker(RevisionsWalker):
    """
    Revisions walker that returns revisions in reverse chronological
    order according to committer date (same behaviour as ``git log``)
    """

    rw_type = 'committer_date'

    def process_rev(self, rev_id):
        """
        Add the revision to a priority queue according to the committer date.

        Args:
            rev_id (bytes): the newly visited revision identifier
        """
        if rev_id not in self._done:
            rev = self._get_rev(rev_id)
            commit_time = rev['committer_date']['timestamp']['seconds']
            heapq.heappush(self._revs_to_visit, (-commit_time, rev_id))

    def get_next_rev_id(self):
        """
        Return the smallest revision from the priority queue, i.e.
        the one with highest committer date.

        Returns:
            dict: A dict describing a revision as returned by
            :meth:`swh.storage.storage.Storage.revision_get`
        """
        _, rev_id = heapq.heappop(self._revs_to_visit)
        return rev_id


class BFSRevisionsWalker(RevisionsWalker):
    """
    Revisions walker that returns revisions in the same order
    as when performing a breadth-first search on the revisions
    DAG.
    """

    rw_type = 'bfs'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._revs_to_visit = deque(self._revs_to_visit)

    def process_rev(self, rev_id):
        """
        Append the revision to a queue.

        Args:
            rev_id (bytes): the newly visited revision identifier
        """
        if rev_id not in self._done:
            self._revs_to_visit.append(rev_id)

    def get_next_rev_id(self):
        """
        Return the next revision from the queue.

        Returns:
            dict: A dict describing a revision as returned by
            :meth:`swh.storage.storage.Storage.revision_get`
        """
        return self._revs_to_visit.popleft()


class DFSPostRevisionsWalker(RevisionsWalker):
    """
    Revisions walker that returns revisions in the same order
    as when performing a depth-first search in post-order on the
    revisions DAG (i.e. after visiting a merge commit,
    the merged commit will be visited before the base it was
    merged on).
    """

    rw_type = 'dfs_post'

    def process_rev(self, rev_id):
        """
        Append the revision to a stack.

        Args:
            rev_id (bytes): the newly visited revision identifier
        """
        if rev_id not in self._done:
            self._revs_to_visit.append(rev_id)

    def get_next_rev_id(self):
        """
        Return the next revision from the stack.

        Returns:
            dict: A dict describing a revision as returned by
            :meth:`swh.storage.storage.Storage.revision_get`
        """
        return self._revs_to_visit.pop()


class DFSRevisionsWalker(DFSPostRevisionsWalker):
    """
    Revisions walker that returns revisions in the same order
    as when performing a depth-first search in pre-order on the
    revisions DAG (i.e. after visiting a merge commit,
    the base commit it was merged on will be visited before
    the merged commit).
    """

    rw_type = 'dfs'

    def process_parent_revs(self, rev):
        """
        Process the parents of a revision when it is iterated in
        the reversed order they are declared.

        Args:
            rev (dict): A dict describing a revision as returned by
                :meth:`swh.storage.storage.Storage.revision_get`
        """
        for parent_id in reversed(rev['parents']):
            self.process_rev(parent_id)


class PathRevisionsWalker(CommitterDateRevisionsWalker):
    """
    Revisions walker that returns revisions where a specific
    path in the source tree has been modified, in other terms
    it allows to get the history for a specific file or directory.

    It has a behaviour similar to what ``git log`` offers by default,
    meaning the returned history is simplified in order to only
    show relevant revisions (see the `History Simplification
    <https://git-scm.com/docs/git-log#_history_simplification>`_
    section of the associated manual for more details).

    Please note that to avoid walking the entire history, the iteration
    will stop once a revision where the path has been added is found.

    .. warning:: Due to client-side implementation, performances
        are not optimal when the total numbers of revisions to walk
        is large. This should only be used when the total number of
        revisions does not exceed a couple of thousands.

    Args:
        storage (swh.storage.storage.Storage): instance of swh storage
            (either local or remote)
        rev_start (bytes): a revision identifier
        path (str): the path in the source tree to retrieve the history
        max_revs (Optional[int]): maximum number of revisions to return
        state (Optional[dict]): previous state of that revisions walker
    """

    rw_type = 'path'

    def __init__(self, storage, rev_start, path, **kwargs):
        super().__init__(storage, rev_start, **kwargs)
        paths = path.strip('/').split('/')
        self._path = list(map(lambda p: p.encode('utf-8'), paths))
        self._rev_dir_path = {}

    def _get_path_id(self, rev_id):
        """
        Return the path checksum identifier in the source tree of the
        provided revision. If the path corresponds to a directory, the
        value computed by :func:`swh.model.identifiers.directory_identifier`
        will be returned. If the path corresponds to a file, its sha1
        checksum will be returned.

        Args:
            rev_id (bytes): a revision identifier

        Returns:
            bytes: the path identifier
        """

        rev = self._get_rev(rev_id)

        rev_dir_id = rev['directory']

        if rev_dir_id not in self._rev_dir_path:
            try:
                dir_info = \
                    self.storage.directory_entry_get_by_path(rev_dir_id,
                                                             self._path)
                self._rev_dir_path[rev_dir_id] = dir_info['target']
            except Exception:
                self._rev_dir_path[rev_dir_id] = None

        return self._rev_dir_path[rev_dir_id]

    def is_finished(self):
        """
        Check if the revisions iteration is finished.
        This checks for the specified path's existence in the last
        returned revision's parents' source trees.
        If not, the iteration is considered finished.

        Returns:
            bool: Whether to return the revision in the iteration
        """
        if self._path and self._last_rev:
            last_rev_parents = self._last_rev['parents']
            last_rev_parents_path_ids = [self._get_path_id(p_rev)
                                         for p_rev in last_rev_parents]
            no_path = all([path_id is None
                           for path_id in last_rev_parents_path_ids])
            if no_path:
                return True
        return super().is_finished()

    def process_parent_revs(self, rev):
        """
        Process parents when a new revision is iterated.
        It enables to get a simplified revisions history in the same
        manner as ``git log``. When a revision has multiple parents,
        the following process is applied. If the revision was a merge,
        and has the same path identifier to one parent, follow only that
        parent (even if there are several parents with the same path
        identifier, follow only one of them.) Otherwise, follow all parents.

        Args:
            rev (dict): A dict describing a revision as returned by
                :meth:`swh.storage.storage.Storage.revision_get`
        """
        rev_path_id = self._get_path_id(rev['id'])

        if rev_path_id:
            if len(rev['parents']) == 1:
                self.process_rev(rev['parents'][0])
            else:
                parent_rev_path_ids = [self._get_path_id(p_rev)
                                       for p_rev in rev['parents']]
                different_trees = all([path_id != rev_path_id
                                       for path_id in parent_rev_path_ids])
                for i, p_rev in enumerate(rev['parents']):
                    if different_trees or \
                            parent_rev_path_ids[i] == rev_path_id:
                        self.process_rev(p_rev)
                        if not different_trees:
                            break
        else:
            super().process_parent_revs(rev)

    def should_return(self, rev):
        """
        Check if a revision should be returned when iterating.
        It verifies that the specified path has been modified
        by the revision but also that all parents have a path
        identifier different from the revision one in order
        to get a simplified history.

        Args:
            rev (dict): A dict describing a revision as returned by
                :meth:`swh.storage.storage.Storage.revision_get`

        Returns:
            bool: Whether to return the revision in the iteration
        """
        rev_path_id = self._get_path_id(rev['id'])

        if not rev['parents']:
            return rev_path_id is not None

        parent_rev_path_ids = [self._get_path_id(p_rev)
                               for p_rev in rev['parents']]
        different_trees = all([path_id != rev_path_id
                               for path_id in parent_rev_path_ids])

        if rev_path_id != parent_rev_path_ids[0] and different_trees:
            return True

        return False


def get_revisions_walker(rev_walker_type, *args, **kwargs):
    """
    Instantiate a revisions walker of a given type.

    The following code snippet demonstrates how to use a revisions
    walker for processing a whole revisions history::

        from swh.storage import get_storage

        storage = get_storage(...)

        revs_walker = get_revisions_walker('committer_date', storage, rev_id)
        for rev in revs_walker:
            # process revision rev

    It is also possible to walk a revisions history in a paginated
    way as illustrated below::

        def get_revs_history_page(rw_type, storage, rev_id, page_num,
                                  page_size, rw_state):
            max_revs = (page_num + 1) * page_size
            revs_walker = get_revisions_walker(rw_type, storage, rev_id,
                                               max_revs=max_revs,
                                               state=rw_state)
            revs = list(revs_walker)
            rw_state = revs_walker.export_state()
            return revs

        rev_start = ...
        per_page = 50
        rw_state = {}

        for page in range(0, 10):
            revs_page = get_revs_history_page('dfs', storage, rev_start, page,
                                              per_page, rw_state)
            # process revisions page


    Args:
        rev_walker_type (str): the type of revisions walker to return,
            possible values are: *committer_date*, *dfs*, *dfs_post*,
            *bfs* and *path*
        args (list): position arguments to pass to the revisions walker
            constructor
        kwargs (dict): keyword arguments to pass to the revisions walker
            constructor

    """
    if rev_walker_type not in _revs_walker_classes:
        raise Exception('No revisions walker found for type "%s"'
                        % rev_walker_type)
    revs_walker_class = _revs_walker_classes[rev_walker_type]
    return revs_walker_class(*args, **kwargs)
