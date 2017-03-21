# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import fastimport.commands
import functools
import logging
import os

from .base import BaseVaultCooker


class RevisionGitCooker(BaseVaultCooker):
    """Cooker to create a git fast-import bundle """
    CACHE_TYPE_KEY = 'revision_git'

    def prepare_bundle(self):
        commands = self.fastexport(self.storage.revision_log([self.obj_id]))
        bundle_content = b'\n'.join(bytes(command) for command in commands)
        return bundle_content

    def fastexport(self, log):
        """Generate all the git fast-import commands from a given log.
        """
        self.rev_by_id = {r['id']: r for r in log}
        self.rev_sorted = list(self._toposort(self.rev_by_id))
        self.obj_done = set()
        self.obj_to_mark = {}
        self.next_available_mark = 1

        # We want a single transaction for the whole export, so we store a
        # cursor and use it during the process.
        with self.storage.db.transaction() as self.cursor:
            for i, rev in enumerate(self.rev_sorted, 1):
                logging.info('Computing revision %d/%d', i,
                             len(self.rev_sorted))
                yield from self._compute_commit_command(rev)

    def _toposort(self, rev_by_id):
        """Perform a topological sort on the revision graph.
        """
        children = collections.defaultdict(list)  # rev -> children
        in_degree = {}  # rev -> numbers of parents left to compute

        # Compute the in_degrees and the parents of all the revisions.
        # Add the roots to the processing queue.
        queue = collections.deque()
        for rev_id, rev in rev_by_id.items():
            in_degree[rev_id] = len(rev['parents'])
            if not rev['parents']:
                queue.append(rev_id)
            for parent in rev['parents']:
                children[parent].append(rev_id)

        # Topological sort: yield the 'ready' nodes, decrease the in degree of
        # their children and add the 'ready' ones to the queue.
        while queue:
            rev_id = queue.popleft()
            yield rev_by_id[rev_id]
            for child in children[rev_id]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

    def mark(self, obj_id):
        """Get the mark ID as bytes of a git object.

        If the object has not yet been marked, assign a new ID and add it to
        the mark dictionary.
        """
        if obj_id not in self.obj_to_mark:
            self.obj_to_mark[obj_id] = self.next_available_mark
            self.next_available_mark += 1
        return str(self.obj_to_mark[obj_id]).encode()

    def _compute_blob_command_content(self, file_data):
        """Compute the blob command of a file entry if it has not been
        computed yet.
        """
        obj_id = file_data['sha1']
        if obj_id in self.obj_done:
            return
        content = list(self.storage.content_get([obj_id]))[0]['data']
        yield fastimport.commands.BlobCommand(
            mark=self.mark(obj_id),
            data=content,
        )
        self.obj_done.add(obj_id)

    def _compute_commit_command(self, rev):
        """Compute a commit command from a specific revision.
        """
        if 'parents' in rev and rev['parents']:
            from_ = b':' + self.mark(rev['parents'][0])
            merges = [b':' + self.mark(r) for r in rev['parents'][1:]]
            parent = self.rev_by_id[rev['parents'][0]]
        else:
            # We issue a reset command before all the new roots so that they
            # are not automatically added as children of the current branch.
            yield fastimport.commands.ResetCommand(b'refs/heads/master', None)
            from_ = None
            merges = None
            parent = None

        # Retrieve the file commands while yielding new blob commands if
        # needed.
        files = yield from self._compute_file_commands(rev, parent)

        # Construct and yield the commit command
        author = (rev['author']['name'],
                  rev['author']['email'],
                  rev['date']['timestamp']['seconds'],
                  rev['date']['offset'] * 60)
        committer = (rev['committer']['name'],
                     rev['committer']['email'],
                     rev['committer_date']['timestamp']['seconds'],
                     rev['committer_date']['offset'] * 60)
        yield fastimport.commands.CommitCommand(
            ref=b'refs/heads/master',
            mark=self.mark(rev['id']),
            author=author,
            committer=committer,
            message=rev['message'],
            from_=from_,
            merges=merges,
            file_iter=files,
        )

    @functools.lru_cache(maxsize=4096)
    def _get_dir_ents(self, dir_id=None):
        """Get the entities of a directory as a dictionary (name -> entity).

        This function has a cache to avoid doing multiple requests to retrieve
        the same entities, as doing a directory_ls() is expensive.
        """
        data = (self.storage.directory_ls(dir_id, cur=self.cursor)
                if dir_id is not None else [])
        return {f['name']: f for f in data}

    def _compute_file_commands(self, rev, parent=None):
        """Compute all the file commands of a revision.

        Generate a diff of the files between the revision and its main parent
        to find the necessary file commands to apply.
        """
        commands = []

        # Initialize the stack with the root of the tree.
        cur_dir = rev['directory']
        parent_dir = parent['directory'] if parent else None
        stack = [(b'', cur_dir, parent_dir)]

        while stack:
            # Retrieve the current directory and the directory of the parent
            # commit in order to compute the diff of the trees.
            root, cur_dir_id, prev_dir_id = stack.pop()
            cur_dir = self._get_dir_ents(cur_dir_id)
            prev_dir = self._get_dir_ents(prev_dir_id)

            # Find subtrees to delete:
            #  - Subtrees that are not in the new tree (file or directory
            #    deleted).
            #  - Subtrees that do not have the same type in the new tree
            #    (file -> directory or directory -> file)
            # After this step, every node remaining in the previous directory
            # has the same type than the one in the current directory.
            for fname, f in prev_dir.items():
                if ((fname not in cur_dir
                     or f['type'] != cur_dir[fname]['type'])):
                    commands.append(fastimport.commands.FileDeleteCommand(
                        path=os.path.join(root, fname)
                    ))

            # Find subtrees to modify:
            #  - Leaves (files) will be added or modified using `filemodify`
            #  - Other subtrees (directories) will be added to the stack and
            #    processed in the next iteration.
            for fname, f in cur_dir.items():
                # A file is added or modified if it was not in the tree, if its
                # permissions changed or if its content changed.
                if (f['type'] == 'file'
                    and (fname not in prev_dir
                         or f['sha1'] != prev_dir[fname]['sha1']
                         or f['perms'] != prev_dir[fname]['perms'])):
                    # Issue a blob command for the new blobs if needed.
                    yield from self._compute_blob_command_content(f)
                    commands.append(fastimport.commands.FileModifyCommand(
                        path=os.path.join(root, fname),
                        mode=f['perms'],
                        dataref=(b':' + self.mark(f['sha1'])),
                        data=None,
                    ))
                # A directory is added or modified if it was not in the tree or
                # if its target changed.
                elif f['type'] == 'dir':
                    f_prev_target = None
                    if fname in prev_dir and prev_dir[fname]['type'] == 'dir':
                        f_prev_target = prev_dir[fname]['target']
                    if f_prev_target is None or f['target'] != f_prev_target:
                        stack.append((os.path.join(root, fname),
                                      f['target'], f_prev_target))
        return commands
