# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import fastimport.commands

from .base import BaseVaultCooker


class RevisionGitCooker(BaseVaultCooker):
    """Cooker to create a git fast-import bundle """
    CACHE_TYPE_KEY = 'revision_git'

    def prepare_bundle(self, obj_id):
        commands = self.fastexport(self.storage.revision_log([obj_id]))
        bundle_content = b'\n'.join(bytes(command) for command in commands)
        return bundle_content

    def fastexport(self, log):
        """Generate all the git fast-import commands from a given log.
        """
        self.rev_by_id = {r['id']: r for r in log}
        self.rev_sorted = list(self._toposort(self.rev_by_id))
        self.dir_by_id = {}
        self.obj_done = set()
        self.obj_to_mark = {}
        self.next_available_mark = 1

        yield from self._compute_all_blob_commands()
        yield from self._compute_all_commit_commands()

    def _toposort(self, rev_by_id):
        """Perform a topological sort on the revision graph.
        """
        children = collections.defaultdict(list)
        in_degree = collections.defaultdict(int)
        for rev_id, rev in rev_by_id.items():
            for parent in rev['parents']:
                in_degree[rev_id] += 1
                children[parent].append(rev_id)

        queue = collections.deque()
        for rev_id in rev_by_id.keys():
            if in_degree[rev_id] == 0:
                queue.append(rev_id)

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

    def _compute_all_blob_commands(self):
        """Compute all the blob commands to populate the empty git repository.

        Mark the populated blobs so that we are able to reference them in file
        commands.

        """
        for rev in self.rev_sorted:
            yield from self._compute_blob_commands_in_dir(rev['directory'])

    def _compute_blob_commands_in_dir(self, dir_id):
        """Find all the blobs in a directory and generate their blob commands.

        If a blob has already been visited and marked, skip it.
        """
        data = self.storage.directory_ls(dir_id, recursive=True)
        files_data = list(entry for entry in data if entry['type'] == 'file')
        self.dir_by_id[dir_id] = files_data
        for file_data in files_data:
            obj_id = file_data['sha1']
            if obj_id in self.obj_done:
                continue
            content = list(self.storage.content_get([obj_id]))[0]['data']
            yield fastimport.commands.BlobCommand(
                mark=self.mark(obj_id),
                data=content,
            )
            self.obj_done.add(obj_id)

    def _compute_all_commit_commands(self):
        """Compute all the commit commands.
        """
        for rev in self.rev_sorted:
            yield from self._compute_commit_command(rev)

    def _compute_commit_command(self, rev):
        """Compute a commit command from a specific revision.
        """
        from_ = None
        merges = None
        parent = None
        if 'parents' in rev and rev['parents']:
            from_ = b':' + self.mark(rev['parents'][0])
            merges = [b':' + self.mark(r) for r in rev['parents'][1:]]
            parent = self.rev_by_id[rev['parents'][0]]
        files = self._compute_file_commands(rev, parent)
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

    def _compute_file_commands(self, rev, parent=None):
        """Compute all the file commands of a revision.

        Generate a diff of the files between the revision and its main parent
        to find the necessary file commands to apply.
        """
        if not parent:
            parent_dir = []
        else:
            parent_dir = self.dir_by_id[parent['directory']]
        cur_dir = self.dir_by_id[rev['directory']]
        parent_dir = {f['name']: f for f in parent_dir}
        cur_dir = {f['name']: f for f in cur_dir}

        for fname, f in cur_dir.items():
            if ((fname not in parent_dir
                 or f['sha1'] != parent_dir[fname]['sha1']
                 or f['perms'] != parent_dir[fname]['perms'])):
                yield fastimport.commands.FileModifyCommand(
                    path=f['name'],
                    mode=f['perms'],
                    dataref=(b':' + self.mark(f['sha1'])),
                    data=None,
                )

        for fname, f in parent_dir.items():
            if fname not in cur_dir:
                yield fastimport.commands.FileDeleteCommand(
                    path=f['name']
                )
