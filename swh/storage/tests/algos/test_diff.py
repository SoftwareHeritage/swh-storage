# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# flake8: noqa

import unittest

from nose.tools import istest, nottest
from unittest.mock import patch

from swh.model.identifiers import directory_identifier
from swh.storage.algos import diff


class DirectoryModel(object):
    """
    Quick and dirty directory model to ease the writing
    of revision trees differential tests.
    """
    def __init__(self, name=''):
        self.data = {}
        self.data['name'] = name
        self.data['perms'] = 16384
        self.data['type'] = 'dir'
        self.data['entries'] = []
        self.data['entry_idx'] = {}

    def __getitem__(self, item):
        if item == 'target':
            return directory_identifier(self)
        else:
            return self.data[item]

    def add_file(self, path, sha1=None):
        path_parts = path.split(b'/')
        if len(path_parts) == 1:
            self['entry_idx'][path] = len(self['entries'])
            self['entries'].append({
                'target': sha1,
                'name': path,
                'perms': 33188,
                'type': 'file'
            })
        else:
            if not path_parts[0] in self['entry_idx']:
                self['entry_idx'][path_parts[0]] = len(self['entries'])
                self['entries'].append(DirectoryModel(path_parts[0]))
            if path_parts[1]:
                dir_idx = self['entry_idx'][path_parts[0]]
                self['entries'][dir_idx].add_file(b'/'.join(path_parts[1:]), sha1)

    def get_hash_data(self, entry_hash):
        if self['target'] == entry_hash:
            ret = []
            for e in self['entries']:
                ret.append({
                    'target': e['target'],
                    'name': e['name'],
                    'perms': e['perms'],
                    'type': e['type']
                })
            return ret
        else:
            for e in self['entries']:
                if e['type'] == 'file' and e['target'] == entry_hash:
                    return e
                elif e['type'] == 'dir':
                    data = e.get_hash_data(entry_hash)
                    if data:
                        return data
            return None

    def get_path_data(self, path):
        path_parts = path.split(b'/')
        entry_idx = self['entry_idx'][path_parts[0]]
        entry = self['entries'][entry_idx]
        if len(path_parts) == 1:
            return {
                'target': entry['target'],
                'name': entry['name'],
                'perms': entry['perms'],
                'type': entry['type']
            }
        else:
            return entry.get_path_data(b'/'.join(path_parts[1:]))


@patch('swh.storage.algos.diff._get_rev')
@patch('swh.storage.algos.dir_iterators._get_dir')
class TestDiffRevisions(unittest.TestCase):

    @nottest
    def diff_revisions(self, rev_from, rev_to, from_dir_model, to_dir_model,
                       expected_changes, mock_get_dir, mock_get_rev):

        def _get_rev(*args, **kwargs):
            if args[1] == rev_from:
                return {'directory': from_dir_model['target']}
            else:
                return {'directory': to_dir_model['target']}

        def _get_dir(*args, **kwargs):
            return from_dir_model.get_hash_data(args[1]) or \
                   to_dir_model.get_hash_data(args[1])

        mock_get_rev.side_effect = _get_rev
        mock_get_dir.side_effect = _get_dir

        changes = diff.diff_revisions(None, rev_from, rev_to, track_renaming=True)

        self.assertEqual(changes, expected_changes)

    @istest
    def test_insert_delete(self, mock_get_dir, mock_get_rev):
        rev_from = '898ff03e1e7925ecde3da66327d3cdc7e07625ba'
        rev_to = '647c3d381e67490e82cdbbe6c96e46d5e1628ce2'

        from_dir_model = DirectoryModel()

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b'file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        to_dir_model.add_file(b'file2', '3e5faecb3836ffcadf82cc160787e35d4e2bec6a')
        to_dir_model.add_file(b'file3', '2ae33b2984974d35eababe4890d37fbf4bce6b2c')

        expected_changes = \
            [{
                'type': 'insert',
                'from': None,
                'from_path': None,
                'to': to_dir_model.get_path_data(b'file1'),
                'to_path': b'file1'
            },
            {
                'type': 'insert',
                'from': None,
                'from_path': None,
                'to': to_dir_model.get_path_data(b'file2'),
                'to_path': b'file2'
            },
            {
                'type': 'insert',
                'from': None,
                'from_path': None,
                'to': to_dir_model.get_path_data(b'file3'),
                'to_path': b'file3'
            }]


        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b'file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        from_dir_model.add_file(b'file2', '3e5faecb3836ffcadf82cc160787e35d4e2bec6a')
        from_dir_model.add_file(b'file3', '2ae33b2984974d35eababe4890d37fbf4bce6b2c')

        to_dir_model = DirectoryModel()

        expected_changes = \
            [{
                'type': 'delete',
                'from': from_dir_model.get_path_data(b'file1'),
                'from_path': b'file1',
                'to': None,
                'to_path': None
            },
            {
                'type': 'delete',
                'from': from_dir_model.get_path_data(b'file2'),
                'from_path': b'file2',
                'to': None,
                'to_path': None
            },
            {
                'type': 'delete',
                'from': from_dir_model.get_path_data(b'file3'),
                'from_path': b'file3',
                'to': None,
                'to_path': None
            }]


        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)

    @istest
    def test_onelevel_diff(self, mock_get_dir, mock_get_rev):
        rev_from = '898ff03e1e7925ecde3da66327d3cdc7e07625ba'
        rev_to = '647c3d381e67490e82cdbbe6c96e46d5e1628ce2'

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b'file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        from_dir_model.add_file(b'file2', 'f4a96b2000be83b61254d107046fa9777b17eb34')
        from_dir_model.add_file(b'file3', 'd3c00f9396c6d0277727cec522ff6ad1ea0bc2da')

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b'file2', '3ee0f38ee0ea23cc2c8c0b9d66b27be4596b002b')
        to_dir_model.add_file(b'file3', 'd3c00f9396c6d0277727cec522ff6ad1ea0bc2da')
        to_dir_model.add_file(b'file4', '40460b9653b1dc507e1b6eb333bd4500634bdffc')

        expected_changes =  \
            [{
                'type': 'delete',
                'from': from_dir_model.get_path_data(b'file1'),
                'from_path': b'file1',
                'to': None,
                'to_path': None},
            {
                'type': 'modify',
                'from': from_dir_model.get_path_data(b'file2'),
                'from_path': b'file2',
                'to': to_dir_model.get_path_data(b'file2'),
                'to_path': b'file2'},
            {
                'type': 'insert',
                'from': None,
                'from_path': None,
                'to': to_dir_model.get_path_data(b'file4'),
                'to_path': b'file4'
            }]

        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)

    @istest
    def test_twolevels_diff(self, mock_get_dir, mock_get_rev):
        rev_from = '898ff03e1e7925ecde3da66327d3cdc7e07625ba'
        rev_to = '647c3d381e67490e82cdbbe6c96e46d5e1628ce2'

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b'file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        from_dir_model.add_file(b'dir1/file1', '8335fca266811bac7ae5c8e1621476b4cf4156b6')
        from_dir_model.add_file(b'dir1/file2', 'a6127d909e79f1fcb28bbf220faf86e7be7831e5')
        from_dir_model.add_file(b'dir1/file3', '18049b8d067ce1194a7e1cce26cfa3ae4242a43d')
        from_dir_model.add_file(b'file2', 'd3c00f9396c6d0277727cec522ff6ad1ea0bc2da')

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b'file1', '3ee0f38ee0ea23cc2c8c0b9d66b27be4596b002b')
        to_dir_model.add_file(b'dir1/file2', 'de3548b32a8669801daa02143a66dae21fe852fd')
        to_dir_model.add_file(b'dir1/file3', '18049b8d067ce1194a7e1cce26cfa3ae4242a43d')
        to_dir_model.add_file(b'dir1/file4', 'f5c3f42aec5fe7b92276196c350cbadaf4c51f87')
        to_dir_model.add_file(b'file2', 'd3c00f9396c6d0277727cec522ff6ad1ea0bc2da')

        expected_changes = \
            [{
                'type': 'delete',
                'from': from_dir_model.get_path_data(b'dir1/file1'),
                'from_path': b'dir1/file1',
                'to': None,
                'to_path': None
            },
            {
                'type': 'modify',
                'from': from_dir_model.get_path_data(b'dir1/file2'),
                'from_path': b'dir1/file2',
                'to': to_dir_model.get_path_data(b'dir1/file2'),
                'to_path': b'dir1/file2'
            },
            {
                'type': 'insert',
                'from': None,
                'from_path': None,
                'to': to_dir_model.get_path_data(b'dir1/file4'),
                'to_path': b'dir1/file4'
            },
            {
                'type': 'modify',
                'from': from_dir_model.get_path_data(b'file1'),
                'from_path': b'file1',
                'to': to_dir_model.get_path_data(b'file1'),
                'to_path': b'file1'
            }]

        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)

    @istest
    def test_insert_delete_empty_dirs(self, mock_get_dir, mock_get_rev):
        rev_from = '898ff03e1e7925ecde3da66327d3cdc7e07625ba'
        rev_to = '647c3d381e67490e82cdbbe6c96e46d5e1628ce2'

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b'dir3/file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b'dir3/file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        to_dir_model.add_file(b'dir3/dir1/')

        expected_changes = \
            [{
                'type': 'insert',
                'from': None,
                'from_path': None,
                'to': to_dir_model.get_path_data(b'dir3/dir1'),
                'to_path': b'dir3/dir1'
            }]

        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b'dir1/dir2/')
        from_dir_model.add_file(b'dir1/file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b'dir1/file1', 'ea15f54ca215e7920c60f564315ebb7f911a5204')

        expected_changes = \
            [{
                'type': 'delete',
                'from': from_dir_model.get_path_data(b'dir1/dir2'),
                'from_path': b'dir1/dir2',
                'to': None,
                'to_path': None
            }]

        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)

    @istest
    def test_track_renaming(self, mock_get_dir, mock_get_rev):
        rev_from = '898ff03e1e7925ecde3da66327d3cdc7e07625ba'
        rev_to = '647c3d381e67490e82cdbbe6c96e46d5e1628ce2'

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b'file1_oldname', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        from_dir_model.add_file(b'dir1/file1_oldname', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        from_dir_model.add_file(b'file2_oldname', 'd3c00f9396c6d0277727cec522ff6ad1ea0bc2da')

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b'dir1/file1_newname', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        to_dir_model.add_file(b'dir2/file1_newname', 'ea15f54ca215e7920c60f564315ebb7f911a5204')
        to_dir_model.add_file(b'file2_newname', 'd3c00f9396c6d0277727cec522ff6ad1ea0bc2da')

        expected_changes = \
            [{
                'type': 'rename',
                'from': from_dir_model.get_path_data(b'dir1/file1_oldname'),
                'from_path': b'dir1/file1_oldname',
                'to': to_dir_model.get_path_data(b'dir1/file1_newname'),
                'to_path': b'dir1/file1_newname'
            },
            {
                'type': 'rename',
                'from': from_dir_model.get_path_data(b'file1_oldname'),
                'from_path': b'file1_oldname',
                'to': to_dir_model.get_path_data(b'dir2/file1_newname'),
                'to_path': b'dir2/file1_newname'
            },
            {
                'type': 'rename',
                'from': from_dir_model.get_path_data(b'file2_oldname'),
                'from_path': b'file2_oldname',
                'to': to_dir_model.get_path_data(b'file2_newname'),
                'to_path': b'file2_newname'
            }]

        self.diff_revisions(rev_from, rev_to, from_dir_model,
                            to_dir_model, expected_changes,
                            mock_get_dir, mock_get_rev)
