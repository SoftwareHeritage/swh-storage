# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# flake8: noqa

import unittest
from unittest.mock import patch

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.storage.algos import diff

from .test_dir_iterator import DirectoryModel


def test__get_rev(swh_storage, sample_data):
    revision = sample_data.revision

    # does not exist then raises
    with pytest.raises(AssertionError):
        diff._get_rev(swh_storage, revision.id)

    # otherwise, we retrieve its dict representation
    swh_storage.revision_add([revision])
    actual_revision = diff._get_rev(swh_storage, revision.id)
    assert actual_revision == revision.to_dict()


@patch("swh.storage.algos.diff._get_rev")
@patch("swh.storage.algos.dir_iterators._get_dir")
class TestDiffRevisions(unittest.TestCase):
    def diff_revisions(
        self,
        rev_from,
        rev_to,
        from_dir_model,
        to_dir_model,
        expected_changes,
        mock_get_dir,
        mock_get_rev,
    ):
        rev_from_bytes = hash_to_bytes(rev_from)
        rev_to_bytes = hash_to_bytes(rev_to)

        def _get_rev(*args, **kwargs):
            if args[1] == rev_from_bytes:
                return {"directory": from_dir_model["target"]}
            else:
                return {"directory": to_dir_model["target"]}

        def _get_dir(*args, **kwargs):
            from_dir = from_dir_model.get_hash_data(args[1])
            to_dir = to_dir_model.get_hash_data(args[1])
            return from_dir if from_dir != None else to_dir

        mock_get_rev.side_effect = _get_rev
        mock_get_dir.side_effect = _get_dir

        changes = diff.diff_revisions(
            None, rev_from_bytes, rev_to_bytes, track_renaming=True
        )

        self.assertEqual(changes, expected_changes)

    def test_insert_delete(self, mock_get_dir, mock_get_rev):
        rev_from = "898ff03e1e7925ecde3da66327d3cdc7e07625ba"
        rev_to = "647c3d381e67490e82cdbbe6c96e46d5e1628ce2"

        from_dir_model = DirectoryModel()

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b"file1", "ea15f54ca215e7920c60f564315ebb7f911a5204")
        to_dir_model.add_file(b"file2", "3e5faecb3836ffcadf82cc160787e35d4e2bec6a")
        to_dir_model.add_file(b"file3", "2ae33b2984974d35eababe4890d37fbf4bce6b2c")

        expected_changes = [
            {
                "type": "insert",
                "from": None,
                "from_path": None,
                "to": to_dir_model.get_path_data(b"file1"),
                "to_path": b"file1",
            },
            {
                "type": "insert",
                "from": None,
                "from_path": None,
                "to": to_dir_model.get_path_data(b"file2"),
                "to_path": b"file2",
            },
            {
                "type": "insert",
                "from": None,
                "from_path": None,
                "to": to_dir_model.get_path_data(b"file3"),
                "to_path": b"file3",
            },
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b"file1", "ea15f54ca215e7920c60f564315ebb7f911a5204")
        from_dir_model.add_file(b"file2", "3e5faecb3836ffcadf82cc160787e35d4e2bec6a")
        from_dir_model.add_file(b"file3", "2ae33b2984974d35eababe4890d37fbf4bce6b2c")

        to_dir_model = DirectoryModel()

        expected_changes = [
            {
                "type": "delete",
                "from": from_dir_model.get_path_data(b"file1"),
                "from_path": b"file1",
                "to": None,
                "to_path": None,
            },
            {
                "type": "delete",
                "from": from_dir_model.get_path_data(b"file2"),
                "from_path": b"file2",
                "to": None,
                "to_path": None,
            },
            {
                "type": "delete",
                "from": from_dir_model.get_path_data(b"file3"),
                "from_path": b"file3",
                "to": None,
                "to_path": None,
            },
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )

    def test_onelevel_diff(self, mock_get_dir, mock_get_rev):
        rev_from = "898ff03e1e7925ecde3da66327d3cdc7e07625ba"
        rev_to = "647c3d381e67490e82cdbbe6c96e46d5e1628ce2"

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b"file1", "ea15f54ca215e7920c60f564315ebb7f911a5204")
        from_dir_model.add_file(b"file2", "f4a96b2000be83b61254d107046fa9777b17eb34")
        from_dir_model.add_file(b"file3", "d3c00f9396c6d0277727cec522ff6ad1ea0bc2da")

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b"file2", "3ee0f38ee0ea23cc2c8c0b9d66b27be4596b002b")
        to_dir_model.add_file(b"file3", "d3c00f9396c6d0277727cec522ff6ad1ea0bc2da")
        to_dir_model.add_file(b"file4", "40460b9653b1dc507e1b6eb333bd4500634bdffc")

        expected_changes = [
            {
                "type": "delete",
                "from": from_dir_model.get_path_data(b"file1"),
                "from_path": b"file1",
                "to": None,
                "to_path": None,
            },
            {
                "type": "modify",
                "from": from_dir_model.get_path_data(b"file2"),
                "from_path": b"file2",
                "to": to_dir_model.get_path_data(b"file2"),
                "to_path": b"file2",
            },
            {
                "type": "insert",
                "from": None,
                "from_path": None,
                "to": to_dir_model.get_path_data(b"file4"),
                "to_path": b"file4",
            },
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )

    def test_twolevels_diff(self, mock_get_dir, mock_get_rev):
        rev_from = "898ff03e1e7925ecde3da66327d3cdc7e07625ba"
        rev_to = "647c3d381e67490e82cdbbe6c96e46d5e1628ce2"

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b"file1", "ea15f54ca215e7920c60f564315ebb7f911a5204")
        from_dir_model.add_file(
            b"dir1/file1", "8335fca266811bac7ae5c8e1621476b4cf4156b6"
        )
        from_dir_model.add_file(
            b"dir1/file2", "a6127d909e79f1fcb28bbf220faf86e7be7831e5"
        )
        from_dir_model.add_file(
            b"dir1/file3", "18049b8d067ce1194a7e1cce26cfa3ae4242a43d"
        )
        from_dir_model.add_file(b"file2", "d3c00f9396c6d0277727cec522ff6ad1ea0bc2da")

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b"file1", "3ee0f38ee0ea23cc2c8c0b9d66b27be4596b002b")
        to_dir_model.add_file(b"dir1/file2", "de3548b32a8669801daa02143a66dae21fe852fd")
        to_dir_model.add_file(b"dir1/file3", "18049b8d067ce1194a7e1cce26cfa3ae4242a43d")
        to_dir_model.add_file(b"dir1/file4", "f5c3f42aec5fe7b92276196c350cbadaf4c51f87")
        to_dir_model.add_file(b"file2", "d3c00f9396c6d0277727cec522ff6ad1ea0bc2da")

        expected_changes = [
            {
                "type": "delete",
                "from": from_dir_model.get_path_data(b"dir1/file1"),
                "from_path": b"dir1/file1",
                "to": None,
                "to_path": None,
            },
            {
                "type": "modify",
                "from": from_dir_model.get_path_data(b"dir1/file2"),
                "from_path": b"dir1/file2",
                "to": to_dir_model.get_path_data(b"dir1/file2"),
                "to_path": b"dir1/file2",
            },
            {
                "type": "insert",
                "from": None,
                "from_path": None,
                "to": to_dir_model.get_path_data(b"dir1/file4"),
                "to_path": b"dir1/file4",
            },
            {
                "type": "modify",
                "from": from_dir_model.get_path_data(b"file1"),
                "from_path": b"file1",
                "to": to_dir_model.get_path_data(b"file1"),
                "to_path": b"file1",
            },
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )

    def test_insert_delete_empty_dirs(self, mock_get_dir, mock_get_rev):
        rev_from = "898ff03e1e7925ecde3da66327d3cdc7e07625ba"
        rev_to = "647c3d381e67490e82cdbbe6c96e46d5e1628ce2"

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(
            b"dir3/file1", "ea15f54ca215e7920c60f564315ebb7f911a5204"
        )

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b"dir3/file1", "ea15f54ca215e7920c60f564315ebb7f911a5204")
        to_dir_model.add_file(b"dir3/dir1/")

        expected_changes = [
            {
                "type": "insert",
                "from": None,
                "from_path": None,
                "to": to_dir_model.get_path_data(b"dir3/dir1"),
                "to_path": b"dir3/dir1",
            }
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(b"dir1/dir2/")
        from_dir_model.add_file(
            b"dir1/file1", "ea15f54ca215e7920c60f564315ebb7f911a5204"
        )

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(b"dir1/file1", "ea15f54ca215e7920c60f564315ebb7f911a5204")

        expected_changes = [
            {
                "type": "delete",
                "from": from_dir_model.get_path_data(b"dir1/dir2"),
                "from_path": b"dir1/dir2",
                "to": None,
                "to_path": None,
            }
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )

    def test_track_renaming(self, mock_get_dir, mock_get_rev):
        rev_from = "898ff03e1e7925ecde3da66327d3cdc7e07625ba"
        rev_to = "647c3d381e67490e82cdbbe6c96e46d5e1628ce2"

        from_dir_model = DirectoryModel()
        from_dir_model.add_file(
            b"file1_oldname", "ea15f54ca215e7920c60f564315ebb7f911a5204"
        )
        from_dir_model.add_file(
            b"dir1/file1_oldname", "ea15f54ca215e7920c60f564315ebb7f911a5204"
        )
        from_dir_model.add_file(
            b"file2_oldname", "d3c00f9396c6d0277727cec522ff6ad1ea0bc2da"
        )

        to_dir_model = DirectoryModel()
        to_dir_model.add_file(
            b"dir1/file1_newname", "ea15f54ca215e7920c60f564315ebb7f911a5204"
        )
        to_dir_model.add_file(
            b"dir2/file1_newname", "ea15f54ca215e7920c60f564315ebb7f911a5204"
        )
        to_dir_model.add_file(
            b"file2_newname", "d3c00f9396c6d0277727cec522ff6ad1ea0bc2da"
        )

        expected_changes = [
            {
                "type": "rename",
                "from": from_dir_model.get_path_data(b"dir1/file1_oldname"),
                "from_path": b"dir1/file1_oldname",
                "to": to_dir_model.get_path_data(b"dir1/file1_newname"),
                "to_path": b"dir1/file1_newname",
            },
            {
                "type": "rename",
                "from": from_dir_model.get_path_data(b"file1_oldname"),
                "from_path": b"file1_oldname",
                "to": to_dir_model.get_path_data(b"dir2/file1_newname"),
                "to_path": b"dir2/file1_newname",
            },
            {
                "type": "rename",
                "from": from_dir_model.get_path_data(b"file2_oldname"),
                "from_path": b"file2_oldname",
                "to": to_dir_model.get_path_data(b"file2_newname"),
                "to_path": b"file2_newname",
            },
        ]

        self.diff_revisions(
            rev_from,
            rev_to,
            from_dir_model,
            to_dir_model,
            expected_changes,
            mock_get_dir,
            mock_get_rev,
        )
