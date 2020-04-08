# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from unittest.mock import patch

from swh.model.from_disk import DentryPerms
from swh.model.hashutil import MultiHash, hash_to_bytes
from swh.model.identifiers import directory_identifier
from swh.storage.algos.dir_iterators import dir_iterator

# flake8: noqa


class DirectoryModel(object):
    """
    Quick and dirty directory model to ease the writing
    of directory iterators and revision trees differential tests.
    """

    def __init__(self, name=""):
        self.data = {}
        self.data["name"] = name
        self.data["perms"] = DentryPerms.directory
        self.data["type"] = "dir"
        self.data["entries"] = []
        self.data["entry_idx"] = {}

    def __getitem__(self, item):
        if item == "target":
            return hash_to_bytes(directory_identifier(self))
        else:
            return self.data[item]

    def add_file(self, path, sha1=None):
        path_parts = path.split(b"/")
        sha1 = (
            hash_to_bytes(sha1) if sha1 else MultiHash.from_data(path).digest()["sha1"]
        )
        if len(path_parts) == 1:
            self["entry_idx"][path] = len(self["entries"])
            self["entries"].append(
                {
                    "target": sha1,
                    "name": path,
                    "perms": DentryPerms.content,
                    "type": "file",
                }
            )
        else:
            if not path_parts[0] in self["entry_idx"]:
                self["entry_idx"][path_parts[0]] = len(self["entries"])
                self["entries"].append(DirectoryModel(path_parts[0]))
            if path_parts[1]:
                dir_idx = self["entry_idx"][path_parts[0]]
                self["entries"][dir_idx].add_file(b"/".join(path_parts[1:]), sha1)

    def get_hash_data(self, entry_hash):
        if self["target"] == entry_hash:
            ret = []
            for e in self["entries"]:
                ret.append(
                    {
                        "target": e["target"],
                        "name": e["name"],
                        "perms": e["perms"],
                        "type": e["type"],
                    }
                )
            return ret
        else:
            for e in self["entries"]:
                if e["type"] == "file" and e["target"] == entry_hash:
                    return e
                elif e["type"] == "dir":
                    data = e.get_hash_data(entry_hash)
                    if data:
                        return data
            return None

    def get_path_data(self, path):
        path_parts = path.split(b"/")
        entry_idx = self["entry_idx"][path_parts[0]]
        entry = self["entries"][entry_idx]
        if len(path_parts) == 1:
            return {
                "target": entry["target"],
                "name": entry["name"],
                "perms": entry["perms"],
                "type": entry["type"],
            }
        else:
            return entry.get_path_data(b"/".join(path_parts[1:]))


@patch("swh.storage.algos.dir_iterators._get_dir")
class TestDirectoryIterator(unittest.TestCase):
    def check_iterated_paths(self, dir_model, expected_paths_order, mock_get_dir):
        def _get_dir(*args, **kwargs):
            return dir_model.get_hash_data(args[1])

        mock_get_dir.side_effect = _get_dir  # noqa
        paths_order = [e["path"] for e in dir_iterator(None, dir_model["target"])]
        self.assertEqual(paths_order, expected_paths_order)

    def test_dir_iterator_empty_dir(self, mock_get_dir):
        dir_model = DirectoryModel()
        expected_paths_order = []
        self.check_iterated_paths(dir_model, expected_paths_order, mock_get_dir)

    def test_dir_iterator_no_empty_dirs(self, mock_get_dir):
        dir_model = DirectoryModel()
        dir_model.add_file(b"xyz/gtr/uhb")
        dir_model.add_file(b"bca/ef")
        dir_model.add_file(b"abc/ab")
        dir_model.add_file(b"abc/bc")
        dir_model.add_file(b"xyz/ouy/poi")

        expected_paths_order = [
            b"abc",
            b"abc/ab",
            b"abc/bc",
            b"bca",
            b"bca/ef",
            b"xyz",
            b"xyz/gtr",
            b"xyz/gtr/uhb",
            b"xyz/ouy",
            b"xyz/ouy/poi",
        ]

        self.check_iterated_paths(dir_model, expected_paths_order, mock_get_dir)

    def test_dir_iterator_with_empty_dirs(self, mock_get_dir):
        dir_model = DirectoryModel()
        dir_model.add_file(b"xyz/gtr/")
        dir_model.add_file(b"bca/ef")
        dir_model.add_file(b"abc/")
        dir_model.add_file(b"xyz/ouy/poi")

        expected_paths_order = [
            b"abc",
            b"bca",
            b"bca/ef",
            b"xyz",
            b"xyz/gtr",
            b"xyz/ouy",
            b"xyz/ouy/poi",
        ]

        self.check_iterated_paths(dir_model, expected_paths_order, mock_get_dir)
