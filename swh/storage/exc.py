# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List

from swh.model.hashutil import hash_to_hex
from swh.storage.utils import content_bytes_hashes, content_hex_hashes


class StorageDBError(Exception):
    """Specific storage db error (connection, erroneous queries, etc...)

    """

    def __str__(self):
        return "An unexpected error occurred in the backend: %s" % self.args


class StorageAPIError(Exception):
    """Specific internal storage api (mainly connection)

    """

    def __str__(self):
        args = self.args
        return "An unexpected error occurred in the api backend: %s" % args


class StorageArgumentException(Exception):
    """Argument passed to a Storage endpoint is invalid."""

    pass


class HashCollision(Exception):
    """Exception raised when a content collides in a storage backend

    """

    def __init__(self, algo, hash_id, colliding_contents):
        self.algo = algo
        self.hash_id = hash_to_hex(hash_id)
        self.colliding_contents = [content_hex_hashes(c) for c in colliding_contents]
        super().__init__(self.algo, self.hash_id, self.colliding_contents)

    def colliding_content_hashes(self) -> List[Dict[str, bytes]]:
        return [content_bytes_hashes(c) for c in self.colliding_contents]
