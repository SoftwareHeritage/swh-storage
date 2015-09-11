# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import psycopg2
import tempfile

from .db import Db
from .objstorage import ObjStorage


def db_transaction(meth):
    """decorator to execute Storage methods within DB transactions

    Decorated methods will have access to the following attributes:
        self.cur: psycopg2 DB cursor

    """
    def _meth(self, *args, **kwargs):
        with self.db.transaction() as cur:
            try:
                self.cur = cur
                meth(self, *args, **kwargs)
            finally:
                self.cur = None
    return _meth


class Storage():
    """SWH storage proxy, encompassing DB and object storage

    """

    def __init__(self, db_conn, obj_root):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection
            obj_root: path to the root of the object storage

        """
        if isinstance(db_conn, psycopg2.extensions.connection):
            self.db = Db(db_conn)
        else:
            self.db = Db.connect(db_conn)

        self.objstorage = ObjStorage(obj_root)

    @db_transaction
    def content_add(self, content):
        """Add content blobs to the storage

        Note: in case of DB errors, objects might have already been added to
        the object storage and will not be removed. Since addition to the
        object storage is idempotent, that should not be a problem.

        Args:
            content: iterable of dictionaries representing individual pieces of
                content to add. Each dictionary has the following keys:
                - data (bytes): the actual content
                - length (int): content length
                - one key for each checksum algorithm in
                  swh.core.hashutil.ALGORITHMS, mapped to the corresponding
                  checksum

        """
        (db, cur) = (self.db, self.cur)
        # create temporary table for metadata injection
        db.mktemp('content', cur)

        def add_to_objstorage(cont):
            self.objstorage.add_bytes(cont['data'], obj_id=cont['sha1'])

        db.copy_to(content, 'tmp_content',
                   ['sha1', 'sha1_git', 'sha256', 'length'],
                   cur, item_cb=add_to_objstorage)

        # move metadata in place
        db.content_add_from_temp(cur)
        db.conn.commit()

    def content_missing(self, content):
        """List content missing from storage

        Args:
            content: iterable of dictionaries containing one key for
            each checksum algorithm in swh.core.hashutil.ALGORITHMS,
            mapped to the corresponding checksum, and a length key
            mapped to the content length.

        Returns:
            a list of sha1s missing from the storage

        Raises:
            TODO: an exception when we get a hash collision.

        """
        pass

    def directory_add(self, directories):
        """Add directories to the storage

        Args:
            directories: iterable of dictionaries representing the
                individual directories to add. Each dict has the following keys:
                - id (sha1_git): the id of the directory to add
                - entries (list): list of dicts for each entry in the directory.
                    Each dict has the following keys:
                    - name (bytes)
                    - type (one of 'file', 'dir', 'rev'):
                        type of the directory entry (file, directory, revision)
                    - id (sha1_git): id of the object pointed at by the directory entry
                    - perms (int): entry permissions
                    - atime (datetime.DateTime): entry access time
                    - ctime (datetime.DateTime): entry creation time
                    - mtime (datetime.DateTime): entry modification time
        """
        pass

    def directory_missing(self, directories):
        """List directories missing from storage

        Args: an iterable of directory ids
        Returns: a list of missing directory ids
        """
        pass

    def revision_add(self, revisions):
        """Add revisions to the storage

        Args:
        """
        pass

    def revision_missing(self, s):
        """List revisions missing from storage

        Args: an iterable of revision ids
        Returns: a list of missing revision ids
        """
        pass

    def release_add(self, releases):
        """Add releases to the storage

        Args:
        """
        pass

    def release_missing(self, releases):
        """List releases missing from storage

        Args: an iterable of release ids
        Returns: a list of missing release ids
        """
        pass

    def occurence_add(self, occurences):
        """Add occurences to the storage

        Args:
        """
        pass

    def origin_add(self, origins):
        """Add origins to the storage

        Args:
        """
        pass
