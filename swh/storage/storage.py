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
    def add_content(self, content):
        """Add content blobs to the storage

        Note: in case of DB errors, objects might have already been added to
        the object storage and will not be removed. Since addition to the
        object storage is idempotent, that should not be a problem.

        Args:
            content: iterable of dictionaries representing individual pieces of
                content to add. Each dictionary has the following keys:
                - data (bytes): the actual content
                - one key for each checksum algorithm in
                  swh.core.hashutil(ALGORITHMS), mapped to the corresponding
                  checksum

        """
        (db, cur) = (self.db, self.cur)
        # create temporary table for metadata injection
        db.content_mktemp(cur)

        with tempfile.TemporaryFile('w+') as f:
            # prepare tempfile for metadata COPY + add content data to
            # object storage
            for cont in content:
                cont['length'] = len(cont['data'])
                line = '\t'.join([cont['sha1'], cont['sha1_git'],
                                  cont['sha256'], str(len(cont['data']))])\
                    + '\n'
                f.write(line)
                self.objstorage.add_bytes(cont['data'],
                                          obj_id=cont['sha1'])

            # COPY metadata to temporary table
            f.seek(0)
            db.content_copy_to_temp(f, cur)

        # move metadata in place
        db.content_add_from_temp(cur)
        db.conn.commit()
