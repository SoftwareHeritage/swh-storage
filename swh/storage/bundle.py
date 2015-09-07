# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import tempfile

from .db import Db
from .objstorage import ObjStorage

TMP_CONTENT_TABLE = 'tmp_content'


class Storage():
    """SWH storage proxy, encompassing DB and object storage

    """

    def __init__(self, db_connstring, obj_root):
        """
        Args:
            db_connstring: libpq connection string to reach the SWH db
            obj_root: path to the root of the object storage

        """
        self.db = Db(db_connstring)
        self.objstorage = ObjStorage(obj_root)

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
        with self.db.cursor() as c:
            # create temporary table for metadata injection
            c.execute('SELECT swh_content_mktemp()')

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
                c.copy_from(f, TMP_CONTENT_TABLE,
                            columns=('sha1', 'sha1_git', 'sha256', 'length'))

            # move metadata in place and save
            c.execute('SELECT swh_content_add()')

        self.db.commit()
