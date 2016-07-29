# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2

from ..common import db_transaction_generator, db_transaction
from ..db import Db
from ..exc import StorageDBError


class ArchiverStorage():
    """SWH Archiver storage proxy, encompassing DB

    """
    def __init__(self, db_conn):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection

        """
        try:
            if isinstance(db_conn, psycopg2.extensions.connection):
                self.db = Db(db_conn)
            else:
                self.db = Db.connect(db_conn)
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)

    @db_transaction_generator
    def archive_ls(self, cur=None):
        """ Get all the archives registered on the server.

        Yields:
            a tuple (server_id, server_url) for each archive server.
        """
        yield from self.db.archive_ls(cur)

    @db_transaction_generator
    def content_archive_get(self, content_id, cur=None):
        """ Get the archival status of a content.

        Retrieve from the database the archival status of the given content

        Args:
            content_id: the sha1 of the content

        Yields:
            A tuple (content_id, present_copies, ongoing_copies), where
            ongoing_copies is a dict mapping copy to mtime.
        """
        return self.db.content_archive_get(content_id, cur)

    @db_transaction_generator
    def content_archive_get_copies(self, previous_content=None, limit=1000,
                                   cur=None):
        """Get the list of copies for `limit` contents starting after
           `previous_content`.

        Args:
            previous_content: sha1 of the last content retrieved. May be None
                              to start at the beginning.
            limit: number of contents to retrieve. Can be None to retrieve all
                   objects (will be slow).

        Yields:
            A tuple (content_id, present_copies, ongoing_copies), where
            ongoing_copies is a dict mapping copy to mtime.

        """
        yield from self.db.content_archive_get_copies(previous_content, limit,
                                                      cur)

    @db_transaction
    def content_archive_update(self, content_id, archive_id,
                               new_status=None, cur=None):
        """ Update the status of an archive content and set its mtime to now

        Change the mtime of an archived content for the given archive and set
        it's mtime to the current time.

        Args:
            content_id (str): content sha1
            archive_id (str): name of the archive
            new_status (str): one of 'missing', 'present' or 'ongoing'.
                this status will replace the previous one. If not given,
                the function only change the mtime of the content for the
                given archive.
        """
        self.db.content_archive_update(content_id, archive_id, new_status, cur)
