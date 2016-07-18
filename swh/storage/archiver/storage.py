# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2

from ..common import db_transaction_generator
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
    def content_archive_ls(self, cur=None):
        """ Get the archival status of the content

        Get an iterable over all the content that is referenced
        in a backup server.

        Yields:
            the sha1 of each content referenced at least one time
            in the database of archiveal status.

        """
        yield from self.db.content_archive_ls(cur)

    @db_transaction_generator
    def content_archive_get(self, content=None, archive=None, cur=None):
        """ Get the archival status of a content in a specific server.

        Retreive from the database the archival status of the given content
        in the given archive server.

        Args:
            content: the sha1 of the content. May be None for any id.
            archive: the database id of the server we're looking into
                may be None for any server.

        Yields:
            A tuple (content_id, server_id, archival status, mtime, tzinfo).
        """
        yield from self.db.content_archive_get(content, archive, cur)

    @db_transaction_generator
    def content_archive_update(self, content_id, archive_id,
                               new_status=None, cur=None):
        """ Update the status of a archive content and set it's mtime to now()

        Change the last modification time of an archived content and change
        its status to the given one.

        Args:
            content_id (string): The content id.
            archive_id (string): The id of the concerned archive.
            new_status (string): One of missing, ongoing or present, this
                status will replace the previous one. If not given, the
                function only changes the mtime of the content.
        """
        yield from self.db.content_archive_update(content_id,
                                                  archive_id,
                                                  new_status,
                                                  cur)
