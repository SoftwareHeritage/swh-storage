# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import time

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
    def content_archive_get(self, content=None, cur=None):
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
        yield from self.db.content_archive_get(content, cur)

    @db_transaction
    def content_archive_update(self, content_id, archive_id,
                               new_status=None, cur=None):
        """ Update the status of an archive content and set its mtime to

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
        # FIXME check how to alter direclty the json object with postgres
        # Get the data and alter it
        copies = self.db.content_archive_get(content_id)['copies']
        if new_status is not None:
            copies[archive_id]['status'] = new_status
        copies[archive_id]['mtime'] = int(time.time())

        # Then save the new data
        self.db.content_archive_update(content_id, copies)
