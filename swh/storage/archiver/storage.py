# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2

from .db import ArchiverDb

from swh.storage.common import db_transaction_generator, db_transaction
from swh.storage.exc import StorageDBError


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
                self.db = ArchiverDb(db_conn)
            else:
                self.db = ArchiverDb.connect(db_conn)
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)

    @db_transaction_generator
    def archive_ls(self, cur=None):
        """ Get all the archives registered on the server.

        Yields:
            a tuple (server_id, server_url) for each archive server.
        """
        yield from self.db.archive_ls(cur)

    @db_transaction
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
    def content_archive_get_copies(self, last_content=None, limit=1000,
                                   cur=None):
        """ Get the list of copies for `limit` contents starting after
           `last_content`.

        Args:
            last_content: sha1 of the last content retrieved. May be None
                              to start at the beginning.
            limit: number of contents to retrieve. Can be None to retrieve all
                   objects (will be slow).

        Yields:
            A tuple (content_id, present_copies, ongoing_copies), where
            ongoing_copies is a dict mapping copy to mtime.

        """
        yield from self.db.content_archive_get_copies(last_content, limit,
                                                      cur)

    @db_transaction_generator
    def content_archive_get_unarchived_copies(
            self, retention_policy, last_content=None,
            limit=1000, cur=None):
        """ Get the list of copies for `limit` contents starting after
            `last_content`. Yields only copies with number of present
            smaller than `retention policy`.

        Args:
            last_content: sha1 of the last content retrieved. May be None
                              to start at the beginning.
            retention_policy: number of required present copies
            limit: number of contents to retrieve. Can be None to retrieve all
                   objects (will be slow).

        Yields:
            A tuple (content_id, present_copies, ongoing_copies), where
            ongoing_copies is a dict mapping copy to mtime.

        """
        yield from self.db.content_archive_get_unarchived_copies(
            retention_policy, last_content, limit, cur)

    @db_transaction_generator
    def content_archive_get_missing(self, content_ids, backend_name, cur=None):
        """Retrieve missing sha1s from source_name.

        Args:
            content_ids ([sha1s]): list of sha1s to test
            source_name (str): Name of the backend to check for content

        Yields:
            missing sha1s from backend_name

        """
        db = self.db

        db.mktemp_content_archive()

        db.copy_to(content_ids, 'tmp_content_archive', ['content_id'], cur)

        for content_id in db.content_archive_get_missing(backend_name, cur):
            yield content_id[0]

    @db_transaction_generator
    def content_archive_get_unknown(self, content_ids, cur=None):
        """Retrieve unknown sha1s from content_archive.

        Args:
            content_ids ([sha1s]): list of sha1s to test

        Yields:
            Unknown sha1s from content_archive

        """
        db = self.db

        db.mktemp_content_archive()

        db.copy_to(content_ids, 'tmp_content_archive', ['content_id'], cur)

        for content_id in db.content_archive_get_unknown(cur):
            yield content_id[0]

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

    @db_transaction
    def content_archive_insert(self, content_id, source, status, cur=None):
        """Insert a new entry in db about content_id.

        Args:
            content_id: content concerned
            source: name of the source
            status: the status of the content for that source

        """
        self.db.content_archive_insert(content_id, source, status, cur)
