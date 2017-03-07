# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import os
import psycopg2
import time

from .db import ArchiverDb

from swh.core import hashutil

from swh.storage.common import db_transaction_generator, db_transaction
from swh.storage.exc import StorageDBError


class ArchiverStorage():
    """SWH Archiver storage proxy, encompassing DB

    """
    def __init__(self, dbconn):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection

        """
        try:
            if isinstance(dbconn, psycopg2.extensions.connection):
                self.db = ArchiverDb(dbconn)
            else:
                self.db = ArchiverDb.connect(dbconn)
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
    def content_archive_add(
            self, content_ids, sources_present, cur=None):
        """Insert a new entry in db about content_id.

        Args:
            content_ids ([bytes|str]): content identifiers
            sources_present ([str]): List of source names where
                                     contents are present
        """
        db = self.db

        # Prepare copies dictionary
        copies = {}
        for source in sources_present:
            copies[source] = {
                "status": "present",
                "mtime": int(time.time()),
            }

        copies = json.dumps(copies)
        num_present = len(sources_present)

        db.mktemp('content_archive')
        db.copy_to(
            ({'content_id': id,
              'copies': copies,
              'num_present': num_present}
             for id in content_ids),
            'tmp_content_archive',
            ['content_id', 'copies', 'num_present'],
            cur)
        db.content_archive_add_from_temp(cur)


class StubArchiverStorage():
    def __init__(self, archives, present, missing, logfile_base):
        """
        A stub storage for the archiver that doesn't write to disk

        Args:
            - archives: a dictionary mapping archive names to archive URLs
            - present: archives where the objects are all considered present
            - missing: archives where the objects are all considered missing
            - logfile_base: basename for the logfile
        """
        self.archives = archives
        self.present = set(present)
        self.missing = set(missing)
        if set(archives) != self.present | self.missing:
            raise ValueError("Present and missing archives don't match")
        self.logfile_base = logfile_base
        self.__logfile = None

    def open_logfile(self):
        if self.__logfile:
            return

        logfile_name = "%s.%d" % (self.logfile_base, os.getpid())
        self.__logfile = open(logfile_name, 'a')

    def close_logfile(self):
        if not self.__logfile:
            return

        self.__logfile.close()
        self.__logfile = None

    def archive_ls(self, cur=None):
        """ Get all the archives registered on the server.

        Yields:
            a tuple (server_id, server_url) for each archive server.
        """
        yield from self.archives.items()

    def content_archive_get(self, content_id, cur=None):
        """ Get the archival status of a content.

        Retrieve from the database the archival status of the given content

        Args:
            content_id: the sha1 of the content

        Yields:
            A tuple (content_id, present_copies, ongoing_copies), where
            ongoing_copies is a dict mapping copy to mtime.
        """
        return (content_id, self.present, {})

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
        yield from []

    def content_archive_get_unarchived_copies(self, retention_policy,
                                              last_content=None, limit=1000,
                                              cur=None):
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
        yield from []

    def content_archive_get_missing(self, content_ids, backend_name, cur=None):
        """Retrieve missing sha1s from source_name.

        Args:
            content_ids ([sha1s]): list of sha1s to test
            source_name (str): Name of the backend to check for content

        Yields:
            missing sha1s from backend_name

        """
        if backend_name in self.missing:
            yield from content_ids
        elif backend_name in self.present:
            yield from []
        else:
            raise ValueError('Unknown backend `%s`' % backend_name)

    def content_archive_get_unknown(self, content_ids, cur=None):
        """Retrieve unknown sha1s from content_archive.

        Args:
            content_ids ([sha1s]): list of sha1s to test

        Yields:
            Unknown sha1s from content_archive

        """
        yield from []

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
        if not self.__logfile:
            self.open_logfile()

        print(time.time(), archive_id, new_status,
              hashutil.hash_to_hex(content_id), file=self.__logfile)

    def content_archive_add(
            self, content_ids, sources_present, cur=None):
        """Insert a new entry in db about content_id.

        Args:
            content_ids ([bytes|str]): content identifiers
            sources_present ([str]): List of source names where
                                     contents are present
        """
        pass


def get_archiver_storage(cls, args):
    """Instantiate an archiver database with the proper class and arguments"""
    if cls == 'db':
        return ArchiverStorage(**args)
    elif cls == 'stub':
        return StubArchiverStorage(**args)
    else:
        raise ValueError('Unknown Archiver Storage class `%s`' % cls)
