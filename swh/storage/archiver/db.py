# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import time

from swh.core import hashutil
from swh.storage.db import BaseDb, cursor_to_bytes, stored_procedure


class ArchiverDb(BaseDb):
    """Proxy to the SWH's archiver DB

    """

    def archive_ls(self, cur=None):
        """ Get all the archives registered on the server.

        Yields:
            a tuple (server_id, server_url) for each archive server.
        """
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM archive")
        yield from cursor_to_bytes(cur)

    def content_archive_get(self, content_id, cur=None):
        """ Get the archival status of a content in a specific server.

        Retrieve from the database the archival status of the given content
        in the given archive server.

        Args:
            content_id: the sha1 of the content.

        Yields:
            A tuple (content_id, present_copies, ongoing_copies), where
            ongoing_copies is a dict mapping copy to mtime.
        """
        query = """SELECT content_id,
                          array(
                            SELECT key
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'present'
                            ORDER BY key
                          ) AS present,
                          array(
                            SELECT key
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'ongoing'
                            ORDER BY key
                          ) AS ongoing,
                          array(
                            SELECT value->'mtime'
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'ongoing'
                            ORDER BY key
                          ) AS ongoing_mtime
                   FROM content_archive
                   WHERE content_id = %s
                   ORDER BY content_id
        """
        cur = self._cursor(cur)
        cur.execute(query, (content_id,))
        row = cur.fetchone()
        if not row:
            return None
        content_id, present, ongoing, mtimes = row
        return (content_id, present, dict(zip(ongoing, mtimes)))

    def content_archive_get_copies(self, last_content=None, limit=1000,
                                   cur=None):
        """Get the list of copies for `limit` contents starting after
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

        query = """SELECT content_id,
                          array(
                            SELECT key
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'present'
                            ORDER BY key
                          ) AS present,
                          array(
                            SELECT key
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'ongoing'
                            ORDER BY key
                          ) AS ongoing,
                          array(
                            SELECT value->'mtime'
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'ongoing'
                            ORDER BY key
                          ) AS ongoing_mtime
                   FROM content_archive
                   WHERE content_id > %s
                   ORDER BY content_id
                   LIMIT %s
        """

        if last_content is None:
            last_content = b''

        cur = self._cursor(cur)
        cur.execute(query, (last_content, limit))
        for content_id, present, ongoing, mtimes in cursor_to_bytes(cur):
            yield (content_id, present, dict(zip(ongoing, mtimes)))

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

        query = """SELECT content_id,
                          array(
                            SELECT key
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'present'
                            ORDER BY key
                          ) AS present,
                          array(
                            SELECT key
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'ongoing'
                            ORDER BY key
                          ) AS ongoing,
                          array(
                            SELECT value->'mtime'
                            FROM jsonb_each(copies)
                            WHERE value->>'status' = 'ongoing'
                            ORDER BY key
                          ) AS ongoing_mtime
                   FROM content_archive
                   WHERE content_id > %s AND num_present < %s
                   ORDER BY content_id
                   LIMIT %s
        """

        if last_content is None:
            last_content = b''

        cur = self._cursor(cur)
        cur.execute(query, (last_content, retention_policy, limit))
        for content_id, present, ongoing, mtimes in cursor_to_bytes(cur):
            yield (content_id, present, dict(zip(ongoing, mtimes)))

    @stored_procedure('swh_mktemp_content_archive')
    def mktemp_content_archive(self, cur=None):
        """Trigger the creation of the temporary table tmp_content_archive
        during the lifetime of the transaction.

        Use from archiver.storage module:
            self.db.mktemp_content_archive()
            # copy data over to the temp table
            self.db.copy_to([{'colname': id0}, {'colname': id1}],
                            'tmp_cache_content',
                            ['colname'], cur)

        """
        pass

    def content_archive_get_missing(self, backend_name, cur=None):
        """Retrieve the content missing from backend_name.

        """
        cur = self._cursor(cur)
        cur.execute("select * from swh_content_archive_missing(%s)",
                    (backend_name,))
        yield from cursor_to_bytes(cur)

    def content_archive_get_unknown(self, cur=None):
        """Retrieve unknown sha1 from archiver db.

        """
        cur = self._cursor(cur)
        cur.execute('select * from swh_content_archive_unknown()')
        yield from cursor_to_bytes(cur)

    def content_archive_insert(self, content_id, source, status, cur=None):
        """Insert a new entry in the db for the content_id.

        Args:
            content_id: content concerned
            source: name of the source
            status: the status of the content for that source

        """
        if isinstance(content_id, bytes):
            content_id = '\\x%s' % hashutil.hash_to_hex(content_id)

        query = """INSERT INTO content_archive(content_id, copies, num_present)
                   VALUES('%s', '{"%s": {"status": "%s", "mtime": %d}}', 1)
                    """ % (content_id, source, status, int(time.time()))
        cur = self._cursor(cur)
        cur.execute(query)

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
        if isinstance(content_id, bytes):
            content_id = '\\x%s' % hashutil.hash_to_hex(content_id)

        if new_status is not None:
            query = """UPDATE content_archive
                    SET copies=jsonb_set(
                        copies, '{%s}',
                        '{"status":"%s", "mtime":%d}'
                    )
                    WHERE content_id='%s'
                    """ % (archive_id,
                           new_status, int(time.time()),
                           content_id)
        else:
            query = """ UPDATE content_archive
                    SET copies=jsonb_set(copies, '{%s,mtime}', '%d')
                    WHERE content_id='%s'
                    """ % (archive_id, int(time.time()))

        cur = self._cursor(cur)
        cur.execute(query)

    def content_archive_content_add(
            self, content_id, sources_present, sources_missing, cur=None):

        if isinstance(content_id, bytes):
            content_id = '\\x%s' % hashutil.hash_to_hex(content_id)

        copies = {}
        num_present = 0
        for source in sources_present:
            copies[source] = {
                "status": "present",
                "mtime": int(time.time()),
            }
            num_present += 1

        for source in sources_missing:
            copies[source] = {
                "status": "absent",
            }

        query = """INSERT INTO content_archive(content_id, copies, num_present)
                   VALUES('%s', '%s', %s)
                    """ % (content_id, json.dumps(copies), num_present)
        cur = self._cursor(cur)
        cur.execute(query)
