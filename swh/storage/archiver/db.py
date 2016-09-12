# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import time

from swh.storage.db import BaseDb, cursor_to_bytes


class ArchiverDb(BaseDb):
    """Proxy to the SWH's archiver DB

    """

    def archive_ls(self, cur=None):
        """ Get all the archives registered on the server.

        Yields:
            a tuple (server_id, server_url) for each archive server.
        """
        cur = self._cursor(cur)
        cur.execute("""SELECT id, url
                    FROM archive
                    """)
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
        content_id, present, ongoing, mtimes = cur.fetchone()
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
