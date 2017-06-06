# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.storage.db import BaseDb, cursor_to_bytes, stored_procedure


def utcnow():
    return datetime.datetime.now(tz=datetime.timezone.utc)


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
        query = """select archive.name, status, mtime
                   from content_copies
                   left join archive on content_copies.archive_id = archive.id
                   where content_copies.content_id = (
                          select id from content where sha1 = %s)
        """
        cur = self._cursor(cur)
        cur.execute(query, (content_id,))
        rows = cur.fetchall()
        if not rows:
            return None
        present = []
        ongoing = {}
        for archive, status, mtime in rows:
            if status == 'present':
                present.append(archive)
            elif status == 'ongoing':
                ongoing[archive] = mtime
        return (content_id, present, ongoing)

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

        vars = {
            'limit': limit,
        }

        if last_content is None:
            last_content_clause = 'true'
        else:
            last_content_clause = """content_id > (
                                      select id from content
                                      where sha1 = %(last_content)s)"""
            vars['last_content'] = last_content

        query = """select
                       (select sha1 from content where id = content_id),
                       array_agg((select name from archive
                                  where id = archive_id))
                   from content_copies
                   where status = 'present' and %s
                   group by content_id
                   order by content_id
                   limit %%(limit)s""" % last_content_clause

        cur = self._cursor(cur)
        cur.execute(query, vars)
        for content_id, present in cursor_to_bytes(cur):
            yield (content_id, present, {})

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

        vars = {
            'limit': limit,
            'retention_policy': retention_policy,
        }

        if last_content is None:
            last_content_clause = 'true'
        else:
            last_content_clause = """content_id > (
                                      select id from content
                                      where sha1 = %(last_content)s)"""
            vars['last_content'] = last_content

        query = """select
                       (select sha1 from content where id = content_id),
                       array_agg((select name from archive
                                  where id = archive_id))
                   from content_copies
                   where status = 'present' and %s
                   group by content_id
                   having count(archive_id) < %%(retention_policy)s
                   order by content_id
                   limit %%(limit)s""" % last_content_clause

        cur = self._cursor(cur)
        cur.execute(query, vars)
        for content_id, present in cursor_to_bytes(cur):
            yield (content_id, present, {})

    @stored_procedure('swh_mktemp_content_archive')
    def mktemp_content_archive(self, cur=None):
        """Trigger the creation of the temporary table tmp_content_archive
        during the lifetime of the transaction.

        """
        pass

    @stored_procedure('swh_content_archive_add')
    def content_archive_add_from_temp(self, cur=None):
        """Add new content archive entries from temporary table.

        Use from archiver.storage module:
            self.db.mktemp_content_archive()
            # copy data over to the temp table
            self.db.copy_to([{'colname': id0}, {'colname': id1}],
                            'tmp_cache_content',
                            ['colname'], cur)
            # insert into the main table
            self.db.add_content_archive_from_temp(cur)

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
        assert isinstance(content_id, bytes)
        assert new_status is not None

        query = """insert into content_copies (archive_id, content_id, status, mtime)
                   values ((select id from archive where name=%s),
                           (select id from content where sha1=%s),
                           %s, %s)
                   on conflict (archive_id, content_id) do
                   update set status = excluded.status, mtime = excluded.mtime
        """

        cur = self._cursor(cur)
        cur.execute(query, (archive_id, content_id, new_status, utcnow()))
