# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import select

from swh.core.db import BaseDb
from swh.core.db.db_utils import stored_procedure, jsonize
from swh.core.db.db_utils import execute_values_generator
from swh.model.model import SHA1_SIZE


class Db(BaseDb):
    """Proxy to the SWH DB, with wrappers around stored procedures

    """

    def mktemp_dir_entry(self, entry_type, cur=None):
        self._cursor(cur).execute('SELECT swh_mktemp_dir_entry(%s)',
                                  (('directory_entry_%s' % entry_type),))

    @stored_procedure('swh_mktemp_revision')
    def mktemp_revision(self, cur=None): pass

    @stored_procedure('swh_mktemp_release')
    def mktemp_release(self, cur=None): pass

    @stored_procedure('swh_mktemp_snapshot_branch')
    def mktemp_snapshot_branch(self, cur=None): pass

    def register_listener(self, notify_queue, cur=None):
        """Register a listener for NOTIFY queue `notify_queue`"""
        self._cursor(cur).execute("LISTEN %s" % notify_queue)

    def listen_notifies(self, timeout):
        """Listen to notifications for `timeout` seconds"""
        if select.select([self.conn], [], [], timeout) == ([], [], []):
            return
        else:
            self.conn.poll()
            while self.conn.notifies:
                yield self.conn.notifies.pop(0)

    @stored_procedure('swh_content_add')
    def content_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_directory_add')
    def directory_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_skipped_content_add')
    def skipped_content_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_revision_add')
    def revision_add_from_temp(self, cur=None): pass

    @stored_procedure('swh_release_add')
    def release_add_from_temp(self, cur=None): pass

    def content_update_from_temp(self, keys_to_update, cur=None):
        cur = self._cursor(cur)
        cur.execute("""select swh_content_update(ARRAY[%s] :: text[])""" %
                    keys_to_update)

    content_get_metadata_keys = [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length', 'status']

    content_add_keys = content_get_metadata_keys + ['ctime']

    skipped_content_keys = [
        'sha1', 'sha1_git', 'sha256', 'blake2s256',
        'length', 'reason', 'status', 'origin']

    def content_get_metadata_from_sha1s(self, sha1s, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur, """
            select t.sha1, %s from (values %%s) as t (sha1)
            inner join content using (sha1)
            """ % ', '.join(self.content_get_metadata_keys[1:]),
            ((sha1,) for sha1 in sha1s),
        )

    def content_get_range(self, start, end, limit=None, cur=None):
        """Retrieve contents within range [start, end].

        """
        cur = self._cursor(cur)
        query = """select %s from content
                   where %%s <= sha1 and sha1 <= %%s
                   order by sha1
                   limit %%s""" % ', '.join(self.content_get_metadata_keys)
        cur.execute(query, (start, end, limit))
        yield from cur

    content_hash_keys = ['sha1', 'sha1_git', 'sha256', 'blake2s256']

    def content_missing_from_list(self, contents, cur=None):
        cur = self._cursor(cur)

        keys = ', '.join(self.content_hash_keys)
        equality = ' AND '.join(
            ('t.%s = c.%s' % (key, key))
            for key in self.content_hash_keys
        )

        yield from execute_values_generator(
            cur, """
            SELECT %s
            FROM (VALUES %%s) as t(%s)
            WHERE NOT EXISTS (
                SELECT 1 FROM content c
                WHERE %s
            )
            """ % (keys, keys, equality),
            (tuple(c[key] for key in self.content_hash_keys) for c in contents)
        )

    def content_missing_per_sha1(self, sha1s, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(cur, """
        SELECT t.sha1 FROM (VALUES %s) AS t(sha1)
        WHERE NOT EXISTS (
            SELECT 1 FROM content c WHERE c.sha1 = t.sha1
        )""", ((sha1,) for sha1 in sha1s))

    def content_missing_per_sha1_git(self, contents, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(cur, """
        SELECT t.sha1_git FROM (VALUES %s) AS t(sha1_git)
        WHERE NOT EXISTS (
            SELECT 1 FROM content c WHERE c.sha1_git = t.sha1_git
        )""", ((sha1,) for sha1 in contents))

    def skipped_content_missing(self, contents, cur=None):
        if not contents:
            return []
        cur = self._cursor(cur)

        query = """SELECT * FROM (VALUES %s) AS t (%s)
                   WHERE not exists
                   (SELECT 1 FROM skipped_content s WHERE
                       s.sha1 is not distinct from t.sha1::sha1 and
                       s.sha1_git is not distinct from t.sha1_git::sha1 and
                       s.sha256 is not distinct from t.sha256::bytea);""" % \
                ((', '.join('%s' for _ in contents)),
                 ', '.join(self.content_hash_keys))
        cur.execute(query,
                    [tuple(cont[key] for key in self.content_hash_keys)
                     for cont in contents])

        yield from cur

    def snapshot_exists(self, snapshot_id, cur=None):
        """Check whether a snapshot with the given id exists"""
        cur = self._cursor(cur)

        cur.execute("""SELECT 1 FROM snapshot where id=%s""", (snapshot_id,))

        return bool(cur.fetchone())

    def snapshot_missing_from_list(self, snapshots, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur, """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM snapshot d WHERE d.id = t.id
            )
                """, ((id,) for id in snapshots))

    def snapshot_add(self, snapshot_id, cur=None):
        """Add a snapshot from the temporary table"""
        cur = self._cursor(cur)

        cur.execute("""SELECT swh_snapshot_add(%s)""", (snapshot_id,))

    snapshot_count_cols = ['target_type', 'count']

    def snapshot_count_branches(self, snapshot_id, cur=None):
        cur = self._cursor(cur)
        query = """\
           SELECT %s FROM swh_snapshot_count_branches(%%s)
        """ % ', '.join(self.snapshot_count_cols)

        cur.execute(query, (snapshot_id,))

        yield from cur

    snapshot_get_cols = ['snapshot_id', 'name', 'target', 'target_type']

    def snapshot_get_by_id(self, snapshot_id, branches_from=b'',
                           branches_count=None, target_types=None,
                           cur=None):
        cur = self._cursor(cur)
        query = """\
           SELECT %s
           FROM swh_snapshot_get_by_id(%%s, %%s, %%s, %%s :: snapshot_target[])
        """ % ', '.join(self.snapshot_get_cols)

        cur.execute(query, (snapshot_id, branches_from, branches_count,
                            target_types))

        yield from cur

    def snapshot_get_by_origin_visit(self, origin_url, visit_id, cur=None):
        cur = self._cursor(cur)
        query = """\
           SELECT snapshot FROM origin_visit
           INNER JOIN origin ON origin.id = origin_visit.origin
           WHERE origin.url=%s AND origin_visit.visit=%s;
        """

        cur.execute(query, (origin_url, visit_id))
        ret = cur.fetchone()
        if ret:
            return ret[0]

    def snapshot_get_random(self, cur=None):
        return self._get_random_row_from_table(
            'snapshot', ['id'], 'id', cur)

    content_find_cols = ['sha1', 'sha1_git', 'sha256', 'blake2s256', 'length',
                         'ctime', 'status']

    def content_find(self, sha1=None, sha1_git=None, sha256=None,
                     blake2s256=None, cur=None):
        """Find the content optionally on a combination of the following
        checksums sha1, sha1_git, sha256 or blake2s256.

        Args:
            sha1: sha1 content
            git_sha1: the sha1 computed `a la git` sha1 of the content
            sha256: sha256 content
            blake2s256: blake2s256 content

        Returns:
            The tuple (sha1, sha1_git, sha256, blake2s256) if found or None.

        """
        cur = self._cursor(cur)

        checksum_dict = {'sha1': sha1, 'sha1_git': sha1_git,
                         'sha256': sha256, 'blake2s256': blake2s256}
        where_parts = []
        args = []
        # Adds only those keys which have value other than None
        for algorithm in checksum_dict:
            if checksum_dict[algorithm] is not None:
                args.append(checksum_dict[algorithm])
                where_parts.append(algorithm + '= %s')
        query = ' AND '.join(where_parts)
        cur.execute("""SELECT %s
                       FROM content WHERE %s
                       """
                    % (','.join(self.content_find_cols),  query),
                    args)
        content = cur.fetchall()
        return content

    def content_get_random(self, cur=None):
        return self._get_random_row_from_table(
            'content', ['sha1_git'], 'sha1_git', cur)

    def directory_missing_from_list(self, directories, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur, """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM directory d WHERE d.id = t.id
            )
            """, ((id,) for id in directories))

    directory_ls_cols = ['dir_id', 'type', 'target', 'name', 'perms',
                         'status', 'sha1', 'sha1_git', 'sha256', 'length']

    def directory_walk_one(self, directory, cur=None):
        cur = self._cursor(cur)
        cols = ', '.join(self.directory_ls_cols)
        query = 'SELECT %s FROM swh_directory_walk_one(%%s)' % cols
        cur.execute(query, (directory,))
        yield from cur

    def directory_walk(self, directory, cur=None):
        cur = self._cursor(cur)
        cols = ', '.join(self.directory_ls_cols)
        query = 'SELECT %s FROM swh_directory_walk(%%s)' % cols
        cur.execute(query, (directory,))
        yield from cur

    def directory_entry_get_by_path(self, directory, paths, cur=None):
        """Retrieve a directory entry by path.

        """
        cur = self._cursor(cur)

        cols = ', '.join(self.directory_ls_cols)
        query = (
            'SELECT %s FROM swh_find_directory_entry_by_path(%%s, %%s)' % cols)
        cur.execute(query, (directory, paths))

        data = cur.fetchone()
        if set(data) == {None}:
            return None
        return data

    def directory_get_random(self, cur=None):
        return self._get_random_row_from_table(
            'directory', ['id'], 'id', cur)

    def revision_missing_from_list(self, revisions, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(
            cur, """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM revision r WHERE r.id = t.id
            )
            """, ((id,) for id in revisions))

    revision_add_cols = [
        'id', 'date', 'date_offset', 'date_neg_utc_offset', 'committer_date',
        'committer_date_offset', 'committer_date_neg_utc_offset', 'type',
        'directory', 'message', 'author_fullname', 'author_name',
        'author_email', 'committer_fullname', 'committer_name',
        'committer_email', 'metadata', 'synthetic',
    ]

    revision_get_cols = revision_add_cols + ['parents']

    def origin_visit_add(self, origin, ts, type, cur=None):
        """Add a new origin_visit for origin origin at timestamp ts with
        status 'ongoing'.

        Args:
            origin: origin concerned by the visit
            ts: the date of the visit
            type: type of loader for the visit

        Returns:
            The new visit index step for that origin

        """
        cur = self._cursor(cur)
        self._cursor(cur).execute('SELECT swh_origin_visit_add(%s, %s, %s)',
                                  (origin, ts, type))
        return cur.fetchone()[0]

    def origin_visit_update(self, origin_id, visit_id, updates, cur=None):
        """Update origin_visit's status."""
        cur = self._cursor(cur)
        update_cols = []
        values = []
        where = ['origin.id = origin_visit.origin',
                 'origin.url=%s',
                 'visit=%s']
        where_values = [origin_id, visit_id]
        if 'status' in updates:
            update_cols.append('status=%s')
            values.append(updates.pop('status'))
        if 'metadata' in updates:
            update_cols.append('metadata=%s')
            values.append(jsonize(updates.pop('metadata')))
        if 'snapshot' in updates:
            update_cols.append('snapshot=%s')
            values.append(updates.pop('snapshot'))
        assert not updates, 'Unknown fields: %r' % updates
        query = """UPDATE origin_visit
                   SET {update_cols}
                   FROM origin
                   WHERE {where}""".format(**{
            'update_cols': ', '.join(update_cols),
            'where': ' AND '.join(where)
        })
        cur.execute(query, (*values, *where_values))

    def origin_visit_upsert(self, origin, visit, date, type, status,
                            metadata, snapshot, cur=None):
        # doing an extra query like this is way simpler than trying to join
        # the origin id in the query below
        origin_id = next(self.origin_id_get_by_url([origin]))

        cur = self._cursor(cur)
        query = """INSERT INTO origin_visit ({cols}) VALUES ({values})
                   ON CONFLICT ON CONSTRAINT origin_visit_pkey DO
                   UPDATE SET {updates}""".format(
                cols=', '.join(self.origin_visit_get_cols),
                values=', '.join('%s' for col in self.origin_visit_get_cols),
                updates=', '.join('{0}=excluded.{0}'.format(col)
                                  for col in self.origin_visit_get_cols))
        cur.execute(
            query, (origin_id, visit, date, type, status, metadata, snapshot))

    origin_visit_get_cols = [
        'origin', 'visit', 'date', 'type',
        'status', 'metadata', 'snapshot']
    origin_visit_select_cols = [
        'origin.url AS origin', 'visit', 'date', 'origin_visit.type AS type',
        'status', 'metadata', 'snapshot']

    def origin_visit_get_all(self, origin_id,
                             last_visit=None, limit=None, cur=None):
        """Retrieve all visits for origin with id origin_id.

        Args:
            origin_id: The occurrence's origin

        Yields:
            The occurrence's history visits

        """
        cur = self._cursor(cur)

        if last_visit:
            extra_condition = 'and visit > %s'
            args = (origin_id, last_visit, limit)
        else:
            extra_condition = ''
            args = (origin_id, limit)

        query = """\
        SELECT %s
        FROM origin_visit
        INNER JOIN origin ON origin.id = origin_visit.origin
        WHERE origin.url=%%s %s
        order by visit asc
        limit %%s""" % (
            ', '.join(self.origin_visit_select_cols), extra_condition
        )

        cur.execute(query, args)

        yield from cur

    def origin_visit_get(self, origin_id, visit_id, cur=None):
        """Retrieve information on visit visit_id of origin origin_id.

        Args:
            origin_id: the origin concerned
            visit_id: The visit step for that origin

        Returns:
            The origin_visit information

        """
        cur = self._cursor(cur)

        query = """\
            SELECT %s
            FROM origin_visit
            INNER JOIN origin ON origin.id = origin_visit.origin
            WHERE origin.url = %%s AND visit = %%s
            """ % (', '.join(self.origin_visit_select_cols))

        cur.execute(query, (origin_id, visit_id))
        r = cur.fetchall()
        if not r:
            return None
        return r[0]

    def origin_visit_find_by_date(self, origin, visit_date, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            'SELECT * FROM swh_visit_find_by_date(%s, %s)',
            (origin, visit_date))
        r = cur.fetchall()
        if r:
            return r[0]

    def origin_visit_exists(self, origin_id, visit_id, cur=None):
        """Check whether an origin visit with the given ids exists"""
        cur = self._cursor(cur)

        query = "SELECT 1 FROM origin_visit where origin = %s AND visit = %s"

        cur.execute(query, (origin_id, visit_id))

        return bool(cur.fetchone())

    def origin_visit_get_latest(
            self, origin_id, allowed_statuses=None, require_snapshot=False,
            cur=None):
        """Retrieve the most recent origin_visit of the given origin,
        with optional filters.

        Args:
            origin_id: the origin concerned
            allowed_statuses: the visit statuses allowed for the returned visit
            require_snapshot (bool): If True, only a visit with a known
                snapshot will be returned.

        Returns:
            The origin_visit information, or None if no visit matches.
        """
        cur = self._cursor(cur)

        query_parts = [
            'SELECT %s' % ', '.join(self.origin_visit_select_cols),
            'FROM origin_visit',
            'INNER JOIN origin ON origin.id = origin_visit.origin']

        query_parts.append('WHERE origin.url = %s')

        if require_snapshot:
            query_parts.append('AND snapshot is not null')

        if allowed_statuses:
            query_parts.append(
                cur.mogrify('AND status IN %s',
                            (tuple(allowed_statuses),)).decode())

        query_parts.append('ORDER BY date DESC, visit DESC LIMIT 1')

        query = '\n'.join(query_parts)

        cur.execute(query, (origin_id,))
        r = cur.fetchone()
        if not r:
            return None
        return r

    def origin_visit_get_random(self, type, cur=None):
        """Randomly select one origin visit that was full and in the last 3
           months

        """
        cur = self._cursor(cur)
        columns = ','.join(self.origin_visit_select_cols)
        query = f"""with visits as (
                      select *
                      from origin_visit
                      where origin_visit.status='full' and
                            origin_visit.type=%s and
                            origin_visit.date > now() - '3 months'::interval
                    )
                    select {columns}
                    from visits as origin_visit
                    inner join origin
                    on origin_visit.origin=origin.id
                    where random() < 0.1
                    limit 1
                 """
        cur.execute(query, (type, ))
        return cur.fetchone()

    @staticmethod
    def mangle_query_key(key, main_table):
        if key == 'id':
            return 't.id'
        if key == 'parents':
            return '''
            ARRAY(
            SELECT rh.parent_id::bytea
            FROM revision_history rh
            WHERE rh.id = t.id
            ORDER BY rh.parent_rank
            )'''
        if '_' not in key:
            return '%s.%s' % (main_table, key)

        head, tail = key.split('_', 1)
        if (head in ('author', 'committer')
                and tail in ('name', 'email', 'id', 'fullname')):
            return '%s.%s' % (head, tail)

        return '%s.%s' % (main_table, key)

    def revision_get_from_list(self, revisions, cur=None):
        cur = self._cursor(cur)

        query_keys = ', '.join(
            self.mangle_query_key(k, 'revision')
            for k in self.revision_get_cols
        )

        yield from execute_values_generator(
            cur, """
            SELECT %s FROM (VALUES %%s) as t(id)
            LEFT JOIN revision ON t.id = revision.id
            LEFT JOIN person author ON revision.author = author.id
            LEFT JOIN person committer ON revision.committer = committer.id
            """ % query_keys,
            ((id,) for id in revisions))

    def revision_log(self, root_revisions, limit=None, cur=None):
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM swh_revision_log(%%s, %%s)
                """ % ', '.join(self.revision_get_cols)

        cur.execute(query, (root_revisions, limit))
        yield from cur

    revision_shortlog_cols = ['id', 'parents']

    def revision_shortlog(self, root_revisions, limit=None, cur=None):
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM swh_revision_list(%%s, %%s)
                """ % ', '.join(self.revision_shortlog_cols)

        cur.execute(query, (root_revisions, limit))
        yield from cur

    def revision_get_random(self, cur=None):
        return self._get_random_row_from_table(
            'revision', ['id'], 'id', cur)

    def release_missing_from_list(self, releases, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur, """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM release r WHERE r.id = t.id
            )
            """, ((id,) for id in releases))

    object_find_by_sha1_git_cols = ['sha1_git', 'type']

    def object_find_by_sha1_git(self, ids, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(
            cur, """
            WITH t (sha1_git) AS (VALUES %s),
            known_objects as ((
                select
                  id as sha1_git,
                  'release'::object_type as type,
                  object_id
                from release r
                where exists (select 1 from t where t.sha1_git = r.id)
            ) union all (
                select
                  id as sha1_git,
                  'revision'::object_type as type,
                  object_id
                from revision r
                where exists (select 1 from t where t.sha1_git = r.id)
            ) union all (
                select
                  id as sha1_git,
                  'directory'::object_type as type,
                  object_id
                from directory d
                where exists (select 1 from t where t.sha1_git = d.id)
            ) union all (
                select
                  sha1_git as sha1_git,
                  'content'::object_type as type,
                  object_id
                from content c
                where exists (select 1 from t where t.sha1_git = c.sha1_git)
            ))
            select t.sha1_git as sha1_git, k.type
            from t
            left join known_objects k on t.sha1_git = k.sha1_git
            """,
            ((id,) for id in ids)
        )

    def stat_counters(self, cur=None):
        cur = self._cursor(cur)
        cur.execute('SELECT * FROM swh_stat_counters()')
        yield from cur

    def origin_add(self, url, cur=None):
        """Insert a new origin and return the new identifier."""
        insert = """INSERT INTO origin (url) values (%s)
                    RETURNING url"""

        cur.execute(insert, (url,))
        return cur.fetchone()[0]

    origin_cols = ['url']

    def origin_get_by_url(self, origins, cur=None):
        """Retrieve origin `(type, url)` from urls if found."""
        cur = self._cursor(cur)

        query = """SELECT %s FROM (VALUES %%s) as t(url)
                   LEFT JOIN origin ON t.url = origin.url
                """ % ','.join('origin.' + col for col in self.origin_cols)

        yield from execute_values_generator(
            cur, query, ((url,) for url in origins))

    def origin_get_by_sha1(self, sha1s, cur=None):
        """Retrieve origin urls from sha1s if found."""
        cur = self._cursor(cur)

        query = """SELECT %s FROM (VALUES %%s) as t(sha1)
                   LEFT JOIN origin ON t.sha1 = digest(origin.url, 'sha1')
                """ % ','.join('origin.' + col for col in self.origin_cols)

        yield from execute_values_generator(
            cur, query, ((sha1,) for sha1 in sha1s))

    def origin_id_get_by_url(self, origins, cur=None):
        """Retrieve origin `(type, url)` from urls if found."""
        cur = self._cursor(cur)

        query = """SELECT id FROM (VALUES %s) as t(url)
                   LEFT JOIN origin ON t.url = origin.url
                """

        for row in execute_values_generator(
                cur, query, ((url,) for url in origins)):
            yield row[0]

    origin_get_range_cols = ['id', 'url']

    def origin_get_range(self, origin_from=1, origin_count=100, cur=None):
        """Retrieve ``origin_count`` origins whose ids are greater
        or equal than ``origin_from``.

        Origins are sorted by id before retrieving them.

        Args:
            origin_from (int): the minimum id of origins to retrieve
            origin_count (int): the maximum number of origins to retrieve
        """
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM origin WHERE id >= %%s
                   ORDER BY id LIMIT %%s
                """ % ','.join(self.origin_get_range_cols)

        cur.execute(query, (origin_from, origin_count))
        yield from cur

    def _origin_query(self, url_pattern, count=False, offset=0, limit=50,
                      regexp=False, with_visit=False, cur=None):
        """
        Method factorizing query creation for searching and counting origins.
        """
        cur = self._cursor(cur)

        if count:
            origin_cols = 'COUNT(*)'
        else:
            origin_cols = ','.join(self.origin_cols)

        query = """SELECT %s
                   FROM origin
                   WHERE """
        if with_visit:
            query += """
                   EXISTS (
                     SELECT 1
                     FROM origin_visit
                       INNER JOIN snapshot ON snapshot=snapshot.id
                     WHERE origin=origin.id
                     )
                   AND """
        query += 'url %s %%s '
        if not count:
            query += 'ORDER BY id OFFSET %%s LIMIT %%s'

        if not regexp:
            query = query % (origin_cols, 'ILIKE')
            query_params = ('%'+url_pattern+'%', offset, limit)
        else:
            query = query % (origin_cols, '~*')
            query_params = (url_pattern, offset, limit)

        if count:
            query_params = (query_params[0],)

        cur.execute(query, query_params)

    def origin_search(self, url_pattern, offset=0, limit=50,
                      regexp=False, with_visit=False, cur=None):
        """Search for origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The search is performed in a case insensitive way.

        Args:
            url_pattern (str): the string pattern to search for in origin urls
            offset (int): number of found origins to skip before returning
                results
            limit (int): the maximum number of found origins to return
            regexp (bool): if True, consider the provided pattern as a regular
                expression and returns origins whose urls match it
            with_visit (bool): if True, filter out origins with no visit

        """
        self._origin_query(url_pattern, offset=offset, limit=limit,
                           regexp=regexp, with_visit=with_visit, cur=cur)
        yield from cur

    def origin_count(self, url_pattern, regexp=False,
                     with_visit=False, cur=None):
        """Count origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The pattern search in origin urls is performed in a case insensitive
        way.

        Args:
            url_pattern (str): the string pattern to search for in origin urls
            regexp (bool): if True, consider the provided pattern as a regular
                expression and returns origins whose urls match it
            with_visit (bool): if True, filter out origins with no visit
        """
        self._origin_query(url_pattern, count=True,
                           regexp=regexp, with_visit=with_visit, cur=cur)
        return cur.fetchone()[0]

    release_add_cols = [
        'id', 'target', 'target_type', 'date', 'date_offset',
        'date_neg_utc_offset', 'name', 'comment', 'synthetic',
        'author_fullname', 'author_name', 'author_email',
    ]
    release_get_cols = release_add_cols

    def release_get_from_list(self, releases, cur=None):
        cur = self._cursor(cur)
        query_keys = ', '.join(
            self.mangle_query_key(k, 'release')
            for k in self.release_get_cols
        )

        yield from execute_values_generator(
            cur, """
            SELECT %s FROM (VALUES %%s) as t(id)
            LEFT JOIN release ON t.id = release.id
            LEFT JOIN person author ON release.author = author.id
            """ % query_keys,
            ((id,) for id in releases))

    def release_get_random(self, cur=None):
        return self._get_random_row_from_table(
            'release', ['id'], 'id', cur)

    def origin_metadata_add(self, origin, ts, provider, tool,
                            metadata, cur=None):
        """ Add an origin_metadata for the origin at ts with provider, tool and
        metadata.

        Args:
            origin (int): the origin's id for which the metadata is added
            ts (datetime): time when the metadata was found
            provider (int): the metadata provider identifier
            tool (int): the tool's identifier used to extract metadata
            metadata (jsonb): the metadata retrieved at the time and location

        Returns:
            id (int): the origin_metadata unique id

        """
        cur = self._cursor(cur)
        insert = """INSERT INTO origin_metadata (origin_id, discovery_date,
                    provider_id, tool_id, metadata)
                    SELECT id, %s, %s, %s, %s FROM origin WHERE url = %s"""
        cur.execute(insert, (ts, provider, tool, jsonize(metadata), origin))

    origin_metadata_get_cols = ['origin_url', 'discovery_date',
                                'tool_id', 'metadata', 'provider_id',
                                'provider_name', 'provider_type',
                                'provider_url']

    def origin_metadata_get_by(self, origin_url, provider_type=None, cur=None):
        """Retrieve all origin_metadata entries for one origin_url

        """
        cur = self._cursor(cur)
        if not provider_type:
            query = '''SELECT %s
                       FROM swh_origin_metadata_get_by_origin(
                            %%s)''' % (','.join(
                self.origin_metadata_get_cols))

            cur.execute(query, (origin_url, ))

        else:
            query = '''SELECT %s
                       FROM swh_origin_metadata_get_by_provider_type(
                            %%s, %%s)''' % (','.join(
                self.origin_metadata_get_cols))

            cur.execute(query, (origin_url, provider_type))

        yield from cur

    tool_cols = ['id', 'name', 'version', 'configuration']

    @stored_procedure('swh_mktemp_tool')
    def mktemp_tool(self, cur=None):
        pass

    def tool_add_from_temp(self, cur=None):
        cur = self._cursor(cur)
        cur.execute("SELECT %s from swh_tool_add()" % (
            ','.join(self.tool_cols), ))
        yield from cur

    def tool_get(self, name, version, configuration, cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from tool
                       where name=%%s and
                             version=%%s and
                             configuration=%%s''' % (
            ','.join(self.tool_cols)),
            (name, version, configuration))

        return cur.fetchone()

    metadata_provider_cols = ['id', 'provider_name', 'provider_type',
                              'provider_url', 'metadata']

    def metadata_provider_add(self, provider_name, provider_type,
                              provider_url, metadata, cur=None):
        """Insert a new provider and return the new identifier."""
        cur = self._cursor(cur)
        insert = """INSERT INTO metadata_provider (provider_name, provider_type,
                    provider_url, metadata) values (%s, %s, %s, %s)
                    RETURNING id"""

        cur.execute(insert, (provider_name, provider_type, provider_url,
                             jsonize(metadata)))
        return cur.fetchone()[0]

    def metadata_provider_get(self, provider_id, cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from metadata_provider
                       where id=%%s ''' % (
            ','.join(self.metadata_provider_cols)),
            (provider_id, ))

        return cur.fetchone()

    def metadata_provider_get_by(self, provider_name, provider_url,
                                 cur=None):
        cur = self._cursor(cur)
        cur.execute('''select %s
                       from metadata_provider
                       where provider_name=%%s and
                             provider_url=%%s''' % (
            ','.join(self.metadata_provider_cols)),
            (provider_name, provider_url))

        return cur.fetchone()

    def _get_random_row_from_table(self, table_name, cols, id_col, cur=None):
        random_sha1 = bytes(random.randint(0, 255) for _ in range(SHA1_SIZE))
        cur = self._cursor(cur)
        query = '''
            (SELECT {cols} FROM {table} WHERE {id_col} >= %s
             ORDER BY {id_col} LIMIT 1)
            UNION
            (SELECT {cols} FROM {table} WHERE {id_col} < %s
             ORDER BY {id_col} DESC LIMIT 1)
            LIMIT 1
            '''.format(cols=', '.join(cols), table=table_name, id_col=id_col)
        cur.execute(query, (random_sha1, random_sha1))
        row = cur.fetchone()
        if row:
            return row[0]
