# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import random
import select
from typing import Any, Dict, Iterable, List, Optional, Tuple

from swh.core.db import BaseDb
from swh.core.db.db_utils import stored_procedure, jsonize
from swh.core.db.db_utils import execute_values_generator
from swh.model.model import OriginVisit, OriginVisitStatus, SHA1_SIZE


class Db(BaseDb):
    """Proxy to the SWH DB, with wrappers around stored procedures

    """

    def mktemp_dir_entry(self, entry_type, cur=None):
        self._cursor(cur).execute(
            "SELECT swh_mktemp_dir_entry(%s)", (("directory_entry_%s" % entry_type),)
        )

    @stored_procedure("swh_mktemp_revision")
    def mktemp_revision(self, cur=None):
        pass

    @stored_procedure("swh_mktemp_release")
    def mktemp_release(self, cur=None):
        pass

    @stored_procedure("swh_mktemp_snapshot_branch")
    def mktemp_snapshot_branch(self, cur=None):
        pass

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

    @stored_procedure("swh_content_add")
    def content_add_from_temp(self, cur=None):
        pass

    @stored_procedure("swh_directory_add")
    def directory_add_from_temp(self, cur=None):
        pass

    @stored_procedure("swh_skipped_content_add")
    def skipped_content_add_from_temp(self, cur=None):
        pass

    @stored_procedure("swh_revision_add")
    def revision_add_from_temp(self, cur=None):
        pass

    @stored_procedure("swh_release_add")
    def release_add_from_temp(self, cur=None):
        pass

    def content_update_from_temp(self, keys_to_update, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            """select swh_content_update(ARRAY[%s] :: text[])""" % keys_to_update
        )

    content_get_metadata_keys = [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "status",
    ]

    content_add_keys = content_get_metadata_keys + ["ctime"]

    skipped_content_keys = [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "reason",
        "status",
        "origin",
    ]

    def content_get_metadata_from_sha1s(self, sha1s, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur,
            """
            select t.sha1, %s from (values %%s) as t (sha1)
            inner join content using (sha1)
            """
            % ", ".join(self.content_get_metadata_keys[1:]),
            ((sha1,) for sha1 in sha1s),
        )

    def content_get_range(self, start, end, limit=None, cur=None):
        """Retrieve contents within range [start, end].

        """
        cur = self._cursor(cur)
        query = """select %s from content
                   where %%s <= sha1 and sha1 <= %%s
                   order by sha1
                   limit %%s""" % ", ".join(
            self.content_get_metadata_keys
        )
        cur.execute(query, (start, end, limit))
        yield from cur

    content_hash_keys = ["sha1", "sha1_git", "sha256", "blake2s256"]

    def content_missing_from_list(self, contents, cur=None):
        cur = self._cursor(cur)

        keys = ", ".join(self.content_hash_keys)
        equality = " AND ".join(
            ("t.%s = c.%s" % (key, key)) for key in self.content_hash_keys
        )

        yield from execute_values_generator(
            cur,
            """
            SELECT %s
            FROM (VALUES %%s) as t(%s)
            WHERE NOT EXISTS (
                SELECT 1 FROM content c
                WHERE %s
            )
            """
            % (keys, keys, equality),
            (tuple(c[key] for key in self.content_hash_keys) for c in contents),
        )

    def content_missing_per_sha1(self, sha1s, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(
            cur,
            """
        SELECT t.sha1 FROM (VALUES %s) AS t(sha1)
        WHERE NOT EXISTS (
            SELECT 1 FROM content c WHERE c.sha1 = t.sha1
        )""",
            ((sha1,) for sha1 in sha1s),
        )

    def content_missing_per_sha1_git(self, contents, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(
            cur,
            """
        SELECT t.sha1_git FROM (VALUES %s) AS t(sha1_git)
        WHERE NOT EXISTS (
            SELECT 1 FROM content c WHERE c.sha1_git = t.sha1_git
        )""",
            ((sha1,) for sha1 in contents),
        )

    def skipped_content_missing(self, contents, cur=None):
        if not contents:
            return []
        cur = self._cursor(cur)

        query = """SELECT * FROM (VALUES %s) AS t (%s)
                   WHERE not exists
                   (SELECT 1 FROM skipped_content s WHERE
                       s.sha1 is not distinct from t.sha1::sha1 and
                       s.sha1_git is not distinct from t.sha1_git::sha1 and
                       s.sha256 is not distinct from t.sha256::bytea);""" % (
            (", ".join("%s" for _ in contents)),
            ", ".join(self.content_hash_keys),
        )
        cur.execute(
            query,
            [tuple(cont[key] for key in self.content_hash_keys) for cont in contents],
        )

        yield from cur

    def snapshot_exists(self, snapshot_id, cur=None):
        """Check whether a snapshot with the given id exists"""
        cur = self._cursor(cur)

        cur.execute("""SELECT 1 FROM snapshot where id=%s""", (snapshot_id,))

        return bool(cur.fetchone())

    def snapshot_missing_from_list(self, snapshots, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur,
            """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM snapshot d WHERE d.id = t.id
            )
                """,
            ((id,) for id in snapshots),
        )

    def snapshot_add(self, snapshot_id, cur=None):
        """Add a snapshot from the temporary table"""
        cur = self._cursor(cur)

        cur.execute("""SELECT swh_snapshot_add(%s)""", (snapshot_id,))

    snapshot_count_cols = ["target_type", "count"]

    def snapshot_count_branches(self, snapshot_id, cur=None):
        cur = self._cursor(cur)
        query = """\
           SELECT %s FROM swh_snapshot_count_branches(%%s)
        """ % ", ".join(
            self.snapshot_count_cols
        )

        cur.execute(query, (snapshot_id,))

        yield from cur

    snapshot_get_cols = ["snapshot_id", "name", "target", "target_type"]

    def snapshot_get_by_id(
        self,
        snapshot_id,
        branches_from=b"",
        branches_count=None,
        target_types=None,
        cur=None,
    ):
        cur = self._cursor(cur)
        query = """\
           SELECT %s
           FROM swh_snapshot_get_by_id(%%s, %%s, %%s, %%s :: snapshot_target[])
        """ % ", ".join(
            self.snapshot_get_cols
        )

        cur.execute(query, (snapshot_id, branches_from, branches_count, target_types))

        yield from cur

    def snapshot_get_by_origin_visit(self, origin_url, visit_id, cur=None):
        cur = self._cursor(cur)
        query = """\
           SELECT ovs.snapshot
           FROM origin_visit ov
           INNER JOIN origin o ON o.id = ov.origin
           INNER JOIN origin_visit_status ovs
             ON ov.origin = ovs.origin AND ov.visit = ovs.visit
           WHERE o.url=%s AND ov.visit=%s
           ORDER BY ovs.date DESC LIMIT 1
        """

        cur.execute(query, (origin_url, visit_id))
        ret = cur.fetchone()
        if ret:
            return ret[0]

    def snapshot_get_random(self, cur=None):
        return self._get_random_row_from_table("snapshot", ["id"], "id", cur)

    content_find_cols = [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "ctime",
        "status",
    ]

    def content_find(
        self, sha1=None, sha1_git=None, sha256=None, blake2s256=None, cur=None
    ):
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

        checksum_dict = {
            "sha1": sha1,
            "sha1_git": sha1_git,
            "sha256": sha256,
            "blake2s256": blake2s256,
        }
        where_parts = []
        args = []
        # Adds only those keys which have value other than None
        for algorithm in checksum_dict:
            if checksum_dict[algorithm] is not None:
                args.append(checksum_dict[algorithm])
                where_parts.append(algorithm + "= %s")
        query = " AND ".join(where_parts)
        cur.execute(
            """SELECT %s
                       FROM content WHERE %s
                       """
            % (",".join(self.content_find_cols), query),
            args,
        )
        content = cur.fetchall()
        return content

    def content_get_random(self, cur=None):
        return self._get_random_row_from_table("content", ["sha1_git"], "sha1_git", cur)

    def directory_missing_from_list(self, directories, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur,
            """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM directory d WHERE d.id = t.id
            )
            """,
            ((id,) for id in directories),
        )

    directory_ls_cols = [
        "dir_id",
        "type",
        "target",
        "name",
        "perms",
        "status",
        "sha1",
        "sha1_git",
        "sha256",
        "length",
    ]

    def directory_walk_one(self, directory, cur=None):
        cur = self._cursor(cur)
        cols = ", ".join(self.directory_ls_cols)
        query = "SELECT %s FROM swh_directory_walk_one(%%s)" % cols
        cur.execute(query, (directory,))
        yield from cur

    def directory_walk(self, directory, cur=None):
        cur = self._cursor(cur)
        cols = ", ".join(self.directory_ls_cols)
        query = "SELECT %s FROM swh_directory_walk(%%s)" % cols
        cur.execute(query, (directory,))
        yield from cur

    def directory_entry_get_by_path(self, directory, paths, cur=None):
        """Retrieve a directory entry by path.

        """
        cur = self._cursor(cur)

        cols = ", ".join(self.directory_ls_cols)
        query = "SELECT %s FROM swh_find_directory_entry_by_path(%%s, %%s)" % cols
        cur.execute(query, (directory, paths))

        data = cur.fetchone()
        if set(data) == {None}:
            return None
        return data

    def directory_get_random(self, cur=None):
        return self._get_random_row_from_table("directory", ["id"], "id", cur)

    def revision_missing_from_list(self, revisions, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(
            cur,
            """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM revision r WHERE r.id = t.id
            )
            """,
            ((id,) for id in revisions),
        )

    revision_add_cols = [
        "id",
        "date",
        "date_offset",
        "date_neg_utc_offset",
        "committer_date",
        "committer_date_offset",
        "committer_date_neg_utc_offset",
        "type",
        "directory",
        "message",
        "author_fullname",
        "author_name",
        "author_email",
        "committer_fullname",
        "committer_name",
        "committer_email",
        "metadata",
        "synthetic",
    ]

    revision_get_cols = revision_add_cols + ["parents"]

    def origin_visit_add(self, origin, ts, type, cur=None):
        """Add a new origin_visit for origin origin at timestamp ts.

        Args:
            origin: origin concerned by the visit
            ts: the date of the visit
            type: type of loader for the visit

        Returns:
            The new visit index step for that origin

        """
        cur = self._cursor(cur)
        self._cursor(cur).execute(
            "SELECT swh_origin_visit_add(%s, %s, %s)", (origin, ts, type)
        )
        return cur.fetchone()[0]

    origin_visit_status_cols = [
        "origin",
        "visit",
        "date",
        "status",
        "snapshot",
        "metadata",
    ]

    def origin_visit_status_add(
        self, visit_status: OriginVisitStatus, cur=None
    ) -> None:
        """Add new origin visit status

        """
        assert self.origin_visit_status_cols[0] == "origin"
        assert self.origin_visit_status_cols[-1] == "metadata"
        cols = self.origin_visit_status_cols[1:-1]
        cur = self._cursor(cur)
        cur.execute(
            f"WITH origin_id as (select id from origin where url=%s) "
            f"INSERT INTO origin_visit_status "
            f"(origin, {', '.join(cols)}, metadata) "
            f"VALUES ((select id from origin_id), "
            f"{', '.join(['%s']*len(cols))}, %s) "
            f"ON CONFLICT (origin, visit, date) do nothing",
            [visit_status.origin]
            + [getattr(visit_status, key) for key in cols]
            + [jsonize(visit_status.metadata)],
        )

    origin_visit_upsert_cols = [
        "origin",
        "visit",
        "date",
        "type",
    ]

    def origin_visit_upsert(self, origin_visit: OriginVisit, cur=None) -> None:
        # doing an extra query like this is way simpler than trying to join
        # the origin id in the query below
        ov = origin_visit
        origin_id = next(self.origin_id_get_by_url([ov.origin]))

        cur = self._cursor(cur)
        query = """INSERT INTO origin_visit ({cols}) VALUES ({values})
                   ON CONFLICT ON CONSTRAINT origin_visit_pkey DO
                   UPDATE SET {updates}""".format(
            cols=", ".join(self.origin_visit_upsert_cols),
            values=", ".join("%s" for col in self.origin_visit_upsert_cols),
            updates=", ".join(
                "{0}=excluded.{0}".format(col) for col in self.origin_visit_upsert_cols
            ),
        )
        cur.execute(
            query, (origin_id, ov.visit, ov.date, ov.type),
        )

    origin_visit_get_cols = [
        "origin",
        "visit",
        "date",
        "type",
        "status",
        "metadata",
        "snapshot",
    ]
    origin_visit_select_cols = [
        "o.url AS origin",
        "ov.visit",
        "ov.date",
        "ov.type AS type",
        "ovs.status",
        "ovs.metadata",
        "ovs.snapshot",
    ]

    origin_visit_status_select_cols = [
        "o.url AS origin",
        "ovs.visit",
        "ovs.date",
        "ovs.status",
        "ovs.snapshot",
        "ovs.metadata",
    ]

    def _make_origin_visit_status(
        self, row: Optional[Tuple[Any]]
    ) -> Optional[Dict[str, Any]]:
        """Make an origin_visit_status dict out of a row

        """
        if not row:
            return None
        return dict(zip(self.origin_visit_status_cols, row))

    def origin_visit_status_get_latest(
        self,
        origin_url: str,
        visit: int,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        cur=None,
    ) -> Optional[Dict[str, Any]]:
        """Given an origin visit id, return its latest origin_visit_status

        """
        cur = self._cursor(cur)

        query_parts = [
            "SELECT %s" % ", ".join(self.origin_visit_status_select_cols),
            "FROM origin_visit_status ovs ",
            "INNER JOIN origin o ON o.id = ovs.origin",
        ]
        query_parts.append("WHERE o.url = %s")
        query_params: List[Any] = [origin_url]
        query_parts.append("AND ovs.visit = %s")
        query_params.append(visit)

        if require_snapshot:
            query_parts.append("AND ovs.snapshot is not null")

        if allowed_statuses:
            query_parts.append("AND ovs.status IN %s")
            query_params.append(tuple(allowed_statuses))

        query_parts.append("ORDER BY ovs.date DESC LIMIT 1")
        query = "\n".join(query_parts)

        cur.execute(query, tuple(query_params))
        row = cur.fetchone()
        return self._make_origin_visit_status(row)

    def origin_visit_get_all(
        self, origin_id, last_visit=None, order="asc", limit=None, cur=None
    ):
        """Retrieve all visits for origin with id origin_id.

        Args:
            origin_id: The occurrence's origin

        Yields:
            The visits for that origin

        """
        cur = self._cursor(cur)
        assert order.lower() in ["asc", "desc"]

        query_parts = [
            "SELECT DISTINCT ON (ov.visit) %s "
            % ", ".join(self.origin_visit_select_cols),
            "FROM origin_visit ov",
            "INNER JOIN origin o ON o.id = ov.origin",
            "INNER JOIN origin_visit_status ovs",
            "ON ov.origin = ovs.origin AND ov.visit = ovs.visit",
        ]
        query_parts.append("WHERE o.url = %s")
        query_params: List[Any] = [origin_id]

        if last_visit is not None:
            op_comparison = ">" if order == "asc" else "<"
            query_parts.append(f"and ov.visit {op_comparison} %s")
            query_params.append(last_visit)

        if order == "asc":
            query_parts.append("ORDER BY ov.visit ASC, ovs.date DESC")
        elif order == "desc":
            query_parts.append("ORDER BY ov.visit DESC, ovs.date DESC")
        else:
            assert False

        if limit is not None:
            query_parts.append("LIMIT %s")
            query_params.append(limit)

        query = "\n".join(query_parts)
        cur.execute(query, tuple(query_params))
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
            FROM origin_visit ov
            INNER JOIN origin o ON o.id = ov.origin
            INNER JOIN origin_visit_status ovs
            ON ov.origin = ovs.origin AND ov.visit = ovs.visit
            WHERE o.url = %%s AND ov.visit = %%s
            ORDER BY ovs.date DESC
            LIMIT 1
            """ % (
            ", ".join(self.origin_visit_select_cols)
        )

        cur.execute(query, (origin_id, visit_id))
        r = cur.fetchall()
        if not r:
            return None
        return r[0]

    def origin_visit_find_by_date(self, origin, visit_date, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            "SELECT * FROM swh_visit_find_by_date(%s, %s)", (origin, visit_date)
        )
        rows = cur.fetchall()
        if rows:
            visit = dict(zip(self.origin_visit_get_cols, rows[0]))
            visit["origin"] = origin
            return visit

    def origin_visit_exists(self, origin_id, visit_id, cur=None):
        """Check whether an origin visit with the given ids exists"""
        cur = self._cursor(cur)

        query = "SELECT 1 FROM origin_visit where origin = %s AND visit = %s"

        cur.execute(query, (origin_id, visit_id))

        return bool(cur.fetchone())

    def origin_visit_get_latest(
        self,
        origin_id: str,
        type: Optional[str],
        allowed_statuses: Optional[Iterable[str]],
        require_snapshot: bool,
        cur=None,
    ):
        """Retrieve the most recent origin_visit of the given origin,
        with optional filters.

        Args:
            origin_id: the origin concerned
            type: Optional visit type to filter on
            allowed_statuses: the visit statuses allowed for the returned visit
            require_snapshot (bool): If True, only a visit with a known
                snapshot will be returned.

        Returns:
            The origin_visit information, or None if no visit matches.
        """
        cur = self._cursor(cur)

        query_parts = [
            "SELECT %s" % ", ".join(self.origin_visit_select_cols),
            "FROM origin_visit ov ",
            "INNER JOIN origin o ON o.id = ov.origin",
            "INNER JOIN origin_visit_status ovs ",
            "ON o.id = ovs.origin AND ov.visit = ovs.visit ",
        ]
        query_parts.append("WHERE o.url = %s")
        query_params: List[Any] = [origin_id]

        if type is not None:
            query_parts.append("AND ov.type = %s")
            query_params.append(type)

        if require_snapshot:
            query_parts.append("AND ovs.snapshot is not null")

        if allowed_statuses:
            query_parts.append("AND ovs.status IN %s")
            query_params.append(tuple(allowed_statuses))

        query_parts.append(
            "ORDER BY ov.date DESC, ov.visit DESC, ovs.date DESC LIMIT 1"
        )

        query = "\n".join(query_parts)

        cur.execute(query, tuple(query_params))
        r = cur.fetchone()
        if not r:
            return None
        return r

    def origin_visit_get_random(self, type, cur=None):
        """Randomly select one origin visit that was full and in the last 3
           months

        """
        cur = self._cursor(cur)
        columns = ",".join(self.origin_visit_select_cols)
        query = f"""select {columns}
                    from origin_visit ov
                    inner join origin o on ov.origin=o.id
                    inner join origin_visit_status ovs
                      on ov.origin = ovs.origin and ov.visit = ovs.visit
                    where ovs.status='full'
                      and ov.type=%s
                      and ov.date > now() - '3 months'::interval
                      and random() < 0.1
                    limit 1
                 """
        cur.execute(query, (type,))
        return cur.fetchone()

    @staticmethod
    def mangle_query_key(key, main_table):
        if key == "id":
            return "t.id"
        if key == "parents":
            return """
            ARRAY(
            SELECT rh.parent_id::bytea
            FROM revision_history rh
            WHERE rh.id = t.id
            ORDER BY rh.parent_rank
            )"""
        if "_" not in key:
            return "%s.%s" % (main_table, key)

        head, tail = key.split("_", 1)
        if head in ("author", "committer") and tail in (
            "name",
            "email",
            "id",
            "fullname",
        ):
            return "%s.%s" % (head, tail)

        return "%s.%s" % (main_table, key)

    def revision_get_from_list(self, revisions, cur=None):
        cur = self._cursor(cur)

        query_keys = ", ".join(
            self.mangle_query_key(k, "revision") for k in self.revision_get_cols
        )

        yield from execute_values_generator(
            cur,
            """
            SELECT %s FROM (VALUES %%s) as t(sortkey, id)
            LEFT JOIN revision ON t.id = revision.id
            LEFT JOIN person author ON revision.author = author.id
            LEFT JOIN person committer ON revision.committer = committer.id
            ORDER BY sortkey
            """
            % query_keys,
            ((sortkey, id) for sortkey, id in enumerate(revisions)),
        )

    def revision_log(self, root_revisions, limit=None, cur=None):
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM swh_revision_log(%%s, %%s)
                """ % ", ".join(
            self.revision_get_cols
        )

        cur.execute(query, (root_revisions, limit))
        yield from cur

    revision_shortlog_cols = ["id", "parents"]

    def revision_shortlog(self, root_revisions, limit=None, cur=None):
        cur = self._cursor(cur)

        query = """SELECT %s
                   FROM swh_revision_list(%%s, %%s)
                """ % ", ".join(
            self.revision_shortlog_cols
        )

        cur.execute(query, (root_revisions, limit))
        yield from cur

    def revision_get_random(self, cur=None):
        return self._get_random_row_from_table("revision", ["id"], "id", cur)

    def release_missing_from_list(self, releases, cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur,
            """
            SELECT id FROM (VALUES %s) as t(id)
            WHERE NOT EXISTS (
                SELECT 1 FROM release r WHERE r.id = t.id
            )
            """,
            ((id,) for id in releases),
        )

    object_find_by_sha1_git_cols = ["sha1_git", "type"]

    def object_find_by_sha1_git(self, ids, cur=None):
        cur = self._cursor(cur)

        yield from execute_values_generator(
            cur,
            """
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
            ((id,) for id in ids),
        )

    def stat_counters(self, cur=None):
        cur = self._cursor(cur)
        cur.execute("SELECT * FROM swh_stat_counters()")
        yield from cur

    def origin_add(self, url, cur=None):
        """Insert a new origin and return the new identifier."""
        insert = """INSERT INTO origin (url) values (%s)
                    RETURNING url"""

        cur.execute(insert, (url,))
        return cur.fetchone()[0]

    origin_cols = ["url"]

    def origin_get_by_url(self, origins, cur=None):
        """Retrieve origin `(type, url)` from urls if found."""
        cur = self._cursor(cur)

        query = """SELECT %s FROM (VALUES %%s) as t(url)
                   LEFT JOIN origin ON t.url = origin.url
                """ % ",".join(
            "origin." + col for col in self.origin_cols
        )

        yield from execute_values_generator(cur, query, ((url,) for url in origins))

    def origin_get_by_sha1(self, sha1s, cur=None):
        """Retrieve origin urls from sha1s if found."""
        cur = self._cursor(cur)

        query = """SELECT %s FROM (VALUES %%s) as t(sha1)
                   LEFT JOIN origin ON t.sha1 = digest(origin.url, 'sha1')
                """ % ",".join(
            "origin." + col for col in self.origin_cols
        )

        yield from execute_values_generator(cur, query, ((sha1,) for sha1 in sha1s))

    def origin_id_get_by_url(self, origins, cur=None):
        """Retrieve origin `(type, url)` from urls if found."""
        cur = self._cursor(cur)

        query = """SELECT id FROM (VALUES %s) as t(url)
                   LEFT JOIN origin ON t.url = origin.url
                """

        for row in execute_values_generator(cur, query, ((url,) for url in origins)):
            yield row[0]

    origin_get_range_cols = ["id", "url"]

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
                """ % ",".join(
            self.origin_get_range_cols
        )

        cur.execute(query, (origin_from, origin_count))
        yield from cur

    def _origin_query(
        self,
        url_pattern,
        count=False,
        offset=0,
        limit=50,
        regexp=False,
        with_visit=False,
        cur=None,
    ):
        """
        Method factorizing query creation for searching and counting origins.
        """
        cur = self._cursor(cur)

        if count:
            origin_cols = "COUNT(*)"
        else:
            origin_cols = ",".join(self.origin_cols)

        query = """SELECT %s
                   FROM origin o
                   WHERE """
        if with_visit:
            query += """
                   EXISTS (
                     SELECT 1
                     FROM origin_visit ov
                     INNER JOIN origin_visit_status ovs
                       ON ov.origin = ovs.origin AND ov.visit = ovs.visit
                     INNER JOIN snapshot ON ovs.snapshot=snapshot.id
                     WHERE ov.origin=o.id
                     )
                   AND """
        query += "url %s %%s "
        if not count:
            query += "ORDER BY id OFFSET %%s LIMIT %%s"

        if not regexp:
            query = query % (origin_cols, "ILIKE")
            query_params = ("%" + url_pattern + "%", offset, limit)
        else:
            query = query % (origin_cols, "~*")
            query_params = (url_pattern, offset, limit)

        if count:
            query_params = (query_params[0],)

        cur.execute(query, query_params)

    def origin_search(
        self, url_pattern, offset=0, limit=50, regexp=False, with_visit=False, cur=None
    ):
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
        self._origin_query(
            url_pattern,
            offset=offset,
            limit=limit,
            regexp=regexp,
            with_visit=with_visit,
            cur=cur,
        )
        yield from cur

    def origin_count(self, url_pattern, regexp=False, with_visit=False, cur=None):
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
        self._origin_query(
            url_pattern, count=True, regexp=regexp, with_visit=with_visit, cur=cur
        )
        return cur.fetchone()[0]

    release_add_cols = [
        "id",
        "target",
        "target_type",
        "date",
        "date_offset",
        "date_neg_utc_offset",
        "name",
        "comment",
        "synthetic",
        "author_fullname",
        "author_name",
        "author_email",
    ]
    release_get_cols = release_add_cols

    def release_get_from_list(self, releases, cur=None):
        cur = self._cursor(cur)
        query_keys = ", ".join(
            self.mangle_query_key(k, "release") for k in self.release_get_cols
        )

        yield from execute_values_generator(
            cur,
            """
            SELECT %s FROM (VALUES %%s) as t(sortkey, id)
            LEFT JOIN release ON t.id = release.id
            LEFT JOIN person author ON release.author = author.id
            ORDER BY sortkey
            """
            % query_keys,
            ((sortkey, id) for sortkey, id in enumerate(releases)),
        )

    def release_get_random(self, cur=None):
        return self._get_random_row_from_table("release", ["id"], "id", cur)

    origin_metadata_get_cols = [
        "origin.url",
        "discovery_date",
        "metadata_authority.type",
        "metadata_authority.url",
        "metadata_fetcher.id",
        "metadata_fetcher.name",
        "metadata_fetcher.version",
        "format",
        "metadata",
    ]

    def origin_metadata_add(
        self,
        origin: str,
        discovery_date: datetime.datetime,
        authority: int,
        fetcher: int,
        format: str,
        metadata: bytes,
        cur=None,
    ) -> None:
        """ Add an origin_metadata for the origin at ts with provider, tool and
        metadata.

        Args:
            origin: the origin's id for which the metadata is added
            discovery_date: time when the metadata was found
            authority: the metadata provider identifier
            fetcher: the tool's identifier used to extract metadata
            format: the format of the metadata
            metadata: the metadata retrieved at the time and location
        """
        cur = self._cursor(cur)
        insert = """INSERT INTO origin_metadata (origin_id, discovery_date,
                    authority_id, fetcher_id, format, metadata)
                    SELECT id, %s, %s, %s, %s, %s FROM origin WHERE url = %s
                    ON CONFLICT (origin_id, authority_id, discovery_date, fetcher_id)
                    DO UPDATE SET
                        format=EXCLUDED.format,
                        metadata=EXCLUDED.metadata
                 """
        cur.execute(
            insert, (discovery_date, authority, fetcher, format, metadata, origin),
        )

    def origin_metadata_get(
        self,
        origin_url: str,
        authority: int,
        after_time: Optional[datetime.datetime],
        after_fetcher: Optional[int],
        limit: Optional[int],
        cur=None,
    ):
        cur = self._cursor(cur)
        assert self.origin_metadata_get_cols[-1] == "metadata"
        query_parts = [
            f"SELECT {', '.join(self.origin_metadata_get_cols[0:-1])}, "
            f"  origin_metadata.metadata AS metadata "
            f"FROM origin_metadata "
            f"INNER JOIN metadata_authority "
            f"  ON (metadata_authority.id=authority_id) "
            f"INNER JOIN metadata_fetcher ON (metadata_fetcher.id=fetcher_id) "
            f"INNER JOIN origin ON (origin.id=origin_metadata.origin_id) "
            f"WHERE origin.url=%s AND authority_id=%s "
        ]
        args = [origin_url, authority]

        if after_fetcher is not None:
            assert after_time
            query_parts.append("AND (discovery_date, fetcher_id) > (%s, %s)")
            args.extend([after_time, after_fetcher])
        elif after_time is not None:
            query_parts.append("AND discovery_date > %s")
            args.append(after_time)

        query_parts.append("ORDER BY discovery_date, fetcher_id")

        if limit:
            query_parts.append("LIMIT %s")
            args.append(limit)

        cur.execute(" ".join(query_parts), args)
        yield from cur

    metadata_fetcher_cols = ["name", "version", "metadata"]

    def metadata_fetcher_add(
        self, name: str, version: str, metadata: bytes, cur=None
    ) -> None:
        cur = self._cursor(cur)
        cur.execute(
            "INSERT INTO metadata_fetcher (name, version, metadata) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (name, version, jsonize(metadata)),
        )

    def metadata_fetcher_get(self, name: str, version: str, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            f"SELECT {', '.join(self.metadata_fetcher_cols)} "
            f"FROM metadata_fetcher "
            f"WHERE name=%s AND version=%s",
            (name, version),
        )
        return cur.fetchone()

    def metadata_fetcher_get_id(
        self, name: str, version: str, cur=None
    ) -> Optional[int]:
        cur = self._cursor(cur)
        cur.execute(
            "SELECT id FROM metadata_fetcher WHERE name=%s AND version=%s",
            (name, version),
        )
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            return None

    metadata_authority_cols = ["type", "url", "metadata"]

    def metadata_authority_add(
        self, type: str, url: str, metadata: bytes, cur=None
    ) -> None:
        cur = self._cursor(cur)
        cur.execute(
            "INSERT INTO metadata_authority (type, url, metadata) "
            "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (type, url, jsonize(metadata)),
        )

    def metadata_authority_get(self, type: str, url: str, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            f"SELECT {', '.join(self.metadata_authority_cols)} "
            f"FROM metadata_authority "
            f"WHERE type=%s AND url=%s",
            (type, url),
        )
        return cur.fetchone()

    def metadata_authority_get_id(self, type: str, url: str, cur=None) -> Optional[int]:
        cur = self._cursor(cur)
        cur.execute(
            "SELECT id FROM metadata_authority WHERE type=%s AND url=%s", (type, url)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            return None

    def _get_random_row_from_table(self, table_name, cols, id_col, cur=None):
        random_sha1 = bytes(random.randint(0, 255) for _ in range(SHA1_SIZE))
        cur = self._cursor(cur)
        query = """
            (SELECT {cols} FROM {table} WHERE {id_col} >= %s
             ORDER BY {id_col} LIMIT 1)
            UNION
            (SELECT {cols} FROM {table} WHERE {id_col} < %s
             ORDER BY {id_col} DESC LIMIT 1)
            LIMIT 1
            """.format(
            cols=", ".join(cols), table=table_name, id_col=id_col
        )
        cur.execute(query, (random_sha1, random_sha1))
        row = cur.fetchone()
        if row:
            return row[0]
