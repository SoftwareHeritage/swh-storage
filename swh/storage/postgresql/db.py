# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass
import datetime
import logging
import random
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from psycopg2 import sql
from psycopg2.extras import execute_values

from swh.core.db import BaseDb
from swh.core.db.db_utils import execute_values_generator
from swh.core.db.db_utils import jsonize as _jsonize
from swh.core.db.db_utils import stored_procedure
from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import SHA1_SIZE, OriginVisit, OriginVisitStatus, Sha1Git
from swh.model.swhids import ExtendedObjectType, ObjectType
from swh.storage.interface import ListOrder

logger = logging.getLogger(__name__)


def jsonize(d):
    return _jsonize(dict(d) if d is not None else None)


class QueryBuilder:
    def __init__(self) -> None:
        self.parts = sql.Composed([])
        self.params: List[Any] = []

    def add_query_part(self, query_part, params: List[Any] = []) -> None:
        # add a query part along with its run time parameters
        self.parts += query_part
        self.params += params

    def add_pagination_clause(
        self,
        pagination_key: List[str],
        cursor: Optional[Any],
        direction: Optional[ListOrder],
        limit: Optional[int],
        separator: str = "AND",
    ) -> None:
        """Create and add a pagination clause to the query

        Args:
            pagination_key: Pagination key to be used. Use list of strings
               to support alias fields
            cursor: Pagination cursor as a query parameter
            direction: Sort order
            limit: Limit as a query parameter
            separator: Separator to be used as the prefix for the clause
        """

        # Can be used for queries that require pagination
        if cursor:
            operation = "<" if direction == ListOrder.DESC else ">"  # ASC by default
            # pagination_key and the direction will be given as identifiers
            # and cursor is a run time parameter
            pagination_part = sql.SQL(
                "{separator} {pagination_key} {operation} {cursor}"
            ).format(
                separator=sql.SQL(separator),
                pagination_key=sql.Identifier(*pagination_key),
                operation=sql.SQL(operation),
                cursor=sql.Placeholder(),
            )
            self.add_query_part(pagination_part, params=[cursor])
        # Always use order by and limit with pagination
        if direction:
            operation = "DESC" if direction == ListOrder.DESC else "ASC"
            order_part = sql.SQL("ORDER BY {pagination_key} {operation}").format(
                pagination_key=sql.Identifier(*pagination_key),
                operation=sql.SQL(operation),
            )
            self.add_query_part(order_part)
        if limit is not None:
            limit_part = sql.SQL("LIMIT {limit}").format(limit=sql.Placeholder())
            self.add_query_part(limit_part, params=[limit])

    def get_query(self, db_cursor, separator: str = " ") -> str:
        # To get the query as a string; can be used for debugging
        return self.parts.join(separator).as_string(db_cursor)

    def execute(self, db_cursor, separator: str = " ") -> None:
        # Compose and execute
        query = self.parts.join(separator)
        db_cursor.execute(query, self.params)


@dataclass
class ObjectReferencesPartition:
    table_name: str
    year: int
    week: int
    start: datetime.datetime
    end: datetime.datetime


class Db(BaseDb):
    """Proxy to the SWH DB, with wrappers around stored procedures"""

    ##########################
    # Utilities
    ##########################

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

    ##########################
    # Insertion
    ##########################

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

    @stored_procedure("swh_extid_add")
    def extid_add_from_temp(self, cur=None):
        pass

    @stored_procedure("swh_release_add")
    def release_add_from_temp(self, cur=None):
        pass

    ##########################
    # 'content' table
    ##########################

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

    def content_get_metadata_from_hashes(
        self, hashes: List[bytes], algo: str, cur=None
    ):
        cur = self._cursor(cur)
        assert algo in DEFAULT_ALGORITHMS
        query = f"""
            select {", ".join(self.content_get_metadata_keys)}
            from (values %s) as t (hash)
            inner join content on (content.{algo}=hash)
        """
        yield from execute_values_generator(
            cur,
            query,
            ((hash_,) for hash_ in hashes),
        )

    def content_get_range(self, start, end, limit=None, cur=None) -> Iterator[Tuple]:
        """Retrieve contents within range [start, end]."""
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
        self,
        sha1: Optional[bytes] = None,
        sha1_git: Optional[bytes] = None,
        sha256: Optional[bytes] = None,
        blake2s256: Optional[bytes] = None,
        cur=None,
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

        query_parts = [f"SELECT {','.join(self.content_find_cols)} FROM content WHERE "]
        query_params = []
        where_parts = []
        # Adds only those keys which have values exist
        for algorithm in checksum_dict:
            if checksum_dict[algorithm] is not None:
                where_parts.append(f"{algorithm} = %s")
                query_params.append(checksum_dict[algorithm])

        query_parts.append(" AND ".join(where_parts))
        query = "\n".join(query_parts)
        cur.execute(query, query_params)
        content = cur.fetchall()
        return content

    def content_get_random(self, cur=None):
        return self._get_random_row_from_table("content", ["sha1_git"], "sha1_git", cur)

    ##########################
    # 'skipped_content' table
    ##########################

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

    skipped_content_find_cols = [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "status",
        "reason",
        "ctime",
    ]

    def skipped_content_find(
        self,
        sha1: Optional[bytes] = None,
        sha1_git: Optional[bytes] = None,
        sha256: Optional[bytes] = None,
        blake2s256: Optional[bytes] = None,
        cur=None,
    ) -> List[Tuple[Any]]:
        cur = self._cursor(cur)

        checksum_dict = {
            "sha1": sha1,
            "sha1_git": sha1_git,
            "sha256": sha256,
            "blake2s256": blake2s256,
        }

        # XXX: The origin part is untested because of
        # https://gitlab.softwareheritage.org/swh/devel/swh-storage/-/issues/4693
        query_parts = [
            f"""
            SELECT {','.join(self.skipped_content_find_cols)}, origin.url AS origin
            FROM skipped_content
            LEFT JOIN origin ON origin.id = skipped_content.origin
            WHERE
            """
        ]
        query_params = []
        where_parts = []
        # Adds only those keys which have values exist
        for algorithm in checksum_dict:
            if checksum_dict[algorithm] is not None:
                where_parts.append(f"{algorithm} = %s")
                query_params.append(checksum_dict[algorithm])

        query_parts.append(" AND ".join(where_parts))
        query = "\n".join(query_parts)
        cur.execute(query, query_params)
        skipped_contents = cur.fetchall()
        return skipped_contents

    ##########################
    # 'directory*' tables
    ##########################

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
        """Retrieve a directory entry by path."""
        cur = self._cursor(cur)

        cols = ", ".join(self.directory_ls_cols)
        query = "SELECT %s FROM swh_find_directory_entry_by_path(%%s, %%s)" % cols
        cur.execute(query, (directory, paths))

        data = cur.fetchone()
        if set(data) == {None}:
            return None
        return data

    directory_get_entries_cols = ["type", "target", "name", "perms"]

    def directory_get_entries(self, directory: Sha1Git, cur=None) -> List[Tuple]:
        cur = self._cursor(cur)
        cur.execute(
            "SELECT * FROM swh_directory_get_entries(%s::sha1_git)", (directory,)
        )
        return list(cur)

    def directory_get_raw_manifest(
        self, directory_ids: List[Sha1Git], cur=None
    ) -> Iterable[Tuple[Sha1Git, bytes]]:
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur,
            """
            SELECT t.id, raw_manifest FROM (VALUES %s) as t(id)
            INNER JOIN directory ON (t.id=directory.id)
            """,
            ((id_,) for id_ in directory_ids),
        )

    def directory_get_id_range(
        self, start, end, limit=None, cur=None
    ) -> Iterator[Tuple[Sha1Git]]:
        cur = self._cursor(cur)
        cur.execute(
            """
            SELECT id FROM directory
            WHERE %s <= id AND id <= %s
            LIMIT %s
            """,
            (start, end, limit),
        )
        yield from cur

    def directory_get_random(self, cur=None):
        return self._get_random_row_from_table("directory", ["id"], "id", cur)

    ##########################
    # 'revision' table
    ##########################

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
        "date_offset_bytes",
        "committer_date",
        "committer_date_offset",
        "committer_date_neg_utc_offset",
        "committer_date_offset_bytes",
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
        "extra_headers",
        "raw_manifest",
    ]

    revision_get_cols = revision_add_cols + ["parents"]

    @staticmethod
    def mangle_query_key(key, main_table, id_col, ignore_displayname=False):
        if key == "id":
            return id_col
        if key == "parents":
            return f"""
            ARRAY(
            SELECT rh.parent_id::bytea
            FROM revision_history rh
            WHERE rh.id = {id_col}
            ORDER BY rh.parent_rank
            )"""

        if "_" not in key:
            return f"{main_table}.{key}"

        head, tail = key.split("_", 1)
        if head not in ("author", "committer") or tail not in (
            "name",
            "email",
            "id",
            "fullname",
        ):
            return f"{main_table}.{key}"

        if ignore_displayname:
            return f"{head}.{tail}"
        else:
            if tail == "id":
                return f"{head}.{tail}"
            elif tail in ("name", "email"):
                # These fields get populated again from fullname by
                # converters.db_to_author if they're None, so we can just NULLify them
                # when displayname is set.
                return (
                    f"CASE"
                    f" WHEN {head}.displayname IS NULL THEN {head}.{tail} "
                    f" ELSE NULL "
                    f"END AS {key}"
                )
            elif tail == "fullname":
                return f"COALESCE({head}.displayname, {head}.fullname) AS {key}"

        assert False, "All cases should have been handled here"

    def revision_get_from_list(self, revisions, ignore_displayname=False, cur=None):
        cur = self._cursor(cur)

        query_keys = ", ".join(
            self.mangle_query_key(k, "revision", "t.id", ignore_displayname)
            for k in self.revision_get_cols
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

    def revision_get_range(self, start, end, limit, cur=None) -> Iterator[Tuple]:
        cur = self._cursor(cur)

        query_keys = ", ".join(
            self.mangle_query_key(k, "revision", "revision.id")
            for k in self.revision_get_cols
        )

        cur.execute(
            """
            SELECT %s FROM revision
            LEFT JOIN person author ON revision.author = author.id
            LEFT JOIN person committer ON revision.committer = committer.id
            WHERE %%s <= revision.id AND revision.id <= %%s
            LIMIT %%s
            """
            % query_keys,
            (start, end, limit),
        )
        yield from cur

    def revision_log(
        self, root_revisions, ignore_displayname=False, limit=None, cur=None
    ):
        cur = self._cursor(cur)

        query = """\
        SELECT %s
        FROM swh_revision_log(
          "root_revisions" := %%s, num_revs := %%s, "ignore_displayname" := %%s
        )""" % ", ".join(
            self.revision_get_cols
        )

        cur.execute(query, (root_revisions, limit, ignore_displayname))
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

    ##########################
    # 'extid' table
    ##########################

    extid_cols = ["extid", "extid_version", "extid_type", "target", "target_type"]

    def extid_get_from_extid_list(
        self, extid_type: str, ids: List[bytes], version: Optional[int] = None, cur=None
    ):
        cur = self._cursor(cur)
        query_keys = ", ".join(
            self.mangle_query_key(k, "extid", "t.id") for k in self.extid_cols
        )
        filter_query = ""
        if version is not None:
            filter_query = cur.mogrify(
                f"WHERE extid_version={version}", (version,)
            ).decode()

        sql = f"""
            SELECT {query_keys}
            FROM (VALUES %s) as t(sortkey, extid, extid_type)
            LEFT JOIN extid USING (extid, extid_type)
            {filter_query}
            ORDER BY sortkey
            """

        yield from execute_values_generator(
            cur,
            sql,
            (((sortkey, extid, extid_type) for sortkey, extid in enumerate(ids))),
        )

    def extid_get_from_swhid_list(
        self,
        target_type: str,
        ids: List[bytes],
        extid_version: Optional[int] = None,
        extid_type: Optional[str] = None,
        cur=None,
    ):
        cur = self._cursor(cur)
        target_type = ObjectType(
            target_type
        ).name.lower()  # aka "rev" -> "revision", ...
        query_keys = ", ".join(
            self.mangle_query_key(k, "extid", "t.id") for k in self.extid_cols
        )
        filter_query = ""
        if extid_version is not None and extid_type is not None:
            filter_query = cur.mogrify(
                "WHERE extid_version=%s AND extid_type=%s",
                (
                    extid_version,
                    extid_type,
                ),
            ).decode()

        sql = f"""
            SELECT {query_keys}
            FROM (VALUES %s) as t(sortkey, target, target_type)
            LEFT JOIN extid USING (target, target_type)
            {filter_query}
            ORDER BY sortkey
            """

        yield from execute_values_generator(
            cur,
            sql,
            (((sortkey, target, target_type) for sortkey, target in enumerate(ids))),
            template=b"(%s,%s,%s::object_type)",
        )

    def extid_delete_for_target(
        self, target_rows: List[Tuple[str, bytes]], cur=None
    ) -> Dict[str, int]:
        result = {}
        cur = self._cursor(cur)
        execute_values(
            cur,
            """DELETE FROM extid
                USING (VALUES %s) AS t(target_type, target)
                WHERE extid.target_type = t.target_type
                  AND extid.target = t.target""",
            target_rows,
            template=b"(%s::object_type,%s)",
        )
        result["extid:delete"] = cur.rowcount
        return result

    ##########################
    # 'release' table
    ##########################

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

    release_add_cols = [
        "id",
        "target",
        "target_type",
        "date",
        "date_offset",
        "date_neg_utc_offset",
        "date_offset_bytes",
        "name",
        "comment",
        "synthetic",
        "raw_manifest",
        "author_fullname",
        "author_name",
        "author_email",
    ]
    release_get_cols = release_add_cols

    def release_get_from_list(self, releases, ignore_displayname=False, cur=None):
        cur = self._cursor(cur)
        query_keys = ", ".join(
            self.mangle_query_key(k, "release", "t.id", ignore_displayname)
            for k in self.release_get_cols
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

    def release_get_range(self, start, end, limit, cur=None) -> Iterator[Tuple]:
        cur = self._cursor(cur)

        query_keys = ", ".join(
            self.mangle_query_key(k, "release", "release.id")
            for k in self.release_get_cols
        )

        cur.execute(
            """
            SELECT %s FROM release
            LEFT JOIN person author ON release.author = author.id
            WHERE %%s <= release.id AND release.id <= %%s
            LIMIT %%s
            """
            % query_keys,
            (start, end, limit),
        )
        yield from cur

    def release_get_random(self, cur=None):
        return self._get_random_row_from_table("release", ["id"], "id", cur)

    ##########################
    # 'snapshot' table
    ##########################

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

    def snapshot_count_branches(
        self,
        snapshot_id,
        branch_name_exclude_prefix=None,
        cur=None,
    ):
        cur = self._cursor(cur)
        query = """\
           SELECT %s FROM swh_snapshot_count_branches(%%s, %%s)
        """ % ", ".join(
            self.snapshot_count_cols
        )

        cur.execute(query, (snapshot_id, branch_name_exclude_prefix))

        yield from cur

    snapshot_get_cols = ["snapshot_id", "name", "target", "target_type"]

    def snapshot_get_by_id(
        self,
        snapshot_id,
        branches_from=b"",
        branches_count=None,
        target_types=None,
        branch_name_include_substring=None,
        branch_name_exclude_prefix=None,
        cur=None,
    ):
        cur = self._cursor(cur)
        query = """\
        SELECT %s
        FROM swh_snapshot_get_by_id(%%s, %%s, %%s, %%s :: snapshot_target[], %%s, %%s)
        """ % ", ".join(
            self.snapshot_get_cols
        )

        cur.execute(
            query,
            (
                snapshot_id,
                branches_from,
                branches_count,
                target_types,
                branch_name_include_substring,
                branch_name_exclude_prefix,
            ),
        )

        yield from cur

    def snapshot_branch_get_by_name(
        self,
        cols_to_fetch,
        snapshot_id,
        branch_name,
        cur=None,
    ):
        cur = self._cursor(cur)
        query = f"""SELECT {", ".join(cols_to_fetch)}
            FROM snapshot_branch sb
            LEFT JOIN snapshot_branches sbs ON sb.object_id = sbs.branch_id
            LEFT JOIN snapshot ss ON sbs.snapshot_id = ss.object_id
            WHERE ss.id=%s AND sb.name=%s
        """
        cur.execute(query, (snapshot_id, branch_name))
        return cur.fetchone()

    def snapshot_get_id_range(
        self, start, end, limit=None, cur=None
    ) -> Iterator[Tuple[Sha1Git]]:
        cur = self._cursor(cur)
        cur.execute(
            """
            SELECT id FROM snapshot
            WHERE %s <= id AND id <= %s
            LIMIT %s
            """,
            (start, end, limit),
        )

        yield from cur

    def snapshot_get_random(self, cur=None):
        return self._get_random_row_from_table("snapshot", ["id"], "id", cur)

    ##########################
    # 'origin_visit' and 'origin_visit_status' tables
    ##########################

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
        "type",
        "status",
        "snapshot",
        "metadata",
    ]

    def origin_visit_status_add(
        self, visit_status: OriginVisitStatus, cur=None
    ) -> None:
        """Add new origin visit status"""
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

    origin_visit_cols = ["origin", "visit", "date", "type"]

    def origin_visit_add_with_id(self, origin_visit: OriginVisit, cur=None) -> None:
        """Insert origin visit when id are already set"""
        ov = origin_visit
        assert ov.visit is not None
        cur = self._cursor(cur)
        query = """INSERT INTO origin_visit ({cols})
                   VALUES ((select id from origin where url=%s), {values})
                   ON CONFLICT (origin, visit) DO NOTHING""".format(
            cols=", ".join(self.origin_visit_cols),
            values=", ".join("%s" for col in self.origin_visit_cols[1:]),
        )
        cur.execute(query, (ov.origin, ov.visit, ov.date, ov.type))

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
        "ovs.snapshot",
        "ovs.metadata",
    ]

    origin_visit_status_select_cols = [
        "o.url AS origin",
        "ovs.visit",
        "ovs.date",
        "ovs.type AS type",
        "ovs.status",
        "ovs.snapshot",
        "ovs.metadata",
    ]

    def _make_origin_visit_status(
        self, row: Optional[Tuple[Any]]
    ) -> Optional[Dict[str, Any]]:
        """Make an origin_visit_status dict out of a row"""
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
        """Given an origin visit id, return its latest origin_visit_status"""
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

    def origin_visit_status_get_range(
        self,
        origin: str,
        visit: int,
        date_from: Optional[datetime.datetime],
        order: ListOrder,
        limit: int,
        cur=None,
    ):
        """Retrieve visit_status rows for visit (origin, visit) in a paginated way."""
        cur = self._cursor(cur)

        query_parts = [
            f"SELECT {', '.join(self.origin_visit_status_select_cols)} "
            "FROM origin_visit_status ovs ",
            "INNER JOIN origin o ON o.id = ovs.origin ",
        ]
        query_parts.append("WHERE o.url = %s AND ovs.visit = %s ")
        query_params: List[Any] = [origin, visit]

        if date_from is not None:
            op_comparison = ">=" if order == ListOrder.ASC else "<="
            query_parts.append(f"and ovs.date {op_comparison} %s ")
            query_params.append(date_from)

        if order == ListOrder.ASC:
            query_parts.append("ORDER BY ovs.date ASC ")
        elif order == ListOrder.DESC:
            query_parts.append("ORDER BY ovs.date DESC ")
        else:
            assert False

        query_parts.append("LIMIT %s")
        query_params.append(limit)

        query = "\n".join(query_parts)
        cur.execute(query, tuple(query_params))
        yield from cur

    def origin_visit_get_range(
        self,
        origin: str,
        visit_from: int,
        order: ListOrder,
        limit: int,
        cur=None,
    ):
        origin_visit_cols = ["o.url as origin", "ov.visit", "ov.date", "ov.type"]
        builder = QueryBuilder()
        builder.add_query_part(
            sql.SQL(
                f"""SELECT { ', '.join(origin_visit_cols) } FROM origin_visit ov
                INNER JOIN origin o ON o.id = ov.origin
                WHERE ov.origin = (select id from origin where url = %s)"""
            ),
            params=[origin],  # dynamic params
        )
        builder.add_pagination_clause(
            pagination_key=["ov", "visit"],
            cursor=visit_from,
            direction=order,
            limit=limit,
        )
        cur = self._cursor(cur)
        builder.execute(db_cursor=cur)
        yield from cur

    def origin_visit_status_get_all_in_range(
        self,
        origin: str,
        allowed_statuses: Optional[List[str]],
        require_snapshot: bool,
        visit_from: int,
        visit_to: int,
        cur=None,
    ):
        cur = self._cursor(cur)

        query_parts = [
            f"SELECT {', '.join(self.origin_visit_status_select_cols)}",
            " FROM origin_visit_status ovs",
            " INNER JOIN origin o ON o.id = ovs.origin",
        ]
        query_parts.append("WHERE ovs.origin = (select id from origin where url = %s)")
        query_params: List[Any] = [origin]

        assert visit_from <= visit_to

        query_parts.append("AND ovs.visit >= %s")
        query_params.append(visit_from)

        query_parts.append("AND ovs.visit <= %s")
        query_params.append(visit_to)

        if require_snapshot:
            query_parts.append("AND ovs.snapshot is not null")

        if allowed_statuses:
            query_parts.append("AND ovs.status IN %s")
            query_params.append(tuple(allowed_statuses))

        query_parts.append("ORDER BY ovs.visit ASC, ovs.date ASC")

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
            INNER JOIN origin_visit_status ovs USING (origin, visit)
            WHERE ov.origin = (select id from origin where url = %%s) AND ov.visit = %%s
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

    def origin_visit_find_by_date(self, origin, visit_date, type=None, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            "SELECT * FROM swh_visit_find_by_date(%s, %s, %s)",
            (origin, visit_date, type),
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

        # using 'LEFT JOIN origin_visit' instead of an INNER JOIN as a query
        # optimization; end results are equivalent.
        query_parts = [
            "SELECT %s" % ", ".join(self.origin_visit_select_cols),
            "FROM origin_visit_status ovs ",
            "INNER JOIN origin o ON o.id = ovs.origin",
            "LEFT JOIN origin_visit ov USING (origin, visit)",
        ]
        query_parts.append(
            "WHERE ovs.origin = (SELECT id FROM origin o WHERE o.url = %s)"
        )
        query_params: List[Any] = [origin_id]

        if type is not None:
            query_parts.append("AND ovs.type = %s")
            query_params.append(type)

        if require_snapshot:
            query_parts.append("AND ovs.snapshot is not null")

        if allowed_statuses:
            query_parts.append("AND ovs.status IN %s")
            query_params.append(tuple(allowed_statuses))

        query_parts.append("ORDER BY ovs.visit DESC, ovs.date DESC LIMIT 1")

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
                    inner join origin_visit_status ovs using (origin, visit)
                    where ovs.status='full'
                      and ov.type=%s
                      and ov.date > now() - '3 months'::interval
                      and random() < 0.1
                    limit 1
                 """
        cur.execute(query, (type,))
        return cur.fetchone()

    ##########################
    # 'origin' table
    ##########################

    def origin_add(self, url, cur=None):
        """Insert a new origin and return the new identifier."""
        insert = """INSERT INTO origin (url) values (%s)
                    ON CONFLICT DO NOTHING
                    """

        cur.execute(insert, (url,))
        return cur.rowcount

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

    def origin_get_range(self, origin_from: int = 1, origin_count: int = 100, cur=None):
        """Retrieve ``origin_count`` origins whose ids are greater
        or equal than ``origin_from``.

        Origins are sorted by id before retrieving them.

        Args:
            origin_from: the minimum id of origins to retrieve
            origin_count: the maximum number of origins to retrieve

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
        visit_types=None,
        cur=None,
    ):
        """
        Method factorizing query creation for searching and counting origins.
        """
        cur = self._cursor(cur)

        if count:
            origin_cols = "COUNT(*)"
            order_clause = ""
        else:
            origin_cols = ",".join(self.origin_cols)
            order_clause = "ORDER BY id"

        if not regexp:
            operator = "ILIKE"
            query_params = [f"%{url_pattern}%"]
        else:
            operator = "~*"
            query_params = [url_pattern]

        query = f"""
            WITH filtered_origins AS (
                SELECT *
                FROM origin
                WHERE url {operator} %s
                {order_clause}
            )
            SELECT {origin_cols}
            FROM filtered_origins AS o
            """

        if with_visit or visit_types:
            visit_predicat = (
                """
                INNER JOIN origin_visit_status ovs USING (origin, visit)
                INNER JOIN snapshot ON ovs.snapshot=snapshot.id
                """
                if with_visit
                else ""
            )

            type_predicat = (
                f"AND ov.type=any(ARRAY{visit_types})" if visit_types else ""
            )

            query += f"""
                WHERE EXISTS (
                    SELECT 1
                    FROM origin_visit ov
                    {visit_predicat}
                    WHERE ov.origin=o.id {type_predicat}
                )
                """

        if not count:
            query += "OFFSET %s LIMIT %s"
            query_params.extend([offset, limit])

        cur.execute(query, query_params)

    def origin_search(
        self,
        url_pattern: str,
        offset: int = 0,
        limit: int = 50,
        regexp: bool = False,
        with_visit: bool = False,
        visit_types: Optional[List[str]] = None,
        cur=None,
    ):
        """Search for origins whose urls contain a provided string pattern
        or match a provided regular expression.
        The search is performed in a case insensitive way.

        Args:
            url_pattern: the string pattern to search for in origin urls
            offset: number of found origins to skip before returning
                results
            limit: the maximum number of found origins to return
            regexp: if True, consider the provided pattern as a regular
                expression and returns origins whose urls match it
            with_visit: if True, filter out origins with no visit

        """
        self._origin_query(
            url_pattern,
            offset=offset,
            limit=limit,
            regexp=regexp,
            with_visit=with_visit,
            visit_types=visit_types,
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

    def origin_snapshot_get_all(self, origin_url: str, cur=None) -> Iterable[Sha1Git]:
        cur = self._cursor(cur)
        query = f"""\
        SELECT DISTINCT snapshot FROM origin_visit_status ovs
        INNER JOIN origin o ON o.id = ovs.origin
        WHERE o.url = '{origin_url}' and snapshot IS NOT NULL;
        """
        cur.execute(query)
        yield from map(lambda row: row[0], cur)

    ##########################
    # misc.
    ##########################

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

    ##########################
    # 'raw_extrinsic_metadata' table
    ##########################

    _raw_extrinsic_metadata_context_cols = [
        "origin",
        "visit",
        "snapshot",
        "release",
        "revision",
        "path",
        "directory",
    ]
    """The list of context columns for all artifact types."""

    _raw_extrinsic_metadata_insert_cols = [
        "id",
        "type",
        "target",
        "authority_id",
        "fetcher_id",
        "discovery_date",
        "format",
        "metadata",
        *_raw_extrinsic_metadata_context_cols,
    ]
    """List of columns of the raw_extrinsic_metadata table, used when writing
    metadata."""

    _raw_extrinsic_metadata_insert_query = f"""
        INSERT INTO raw_extrinsic_metadata
            ({', '.join(_raw_extrinsic_metadata_insert_cols)})
        VALUES ({', '.join('%s' for _ in _raw_extrinsic_metadata_insert_cols)})
        ON CONFLICT (id)
        DO NOTHING
    """

    raw_extrinsic_metadata_get_cols = [
        "raw_extrinsic_metadata.target",
        "raw_extrinsic_metadata.type",
        "discovery_date",
        "metadata_authority.type",
        "metadata_authority.url",
        "metadata_fetcher.id",
        "metadata_fetcher.name",
        "metadata_fetcher.version",
        *_raw_extrinsic_metadata_context_cols,
        "format",
        "raw_extrinsic_metadata.metadata",
    ]
    """List of columns of the raw_extrinsic_metadata, metadata_authority,
    and metadata_fetcher tables, used when reading object metadata."""

    _raw_extrinsic_metadata_select_query = f"""
        SELECT
            {', '.join(raw_extrinsic_metadata_get_cols)}
        FROM raw_extrinsic_metadata
        INNER JOIN metadata_authority
            ON (metadata_authority.id=authority_id)
        INNER JOIN metadata_fetcher ON (metadata_fetcher.id=fetcher_id)
    """

    def raw_extrinsic_metadata_add(
        self,
        id: bytes,
        type: str,
        target: str,
        discovery_date: datetime.datetime,
        authority_id: int,
        fetcher_id: int,
        format: str,
        metadata: bytes,
        origin: Optional[str],
        visit: Optional[int],
        snapshot: Optional[str],
        release: Optional[str],
        revision: Optional[str],
        path: Optional[bytes],
        directory: Optional[str],
        cur,
    ):
        query = self._raw_extrinsic_metadata_insert_query
        args: Dict[str, Any] = dict(
            id=id,
            type=type,
            target=target,
            authority_id=authority_id,
            fetcher_id=fetcher_id,
            discovery_date=discovery_date,
            format=format,
            metadata=metadata,
            origin=origin,
            visit=visit,
            snapshot=snapshot,
            release=release,
            revision=revision,
            path=path,
            directory=directory,
        )

        params = [args[col] for col in self._raw_extrinsic_metadata_insert_cols]

        cur.execute(query, params)

    def raw_extrinsic_metadata_get(
        self,
        target: str,
        authority_id: int,
        after_time: Optional[datetime.datetime],
        after_fetcher: Optional[int],
        limit: int,
        cur,
    ):
        query_parts = [self._raw_extrinsic_metadata_select_query]
        query_parts.append("WHERE raw_extrinsic_metadata.target=%s AND authority_id=%s")
        args = [target, authority_id]

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

    def raw_extrinsic_metadata_get_by_ids(self, ids: List[Sha1Git], cur=None):
        cur = self._cursor(cur)
        yield from execute_values_generator(
            cur,
            self._raw_extrinsic_metadata_select_query
            + "INNER JOIN (VALUES %s) AS t(id) ON t.id = raw_extrinsic_metadata.id",
            [(id_,) for id_ in ids],
        )

    def raw_extrinsic_metadata_get_authorities(self, id: str, cur=None):
        cur = self._cursor(cur)
        cur.execute(
            """
            SELECT
                DISTINCT metadata_authority.type, metadata_authority.url
            FROM raw_extrinsic_metadata
            INNER JOIN metadata_authority
                ON (metadata_authority.id=authority_id)
            WHERE raw_extrinsic_metadata.target = %s
            """,
            (id,),
        )
        yield from cur

    ##########################
    # 'metadata_fetcher' and 'metadata_authority' tables
    ##########################

    metadata_fetcher_cols = ["name", "version"]

    def metadata_fetcher_add(self, name: str, version: str, cur=None) -> None:
        cur = self._cursor(cur)
        cur.execute(
            "INSERT INTO metadata_fetcher (name, version) "
            "VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (name, version),
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

    metadata_authority_cols = ["type", "url"]

    def metadata_authority_add(self, type: str, url: str, cur=None) -> None:
        cur = self._cursor(cur)
        cur.execute(
            "INSERT INTO metadata_authority (type, url) "
            "VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (type, url),
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

    # 'object_references' table

    _object_references_cols = ("source_type", "source", "target_type", "target")

    def object_references_get(
        self, target_type: str, target: bytes, limit: int, cur=None
    ):
        cur = self._cursor(cur)
        cur.execute(
            """SELECT %s
            FROM object_references
            WHERE target_type = %%s and target=%%s
            LIMIT %%s"""
            % (", ".join(self._object_references_cols)),
            (target_type, target, limit),
        )
        return [dict(zip(self._object_references_cols, row)) for row in cur.fetchall()]

    def object_references_add(self, reference_rows, cur=None) -> None:
        cols = ", ".join(self._object_references_cols)
        cur = self._cursor(cur)
        execute_values(
            cur,
            f"""INSERT INTO object_references ({cols})
            VALUES %s ON CONFLICT (insertion_date, {cols}) DO NOTHING""",
            reference_rows,
        )

    def object_references_create_partition(
        self, year: int, week: int, cur=None
    ) -> Tuple[datetime.date, datetime.date]:
        """Create the partition of the object_references table for the given ISO
        ``year`` and ``week``."""
        # This date is guaranteed to be in week 1 by the ISO standard
        in_week1 = datetime.date(year=year, month=1, day=4)
        monday_of_week1 = in_week1 + datetime.timedelta(days=-in_week1.weekday())

        monday = monday_of_week1 + datetime.timedelta(weeks=week - 1)
        next_monday = monday + datetime.timedelta(days=7)

        cur = self._cursor(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS object_references_%04dw%02d
            PARTITION OF object_references
            FOR VALUES FROM (%%s) TO (%%s)"""
            % (year, week),
            (monday.isoformat(), next_monday.isoformat()),
        )

        return (monday, next_monday)

    def object_references_drop_partition(self, year: int, week: int, cur=None) -> None:
        """Delete the partition of the object_references table for the given ISO
        ``year`` and ``week``."""
        cur = self._cursor(cur)
        cur.execute("DROP TABLE object_references_%04dw%02d" % (year, week))

    def object_references_list_partitions(
        self, cur=None
    ) -> List[ObjectReferencesPartition]:
        """List existing partitions of the object_references table, ordered from
        oldest to the most recent."""
        cur = self._cursor(cur)
        cur.execute(
            """SELECT relname, pg_get_expr(relpartbound, oid)
                 FROM pg_partition_tree('object_references') pt
                      INNER JOIN pg_class ON relid = pg_class.oid
                WHERE isleaf = true
                ORDER BY relname"""
        )
        name_re = re.compile(r"^object_references_([0-9]+)w([0-9]+)$")
        bounds_re = re.compile(r"^FOR VALUES FROM \('([0-9-]+)'\) TO \('([0-9-]+)'\)$")
        partitions = []
        for row in cur:
            name_m = name_re.match(row[0])
            assert name_m is not None
            bounds_m = bounds_re.match(row[1])
            assert bounds_m is not None
            partitions.append(
                ObjectReferencesPartition(
                    table_name=row[0],
                    year=int(name_m[1]),
                    week=int(name_m[2]),
                    start=datetime.datetime.fromisoformat(bounds_m[1]),
                    end=datetime.datetime.fromisoformat(bounds_m[2]),
                )
            )
        return partitions

    #################
    # object deletion
    #################

    def object_delete(
        self, object_rows: List[Tuple[str, bytes]], cur=None
    ) -> Dict[str, int]:
        result = {}
        cur = self._cursor(cur)
        cur.execute(
            """
            CREATE TEMPORARY TABLE objects_to_remove (
                type extended_object_type NOT NULL,
                id BYTEA NOT NULL
            ) ON COMMIT DROP
            """
        )
        execute_values(
            cur,
            """INSERT INTO objects_to_remove (type, id)
               VALUES %s""",
            object_rows,
        )
        # Let’s handle raw extrinsic metadata first as they’ll
        # be referencing other objects
        cur.execute(
            """SELECT COUNT(emd.id), emd.type AS target_type
                 FROM raw_extrinsic_metadata emd
                WHERE emd.id IN (SELECT id
                                   FROM objects_to_remove
                                  WHERE type = 'raw_extrinsic_metadata')
                GROUP BY type"""
        )
        for count, target_type in cur:
            result[
                f"{ExtendedObjectType[target_type.upper()].value}_metadata:delete"
            ] = count
        cur.execute(
            """DELETE FROM raw_extrinsic_metadata emd
                WHERE emd.id IN (SELECT id
                                   FROM objects_to_remove
                                  WHERE type = 'raw_extrinsic_metadata')"""
        )
        # We need to remove lines from `origin_visit_status`,
        # `origin_visit` and `origin`.
        cur.execute(
            """CREATE TEMPORARY TABLE origins_to_remove
                 ON COMMIT DROP
                 AS
                   SELECT origin.id FROM origin
                    INNER JOIN objects_to_remove otr
                       ON DIGEST(url, 'sha1') = otr.id and otr.type = 'origin'"""
        )
        cur.execute(
            """DELETE FROM origin_visit_status ovs
                WHERE ovs.origin IN (SELECT id FROM origins_to_remove)"""
        )
        result["origin_visit_status:delete"] = cur.rowcount
        cur.execute(
            """DELETE FROM origin_visit ov
                WHERE ov.origin IN (SELECT id FROM origins_to_remove)"""
        )
        result["origin_visit:delete"] = cur.rowcount
        cur.execute(
            """DELETE FROM origin
                WHERE origin.id IN (SELECT id FROM origins_to_remove)"""
        )
        result["origin:delete"] = cur.rowcount
        # We must not remove entries for snapshot_branch, they are shared across
        # multiple snapshots, and we don't keep a reverse index (to know if the
        # shared branches are still used by another snapshot).
        cur.execute(
            """CREATE TEMPORARY TABLE snapshots_to_remove
                  ON COMMIT DROP
                  AS
                    SELECT object_id
                      FROM snapshot s
                     INNER JOIN objects_to_remove otr
                        ON s.id = otr.id AND otr.type = 'snapshot'"""
        )
        cur.execute(
            """DELETE FROM snapshot_branches
                WHERE snapshot_branches.snapshot_id IN
                  (SELECT object_id FROM snapshots_to_remove)"""
        )
        cur.execute(
            """DELETE FROM snapshot
                WHERE snapshot.object_id IN (SELECT object_id
                                               FROM snapshots_to_remove)"""
        )
        result["snapshot:delete"] = cur.rowcount
        cur.execute(
            """DELETE FROM release
                WHERE release.id IN (SELECT id
                                       FROM objects_to_remove
                                      WHERE type = 'release')"""
        )
        result["release:delete"] = cur.rowcount
        cur.execute(
            """DELETE FROM revision_history
                WHERE revision_history.id IN (SELECT id
                                                FROM objects_to_remove
                                               WHERE type = 'revision')"""
        )
        cur.execute(
            """DELETE FROM revision
                WHERE revision.id IN (SELECT id
                                        FROM objects_to_remove
                                       WHERE type = 'revision')"""
        )
        result["revision:delete"] = cur.rowcount
        # We do not remove anything from `directory_entry_dir`,
        # `directory_entry_file`, `directory_entry_rev`: these entries are
        # shared across directories and we don't have (or don’t want to keep) an
        # index to know which directory uses what entry.
        cur.execute(
            """DELETE FROM directory
                WHERE directory.id IN (SELECT id
                                         FROM objects_to_remove
                                        WHERE type = 'directory')"""
        )
        result["directory:delete"] = cur.rowcount
        cur.execute(
            """DELETE FROM skipped_content
                WHERE skipped_content.sha1_git IN (
                    SELECT id
                      FROM objects_to_remove
                     WHERE type = 'content')"""
        )
        result["skipped_content:delete"] = cur.rowcount
        cur.execute(
            """DELETE FROM content
                WHERE content.sha1_git IN (
                    SELECT id
                      FROM objects_to_remove
                     WHERE type = 'content')"""
        )
        result["content:delete"] = cur.rowcount
        # We are not an objstorage
        result["content:delete:bytes"] = 0
        return result
