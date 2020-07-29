# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import functools
import json
import logging
import random
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from cassandra import CoordinationFailure
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, ResultSet
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement, BoundStatement
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)

from swh.model.model import (
    Sha1Git,
    TimestampWithTimezone,
    Timestamp,
    Person,
    Content,
    SkippedContent,
    OriginVisit,
    OriginVisitStatus,
    Origin,
)

from swh.storage.interface import ListOrder

from .common import Row, TOKEN_BEGIN, TOKEN_END, hash_url
from .schema import CREATE_TABLES_QUERIES, HASH_ALGORITHMS


logger = logging.getLogger(__name__)


_execution_profiles = {
    EXEC_PROFILE_DEFAULT: ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())
    ),
}
# Configuration for cassandra-driver's access to servers:
# * hit the right server directly when sending a query (TokenAwarePolicy),
# * if there's more than one, then pick one at random that's in the same
#   datacenter as the client (DCAwareRoundRobinPolicy)


def create_keyspace(
    hosts: List[str], keyspace: str, port: int = 9042, *, durable_writes=True
):
    cluster = Cluster(hosts, port=port, execution_profiles=_execution_profiles)
    session = cluster.connect()
    extra_params = ""
    if not durable_writes:
        extra_params = "AND durable_writes = false"
    session.execute(
        """CREATE KEYSPACE IF NOT EXISTS "%s"
                       WITH REPLICATION = {
                           'class' : 'SimpleStrategy',
                           'replication_factor' : 1
                       } %s;
                    """
        % (keyspace, extra_params)
    )
    session.execute('USE "%s"' % keyspace)
    for query in CREATE_TABLES_QUERIES:
        session.execute(query)


T = TypeVar("T")


def _prepared_statement(query: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Returns a decorator usable on methods of CqlRunner, to
    inject them with a 'statement' argument, that is a prepared
    statement corresponding to the query.

    This only works on methods of CqlRunner, as preparing a
    statement requires a connection to a Cassandra server."""

    def decorator(f):
        @functools.wraps(f)
        def newf(self, *args, **kwargs) -> T:
            if f.__name__ not in self._prepared_statements:
                statement: PreparedStatement = self._session.prepare(query)
                self._prepared_statements[f.__name__] = statement
            return f(
                self, *args, **kwargs, statement=self._prepared_statements[f.__name__]
            )

        return newf

    return decorator


def _prepared_insert_statement(table_name: str, columns: List[str]):
    """Shorthand for using `_prepared_statement` for `INSERT INTO`
    statements."""
    return _prepared_statement(
        "INSERT INTO %s (%s) VALUES (%s)"
        % (table_name, ", ".join(columns), ", ".join("?" for _ in columns),)
    )


def _prepared_exists_statement(table_name: str):
    """Shorthand for using `_prepared_statement` for queries that only
    check which ids in a list exist in the table."""
    return _prepared_statement(f"SELECT id FROM {table_name} WHERE id IN ?")


class CqlRunner:
    """Class managing prepared statements and building queries to be sent
    to Cassandra."""

    def __init__(self, hosts: List[str], keyspace: str, port: int):
        self._cluster = Cluster(
            hosts, port=port, execution_profiles=_execution_profiles
        )
        self._session = self._cluster.connect(keyspace)
        self._cluster.register_user_type(
            keyspace, "microtimestamp_with_timezone", TimestampWithTimezone
        )
        self._cluster.register_user_type(keyspace, "microtimestamp", Timestamp)
        self._cluster.register_user_type(keyspace, "person", Person)

        self._prepared_statements: Dict[str, PreparedStatement] = {}

    ##########################
    # Common utility functions
    ##########################

    MAX_RETRIES = 3

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(CoordinationFailure),
    )
    def _execute_with_retries(self, statement, args) -> ResultSet:
        return self._session.execute(statement, args, timeout=1000.0)

    @_prepared_statement(
        "UPDATE object_count SET count = count + ? "
        "WHERE partition_key = 0 AND object_type = ?"
    )
    def _increment_counter(
        self, object_type: str, nb: int, *, statement: PreparedStatement
    ) -> None:
        self._execute_with_retries(statement, [nb, object_type])

    def _add_one(self, statement, object_type: str, obj, keys: List[str]) -> None:
        self._increment_counter(object_type, 1)
        self._execute_with_retries(statement, [getattr(obj, key) for key in keys])

    def _get_random_row(self, statement) -> Optional[Row]:
        """Takes a prepared statement of the form
        "SELECT * FROM <table> WHERE token(<keys>) > ? LIMIT 1"
        and uses it to return a random row"""
        token = random.randint(TOKEN_BEGIN, TOKEN_END)
        rows = self._execute_with_retries(statement, [token])
        if not rows:
            # There are no row with a greater token; wrap around to get
            # the row with the smallest token
            rows = self._execute_with_retries(statement, [TOKEN_BEGIN])
        if rows:
            return rows.one()
        else:
            return None

    def _missing(self, statement, ids):
        res = self._execute_with_retries(statement, [ids])
        found_ids = {id_ for (id_,) in res}
        return [id_ for id_ in ids if id_ not in found_ids]

    ##########################
    # 'content' table
    ##########################

    _content_pk = ["sha1", "sha1_git", "sha256", "blake2s256"]
    _content_keys = [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "ctime",
        "status",
    ]

    def _content_add_finalize(self, statement: BoundStatement) -> None:
        """Returned currified by content_add_prepare, to be called when the
        content row should be added to the primary table."""
        self._execute_with_retries(statement, None)
        self._increment_counter("content", 1)

    @_prepared_insert_statement("content", _content_keys)
    def content_add_prepare(
        self, content, *, statement
    ) -> Tuple[int, Callable[[], None]]:
        """Prepares insertion of a Content to the main 'content' table.
        Returns a token (to be used in secondary tables), and a function to be
        called to perform the insertion in the main table."""
        statement = statement.bind(
            [getattr(content, key) for key in self._content_keys]
        )

        # Type used for hashing keys (usually, it will be
        # cassandra.metadata.Murmur3Token)
        token_class = self._cluster.metadata.token_map.token_class

        # Token of the row when it will be inserted. This is equivalent to
        # "SELECT token({', '.join(self._content_pk)}) FROM content WHERE ..."
        # after the row is inserted; but we need the token to insert in the
        # index tables *before* inserting to the main 'content' table
        token = token_class.from_key(statement.routing_key).value
        assert TOKEN_BEGIN <= token <= TOKEN_END

        # Function to be called after the indexes contain their respective
        # row
        finalizer = functools.partial(self._content_add_finalize, statement)

        return (token, finalizer)

    @_prepared_statement(
        "SELECT * FROM content WHERE "
        + " AND ".join(map("%s = ?".__mod__, HASH_ALGORITHMS))
    )
    def content_get_from_pk(
        self, content_hashes: Dict[str, bytes], *, statement
    ) -> Optional[Row]:
        rows = list(
            self._execute_with_retries(
                statement, [content_hashes[algo] for algo in HASH_ALGORITHMS]
            )
        )
        assert len(rows) <= 1
        if rows:
            return rows[0]
        else:
            return None

    @_prepared_statement(
        "SELECT * FROM content WHERE token(" + ", ".join(_content_pk) + ") = ?"
    )
    def content_get_from_token(self, token, *, statement) -> Iterable[Row]:
        return self._execute_with_retries(statement, [token])

    @_prepared_statement(
        "SELECT * FROM content WHERE token(%s) > ? LIMIT 1" % ", ".join(_content_pk)
    )
    def content_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    @_prepared_statement(
        (
            "SELECT token({0}) AS tok, {1} FROM content "
            "WHERE token({0}) >= ? AND token({0}) <= ? LIMIT ?"
        ).format(", ".join(_content_pk), ", ".join(_content_keys))
    )
    def content_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterable[Row]:
        return self._execute_with_retries(statement, [start, end, limit])

    ##########################
    # 'content_by_*' tables
    ##########################

    @_prepared_statement("SELECT sha1_git FROM content_by_sha1_git WHERE sha1_git IN ?")
    def content_missing_by_sha1_git(
        self, ids: List[bytes], *, statement
    ) -> List[bytes]:
        return self._missing(statement, ids)

    def content_index_add_one(self, algo: str, content: Content, token: int) -> None:
        """Adds a row mapping content[algo] to the token of the Content in
        the main 'content' table."""
        query = (
            f"INSERT INTO content_by_{algo} ({algo}, target_token) " f"VALUES (%s, %s)"
        )
        self._execute_with_retries(query, [content.get_hash(algo), token])

    def content_get_tokens_from_single_hash(
        self, algo: str, hash_: bytes
    ) -> Iterable[int]:
        assert algo in HASH_ALGORITHMS
        query = f"SELECT target_token FROM content_by_{algo} WHERE {algo} = %s"
        return (tok for (tok,) in self._execute_with_retries(query, [hash_]))

    ##########################
    # 'skipped_content' table
    ##########################

    _skipped_content_pk = ["sha1", "sha1_git", "sha256", "blake2s256"]
    _skipped_content_keys = [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "ctime",
        "status",
        "reason",
        "origin",
    ]
    _magic_null_pk = b"<null>"
    """
    NULLs (or all-empty blobs) are not allowed in primary keys; instead use a
    special value that can't possibly be a valid hash.
    """

    def _skipped_content_add_finalize(self, statement: BoundStatement) -> None:
        """Returned currified by skipped_content_add_prepare, to be called
        when the content row should be added to the primary table."""
        self._execute_with_retries(statement, None)
        self._increment_counter("skipped_content", 1)

    @_prepared_insert_statement("skipped_content", _skipped_content_keys)
    def skipped_content_add_prepare(
        self, content, *, statement
    ) -> Tuple[int, Callable[[], None]]:
        """Prepares insertion of a Content to the main 'skipped_content' table.
        Returns a token (to be used in secondary tables), and a function to be
        called to perform the insertion in the main table."""

        # Replace NULLs (which are not allowed in the partition key) with
        # an empty byte string
        content = content.to_dict()
        for key in self._skipped_content_pk:
            if content[key] is None:
                content[key] = self._magic_null_pk

        statement = statement.bind(
            [content.get(key) for key in self._skipped_content_keys]
        )

        # Type used for hashing keys (usually, it will be
        # cassandra.metadata.Murmur3Token)
        token_class = self._cluster.metadata.token_map.token_class

        # Token of the row when it will be inserted. This is equivalent to
        # "SELECT token({', '.join(self._content_pk)})
        #  FROM skipped_content WHERE ..."
        # after the row is inserted; but we need the token to insert in the
        # index tables *before* inserting to the main 'skipped_content' table
        token = token_class.from_key(statement.routing_key).value
        assert TOKEN_BEGIN <= token <= TOKEN_END

        # Function to be called after the indexes contain their respective
        # row
        finalizer = functools.partial(self._skipped_content_add_finalize, statement)

        return (token, finalizer)

    @_prepared_statement(
        "SELECT * FROM skipped_content WHERE "
        + " AND ".join(map("%s = ?".__mod__, HASH_ALGORITHMS))
    )
    def skipped_content_get_from_pk(
        self, content_hashes: Dict[str, bytes], *, statement
    ) -> Optional[Row]:
        rows = list(
            self._execute_with_retries(
                statement,
                [
                    content_hashes[algo] or self._magic_null_pk
                    for algo in HASH_ALGORITHMS
                ],
            )
        )
        assert len(rows) <= 1
        if rows:
            # TODO: convert _magic_null_pk back to None?
            return rows[0]
        else:
            return None

    ##########################
    # 'skipped_content_by_*' tables
    ##########################

    def skipped_content_index_add_one(
        self, algo: str, content: SkippedContent, token: int
    ) -> None:
        """Adds a row mapping content[algo] to the token of the SkippedContent
        in the main 'skipped_content' table."""
        query = (
            f"INSERT INTO skipped_content_by_{algo} ({algo}, target_token) "
            f"VALUES (%s, %s)"
        )
        self._execute_with_retries(
            query, [content.get_hash(algo) or self._magic_null_pk, token]
        )

    ##########################
    # 'revision' table
    ##########################

    _revision_keys = [
        "id",
        "date",
        "committer_date",
        "type",
        "directory",
        "message",
        "author",
        "committer",
        "synthetic",
        "metadata",
        "extra_headers",
    ]

    @_prepared_exists_statement("revision")
    def revision_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement("revision", _revision_keys)
    def revision_add_one(self, revision: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [revision[key] for key in self._revision_keys]
        )
        self._increment_counter("revision", 1)

    @_prepared_statement("SELECT id FROM revision WHERE id IN ?")
    def revision_get_ids(self, revision_ids, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [revision_ids])

    @_prepared_statement("SELECT * FROM revision WHERE id IN ?")
    def revision_get(self, revision_ids, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [revision_ids])

    @_prepared_statement("SELECT * FROM revision WHERE token(id) > ? LIMIT 1")
    def revision_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'revision_parent' table
    ##########################

    _revision_parent_keys = ["id", "parent_rank", "parent_id"]

    @_prepared_insert_statement("revision_parent", _revision_parent_keys)
    def revision_parent_add_one(
        self, id_: Sha1Git, parent_rank: int, parent_id: Sha1Git, *, statement
    ) -> None:
        self._execute_with_retries(statement, [id_, parent_rank, parent_id])

    @_prepared_statement("SELECT parent_id FROM revision_parent WHERE id = ?")
    def revision_parent_get(self, revision_id: Sha1Git, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [revision_id])

    ##########################
    # 'release' table
    ##########################

    _release_keys = [
        "id",
        "target",
        "target_type",
        "date",
        "name",
        "message",
        "author",
        "synthetic",
    ]

    @_prepared_exists_statement("release")
    def release_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement("release", _release_keys)
    def release_add_one(self, release: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [release[key] for key in self._release_keys]
        )
        self._increment_counter("release", 1)

    @_prepared_statement("SELECT * FROM release WHERE id in ?")
    def release_get(self, release_ids: List[str], *, statement) -> None:
        return self._execute_with_retries(statement, [release_ids])

    @_prepared_statement("SELECT * FROM release WHERE token(id) > ? LIMIT 1")
    def release_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'directory' table
    ##########################

    _directory_keys = ["id"]

    @_prepared_exists_statement("directory")
    def directory_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement("directory", _directory_keys)
    def directory_add_one(self, directory_id: Sha1Git, *, statement) -> None:
        """Called after all calls to directory_entry_add_one, to
        commit/finalize the directory."""
        self._execute_with_retries(statement, [directory_id])
        self._increment_counter("directory", 1)

    @_prepared_statement("SELECT * FROM directory WHERE token(id) > ? LIMIT 1")
    def directory_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'directory_entry' table
    ##########################

    _directory_entry_keys = ["directory_id", "name", "type", "target", "perms"]

    @_prepared_insert_statement("directory_entry", _directory_entry_keys)
    def directory_entry_add_one(self, entry: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [entry[key] for key in self._directory_entry_keys]
        )

    @_prepared_statement("SELECT * FROM directory_entry WHERE directory_id IN ?")
    def directory_entry_get(self, directory_ids, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [directory_ids])

    ##########################
    # 'snapshot' table
    ##########################

    _snapshot_keys = ["id"]

    @_prepared_exists_statement("snapshot")
    def snapshot_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement("snapshot", _snapshot_keys)
    def snapshot_add_one(self, snapshot_id: Sha1Git, *, statement) -> None:
        self._execute_with_retries(statement, [snapshot_id])
        self._increment_counter("snapshot", 1)

    @_prepared_statement("SELECT * FROM snapshot WHERE id = ?")
    def snapshot_get(self, snapshot_id: Sha1Git, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [snapshot_id])

    @_prepared_statement("SELECT * FROM snapshot WHERE token(id) > ? LIMIT 1")
    def snapshot_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'snapshot_branch' table
    ##########################

    _snapshot_branch_keys = ["snapshot_id", "name", "target_type", "target"]

    @_prepared_insert_statement("snapshot_branch", _snapshot_branch_keys)
    def snapshot_branch_add_one(self, branch: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [branch[key] for key in self._snapshot_branch_keys]
        )

    @_prepared_statement(
        "SELECT ascii_bins_count(target_type) AS counts "
        "FROM snapshot_branch "
        "WHERE snapshot_id = ? "
    )
    def snapshot_count_branches(self, snapshot_id: Sha1Git, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [snapshot_id])

    @_prepared_statement(
        "SELECT * FROM snapshot_branch WHERE snapshot_id = ? AND name >= ? LIMIT ?"
    )
    def snapshot_branch_get(
        self, snapshot_id: Sha1Git, from_: bytes, limit: int, *, statement
    ) -> None:
        return self._execute_with_retries(statement, [snapshot_id, from_, limit])

    ##########################
    # 'origin' table
    ##########################

    origin_keys = ["sha1", "url", "type", "next_visit_id"]

    @_prepared_statement(
        "INSERT INTO origin (sha1, url, next_visit_id) "
        "VALUES (?, ?, 1) IF NOT EXISTS"
    )
    def origin_add_one(self, origin: Origin, *, statement) -> None:
        self._execute_with_retries(statement, [hash_url(origin.url), origin.url])
        self._increment_counter("origin", 1)

    @_prepared_statement("SELECT * FROM origin WHERE sha1 = ?")
    def origin_get_by_sha1(self, sha1: bytes, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [sha1])

    def origin_get_by_url(self, url: str) -> ResultSet:
        return self.origin_get_by_sha1(hash_url(url))

    @_prepared_statement(
        f'SELECT token(sha1) AS tok, {", ".join(origin_keys)} '
        f"FROM origin WHERE token(sha1) >= ? LIMIT ?"
    )
    def origin_list(self, start_token: int, limit: int, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [start_token, limit])

    @_prepared_statement("SELECT * FROM origin")
    def origin_iter_all(self, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [])

    @_prepared_statement("SELECT next_visit_id FROM origin WHERE sha1 = ?")
    def _origin_get_next_visit_id(self, origin_sha1: bytes, *, statement) -> int:
        rows = list(self._execute_with_retries(statement, [origin_sha1]))
        assert len(rows) == 1  # TODO: error handling
        return rows[0].next_visit_id

    @_prepared_statement(
        "UPDATE origin SET next_visit_id=? WHERE sha1 = ? IF next_visit_id=?"
    )
    def origin_generate_unique_visit_id(self, origin_url: str, *, statement) -> int:
        origin_sha1 = hash_url(origin_url)
        next_id = self._origin_get_next_visit_id(origin_sha1)
        while True:
            res = list(
                self._execute_with_retries(
                    statement, [next_id + 1, origin_sha1, next_id]
                )
            )
            assert len(res) == 1
            if res[0].applied:
                # No data race
                return next_id
            else:
                # Someone else updated it before we did, let's try again
                next_id = res[0].next_visit_id
                # TODO: abort after too many attempts

        return next_id

    ##########################
    # 'origin_visit' table
    ##########################

    _origin_visit_keys = [
        "origin",
        "visit",
        "type",
        "date",
    ]

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? AND visit > ? "
        "ORDER BY visit ASC"
    )
    def _origin_visit_get_pagination_asc_no_limit(
        self, origin_url: str, last_visit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, last_visit])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? AND visit > ? "
        "ORDER BY visit ASC "
        "LIMIT ?"
    )
    def _origin_visit_get_pagination_asc_limit(
        self, origin_url: str, last_visit: int, limit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, last_visit, limit])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? AND visit < ? "
        "ORDER BY visit DESC"
    )
    def _origin_visit_get_pagination_desc_no_limit(
        self, origin_url: str, last_visit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, last_visit])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? AND visit < ? "
        "ORDER BY visit DESC "
        "LIMIT ?"
    )
    def _origin_visit_get_pagination_desc_limit(
        self, origin_url: str, last_visit: int, limit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, last_visit, limit])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? ORDER BY visit ASC LIMIT ?"
    )
    def _origin_visit_get_no_pagination_asc_limit(
        self, origin_url: str, limit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, limit])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? ORDER BY visit ASC "
    )
    def _origin_visit_get_no_pagination_asc_no_limit(
        self, origin_url: str, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? ORDER BY visit DESC"
    )
    def _origin_visit_get_no_pagination_desc_no_limit(
        self, origin_url: str, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url])

    @_prepared_statement(
        "SELECT * FROM origin_visit WHERE origin = ? ORDER BY visit DESC LIMIT ?"
    )
    def _origin_visit_get_no_pagination_desc_limit(
        self, origin_url: str, limit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, limit])

    def origin_visit_get(
        self,
        origin_url: str,
        last_visit: Optional[int],
        limit: Optional[int],
        order: ListOrder,
    ) -> ResultSet:
        args: List[Any] = [origin_url]

        if last_visit is not None:
            page_name = "pagination"
            args.append(last_visit)
        else:
            page_name = "no_pagination"

        if limit is not None:
            limit_name = "limit"
            args.append(limit)
        else:
            limit_name = "no_limit"

        method_name = f"_origin_visit_get_{page_name}_{order.value}_{limit_name}"
        origin_visit_get_method = getattr(self, method_name)
        return origin_visit_get_method(*args)

    @_prepared_statement(
        "SELECT * FROM origin_visit_status WHERE origin = ? "
        "AND visit = ? AND date >= ? "
        "ORDER BY date ASC "
        "LIMIT ?"
    )
    def _origin_visit_status_get_with_date_asc_limit(
        self,
        origin: str,
        visit: int,
        date_from: datetime.datetime,
        limit: int,
        *,
        statement,
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin, visit, date_from, limit])

    @_prepared_statement(
        "SELECT * FROM origin_visit_status WHERE origin = ? "
        "AND visit = ? AND date <= ? "
        "ORDER BY visit DESC "
        "LIMIT ?"
    )
    def _origin_visit_status_get_with_date_desc_limit(
        self,
        origin: str,
        visit: int,
        date_from: datetime.datetime,
        limit: int,
        *,
        statement,
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin, visit, date_from, limit])

    @_prepared_statement(
        "SELECT * FROM origin_visit_status WHERE origin = ? AND visit = ? "
        "ORDER BY visit ASC "
        "LIMIT ?"
    )
    def _origin_visit_status_get_with_no_date_asc_limit(
        self, origin: str, visit: int, limit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin, visit, limit])

    @_prepared_statement(
        "SELECT * FROM origin_visit_status WHERE origin = ? AND visit = ? "
        "ORDER BY visit DESC "
        "LIMIT ?"
    )
    def _origin_visit_status_get_with_no_date_desc_limit(
        self, origin: str, visit: int, limit: int, *, statement
    ) -> ResultSet:
        return self._execute_with_retries(statement, [origin, visit, limit])

    def origin_visit_status_get_range(
        self,
        origin: str,
        visit: int,
        date_from: Optional[datetime.datetime],
        limit: int,
        order: ListOrder,
    ) -> ResultSet:
        args: List[Any] = [origin, visit]

        if date_from is not None:
            date_name = "date"
            args.append(date_from)
        else:
            date_name = "no_date"

        args.append(limit)

        method_name = f"_origin_visit_status_get_with_{date_name}_{order.value}_limit"
        origin_visit_status_get_method = getattr(self, method_name)
        return origin_visit_status_get_method(*args)

    @_prepared_insert_statement("origin_visit", _origin_visit_keys)
    def origin_visit_add_one(self, visit: OriginVisit, *, statement) -> None:
        self._add_one(statement, "origin_visit", visit, self._origin_visit_keys)

    _origin_visit_status_keys = [
        "origin",
        "visit",
        "date",
        "status",
        "snapshot",
        "metadata",
    ]

    @_prepared_insert_statement("origin_visit_status", _origin_visit_status_keys)
    def origin_visit_status_add_one(
        self, visit_update: OriginVisitStatus, *, statement
    ) -> None:
        assert self._origin_visit_status_keys[-1] == "metadata"
        keys = self._origin_visit_status_keys

        metadata = json.dumps(
            dict(visit_update.metadata) if visit_update.metadata is not None else None
        )
        self._execute_with_retries(
            statement, [getattr(visit_update, key) for key in keys[:-1]] + [metadata]
        )

    def origin_visit_status_get_latest(self, origin: str, visit: int,) -> Optional[Row]:
        """Given an origin visit id, return its latest origin_visit_status

         """
        rows = self.origin_visit_status_get(origin, visit)
        return rows[0] if rows else None

    @_prepared_statement(
        "SELECT * FROM origin_visit_status "
        "WHERE origin = ? AND visit = ? "
        "ORDER BY date DESC"
    )
    def origin_visit_status_get(
        self,
        origin: str,
        visit: int,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        *,
        statement,
    ) -> List[Row]:
        """Return all origin visit statuses for a given visit

        """
        return list(self._execute_with_retries(statement, [origin, visit]))

    @_prepared_statement("SELECT * FROM origin_visit WHERE origin = ? AND visit = ?")
    def origin_visit_get_one(
        self, origin_url: str, visit_id: int, *, statement
    ) -> Optional[Row]:
        # TODO: error handling
        rows = list(self._execute_with_retries(statement, [origin_url, visit_id]))
        if rows:
            return rows[0]
        else:
            return None

    @_prepared_statement("SELECT * FROM origin_visit WHERE origin = ?")
    def origin_visit_get_all(self, origin_url: str, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url])

    @_prepared_statement("SELECT * FROM origin_visit WHERE token(origin) >= ?")
    def _origin_visit_iter_from(self, min_token: int, *, statement) -> Iterator[Row]:
        yield from self._execute_with_retries(statement, [min_token])

    @_prepared_statement("SELECT * FROM origin_visit WHERE token(origin) < ?")
    def _origin_visit_iter_to(self, max_token: int, *, statement) -> Iterator[Row]:
        yield from self._execute_with_retries(statement, [max_token])

    def origin_visit_iter(self, start_token: int) -> Iterator[Row]:
        """Returns all origin visits in order from this token,
        and wraps around the token space."""
        yield from self._origin_visit_iter_from(start_token)
        yield from self._origin_visit_iter_to(start_token)

    ##########################
    # 'metadata_authority' table
    ##########################

    _metadata_authority_keys = ["url", "type", "metadata"]

    @_prepared_insert_statement("metadata_authority", _metadata_authority_keys)
    def metadata_authority_add(self, url, type, metadata, *, statement):
        return self._execute_with_retries(statement, [url, type, metadata])

    @_prepared_statement("SELECT * from metadata_authority WHERE type = ? AND url = ?")
    def metadata_authority_get(self, type, url, *, statement) -> Optional[Row]:
        return next(iter(self._execute_with_retries(statement, [type, url])), None)

    ##########################
    # 'metadata_fetcher' table
    ##########################

    _metadata_fetcher_keys = ["name", "version", "metadata"]

    @_prepared_insert_statement("metadata_fetcher", _metadata_fetcher_keys)
    def metadata_fetcher_add(self, name, version, metadata, *, statement):
        return self._execute_with_retries(statement, [name, version, metadata])

    @_prepared_statement(
        "SELECT * from metadata_fetcher WHERE name = ? AND version = ?"
    )
    def metadata_fetcher_get(self, name, version, *, statement) -> Optional[Row]:
        return next(iter(self._execute_with_retries(statement, [name, version])), None)

    #########################
    # 'raw_extrinsic_metadata' table
    #########################

    _raw_extrinsic_metadata_keys = [
        "type",
        "id",
        "authority_type",
        "authority_url",
        "discovery_date",
        "fetcher_name",
        "fetcher_version",
        "format",
        "metadata",
        "origin",
        "visit",
        "snapshot",
        "release",
        "revision",
        "path",
        "directory",
    ]

    @_prepared_statement(
        f"INSERT INTO raw_extrinsic_metadata "
        f"  ({', '.join(_raw_extrinsic_metadata_keys)}) "
        f"VALUES ({', '.join('?' for _ in _raw_extrinsic_metadata_keys)})"
    )
    def raw_extrinsic_metadata_add(
        self, statement, **kwargs,
    ):
        assert set(kwargs) == set(
            self._raw_extrinsic_metadata_keys
        ), f"Bad kwargs: {set(kwargs)}"
        params = [kwargs[key] for key in self._raw_extrinsic_metadata_keys]
        return self._execute_with_retries(statement, params,)

    @_prepared_statement(
        "SELECT * from raw_extrinsic_metadata "
        "WHERE id=? AND authority_url=? AND discovery_date>? AND authority_type=?"
    )
    def raw_extrinsic_metadata_get_after_date(
        self,
        id: str,
        authority_type: str,
        authority_url: str,
        after: datetime.datetime,
        *,
        statement,
    ):
        return self._execute_with_retries(
            statement, [id, authority_url, after, authority_type]
        )

    @_prepared_statement(
        "SELECT * from raw_extrinsic_metadata "
        "WHERE id=? AND authority_type=? AND authority_url=? "
        "AND (discovery_date, fetcher_name, fetcher_version) > (?, ?, ?)"
    )
    def raw_extrinsic_metadata_get_after_date_and_fetcher(
        self,
        id: str,
        authority_type: str,
        authority_url: str,
        after_date: datetime.datetime,
        after_fetcher_name: str,
        after_fetcher_version: str,
        *,
        statement,
    ):
        return self._execute_with_retries(
            statement,
            [
                id,
                authority_type,
                authority_url,
                after_date,
                after_fetcher_name,
                after_fetcher_version,
            ],
        )

    @_prepared_statement(
        "SELECT * from raw_extrinsic_metadata "
        "WHERE id=? AND authority_url=? AND authority_type=?"
    )
    def raw_extrinsic_metadata_get(
        self, id: str, authority_type: str, authority_url: str, *, statement
    ) -> Iterable[Row]:
        return self._execute_with_retries(
            statement, [id, authority_url, authority_type]
        )

    ##########################
    # Miscellaneous
    ##########################

    @_prepared_statement("SELECT uuid() FROM revision LIMIT 1;")
    def check_read(self, *, statement):
        self._execute_with_retries(statement, [])

    @_prepared_statement(
        "SELECT object_type, count FROM object_count WHERE partition_key=0"
    )
    def stat_counters(self, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [])
