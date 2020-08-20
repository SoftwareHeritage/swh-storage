# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dataclasses
import datetime
import functools
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
    Type,
    TypeVar,
    Union,
)

from cassandra import CoordinationFailure
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, ResultSet
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement, BoundStatement, dict_factory
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)
from mypy_extensions import NamedArg

from swh.model.model import (
    Content,
    SkippedContent,
    Sha1Git,
    TimestampWithTimezone,
    Timestamp,
    Person,
)

from swh.storage.interface import ListOrder

from .common import TOKEN_BEGIN, TOKEN_END, hash_url, remove_keys
from .model import (
    BaseRow,
    ContentRow,
    DirectoryEntryRow,
    DirectoryRow,
    MetadataAuthorityRow,
    MetadataFetcherRow,
    ObjectCountRow,
    OriginRow,
    OriginVisitRow,
    OriginVisitStatusRow,
    RawExtrinsicMetadataRow,
    ReleaseRow,
    RevisionParentRow,
    RevisionRow,
    SkippedContentRow,
    SnapshotBranchRow,
    SnapshotRow,
)
from .schema import CREATE_TABLES_QUERIES, HASH_ALGORITHMS


logger = logging.getLogger(__name__)


_execution_profiles = {
    EXEC_PROFILE_DEFAULT: ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        row_factory=dict_factory,
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


TRet = TypeVar("TRet")


def _prepared_statement(
    query: str,
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    """Returns a decorator usable on methods of CqlRunner, to
    inject them with a 'statement' argument, that is a prepared
    statement corresponding to the query.

    This only works on methods of CqlRunner, as preparing a
    statement requires a connection to a Cassandra server."""

    def decorator(f):
        @functools.wraps(f)
        def newf(self, *args, **kwargs) -> TRet:
            if f.__name__ not in self._prepared_statements:
                statement: PreparedStatement = self._session.prepare(query)
                self._prepared_statements[f.__name__] = statement
            return f(
                self, *args, **kwargs, statement=self._prepared_statements[f.__name__]
            )

        return newf

    return decorator


TArg = TypeVar("TArg")
TSelf = TypeVar("TSelf")


def _prepared_insert_statement(
    row_class: Type[BaseRow],
) -> Callable[
    [Callable[[TSelf, TArg, NamedArg(Any, "statement")], TRet]],  # noqa
    Callable[[TSelf, TArg], TRet],
]:
    """Shorthand for using `_prepared_statement` for `INSERT INTO`
    statements."""
    columns = row_class.cols()
    return _prepared_statement(
        "INSERT INTO %s (%s) VALUES (%s)"
        % (row_class.TABLE, ", ".join(columns), ", ".join("?" for _ in columns),)
    )


def _prepared_exists_statement(
    table_name: str,
) -> Callable[
    [Callable[[TSelf, TArg, NamedArg(Any, "statement")], TRet]],  # noqa
    Callable[[TSelf, TArg], TRet],
]:
    """Shorthand for using `_prepared_statement` for queries that only
    check which ids in a list exist in the table."""
    return _prepared_statement(f"SELECT id FROM {table_name} WHERE id IN ?")


def _prepared_select_statement(
    row_class: Type[BaseRow], clauses: str = "", cols: Optional[List[str]] = None,
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    if cols is None:
        cols = row_class.cols()

    return _prepared_statement(
        f"SELECT {', '.join(cols)} FROM {row_class.TABLE} {clauses}"
    )


def _prepared_select_statements(
    row_class: Type[BaseRow], queries: Dict[Any, str],
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    """Like _prepared_statement, but supports multiple statements, passed a dict,
    and passes a dict of prepared statements to the decorated method"""
    cols = row_class.cols()

    statement_start = f"SELECT {', '.join(cols)} FROM {row_class.TABLE} "

    def decorator(f):
        @functools.wraps(f)
        def newf(self, *args, **kwargs) -> TRet:
            if f.__name__ not in self._prepared_statements:
                self._prepared_statements[f.__name__] = {
                    key: self._session.prepare(statement_start + query)
                    for (key, query) in queries.items()
                }
            return f(
                self, *args, **kwargs, statements=self._prepared_statements[f.__name__]
            )

        return newf

    return decorator


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

        # directly a PreparedStatement for methods decorated with
        # @_prepared_statements (and its wrappers, _prepared_insert_statement,
        # _prepared_exists_statement, and _prepared_select_statement);
        # and a dict of PreparedStatements with @_prepared_select_statements
        self._prepared_statements: Dict[
            str, Union[PreparedStatement, Dict[Any, PreparedStatement]]
        ] = {}

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

    def _add_one(self, statement, obj: BaseRow) -> None:
        self._increment_counter(obj.TABLE, 1)
        self._execute_with_retries(statement, dataclasses.astuple(obj))

    _T = TypeVar("_T", bound=BaseRow)

    def _get_random_row(self, row_class: Type[_T], statement) -> Optional[_T]:  # noqa
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
            return row_class.from_dict(rows.one())  # type: ignore
        else:
            return None

    def _missing(self, statement, ids):
        rows = self._execute_with_retries(statement, [ids])
        found_ids = {row["id"] for row in rows}
        return [id_ for id_ in ids if id_ not in found_ids]

    ##########################
    # 'content' table
    ##########################

    def _content_add_finalize(self, statement: BoundStatement) -> None:
        """Returned currified by content_add_prepare, to be called when the
        content row should be added to the primary table."""
        self._execute_with_retries(statement, None)
        self._increment_counter("content", 1)

    @_prepared_insert_statement(ContentRow)
    def content_add_prepare(
        self, content: ContentRow, *, statement
    ) -> Tuple[int, Callable[[], None]]:
        """Prepares insertion of a Content to the main 'content' table.
        Returns a token (to be used in secondary tables), and a function to be
        called to perform the insertion in the main table."""
        statement = statement.bind(dataclasses.astuple(content))

        # Type used for hashing keys (usually, it will be
        # cassandra.metadata.Murmur3Token)
        token_class = self._cluster.metadata.token_map.token_class

        # Token of the row when it will be inserted. This is equivalent to
        # "SELECT token({', '.join(ContentRow.PARTITION_KEY)}) FROM content WHERE ..."
        # after the row is inserted; but we need the token to insert in the
        # index tables *before* inserting to the main 'content' table
        token = token_class.from_key(statement.routing_key).value
        assert TOKEN_BEGIN <= token <= TOKEN_END

        # Function to be called after the indexes contain their respective
        # row
        finalizer = functools.partial(self._content_add_finalize, statement)

        return (token, finalizer)

    @_prepared_select_statement(
        ContentRow, f"WHERE {' AND '.join(map('%s = ?'.__mod__, HASH_ALGORITHMS))}"
    )
    def content_get_from_pk(
        self, content_hashes: Dict[str, bytes], *, statement
    ) -> Optional[ContentRow]:
        rows = list(
            self._execute_with_retries(
                statement, [content_hashes[algo] for algo in HASH_ALGORITHMS]
            )
        )
        assert len(rows) <= 1
        if rows:
            return ContentRow(**rows[0])
        else:
            return None

    @_prepared_select_statement(
        ContentRow, f"WHERE token({', '.join(ContentRow.PARTITION_KEY)}) = ?"
    )
    def content_get_from_token(self, token, *, statement) -> Iterable[ContentRow]:
        return map(ContentRow.from_dict, self._execute_with_retries(statement, [token]))

    @_prepared_select_statement(
        ContentRow, f"WHERE token({', '.join(ContentRow.PARTITION_KEY)}) > ? LIMIT 1"
    )
    def content_get_random(self, *, statement) -> Optional[ContentRow]:
        return self._get_random_row(ContentRow, statement)

    @_prepared_statement(
        (
            "SELECT token({0}) AS tok, {1} FROM content "
            "WHERE token({0}) >= ? AND token({0}) <= ? LIMIT ?"
        ).format(", ".join(ContentRow.PARTITION_KEY), ", ".join(ContentRow.cols()))
    )
    def content_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterable[Tuple[int, ContentRow]]:
        """Returns an iterable of (token, row)"""
        return (
            (row["tok"], ContentRow.from_dict(remove_keys(row, ("tok",))))
            for row in self._execute_with_retries(statement, [start, end, limit])
        )

    ##########################
    # 'content_by_*' tables
    ##########################

    @_prepared_statement(
        "SELECT sha1_git AS id FROM content_by_sha1_git WHERE sha1_git IN ?"
    )
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
        return (
            row["target_token"] for row in self._execute_with_retries(query, [hash_])
        )

    ##########################
    # 'skipped_content' table
    ##########################

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

    @_prepared_insert_statement(SkippedContentRow)
    def skipped_content_add_prepare(
        self, content, *, statement
    ) -> Tuple[int, Callable[[], None]]:
        """Prepares insertion of a Content to the main 'skipped_content' table.
        Returns a token (to be used in secondary tables), and a function to be
        called to perform the insertion in the main table."""

        # Replace NULLs (which are not allowed in the partition key) with
        # an empty byte string
        for key in SkippedContentRow.PARTITION_KEY:
            if getattr(content, key) is None:
                setattr(content, key, self._magic_null_pk)

        statement = statement.bind(dataclasses.astuple(content))

        # Type used for hashing keys (usually, it will be
        # cassandra.metadata.Murmur3Token)
        token_class = self._cluster.metadata.token_map.token_class

        # Token of the row when it will be inserted. This is equivalent to
        # "SELECT token({', '.join(SkippedContentRow.PARTITION_KEY)})
        #  FROM skipped_content WHERE ..."
        # after the row is inserted; but we need the token to insert in the
        # index tables *before* inserting to the main 'skipped_content' table
        token = token_class.from_key(statement.routing_key).value
        assert TOKEN_BEGIN <= token <= TOKEN_END

        # Function to be called after the indexes contain their respective
        # row
        finalizer = functools.partial(self._skipped_content_add_finalize, statement)

        return (token, finalizer)

    @_prepared_select_statement(
        SkippedContentRow,
        f"WHERE {' AND '.join(map('%s = ?'.__mod__, HASH_ALGORITHMS))}",
    )
    def skipped_content_get_from_pk(
        self, content_hashes: Dict[str, bytes], *, statement
    ) -> Optional[SkippedContentRow]:
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
            return SkippedContentRow.from_dict(rows[0])
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

    @_prepared_exists_statement("revision")
    def revision_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement(RevisionRow)
    def revision_add_one(self, revision: RevisionRow, *, statement) -> None:
        self._add_one(statement, revision)

    @_prepared_statement("SELECT id FROM revision WHERE id IN ?")
    def revision_get_ids(self, revision_ids, *, statement) -> Iterable[int]:
        return (
            row["id"] for row in self._execute_with_retries(statement, [revision_ids])
        )

    @_prepared_select_statement(RevisionRow, "WHERE id IN ?")
    def revision_get(
        self, revision_ids: List[Sha1Git], *, statement
    ) -> Iterable[RevisionRow]:
        return map(
            RevisionRow.from_dict, self._execute_with_retries(statement, [revision_ids])
        )

    @_prepared_select_statement(RevisionRow, "WHERE token(id) > ? LIMIT 1")
    def revision_get_random(self, *, statement) -> Optional[RevisionRow]:
        return self._get_random_row(RevisionRow, statement)

    ##########################
    # 'revision_parent' table
    ##########################

    @_prepared_insert_statement(RevisionParentRow)
    def revision_parent_add_one(
        self, revision_parent: RevisionParentRow, *, statement
    ) -> None:
        self._add_one(statement, revision_parent)

    @_prepared_statement("SELECT parent_id FROM revision_parent WHERE id = ?")
    def revision_parent_get(
        self, revision_id: Sha1Git, *, statement
    ) -> Iterable[bytes]:
        return (
            row["parent_id"]
            for row in self._execute_with_retries(statement, [revision_id])
        )

    ##########################
    # 'release' table
    ##########################

    @_prepared_exists_statement("release")
    def release_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement(ReleaseRow)
    def release_add_one(self, release: ReleaseRow, *, statement) -> None:
        self._add_one(statement, release)

    @_prepared_select_statement(ReleaseRow, "WHERE id in ?")
    def release_get(self, release_ids: List[str], *, statement) -> Iterable[ReleaseRow]:
        return map(
            ReleaseRow.from_dict, self._execute_with_retries(statement, [release_ids])
        )

    @_prepared_select_statement(ReleaseRow, "WHERE token(id) > ? LIMIT 1")
    def release_get_random(self, *, statement) -> Optional[ReleaseRow]:
        return self._get_random_row(ReleaseRow, statement)

    ##########################
    # 'directory' table
    ##########################

    @_prepared_exists_statement("directory")
    def directory_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement(DirectoryRow)
    def directory_add_one(self, directory: DirectoryRow, *, statement) -> None:
        """Called after all calls to directory_entry_add_one, to
        commit/finalize the directory."""
        self._add_one(statement, directory)

    @_prepared_select_statement(DirectoryRow, "WHERE token(id) > ? LIMIT 1")
    def directory_get_random(self, *, statement) -> Optional[DirectoryRow]:
        return self._get_random_row(DirectoryRow, statement)

    ##########################
    # 'directory_entry' table
    ##########################

    @_prepared_insert_statement(DirectoryEntryRow)
    def directory_entry_add_one(self, entry: DirectoryEntryRow, *, statement) -> None:
        self._add_one(statement, entry)

    @_prepared_select_statement(DirectoryEntryRow, "WHERE directory_id IN ?")
    def directory_entry_get(
        self, directory_ids, *, statement
    ) -> Iterable[DirectoryEntryRow]:
        return map(
            DirectoryEntryRow.from_dict,
            self._execute_with_retries(statement, [directory_ids]),
        )

    ##########################
    # 'snapshot' table
    ##########################

    @_prepared_exists_statement("snapshot")
    def snapshot_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement(SnapshotRow)
    def snapshot_add_one(self, snapshot: SnapshotRow, *, statement) -> None:
        self._add_one(statement, snapshot)

    @_prepared_select_statement(SnapshotRow, "WHERE token(id) > ? LIMIT 1")
    def snapshot_get_random(self, *, statement) -> Optional[SnapshotRow]:
        return self._get_random_row(SnapshotRow, statement)

    ##########################
    # 'snapshot_branch' table
    ##########################

    @_prepared_insert_statement(SnapshotBranchRow)
    def snapshot_branch_add_one(self, branch: SnapshotBranchRow, *, statement) -> None:
        self._add_one(statement, branch)

    @_prepared_statement(
        "SELECT ascii_bins_count(target_type) AS counts "
        "FROM snapshot_branch "
        "WHERE snapshot_id = ? "
    )
    def snapshot_count_branches(
        self, snapshot_id: Sha1Git, *, statement
    ) -> Dict[Optional[str], int]:
        """Returns a dictionary from type names to the number of branches
        of that type."""
        row = self._execute_with_retries(statement, [snapshot_id]).one()
        (nb_none, counts) = row["counts"]
        return {None: nb_none, **counts}

    @_prepared_select_statement(
        SnapshotBranchRow, "WHERE snapshot_id = ? AND name >= ? LIMIT ?"
    )
    def snapshot_branch_get(
        self, snapshot_id: Sha1Git, from_: bytes, limit: int, *, statement
    ) -> Iterable[SnapshotBranchRow]:
        return map(
            SnapshotBranchRow.from_dict,
            self._execute_with_retries(statement, [snapshot_id, from_, limit]),
        )

    ##########################
    # 'origin' table
    ##########################

    @_prepared_insert_statement(OriginRow)
    def origin_add_one(self, origin: OriginRow, *, statement) -> None:
        self._add_one(statement, origin)

    @_prepared_select_statement(OriginRow, "WHERE sha1 = ?")
    def origin_get_by_sha1(self, sha1: bytes, *, statement) -> Iterable[OriginRow]:
        return map(OriginRow.from_dict, self._execute_with_retries(statement, [sha1]))

    def origin_get_by_url(self, url: str) -> Iterable[OriginRow]:
        return self.origin_get_by_sha1(hash_url(url))

    @_prepared_statement(
        f'SELECT token(sha1) AS tok, {", ".join(OriginRow.cols())} '
        f"FROM origin WHERE token(sha1) >= ? LIMIT ?"
    )
    def origin_list(
        self, start_token: int, limit: int, *, statement
    ) -> Iterable[Tuple[int, OriginRow]]:
        """Returns an iterable of (token, origin)"""
        return (
            (row["tok"], OriginRow.from_dict(remove_keys(row, ("tok",))))
            for row in self._execute_with_retries(statement, [start_token, limit])
        )

    @_prepared_select_statement(OriginRow)
    def origin_iter_all(self, *, statement) -> Iterable[OriginRow]:
        return map(OriginRow.from_dict, self._execute_with_retries(statement, []))

    @_prepared_statement("SELECT next_visit_id FROM origin WHERE sha1 = ?")
    def _origin_get_next_visit_id(self, origin_sha1: bytes, *, statement) -> int:
        rows = list(self._execute_with_retries(statement, [origin_sha1]))
        assert len(rows) == 1  # TODO: error handling
        return rows[0]["next_visit_id"]

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
            if res[0]["[applied]"]:
                # No data race
                return next_id
            else:
                # Someone else updated it before we did, let's try again
                next_id = res[0]["next_visit_id"]
                # TODO: abort after too many attempts

        return next_id

    ##########################
    # 'origin_visit' table
    ##########################

    @_prepared_select_statements(
        OriginVisitRow,
        {
            (True, ListOrder.ASC): (
                "WHERE origin = ? AND visit > ? ORDER BY visit ASC LIMIT ?"
            ),
            (True, ListOrder.DESC): (
                "WHERE origin = ? AND visit < ? ORDER BY visit DESC LIMIT ?"
            ),
            (False, ListOrder.ASC): "WHERE origin = ? ORDER BY visit ASC LIMIT ?",
            (False, ListOrder.DESC): "WHERE origin = ? ORDER BY visit DESC LIMIT ?",
        },
    )
    def origin_visit_get(
        self,
        origin_url: str,
        last_visit: Optional[int],
        limit: int,
        order: ListOrder,
        *,
        statements,
    ) -> Iterable[OriginVisitRow]:
        args: List[Any] = [origin_url]

        if last_visit is not None:
            args.append(last_visit)

        args.append(limit)

        statement = statements[(last_visit is not None, order)]
        return map(
            OriginVisitRow.from_dict, self._execute_with_retries(statement, args)
        )

    @_prepared_insert_statement(OriginVisitRow)
    def origin_visit_add_one(self, visit: OriginVisitRow, *, statement) -> None:
        self._add_one(statement, visit)

    @_prepared_select_statement(OriginVisitRow, "WHERE origin = ? AND visit = ?")
    def origin_visit_get_one(
        self, origin_url: str, visit_id: int, *, statement
    ) -> Optional[OriginVisitRow]:
        # TODO: error handling
        rows = list(self._execute_with_retries(statement, [origin_url, visit_id]))
        if rows:
            return OriginVisitRow.from_dict(rows[0])
        else:
            return None

    @_prepared_select_statement(OriginVisitRow, "WHERE origin = ?")
    def origin_visit_get_all(
        self, origin_url: str, *, statement
    ) -> Iterable[OriginVisitRow]:
        return map(
            OriginVisitRow.from_dict,
            self._execute_with_retries(statement, [origin_url]),
        )

    @_prepared_select_statement(OriginVisitRow, "WHERE token(origin) >= ?")
    def _origin_visit_iter_from(
        self, min_token: int, *, statement
    ) -> Iterable[OriginVisitRow]:
        return map(
            OriginVisitRow.from_dict, self._execute_with_retries(statement, [min_token])
        )

    @_prepared_select_statement(OriginVisitRow, "WHERE token(origin) < ?")
    def _origin_visit_iter_to(
        self, max_token: int, *, statement
    ) -> Iterable[OriginVisitRow]:
        return map(
            OriginVisitRow.from_dict, self._execute_with_retries(statement, [max_token])
        )

    def origin_visit_iter(self, start_token: int) -> Iterator[OriginVisitRow]:
        """Returns all origin visits in order from this token,
        and wraps around the token space."""
        yield from self._origin_visit_iter_from(start_token)
        yield from self._origin_visit_iter_to(start_token)

    ##########################
    # 'origin_visit_status' table
    ##########################

    @_prepared_select_statements(
        OriginVisitStatusRow,
        {
            (True, ListOrder.ASC): (
                "WHERE origin = ? AND visit = ? AND date >= ? "
                "ORDER BY visit ASC LIMIT ?"
            ),
            (True, ListOrder.DESC): (
                "WHERE origin = ? AND visit = ? AND date <= ? "
                "ORDER BY visit DESC LIMIT ?"
            ),
            (False, ListOrder.ASC): (
                "WHERE origin = ? AND visit = ? ORDER BY visit ASC LIMIT ?"
            ),
            (False, ListOrder.DESC): (
                "WHERE origin = ? AND visit = ? ORDER BY visit DESC LIMIT ?"
            ),
        },
    )
    def origin_visit_status_get_range(
        self,
        origin: str,
        visit: int,
        date_from: Optional[datetime.datetime],
        limit: int,
        order: ListOrder,
        *,
        statements,
    ) -> Iterable[OriginVisitStatusRow]:
        args: List[Any] = [origin, visit]

        if date_from is not None:
            args.append(date_from)

        args.append(limit)

        statement = statements[(date_from is not None, order)]

        return map(
            OriginVisitStatusRow.from_dict, self._execute_with_retries(statement, args)
        )

    @_prepared_insert_statement(OriginVisitStatusRow)
    def origin_visit_status_add_one(
        self, visit_update: OriginVisitStatusRow, *, statement
    ) -> None:
        self._add_one(statement, visit_update)

    def origin_visit_status_get_latest(
        self, origin: str, visit: int,
    ) -> Optional[OriginVisitStatusRow]:
        """Given an origin visit id, return its latest origin_visit_status

         """
        return next(self.origin_visit_status_get(origin, visit), None)

    @_prepared_select_statement(
        OriginVisitStatusRow, "WHERE origin = ? AND visit = ? ORDER BY date DESC"
    )
    def origin_visit_status_get(
        self, origin: str, visit: int, *, statement,
    ) -> Iterator[OriginVisitStatusRow]:
        """Return all origin visit statuses for a given visit

        """
        return map(
            OriginVisitStatusRow.from_dict,
            self._execute_with_retries(statement, [origin, visit]),
        )

    ##########################
    # 'metadata_authority' table
    ##########################

    @_prepared_insert_statement(MetadataAuthorityRow)
    def metadata_authority_add(self, authority: MetadataAuthorityRow, *, statement):
        self._add_one(statement, authority)

    @_prepared_select_statement(MetadataAuthorityRow, "WHERE type = ? AND url = ?")
    def metadata_authority_get(
        self, type, url, *, statement
    ) -> Optional[MetadataAuthorityRow]:
        rows = list(self._execute_with_retries(statement, [type, url]))
        if rows:
            return MetadataAuthorityRow.from_dict(rows[0])
        else:
            return None

    ##########################
    # 'metadata_fetcher' table
    ##########################

    @_prepared_insert_statement(MetadataFetcherRow)
    def metadata_fetcher_add(self, fetcher, *, statement):
        self._add_one(statement, fetcher)

    @_prepared_select_statement(MetadataFetcherRow, "WHERE name = ? AND version = ?")
    def metadata_fetcher_get(
        self, name, version, *, statement
    ) -> Optional[MetadataFetcherRow]:
        rows = list(self._execute_with_retries(statement, [name, version]))
        if rows:
            return MetadataFetcherRow.from_dict(rows[0])
        else:
            return None

    #########################
    # 'raw_extrinsic_metadata' table
    #########################

    @_prepared_insert_statement(RawExtrinsicMetadataRow)
    def raw_extrinsic_metadata_add(self, raw_extrinsic_metadata, *, statement):
        self._add_one(statement, raw_extrinsic_metadata)

    @_prepared_select_statement(
        RawExtrinsicMetadataRow,
        "WHERE id=? AND authority_url=? AND discovery_date>? AND authority_type=?",
    )
    def raw_extrinsic_metadata_get_after_date(
        self,
        id: str,
        authority_type: str,
        authority_url: str,
        after: datetime.datetime,
        *,
        statement,
    ) -> Iterable[RawExtrinsicMetadataRow]:
        return map(
            RawExtrinsicMetadataRow.from_dict,
            self._execute_with_retries(
                statement, [id, authority_url, after, authority_type]
            ),
        )

    @_prepared_select_statement(
        RawExtrinsicMetadataRow,
        "WHERE id=? AND authority_type=? AND authority_url=? "
        "AND (discovery_date, fetcher_name, fetcher_version) > (?, ?, ?)",
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
    ) -> Iterable[RawExtrinsicMetadataRow]:
        return map(
            RawExtrinsicMetadataRow.from_dict,
            self._execute_with_retries(
                statement,
                [
                    id,
                    authority_type,
                    authority_url,
                    after_date,
                    after_fetcher_name,
                    after_fetcher_version,
                ],
            ),
        )

    @_prepared_select_statement(
        RawExtrinsicMetadataRow, "WHERE id=? AND authority_url=? AND authority_type=?"
    )
    def raw_extrinsic_metadata_get(
        self, id: str, authority_type: str, authority_url: str, *, statement
    ) -> Iterable[RawExtrinsicMetadataRow]:
        return map(
            RawExtrinsicMetadataRow.from_dict,
            self._execute_with_retries(statement, [id, authority_url, authority_type]),
        )

    ##########################
    # Miscellaneous
    ##########################

    @_prepared_statement("SELECT uuid() FROM revision LIMIT 1;")
    def check_read(self, *, statement):
        self._execute_with_retries(statement, [])

    @_prepared_select_statement(ObjectCountRow, "WHERE partition_key=0")
    def stat_counters(self, *, statement) -> Iterable[ObjectCountRow]:
        return map(ObjectCountRow.from_dict, self._execute_with_retries(statement, []))
