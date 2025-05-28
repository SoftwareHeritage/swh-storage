# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import dataclasses
import datetime
import functools
import importlib
import itertools
import logging
import random
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from cassandra import ConsistencyLevel, CoordinationFailure, ReadTimeout, WriteTimeout
from cassandra.auth import AuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, ResultSet
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BoundStatement, PreparedStatement, dict_factory
from cassandra.util import Date
from mypy_extensions import NamedArg
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

from swh.core.utils import grouper
from swh.model.model import (
    Content,
    Person,
    Sha1Git,
    SkippedContent,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID
from swh.storage.exc import NonRetryableException, QueryTimeout
from swh.storage.interface import ListOrder, TotalHashDict

from ..utils import remove_keys
from .common import TOKEN_BEGIN, TOKEN_END, hash_url
from .model import (
    MAGIC_NULL_PK,
    BaseRow,
    ContentRow,
    DirectoryEntryRow,
    DirectoryRow,
    ExtIDByTargetRow,
    ExtIDRow,
    MetadataAuthorityRow,
    MetadataFetcherRow,
    MigrationRow,
    ObjectCountRow,
    ObjectReferenceRow,
    ObjectReferencesTableRow,
    OriginRow,
    OriginVisitRow,
    OriginVisitStatusRow,
    RawExtrinsicMetadataByIdRow,
    RawExtrinsicMetadataRow,
    ReleaseRow,
    RevisionParentRow,
    RevisionRow,
    SkippedContentRow,
    SnapshotBranchRow,
    SnapshotRow,
    content_index_table_name,
)
from .schema import CREATE_TABLES_QUERIES, HASH_ALGORITHMS, OBJECT_REFERENCES_TEMPLATE

PARTITION_KEY_RESTRICTION_MAX_SIZE = 100
"""Maximum number of restrictions in a single query.
Usually this is a very low number (eg. SELECT ... FROM ... WHERE x=?),
but some queries can request arbitrarily many (eg. SELECT ... FROM ... WHERE x IN ?).

This can cause performance issues, as the node getting the query need to
coordinate with other nodes to get the complete results.
See <https://github.com/scylladb/scylla/pull/4797> for details and rationale.
"""

BATCH_INSERT_MAX_SIZE = 1000


logger = logging.getLogger(__name__)


def _instantiate_auth_provider(configuration: Dict) -> AuthProvider:
    local_config = dict(configuration)
    cls = local_config.pop("cls", None)
    if not cls:
        raise ValueError(
            "Configuration error: The cls property is mandatory "
            " in the auth_provider configuration section"
        )

    (module_path, class_name) = cls.rsplit(".", 1)
    module = importlib.import_module(module_path, package=__package__)
    AuthProvider = getattr(module, class_name)

    return AuthProvider(**local_config)


def get_execution_profiles(
    consistency_level: str = "ONE",
) -> Dict[object, ExecutionProfile]:
    if consistency_level not in ConsistencyLevel.name_to_value:
        raise ValueError(
            f"Configuration error: Unknown consistency level '{consistency_level}'"
        )

    return {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            row_factory=dict_factory,
            consistency_level=ConsistencyLevel.name_to_value[consistency_level],
        )
    }


# Configuration for cassandra-driver's access to servers:
# * hit the right server directly when sending a query (TokenAwarePolicy),
# * if there's more than one, then pick one at random that's in the same
#   datacenter as the client (DCAwareRoundRobinPolicy)


def create_keyspace(cql_runner: "CqlRunner", *, durable_writes=True) -> None:
    extra_params = ""
    if not durable_writes:
        extra_params = "AND durable_writes = false"
    cql_runner.execute_with_retries(
        """CREATE KEYSPACE IF NOT EXISTS "%s"
                       WITH REPLICATION = {
                           'class' : 'SimpleStrategy',
                           'replication_factor' : 1
                       } %s;
                    """
        % (cql_runner.keyspace, extra_params),
        [],
    )
    cql_runner.execute_with_retries(f'USE "{cql_runner.keyspace}"', [])
    for table_name in CREATE_TABLES_QUERIES:
        create_table(cql_runner, table_name)


def create_table(cql_runner: "CqlRunner", table_name: str) -> None:
    query = CREATE_TABLES_QUERIES[table_name]
    current_table_options = cql_runner.table_options.get(table_name, "")
    if current_table_options.strip():
        current_table_options = "AND " + current_table_options
    query = query.format(table_options=current_table_options)
    logger.debug("Running:\n%s", query)
    cql_runner.execute_with_retries(query, [])


def mark_all_migrations_completed(cql_runner: "CqlRunner") -> None:
    from .migrations import MIGRATIONS, MigrationStatus

    cql_runner.migration_add_concurrent(
        [
            MigrationRow(
                id=migration.id,
                dependencies=migration.dependencies,
                min_read_version=migration.min_read_version,
                status=MigrationStatus.COMPLETED.value,
            )
            for migration in MIGRATIONS
        ]
    )


TRet = TypeVar("TRet")


def _prepared_statement(
    query: str,
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    """Returns a decorator usable on methods of CqlRunner, to
    inject them with a 'statement' argument, that is a prepared
    statement corresponding to the query.

    This only works on methods of CqlRunner, as preparing a
    statement requires a connection to a Cassandra server."""

    def decorator(f: Callable[..., TRet]):
        @functools.wraps(f)
        def newf(self: "CqlRunner", *args, **kwargs) -> TRet:
            if f.__name__ not in self._prepared_statements:
                statement: PreparedStatement = self._session.prepare(
                    query.format(keyspace=self.keyspace)
                )
                self._prepared_statements[f.__name__] = statement
            return f(
                self, *args, **kwargs, statement=self._prepared_statements[f.__name__]
            )

        return newf

    return decorator


TArg = TypeVar("TArg")
TSelf = TypeVar("TSelf")


def _insert_query(row_class: Type[BaseRow]) -> str:
    columns = row_class.cols()
    return (
        f"INSERT INTO {{keyspace}}.{row_class.TABLE} ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})"
    )


def _prepared_insert_statement(
    row_class: Type[BaseRow],
) -> Callable[
    [Callable[[TSelf, TArg, NamedArg(Any, "statement")], TRet]],  # noqa
    Callable[[TSelf, TArg], TRet],
]:
    """Shorthand for using `_prepared_statement` for `INSERT INTO`
    statements."""
    return _prepared_statement(_insert_query(row_class))


def _prepared_exists_statement(
    table_name: str,
) -> Callable[
    [Callable[[TSelf, TArg, NamedArg(Any, "statement")], TRet]],  # noqa
    Callable[[TSelf, TArg], TRet],
]:
    """Shorthand for using `_prepared_statement` for queries that only
    check which ids in a list exist in the table."""
    return _prepared_statement(f"SELECT id FROM {{keyspace}}.{table_name} WHERE id = ?")


def _prepared_select_statement(
    row_class: Type[BaseRow],
    clauses: str = "",
    cols: Optional[List[str]] = None,
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    if cols is None:
        cols = row_class.cols()

    return _prepared_statement(
        f"SELECT {', '.join(cols)} FROM {{keyspace}}.{row_class.TABLE} {clauses}"
    )


def _prepared_select_statements(
    row_class: Type[BaseRow],
    queries: Dict[Any, str],
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    """Like _prepared_statement, but supports multiple statements, passed a dict,
    and passes a dict of prepared statements to the decorated method"""
    cols = row_class.cols()

    statement_template = "SELECT {cols} FROM {keyspace}.{table} {rest}"

    def decorator(f: Callable[..., TRet]):
        @functools.wraps(f)
        def newf(self: "CqlRunner", *args, **kwargs) -> TRet:
            if f.__name__ not in self._prepared_statements:
                self._prepared_statements[f.__name__] = {
                    key: self._session.prepare(
                        statement_template.format(
                            cols=", ".join(cols),
                            keyspace=self.keyspace,
                            table=row_class.TABLE,
                            rest=query,
                        )
                    )
                    for (key, query) in queries.items()
                }
            return f(
                self, *args, **kwargs, statements=self._prepared_statements[f.__name__]
            )

        return newf

    return decorator


def _prepared_select_token_range_statement(
    row_class: Type[BaseRow],
    clauses: str = "",
    cols: Optional[List[str]] = None,
) -> Callable[[Callable[..., TRet]], Callable[..., TRet]]:
    """Like _prepared_select_statement, but adds a WHERE clause that selects
    all rows in a token range."""
    pk = ", ".join(row_class.PARTITION_KEY)
    return _prepared_select_statement(
        row_class,
        f"WHERE token({pk}) >= ? AND token({pk}) <= ? {clauses}",
        [f"token({pk}) AS tok", *(cols or row_class.cols())],
    )


def _prepared_delete_statement(
    row_class: Type[BaseRow],
) -> Callable[
    [Callable[[TSelf, TArg, NamedArg(Any, "statement")], TRet]],  # noqa
    Callable[[TSelf, TArg], TRet],
]:
    """Shorthand for using `_prepared_statement` for `DELETE FROM`
    statements."""

    where_conditions = " AND ".join(
        f"{column} = ?" for column in row_class.PARTITION_KEY
    )
    return _prepared_statement(
        f"DELETE FROM {{keyspace}}.{row_class.TABLE} WHERE {where_conditions}"
    )


def _next_bytes_value(value: bytes) -> bytes:
    """Returns the next bytes value by incrementing the integer
    representation of the provided value and converting it back
    to bytes.

    For instance when prefix is b"abcd", it returns b"abce".
    """
    next_value_int = int.from_bytes(value, byteorder="big") + 1
    return next_value_int.to_bytes(
        (next_value_int.bit_length() + 7) // 8, byteorder="big"
    )


class CqlRunner:
    """Class managing prepared statements and building queries to be sent
    to Cassandra."""

    def __init__(
        self,
        hosts: List[str],
        keyspace: str,
        port: int,
        consistency_level: str,
        auth_provider: Optional[Dict] = None,
        table_options: Optional[Dict[str, str]] = None,
        register_user_types: bool = True,
    ):
        auth_provider_impl: Optional[AuthProvider] = None
        if auth_provider:
            auth_provider_impl = _instantiate_auth_provider(auth_provider)
        self.table_options = table_options or {}

        self._cluster = Cluster(
            hosts,
            port=port,
            auth_provider=auth_provider_impl,
            execution_profiles=get_execution_profiles(consistency_level),
            connect_timeout=30,
            control_connection_timeout=30,
        )
        self.keyspace = keyspace
        self._session = self._cluster.connect()

        # directly a PreparedStatement for methods decorated with
        # @_prepared_statements (and its wrappers, _prepared_insert_statement,
        # _prepared_exists_statement, and _prepared_select_statement);
        # and a dict of PreparedStatements with @_prepared_select_statements
        self._prepared_statements: Dict[
            str, Union[PreparedStatement, Dict[Any, PreparedStatement]]
        ] = {}

        if register_user_types:
            self.register_user_types()

    def __del__(self):
        if hasattr(self, "_cluster"):
            self._cluster.shutdown()

    def register_user_types(self):
        self._cluster.register_user_type(
            self.keyspace, "microtimestamp_with_timezone", TimestampWithTimezone
        )
        self._cluster.register_user_type(self.keyspace, "microtimestamp", Timestamp)
        self._cluster.register_user_type(self.keyspace, "person", Person)

    ##########################
    # Common utility functions
    ##########################

    MAX_RETRIES = 3

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(CoordinationFailure),
    )
    def _execute_with_retries_inner(
        self, statement, args: Optional[Sequence]
    ) -> ResultSet:
        return self._session.execute(statement, args, timeout=1000.0)

    def execute_with_retries(self, statement, args: Optional[Sequence]) -> ResultSet:
        try:
            return self._execute_with_retries_inner(statement, args)
        except (ReadTimeout, WriteTimeout) as e:
            raise QueryTimeout(*e.args) from None

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(CoordinationFailure),
    )
    def _execute_many_with_retries_inner(
        self, statement, args_list: Sequence[Tuple]
    ) -> Iterable[Dict[str, Any]]:
        for res in execute_concurrent_with_args(self._session, statement, args_list):
            yield from res.result_or_exc

    def execute_many_with_retries(
        self, statement, args_list: Sequence[Tuple]
    ) -> Iterable[Dict[str, Any]]:
        try:
            return self._execute_many_with_retries_inner(statement, args_list)
        except (ReadTimeout, WriteTimeout) as e:
            raise QueryTimeout(*e.args) from None

    def _add_one(self, statement, obj: "DataclassInstance") -> None:
        self.execute_with_retries(statement, dataclasses.astuple(obj))

    def _add_many(self, statement, objs: Sequence[BaseRow]) -> None:
        tables = {obj.TABLE for obj in objs}
        assert len(tables) == 1, f"Cannot insert to multiple tables: {tables}"
        rows = [dataclasses.astuple(cast("DataclassInstance", obj)) for obj in objs]
        for _ in self.execute_many_with_retries(statement, rows):
            # Need to consume the generator to actually run the INSERTs
            pass

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(CoordinationFailure),
    )
    def _execute_many_statements_with_retries_inner(
        self,
        statements_and_parameters: Sequence[Tuple[Any, Tuple]],
    ) -> Iterable[Dict[str, Any]]:
        for res in execute_concurrent(
            self._session, statements_and_parameters, results_generator=True
        ):
            yield from res.result_or_exc

    def execute_many_statements_with_retries(
        self,
        statements_and_parameters: Sequence[Tuple[Any, Tuple]],
    ) -> Iterable[Dict[str, Any]]:
        try:
            return self._execute_many_statements_with_retries_inner(
                statements_and_parameters
            )
        except (ReadTimeout, WriteTimeout) as e:
            raise QueryTimeout(*e.args) from None

    _T = TypeVar("_T", bound=BaseRow)

    def _get_random_row(self, row_class: Type[_T], statement) -> Optional[_T]:  # noqa
        """Takes a prepared statement of the form
        "SELECT * FROM <table> WHERE token(<keys>) > ? LIMIT 1"
        and uses it to return a random row"""
        token = random.randint(TOKEN_BEGIN, TOKEN_END)
        rows = self.execute_with_retries(statement, [token])
        if not rows:
            # There are no row with a greater token; wrap around to get
            # the row with the smallest token
            rows = self.execute_with_retries(statement, [TOKEN_BEGIN])
        if rows:
            return row_class.from_dict(rows.one())
        else:
            return None

    def _missing(self, statement: PreparedStatement, ids):
        found_ids = set()

        if not ids:
            return []

        for row in self.execute_many_with_retries(statement, [(id_,) for id_ in ids]):
            found_ids.add(row["id"])

        return [id_ for id_ in ids if id_ not in found_ids]

    ##########################
    # 'migration' table
    ##########################

    @_prepared_insert_statement(MigrationRow)
    def migration_add_one(self, migration: MigrationRow, *, statement) -> None:
        self._add_one(statement, migration)

    @_prepared_insert_statement(MigrationRow)
    def migration_add_concurrent(
        self, migrations: List[MigrationRow], *, statement
    ) -> None:
        if len(migrations) == 0:
            # nothing to do
            return
        self._add_many(statement, migrations)

    @_prepared_select_statement(MigrationRow, "WHERE id IN ?")
    def migration_get(self, migration_ids, *, statement) -> Iterable[MigrationRow]:
        return map(
            MigrationRow.from_dict,
            self.execute_with_retries(statement, [migration_ids]),
        )

    @_prepared_select_statement(MigrationRow)
    def migration_list(self, *, statement) -> Iterable[MigrationRow]:
        return map(
            MigrationRow.from_dict,
            self.execute_with_retries(statement, []),
        )

    ##########################
    # 'content' table
    ##########################

    def _content_add_finalize(self, statement: BoundStatement) -> None:
        """Returned currified by content_add_prepare, to be called when the
        content row should be added to the primary table."""
        self.execute_with_retries(statement, None)

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
        self, content_hashes: TotalHashDict, *, statement
    ) -> Optional[ContentRow]:
        rows = list(
            self.execute_with_retries(
                statement,
                [cast(dict, content_hashes)[algo] for algo in HASH_ALGORITHMS],
            )
        )
        assert len(rows) <= 1
        if rows:
            return ContentRow(**rows[0])
        else:
            return None

    def content_missing_from_all_hashes(
        self, contents_hashes: List[TotalHashDict]
    ) -> Iterator[TotalHashDict]:
        for group in grouper(contents_hashes, PARTITION_KEY_RESTRICTION_MAX_SIZE):
            group = list(group)

            # Get all contents that share a sha256 with one of the contents in the group
            present = set(
                self._content_get_hashes_from_sha256(
                    [content["sha256"] for content in group]
                )
            )

            for content in group:
                for algo in HASH_ALGORITHMS:
                    assert content.get(algo) is not None, (
                        "content_missing_from_all_hashes must not be called with "
                        "partial hashes."
                    )
                if tuple(content.get(algo) for algo in HASH_ALGORITHMS) not in present:
                    yield content

    @_prepared_select_statement(ContentRow, "WHERE sha256 IN ?", HASH_ALGORITHMS)
    def _content_get_hashes_from_sha256(
        self, ids: List[bytes], *, statement
    ) -> Iterator[Tuple[bytes, bytes, bytes, bytes]]:
        for row in self.execute_with_retries(statement, [ids]):
            yield tuple(row[algo] for algo in HASH_ALGORITHMS)

    @_prepared_select_statement(
        ContentRow, f"WHERE token({', '.join(ContentRow.PARTITION_KEY)}) = ?"
    )
    def content_get_from_tokens(self, tokens, *, statement) -> Iterable[ContentRow]:
        return map(
            ContentRow.from_dict,
            self.execute_many_with_retries(statement, [(token,) for token in tokens]),
        )

    @_prepared_select_statement(
        ContentRow, f"WHERE token({', '.join(ContentRow.PARTITION_KEY)}) > ? LIMIT 1"
    )
    def content_get_random(self, *, statement) -> Optional[ContentRow]:
        return self._get_random_row(ContentRow, statement)

    @_prepared_select_token_range_statement(ContentRow, "LIMIT ?")
    def content_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterator[Tuple[int, ContentRow]]:
        """Returns an iterable of (token, row)"""
        return (
            (row["tok"], ContentRow.from_dict(remove_keys(row, ("tok",))))
            for row in self.execute_with_retries(statement, [start, end, limit])
        )

    @_prepared_delete_statement(ContentRow)
    def content_delete(self, content_hashes: TotalHashDict, *, statement) -> None:
        self.execute_with_retries(
            statement,
            [cast(dict, content_hashes)[algo] for algo in ContentRow.PARTITION_KEY],
        )
        for algo in HASH_ALGORITHMS:
            table = content_index_table_name(algo, skipped_content=False)
            query = f"""
                DELETE FROM {self.keyspace}.{table} WHERE {algo} = %s
            """
            self.execute_with_retries(query, [cast(dict, content_hashes)[algo]])

    ##########################
    # 'content_by_*' tables
    ##########################

    def content_index_add_one(self, algo: str, content: Content, token: int) -> None:
        """Adds a row mapping content[algo] to the token of the Content in
        the main 'content' table."""
        table = content_index_table_name(algo, skipped_content=False)
        query = f"""
            INSERT INTO {self.keyspace}.{table} ({algo}, target_token) VALUES (%s, %s)
        """
        self.execute_with_retries(query, [content.get_hash(algo), token])

    def content_get_tokens_from_single_algo(
        self, algo: str, hashes: List[bytes]
    ) -> Iterable[int]:
        assert algo in HASH_ALGORITHMS
        table = content_index_table_name(algo, skipped_content=False)
        query = f"SELECT target_token FROM {self.keyspace}.{table} WHERE {algo} = %s"
        return (
            row["target_token"]
            for row in self.execute_many_with_retries(
                query, [(hash_,) for hash_ in hashes]
            )
        )

    ##########################
    # 'skipped_content' table
    ##########################

    def _skipped_content_add_finalize(self, statement: BoundStatement) -> None:
        """Returned currified by skipped_content_add_prepare, to be called
        when the content row should be added to the primary table."""
        self.execute_with_retries(statement, None)

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
                setattr(content, key, MAGIC_NULL_PK)

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
            self.execute_with_retries(
                statement,
                [content_hashes[algo] or MAGIC_NULL_PK for algo in HASH_ALGORITHMS],
            )
        )
        assert len(rows) <= 1
        if rows:
            return SkippedContentRow.from_dict(rows[0])
        else:
            return None

    @_prepared_select_statement(
        SkippedContentRow,
        f"WHERE token({', '.join(SkippedContentRow.PARTITION_KEY)}) = ?",
    )
    def skipped_content_get_from_token(
        self, token, *, statement
    ) -> Iterable[SkippedContentRow]:
        return map(
            SkippedContentRow.from_dict, self.execute_with_retries(statement, [token])
        )

    @_prepared_delete_statement(SkippedContentRow)
    def skipped_content_delete(
        self, skipped_content_hashes: TotalHashDict, *, statement
    ) -> None:
        self.execute_with_retries(
            statement,
            [cast(dict, skipped_content_hashes)[algo] for algo in HASH_ALGORITHMS],
        )
        for algo in HASH_ALGORITHMS:
            table = content_index_table_name(algo, skipped_content=True)
            query = f"""
                DELETE FROM {self.keyspace}.{table} WHERE {algo} = %s
            """
            self.execute_with_retries(query, [cast(dict, skipped_content_hashes)[algo]])

    ##########################
    # 'skipped_content_by_*' tables
    ##########################

    def skipped_content_index_add_one(
        self, algo: str, content: SkippedContent, token: int
    ) -> None:
        """Adds a row mapping content[algo] to the token of the SkippedContent
        in the main 'skipped_content' table."""
        query = (
            f"INSERT INTO {self.keyspace}.skipped_content_by_{algo} ({algo}, target_token) "
            f"VALUES (%s, %s)"
        )
        self.execute_with_retries(
            query, [content.get_hash(algo) or MAGIC_NULL_PK, token]
        )

    def skipped_content_get_tokens_from_single_hash(
        self, algo: str, hash_: bytes
    ) -> Iterable[int]:
        assert algo in HASH_ALGORITHMS
        table = content_index_table_name(algo, skipped_content=True)
        query = f"SELECT target_token FROM {self.keyspace}.{table} WHERE {algo} = %s"
        return (
            row["target_token"] for row in self.execute_with_retries(query, [hash_])
        )

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

    @_prepared_select_statement(DirectoryRow, "WHERE id in ?")
    def directory_get(
        self, directory_ids: List[Sha1Git], *, statement
    ) -> Iterable[DirectoryRow]:
        """Return fields from the main directory table (e.g. raw_manifest, but not
        entries)"""
        return map(
            DirectoryRow.from_dict,
            self.execute_with_retries(statement, [directory_ids]),
        )

    @_prepared_select_token_range_statement(DirectoryRow, "LIMIT ?")
    def directory_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterator[Tuple[int, DirectoryRow]]:
        """Returns an iterable of (token, row)"""
        return (
            (row["tok"], DirectoryRow.from_dict(remove_keys(row, ("tok",))))
            for row in self.execute_with_retries(statement, [start, end, limit])
        )

    @_prepared_delete_statement(DirectoryRow)
    def directory_delete(self, directory_id: bytes, *, statement) -> None:
        self.execute_with_retries(statement, [directory_id])

    ##########################
    # 'directory_entry' table
    ##########################

    @_prepared_insert_statement(DirectoryEntryRow)
    def directory_entry_add_one(self, entry: DirectoryEntryRow, *, statement) -> None:
        self._add_one(statement, entry)

    @_prepared_insert_statement(DirectoryEntryRow)
    def directory_entry_add_concurrent(
        self, entries: List[DirectoryEntryRow], *, statement
    ) -> None:
        if len(entries) == 0:
            # nothing to do
            return
        assert (
            len({entry.directory_id for entry in entries}) == 1
        ), "directory_entry_add_many must be called with entries for a single dir"
        self._add_many(statement, entries)

    @_prepared_statement(
        "BEGIN UNLOGGED BATCH\n"
        + (_insert_query(DirectoryEntryRow) + ";\n") * BATCH_INSERT_MAX_SIZE
        + "APPLY BATCH"
    )
    def directory_entry_add_batch(
        self, entries: List[DirectoryEntryRow], *, statement
    ) -> None:
        if len(entries) == 0:
            # nothing to do
            return
        assert (
            len({entry.directory_id for entry in entries}) == 1
        ), "directory_entry_add_many must be called with entries for a single dir"

        for entry_group in grouper(entries, BATCH_INSERT_MAX_SIZE):
            entry_group_list = list(entry_group)
            if len(entry_group_list) == BATCH_INSERT_MAX_SIZE:
                entry_group_tuples = list(map(dataclasses.astuple, entry_group_list))
                self.execute_with_retries(
                    statement, list(itertools.chain.from_iterable(entry_group_tuples))
                )
            else:
                # Last group, with a smaller size than the BATCH we prepared.
                # Creating a prepared BATCH just for this then discarding it would
                # create too much churn on the server side; and using unprepared
                # statements is annoying (we can't use _insert_query() as they have
                # a different format)
                # Fall back to inserting concurrently.
                self.directory_entry_add_concurrent(entry_group_list)

    @_prepared_select_statement(DirectoryEntryRow, "WHERE directory_id IN ?")
    def directory_entry_get(
        self, directory_ids, *, statement
    ) -> Iterable[DirectoryEntryRow]:
        return map(
            DirectoryEntryRow.from_dict,
            self.execute_with_retries(statement, [directory_ids]),
        )

    @_prepared_select_statement(
        DirectoryEntryRow, "WHERE directory_id = ? AND name >= ? LIMIT ?"
    )
    def directory_entry_get_from_name(
        self, directory_id: Sha1Git, from_: bytes, limit: int, *, statement
    ) -> Iterable[DirectoryEntryRow]:
        return map(
            DirectoryEntryRow.from_dict,
            self.execute_with_retries(statement, [directory_id, from_, limit]),
        )

    @_prepared_delete_statement(DirectoryEntryRow)
    def directory_entry_delete(self, directory_id: Sha1Git, *, statement) -> None:
        self.execute_with_retries(statement, [directory_id])

    ##########################
    # 'revision' table
    ##########################

    @_prepared_exists_statement("revision")
    def revision_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement(RevisionRow)
    def revision_add_one(self, revision: RevisionRow, *, statement) -> None:
        self._add_one(statement, revision)

    @_prepared_select_statement(RevisionRow, "WHERE id IN ?", ["id"])
    def revision_get_ids(self, revision_ids, *, statement) -> Iterable[Sha1Git]:
        return (
            row["id"] for row in self.execute_with_retries(statement, [revision_ids])
        )

    @_prepared_select_statement(RevisionRow, "WHERE id IN ?")
    def revision_get(
        self, revision_ids: List[Sha1Git], *, statement
    ) -> Iterable[RevisionRow]:
        return map(
            RevisionRow.from_dict, self.execute_with_retries(statement, [revision_ids])
        )

    @_prepared_select_statement(RevisionRow, "WHERE token(id) > ? LIMIT 1")
    def revision_get_random(self, *, statement) -> Optional[RevisionRow]:
        return self._get_random_row(RevisionRow, statement)

    @_prepared_select_token_range_statement(RevisionRow, "LIMIT ?")
    def revision_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterator[Tuple[int, RevisionRow]]:
        """Returns an iterable of (token, row)"""
        return (
            (row["tok"], RevisionRow.from_dict(remove_keys(row, ("tok",))))
            for row in self.execute_with_retries(statement, [start, end, limit])
        )

    @_prepared_delete_statement(RevisionRow)
    def revision_delete(self, revision_id: Sha1Git, *, statement) -> None:
        self.execute_with_retries(statement, [revision_id])

    ##########################
    # 'revision_parent' table
    ##########################

    @_prepared_insert_statement(RevisionParentRow)
    def revision_parent_add_one(
        self, revision_parent: RevisionParentRow, *, statement
    ) -> None:
        self._add_one(statement, revision_parent)

    @_prepared_select_statement(RevisionParentRow, "WHERE id = ?", ["parent_id"])
    def revision_parent_get(
        self, revision_id: Sha1Git, *, statement
    ) -> Iterable[bytes]:
        return (
            row["parent_id"]
            for row in self.execute_with_retries(statement, [revision_id])
        )

    @_prepared_delete_statement(RevisionParentRow)
    def revision_parent_delete(self, revision_id: Sha1Git, *, statement) -> None:
        self.execute_with_retries(statement, [revision_id])

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
    def release_get(
        self, release_ids: List[Sha1Git], *, statement
    ) -> Iterable[ReleaseRow]:
        return map(
            ReleaseRow.from_dict, self.execute_with_retries(statement, [release_ids])
        )

    @_prepared_select_statement(ReleaseRow, "WHERE token(id) > ? LIMIT 1")
    def release_get_random(self, *, statement) -> Optional[ReleaseRow]:
        return self._get_random_row(ReleaseRow, statement)

    @_prepared_select_token_range_statement(ReleaseRow, "LIMIT ?")
    def release_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterator[Tuple[int, ReleaseRow]]:
        """Returns an iterable of (token, row)"""
        return (
            (row["tok"], ReleaseRow.from_dict(remove_keys(row, ("tok",))))
            for row in self.execute_with_retries(statement, [start, end, limit])
        )

    @_prepared_delete_statement(ReleaseRow)
    def release_delete(self, release_id: Sha1Git, *, statement) -> None:
        self.execute_with_retries(statement, [release_id])

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

    @_prepared_select_token_range_statement(SnapshotRow, "LIMIT ?")
    def snapshot_get_token_range(
        self, start: int, end: int, limit: int, *, statement
    ) -> Iterator[Tuple[int, SnapshotRow]]:
        """Returns an iterable of (token, row)"""
        return (
            (row["tok"], SnapshotRow.from_dict(remove_keys(row, ("tok",))))
            for row in self.execute_with_retries(statement, [start, end, limit])
        )

    @_prepared_delete_statement(SnapshotRow)
    def snapshot_delete(self, snapshot_id: Sha1Git, *, statement) -> None:
        self.execute_with_retries(statement, [snapshot_id])

    ##########################
    # 'snapshot_branch' table
    ##########################

    @_prepared_insert_statement(SnapshotBranchRow)
    def snapshot_branch_add_one(self, branch: SnapshotBranchRow, *, statement) -> None:
        self._add_one(statement, branch)

    @_prepared_statement(
        f"""
        SELECT target_type
        FROM {{keyspace}}.{SnapshotBranchRow.TABLE}
        WHERE snapshot_id = ? AND name >= ?
        """
    )
    def snapshot_count_branches_from_name(
        self, snapshot_id: Sha1Git, from_: bytes, *, statement
    ) -> Dict[Optional[str], int]:
        return Counter(
            row["target_type"]
            for row in self.execute_with_retries(statement, [snapshot_id, from_])
        )

    @_prepared_statement(
        f"""
        SELECT target_type
        FROM {{keyspace}}.{SnapshotBranchRow.TABLE}
        WHERE snapshot_id = ? AND name < ?
        """
    )
    def snapshot_count_branches_before_name(
        self,
        snapshot_id: Sha1Git,
        before: bytes,
        *,
        statement,
    ) -> Dict[Optional[str], int]:
        return Counter(
            row["target_type"]
            for row in self.execute_with_retries(statement, [snapshot_id, before])
        )

    def snapshot_count_branches(
        self,
        snapshot_id: Sha1Git,
        branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Dict[Optional[str], int]:
        """Returns a dictionary from type names to the number of branches
        of that type."""
        prefix = branch_name_exclude_prefix
        if prefix is None:
            return self.snapshot_count_branches_from_name(snapshot_id, b"")
        else:
            # counts branches before exclude prefix
            counts = Counter(
                self.snapshot_count_branches_before_name(snapshot_id, prefix)
            )

            # no need to execute that part if each bit of the prefix equals 1
            if prefix.replace(b"\xff", b"") != b"":
                # counts branches after exclude prefix and update counters
                counts.update(
                    self.snapshot_count_branches_from_name(
                        snapshot_id, _next_bytes_value(prefix)
                    )
                )
            return counts

    @_prepared_select_statement(
        SnapshotBranchRow, "WHERE snapshot_id = ? AND name >= ? LIMIT ?"
    )
    def snapshot_branch_get_from_name(
        self, snapshot_id: Sha1Git, from_: bytes, limit: int, *, statement
    ) -> Iterable[SnapshotBranchRow]:
        return map(
            SnapshotBranchRow.from_dict,
            self.execute_with_retries(statement, [snapshot_id, from_, limit]),
        )

    @_prepared_select_statement(
        SnapshotBranchRow, "WHERE snapshot_id = ? AND name >= ? AND name < ? LIMIT ?"
    )
    def snapshot_branch_get_range(
        self,
        snapshot_id: Sha1Git,
        from_: bytes,
        before: bytes,
        limit: int,
        *,
        statement,
    ) -> Iterable[SnapshotBranchRow]:
        return map(
            SnapshotBranchRow.from_dict,
            self.execute_with_retries(statement, [snapshot_id, from_, before, limit]),
        )

    def snapshot_branch_get(
        self,
        snapshot_id: Sha1Git,
        from_: bytes,
        limit: int,
        branch_name_exclude_prefix: Optional[bytes] = None,
    ) -> Iterable[SnapshotBranchRow]:
        prefix = branch_name_exclude_prefix
        if prefix is None or (from_ > prefix and not from_.startswith(prefix)):
            return self.snapshot_branch_get_from_name(snapshot_id, from_, limit)
        else:
            # get branches before the exclude prefix
            branches = list(
                self.snapshot_branch_get_range(snapshot_id, from_, prefix, limit)
            )
            nb_branches = len(branches)
            # no need to execute that part if limit is reached
            # or if each bit of the prefix equals 1
            if nb_branches < limit and prefix.replace(b"\xff", b"") != b"":
                # get branches after the exclude prefix and update list to return
                branches.extend(
                    self.snapshot_branch_get_from_name(
                        snapshot_id, _next_bytes_value(prefix), limit - nb_branches
                    )
                )
            return branches

    @_prepared_delete_statement(SnapshotBranchRow)
    def snapshot_branch_delete(self, snapshot_id: Sha1Git, *, statement) -> None:
        self.execute_with_retries(statement, [snapshot_id])

    ##########################
    # 'origin' table
    ##########################

    @_prepared_insert_statement(OriginRow)
    def origin_add_one(self, origin: OriginRow, *, statement) -> None:
        self._add_one(statement, origin)

    @_prepared_select_statement(OriginRow, "WHERE sha1 = ?")
    def origin_get_by_sha1(self, sha1: bytes, *, statement) -> Iterable[OriginRow]:
        return map(OriginRow.from_dict, self.execute_with_retries(statement, [sha1]))

    def origin_get_by_url(self, url: str) -> Iterable[OriginRow]:
        return self.origin_get_by_sha1(hash_url(url))

    @_prepared_select_token_range_statement(OriginRow, "LIMIT ?")
    def origin_list(
        self, start_token: int, limit: int, *, statement
    ) -> Iterable[Tuple[int, OriginRow]]:
        """Returns an iterable of (token, origin)"""
        return (
            (row["tok"], OriginRow.from_dict(remove_keys(row, ("tok",))))
            for row in self.execute_with_retries(
                statement, [start_token, TOKEN_END, limit]
            )
        )

    @_prepared_select_statement(OriginRow)
    def origin_iter_all(self, *, statement) -> Iterable[OriginRow]:
        return map(OriginRow.from_dict, self.execute_with_retries(statement, []))

    @_prepared_statement(
        f"""
        UPDATE {{keyspace}}.{OriginRow.TABLE}
        SET next_visit_id=?
        WHERE sha1 = ? IF next_visit_id<?
        """
    )
    def origin_bump_next_visit_id(
        self, origin_url: str, visit_id: int, *, statement
    ) -> None:
        origin_sha1 = hash_url(origin_url)
        next_id = visit_id + 1
        self.execute_with_retries(statement, [next_id, origin_sha1, next_id])

    @_prepared_select_statement(OriginRow, "WHERE sha1 = ?", ["next_visit_id"])
    def _origin_get_next_visit_id(self, origin_sha1: bytes, *, statement) -> int:
        rows = list(self.execute_with_retries(statement, [origin_sha1]))
        assert len(rows) == 1  # TODO: error handling
        return rows[0]["next_visit_id"]

    @_prepared_statement(
        f"""
        UPDATE {{keyspace}}.{OriginRow.TABLE}
        SET next_visit_id=?
        WHERE sha1 = ? IF next_visit_id=?
        """
    )
    def origin_generate_unique_visit_id(self, origin_url: str, *, statement) -> int:
        origin_sha1 = hash_url(origin_url)
        next_id = self._origin_get_next_visit_id(origin_sha1)
        while True:
            res = list(
                self.execute_with_retries(
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

    @_prepared_delete_statement(OriginRow)
    def origin_delete(self, sha1: bytes, *, statement) -> None:
        self.execute_with_retries(statement, [sha1])

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
        return map(OriginVisitRow.from_dict, self.execute_with_retries(statement, args))

    @_prepared_insert_statement(OriginVisitRow)
    def origin_visit_add_one(self, visit: OriginVisitRow, *, statement) -> None:
        self._add_one(statement, visit)

    @_prepared_select_statement(OriginVisitRow, "WHERE origin = ? AND visit = ?")
    def origin_visit_get_one(
        self, origin_url: str, visit_id: int, *, statement
    ) -> Optional[OriginVisitRow]:
        # TODO: error handling
        rows = list(self.execute_with_retries(statement, [origin_url, visit_id]))
        if rows:
            return OriginVisitRow.from_dict(rows[0])
        else:
            return None

    @_prepared_select_statement(OriginVisitRow, "WHERE origin = ? ORDER BY visit DESC")
    def origin_visit_iter_all(
        self, origin_url: str, *, statement
    ) -> Iterable[OriginVisitRow]:
        """Returns an iterator on visits for a given origin, ordered by descending
        visit id."""
        return map(
            OriginVisitRow.from_dict,
            self.execute_with_retries(statement, [origin_url]),
        )

    @_prepared_select_statement(OriginVisitRow, "WHERE token(origin) >= ?")
    def _origin_visit_iter_from(
        self, min_token: int, *, statement
    ) -> Iterable[OriginVisitRow]:
        return map(
            OriginVisitRow.from_dict, self.execute_with_retries(statement, [min_token])
        )

    @_prepared_select_statement(OriginVisitRow, "WHERE token(origin) < ?")
    def _origin_visit_iter_to(
        self, max_token: int, *, statement
    ) -> Iterable[OriginVisitRow]:
        return map(
            OriginVisitRow.from_dict, self.execute_with_retries(statement, [max_token])
        )

    def origin_visit_iter(self, start_token: int) -> Iterator[OriginVisitRow]:
        """Returns all origin visits in order from this token,
        and wraps around the token space."""
        yield from self._origin_visit_iter_from(start_token)
        yield from self._origin_visit_iter_to(start_token)

    @_prepared_delete_statement(OriginVisitRow)
    def origin_visit_delete(self, origin_url: str, *, statement) -> None:
        self.execute_with_retries(statement, [origin_url])

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
            OriginVisitStatusRow.from_dict, self.execute_with_retries(statement, args)
        )

    @_prepared_select_statement(
        OriginVisitStatusRow,
        "WHERE origin = ? AND visit >= ? AND visit <= ? ORDER BY visit ASC, date ASC",
    )
    def origin_visit_status_get_all_range(
        self,
        origin_url: str,
        visit_from: int,
        visit_to: int,
        *,
        statement,
    ) -> Iterable[OriginVisitStatusRow]:
        args = (origin_url, visit_from, visit_to)

        return map(
            OriginVisitStatusRow.from_dict, self.execute_with_retries(statement, args)
        )

    @_prepared_insert_statement(OriginVisitStatusRow)
    def origin_visit_status_add_one(
        self, visit_update: OriginVisitStatusRow, *, statement
    ) -> None:
        self._add_one(statement, visit_update)

    def origin_visit_status_get_latest(
        self,
        origin: str,
        visit: int,
    ) -> Optional[OriginVisitStatusRow]:
        """Given an origin visit id, return its latest origin_visit_status"""
        return next(self.origin_visit_status_get(origin, visit), None)

    @_prepared_select_statement(
        OriginVisitStatusRow,
        # 'visit DESC,' is optional with Cassandra 4, but ScyllaDB needs it
        "WHERE origin = ? AND visit = ? ORDER BY visit DESC, date DESC",
    )
    def origin_visit_status_get(
        self,
        origin: str,
        visit: int,
        *,
        statement,
    ) -> Iterator[OriginVisitStatusRow]:
        """Return all origin visit statuses for a given visit"""
        return map(
            OriginVisitStatusRow.from_dict,
            self.execute_with_retries(statement, [origin, visit]),
        )

    @_prepared_select_statement(OriginVisitStatusRow, "WHERE origin = ?", ["snapshot"])
    def origin_snapshot_get_all(self, origin: str, *, statement) -> Iterable[Sha1Git]:
        yield from {
            d["snapshot"]
            for d in self.execute_with_retries(statement, [origin])
            if d["snapshot"] is not None
        }

    @_prepared_delete_statement(OriginVisitStatusRow)
    def origin_visit_status_delete(self, origin_url: str, *, statement) -> None:
        self.execute_with_retries(statement, [origin_url])

    #########################
    # 'raw_extrinsic_metadata_by_id' table
    #########################

    @_prepared_insert_statement(RawExtrinsicMetadataByIdRow)
    def raw_extrinsic_metadata_by_id_add(self, row, *, statement):
        self._add_one(statement, row)

    @_prepared_select_statement(RawExtrinsicMetadataByIdRow, "WHERE id IN ?")
    def raw_extrinsic_metadata_get_by_ids(
        self, ids: List[Sha1Git], *, statement
    ) -> Iterable[RawExtrinsicMetadataByIdRow]:
        return map(
            RawExtrinsicMetadataByIdRow.from_dict,
            self.execute_with_retries(statement, [ids]),
        )

    @_prepared_delete_statement(RawExtrinsicMetadataByIdRow)
    def raw_extrinsic_metadata_by_id_delete(self, emd_id, *, statement):
        self.execute_with_retries(statement, [emd_id])

    #########################
    # 'raw_extrinsic_metadata' table
    #########################

    @_prepared_insert_statement(RawExtrinsicMetadataRow)
    def raw_extrinsic_metadata_add(self, raw_extrinsic_metadata, *, statement):
        self._add_one(statement, raw_extrinsic_metadata)

    @_prepared_select_statement(
        RawExtrinsicMetadataRow,
        "WHERE target=? AND authority_url=? AND discovery_date>? AND authority_type=?",
    )
    def raw_extrinsic_metadata_get_after_date(
        self,
        target: str,
        authority_type: str,
        authority_url: str,
        after: datetime.datetime,
        *,
        statement,
    ) -> Iterable[RawExtrinsicMetadataRow]:
        return map(
            RawExtrinsicMetadataRow.from_dict,
            self.execute_with_retries(
                statement, [target, authority_url, after, authority_type]
            ),
        )

    @_prepared_select_statement(
        RawExtrinsicMetadataRow,
        # This is equivalent to:
        #   WHERE target=? AND authority_type = ? AND authority_url = ? "
        #   AND (discovery_date, id) > (?, ?)"
        # but it needs to be written this way to work with ScyllaDB.
        "WHERE target=? AND (authority_type, authority_url) <= (?, ?) "
        "AND (authority_type, authority_url, discovery_date, id) > (?, ?, ?, ?)",
    )
    def raw_extrinsic_metadata_get_after_date_and_id(
        self,
        target: str,
        authority_type: str,
        authority_url: str,
        after_date: datetime.datetime,
        after_id: bytes,
        *,
        statement,
    ) -> Iterable[RawExtrinsicMetadataRow]:
        return map(
            RawExtrinsicMetadataRow.from_dict,
            self.execute_with_retries(
                statement,
                [
                    target,
                    authority_type,
                    authority_url,
                    authority_type,
                    authority_url,
                    after_date,
                    after_id,
                ],
            ),
        )

    @_prepared_select_statement(
        RawExtrinsicMetadataRow,
        "WHERE target=? AND authority_url=? AND authority_type=?",
    )
    def raw_extrinsic_metadata_get(
        self, target: str, authority_type: str, authority_url: str, *, statement
    ) -> Iterable[RawExtrinsicMetadataRow]:
        return map(
            RawExtrinsicMetadataRow.from_dict,
            self.execute_with_retries(
                statement, [target, authority_url, authority_type]
            ),
        )

    @_prepared_select_statement(RawExtrinsicMetadataRow, "WHERE target = ?")
    def raw_extrinsic_metadata_get_authorities(
        self, target: str, *, statement
    ) -> Iterable[Tuple[str, str]]:
        return (
            (entry["authority_type"], entry["authority_url"])
            for entry in self.execute_with_retries(statement, [target])
        )

    @_prepared_statement(
        """DELETE FROM {keyspace}.raw_extrinsic_metadata
            WHERE target = ?
              AND authority_type = ?
              AND authority_url = ?
              AND discovery_date = ?
              AND id = ?"""
    )
    def raw_extrinsic_metadata_delete(
        self,
        target,
        authority_type,
        authority_url,
        discovery_date,
        emd_id,
        *,
        statement,
    ):
        self.execute_with_retries(
            statement, [target, authority_type, authority_url, discovery_date, emd_id]
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
        rows = list(self.execute_with_retries(statement, [type, url]))
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
        rows = list(self.execute_with_retries(statement, [name, version]))
        if rows:
            return MetadataFetcherRow.from_dict(rows[0])
        else:
            return None

    ##########################
    # 'extid' table
    ##########################
    def _extid_add_finalize(self, statement: BoundStatement) -> None:
        """Returned currified by extid_add_prepare, to be called when the
        extid row should be added to the primary table."""
        self.execute_with_retries(statement, None)

    @_prepared_insert_statement(ExtIDRow)
    def extid_add_prepare(
        self, extid: ExtIDRow, *, statement
    ) -> Tuple[int, Callable[[], None]]:
        statement = statement.bind(dataclasses.astuple(extid))
        token_class = self._cluster.metadata.token_map.token_class
        token = token_class.from_key(statement.routing_key).value
        assert TOKEN_BEGIN <= token <= TOKEN_END

        # Function to be called after the indexes contain their respective
        # row
        finalizer = functools.partial(self._extid_add_finalize, statement)

        return (token, finalizer)

    @_prepared_select_statement(
        ExtIDRow,
        "WHERE extid_type=? AND extid=? AND extid_version=? "
        "AND target_type=? AND target=?",
    )
    def extid_get_from_pk(
        self,
        extid_type: str,
        extid: bytes,
        extid_version: int,
        target: CoreSWHID,
        *,
        statement,
    ) -> Optional[ExtIDRow]:
        rows = list(
            self.execute_with_retries(
                statement,
                [
                    extid_type,
                    extid,
                    extid_version,
                    target.object_type.value,
                    target.object_id,
                ],
            ),
        )
        assert len(rows) <= 1
        if rows:
            return ExtIDRow(**rows[0])
        else:
            return None

    @_prepared_select_statement(
        ExtIDRow,
        "WHERE token(extid_type, extid) = ?",
    )
    def extid_get_from_token(self, token: int, *, statement) -> Iterable[ExtIDRow]:
        return map(
            ExtIDRow.from_dict,
            self.execute_with_retries(statement, [token]),
        )

    # Rows are partitioned by token(extid_type, extid), then ordered (aka. "clustered")
    # by (extid_type, extid, extid_version, ...). This means that, without knowing the
    # exact extid_type and extid, we need to scan the whole partition; which should be
    # reasonably small. We can change the schema later if this becomes an issue
    @_prepared_select_statement(
        ExtIDRow,
        "WHERE token(extid_type, extid) = ? AND extid_version = ? ALLOW FILTERING",
    )
    def extid_get_from_token_and_extid_version(
        self, token: int, extid_version: int, *, statement
    ) -> Iterable[ExtIDRow]:
        return map(
            ExtIDRow.from_dict,
            self.execute_with_retries(statement, [token, extid_version]),
        )

    @_prepared_select_statement(
        ExtIDRow,
        "WHERE extid_type=? AND extid=?",
    )
    def extid_get_from_extid(
        self, extid_type: str, extid: bytes, *, statement
    ) -> Iterable[ExtIDRow]:
        return map(
            ExtIDRow.from_dict,
            self.execute_with_retries(statement, [extid_type, extid]),
        )

    @_prepared_select_statement(
        ExtIDRow,
        "WHERE extid_type=? AND extid=? AND extid_version = ?",
    )
    def extid_get_from_extid_and_version(
        self, extid_type: str, extid: bytes, extid_version: int, *, statement
    ) -> Iterable[ExtIDRow]:
        return map(
            ExtIDRow.from_dict,
            self.execute_with_retries(statement, [extid_type, extid, extid_version]),
        )

    def extid_get_from_target(
        self,
        target_type: str,
        target: bytes,
        extid_type: Optional[str] = None,
        extid_version: Optional[int] = None,
    ) -> Iterable[ExtIDRow]:
        for token in self._extid_get_tokens_from_target(target_type, target):
            if token is not None:
                if extid_type is not None and extid_version is not None:
                    extids = self.extid_get_from_token_and_extid_version(
                        token, extid_version
                    )
                else:
                    extids = self.extid_get_from_token(token)

                for extid in extids:
                    # re-check the extid against target (in case of murmur3 collision)
                    if (
                        extid is not None
                        and extid.target_type == target_type
                        and extid.target == target
                        and (
                            (extid_version is None and extid_type is None)
                            or (
                                (
                                    extid_version is not None
                                    and extid.extid_version == extid_version
                                    and extid_type is not None
                                    and extid.extid_type == extid_type
                                )
                            )
                        )
                    ):
                        yield extid

    @_prepared_statement(
        """DELETE FROM {keyspace}.extid
            WHERE extid_type = ?
              AND extid = ?
              AND extid_version = ?
              AND target_type = ?
              AND target = ?"""
    )
    def extid_delete(
        self,
        extid_type: str,
        extid: bytes,
        extid_version: int,
        target_type: str,
        target: bytes,
        *,
        statement,
    ) -> None:
        self.execute_with_retries(
            statement, [extid_type, extid, extid_version, target_type, target]
        )

    ##########################
    # 'extid_by_target' table
    ##########################

    @_prepared_insert_statement(ExtIDByTargetRow)
    def extid_index_add_one(self, row: ExtIDByTargetRow, *, statement) -> None:
        """Adds a row mapping extid[target_type, target] to the token of the ExtID in
        the main 'extid' table."""
        self._add_one(statement, row)

    @_prepared_select_statement(
        ExtIDByTargetRow, "WHERE target_type = ? AND target = ?"
    )
    def _extid_get_tokens_from_target(
        self, target_type: str, target: bytes, *, statement
    ) -> Iterable[int]:
        return (
            row["target_token"]
            for row in self.execute_with_retries(statement, [target_type, target])
        )

    @_prepared_statement(
        """DELETE FROM {keyspace}.extid_by_target
            WHERE target_type = ?
              AND target = ?"""
    )
    def extid_delete_from_by_target_table(
        self, target_type: str, target: bytes, *, statement
    ) -> None:
        self.execute_with_retries(statement, [target_type, target])

    ##########################
    # 'object_references' table
    ##########################

    _object_reference_current_table_and_insert_statement: Optional[
        Tuple[ObjectReferencesTableRow, PreparedStatement]
    ] = None

    @_prepared_insert_statement(ObjectReferenceRow)
    def object_reference_add_concurrent(
        self, entries: List[ObjectReferenceRow], *, statement
    ) -> None:
        if len(entries) == 0:
            # nothing to do
            return

        # find which table we are currently supposed to insert to
        today = datetime.date.today()
        table_and_statement = self._object_reference_current_table_and_insert_statement
        if table_and_statement is None:
            refresh_table_cache = True
        else:
            (table, statement) = table_and_statement
            refresh_table_cache = not (table.start <= Date(today) < table.end)

        # Update cached value _object_reference_current_table_and_statement
        # if we went out of its range
        if refresh_table_cache:
            columns = ObjectReferenceRow.cols()
            try:
                table = next(
                    table
                    for table in self.object_references_list_tables()
                    if table.start.date() <= today < table.end.date()
                )
            except StopIteration:
                raise NonRetryableException(
                    "No 'object_references_*' table open for writing."
                )
            statement = self._session.prepare(
                f"INSERT INTO {self.keyspace}.{table.name} ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})"
            )
            self._object_reference_current_table_and_insert_statement = (
                table,
                statement,
            )

        # actually add rows to the table
        self._add_many(statement, entries)

    def object_reference_get(
        self,
        target: Sha1Git,
        target_type: str,
        limit: int,
    ) -> Iterable[ObjectReferenceRow]:
        cols = ", ".join(ObjectReferenceRow.cols())
        table_names = [table.name for table in self.object_references_list_tables()]
        table_names.append("object_references")  # legacy, unsharded table
        statements_and_parameters = [
            (
                f"SELECT {cols} FROM {self.keyspace}.{table_name} "
                f"WHERE target_type = %s AND target = %s LIMIT %s",
                (target_type, target, limit),
            )
            for table_name in table_names
        ]
        rows = map(
            ObjectReferenceRow.from_dict,
            self.execute_many_statements_with_retries(statements_and_parameters),
        )
        return itertools.islice(rows, limit)

    ##########################
    # 'object_references_*' tables management
    ##########################

    _object_references_tables_cache_expiry = datetime.datetime.min.replace(
        tzinfo=datetime.timezone.utc
    )
    _object_references_tables_cache: List[ObjectReferencesTableRow] = []
    """Sorted by start date"""

    @_prepared_select_statement(
        ObjectReferencesTableRow, "WHERE pk = 0"
    )  # every row has pk=0
    def object_references_list_tables(
        self, *, statement
    ) -> List[ObjectReferencesTableRow]:
        """List existing tables of the object_references table, ordered from
        oldest to the most recent.

        Its result is cached per-:class:`CqlRunner` instance for an hour, to avoid
        a round-trip on every object write."""
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if self._object_references_tables_cache_expiry < now:
            rows = map(
                ObjectReferencesTableRow.from_dict,
                self.execute_with_retries(statement, []),
            )
            self._object_references_tables_cache = list(
                sorted(rows, key=lambda row: row.start)
            )
            self._object_references_tables_cache_expiry = now + datetime.timedelta(
                hours=1
            )

            # Clear object_reference_add_concurrent's cache
            self._object_reference_current_table_and_insert_statement = None

        return self._object_references_tables_cache

    @_prepared_insert_statement(ObjectReferencesTableRow)
    def object_references_create_table(
        self,
        date: Tuple[int, int],  # _prepared_insert_statement supports only one arg
        *,
        statement,
    ) -> Tuple[datetime.date, datetime.date]:
        """Create the table of the object_references table for the given ISO
        ``year`` and ``week``."""
        (year, week) = date

        # This date is guaranteed to be in week 1 by the ISO standard
        in_week1 = datetime.date(year=year, month=1, day=4)
        monday_of_week1 = in_week1 + datetime.timedelta(days=-in_week1.weekday())

        monday = monday_of_week1 + datetime.timedelta(weeks=week - 1)
        next_monday = monday + datetime.timedelta(days=7)

        name = f"object_references_{year:04d}w{week:02d}"

        # We need to create the table before adding a row to object_references_table, because
        # object writers may try writing to the new table as soon as we added that row, even
        # if this process did not create it yet (or crashed in between).
        # Conversely, creating the table is idempotent, so it is not an issue if this process
        # crashes between creating the table and adding the row.
        table_options = self.table_options.get("object_references_*", "")
        if table_options.strip():
            table_options = "AND " + table_options
        self.execute_with_retries(
            OBJECT_REFERENCES_TEMPLATE.format(
                keyspace=self.keyspace,
                name=name,
                table_options=table_options,
            ),
            [],
        )

        row = ObjectReferencesTableRow(
            pk=0,  # always the same value, puts everything in the same Cassandra partition
            name=name,
            year=year,
            week=week,
            start=Date(monday),  # datetime.date -> cassandra.util.Date
            end=Date(next_monday),  # ditto
        )
        self.execute_with_retries(statement, dataclasses.astuple(row))

        return (monday, next_monday)

    def object_references_drop_table(self, year: int, week: int) -> None:
        """Delete the table of the object_references table for the given ISO
        ``year`` and ``week``."""
        name = f"object_references_{year:04d}w{week:02d}"

        # must delete the row first, so other writers don't try to write to it.
        # Note that if we delete the last table, there is still a small chance
        # writers have read this row before, and will write to the table later.
        # Then they'll just crash and try again. This shouldn't happen in practice,
        # because we only delete old tables.
        self.execute_with_retries(
            f"DELETE FROM {self.keyspace}.object_references_table WHERE pk = 0 AND name = %s",
            [name],
        )

        # invalidate cache
        self._object_references_tables_cache_expiry = datetime.datetime.min.replace(
            tzinfo=datetime.timezone.utc
        )

        self.execute_with_retries(f"DROP TABLE {self.keyspace}.{name}", [])

    ##########################
    # Miscellaneous
    ##########################

    def stat_counters(self) -> Iterable[ObjectCountRow]:
        raise NotImplementedError(
            "stat_counters is not implemented by the Cassandra backend"
        )

    @_prepared_statement("SELECT uuid() FROM {keyspace}.revision LIMIT 1;")
    def check_read(self, *, statement):
        self.execute_with_retries(statement, [])
