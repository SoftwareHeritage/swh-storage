# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import json
import logging
import random
from typing import (
    Any, Callable, Dict, Generator, Iterable, List, Optional, TypeVar
)

from cassandra import CoordinationFailure
from cassandra.cluster import (
    Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, ResultSet)
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement
from tenacity import (
    retry, stop_after_attempt, wait_random_exponential,
    retry_if_exception_type,
)

from swh.model.model import (
    Sha1Git, TimestampWithTimezone, Timestamp, Person, Content,
    SkippedContent, OriginVisit, Origin
)

from .common import Row, TOKEN_BEGIN, TOKEN_END, hash_url
from .schema import CREATE_TABLES_QUERIES, HASH_ALGORITHMS


logger = logging.getLogger(__name__)


_execution_profiles = {
    EXEC_PROFILE_DEFAULT: ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy())),
}
# Configuration for cassandra-driver's access to servers:
# * hit the right server directly when sending a query (TokenAwarePolicy),
# * if there's more than one, then pick one at random that's in the same
#   datacenter as the client (DCAwareRoundRobinPolicy)


def create_keyspace(hosts: List[str], keyspace: str, port: int = 9042,
                    *, durable_writes=True):
    cluster = Cluster(
        hosts, port=port, execution_profiles=_execution_profiles)
    session = cluster.connect()
    extra_params = ''
    if not durable_writes:
        extra_params = 'AND durable_writes = false'
    session.execute('''CREATE KEYSPACE IF NOT EXISTS "%s"
                       WITH REPLICATION = {
                           'class' : 'SimpleStrategy',
                           'replication_factor' : 1
                       } %s;
                    ''' % (keyspace, extra_params))
    session.execute('USE "%s"' % keyspace)
    for query in CREATE_TABLES_QUERIES:
        session.execute(query)


T = TypeVar('T')


def _prepared_statement(
        query: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
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
            return f(self, *args, **kwargs,
                     statement=self._prepared_statements[f.__name__])
        return newf
    return decorator


def _prepared_insert_statement(table_name: str, columns: List[str]):
    """Shorthand for using `_prepared_statement` for `INSERT INTO`
    statements."""
    return _prepared_statement(
        'INSERT INTO %s (%s) VALUES (%s)' % (
            table_name,
            ', '.join(columns), ', '.join('?' for _ in columns),
        )
    )


def _prepared_exists_statement(table_name: str):
    """Shorthand for using `_prepared_statement` for queries that only
    check which ids in a list exist in the table."""
    return _prepared_statement(f'SELECT id FROM {table_name} WHERE id IN ?')


class CqlRunner:
    """Class managing prepared statements and building queries to be sent
    to Cassandra."""
    def __init__(self, hosts: List[str], keyspace: str, port: int):
        self._cluster = Cluster(
            hosts, port=port, execution_profiles=_execution_profiles)
        self._session = self._cluster.connect(keyspace)
        self._cluster.register_user_type(
            keyspace, 'microtimestamp_with_timezone', TimestampWithTimezone)
        self._cluster.register_user_type(
            keyspace, 'microtimestamp', Timestamp)
        self._cluster.register_user_type(
            keyspace, 'person', Person)

        self._prepared_statements: Dict[str, PreparedStatement] = {}

    ##########################
    # Common utility functions
    ##########################

    MAX_RETRIES = 3

    @retry(wait=wait_random_exponential(multiplier=1, max=10),
           stop=stop_after_attempt(MAX_RETRIES),
           retry=retry_if_exception_type(CoordinationFailure))
    def _execute_with_retries(self, statement, args) -> ResultSet:
        return self._session.execute(statement, args, timeout=1000.)

    @_prepared_statement('UPDATE object_count SET count = count + ? '
                         'WHERE partition_key = 0 AND object_type = ?')
    def _increment_counter(
            self, object_type: str, nb: int, *, statement: PreparedStatement
            ) -> None:
        self._execute_with_retries(statement, [nb, object_type])

    def _add_one(
            self, statement, object_type: str, obj, keys: List[str]
            ) -> None:
        self._increment_counter(object_type, 1)
        self._execute_with_retries(
            statement, [getattr(obj, key) for key in keys])

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

    _content_pk = ['sha1', 'sha1_git', 'sha256', 'blake2s256']
    _content_keys = [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length',
        'ctime', 'status']

    @_prepared_insert_statement('content', _content_keys)
    def content_add_one(self, content, *, statement) -> None:
        self._add_one(statement, 'content', content, self._content_keys)

    @_prepared_statement('SELECT * FROM content WHERE ' +
                         ' AND '.join(map('%s = ?'.__mod__, HASH_ALGORITHMS)))
    def content_get_from_pk(
            self, content_hashes: Dict[str, bytes], *, statement
            ) -> Optional[Row]:
        rows = list(self._execute_with_retries(
            statement, [content_hashes[algo] for algo in HASH_ALGORITHMS]))
        assert len(rows) <= 1
        if rows:
            return rows[0]
        else:
            return None

    @_prepared_statement('SELECT * FROM content WHERE token(%s) > ? LIMIT 1'
                         % ', '.join(_content_pk))
    def content_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    @_prepared_statement(('SELECT token({0}) AS tok, {1} FROM content '
                         'WHERE token({0}) >= ? AND token({0}) <= ? LIMIT ?')
                         .format(', '.join(_content_pk),
                                 ', '.join(_content_keys)))
    def content_get_token_range(
            self, start: int, end: int, limit: int, *, statement) -> Row:
        return self._execute_with_retries(statement, [start, end, limit])

    ##########################
    # 'content_by_*' tables
    ##########################

    @_prepared_statement('SELECT sha1_git FROM content_by_sha1_git '
                         'WHERE sha1_git IN ?')
    def content_missing_by_sha1_git(
            self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    def content_index_add_one(self, main_algo: str, content: Content) -> None:
        query = 'INSERT INTO content_by_{algo} ({cols}) VALUES ({values})' \
            .format(algo=main_algo, cols=', '.join(self._content_pk),
                    values=', '.join('%s' for _ in self._content_pk))
        self._execute_with_retries(
            query, [content.get_hash(algo) for algo in self._content_pk])

    def content_get_pks_from_single_hash(
            self, algo: str, hash_: bytes) -> List[Row]:
        assert algo in HASH_ALGORITHMS
        query = 'SELECT * FROM content_by_{algo} WHERE {algo} = %s'.format(
            algo=algo)
        return list(self._execute_with_retries(query, [hash_]))

    ##########################
    # 'skipped_content' table
    ##########################

    _skipped_content_pk = ['sha1', 'sha1_git', 'sha256', 'blake2s256']
    _skipped_content_keys = [
        'sha1', 'sha1_git', 'sha256', 'blake2s256', 'length',
        'ctime', 'status', 'reason', 'origin']
    _magic_null_pk = b''
    """
    NULLs are not allowed in primary keys; instead use an empty
    value
    """

    @_prepared_insert_statement('skipped_content', _skipped_content_keys)
    def skipped_content_add_one(self, content, *, statement) -> None:
        content = content.to_dict()
        for key in self._skipped_content_pk:
            if content[key] is None:
                content[key] = self._magic_null_pk
        content = SkippedContent.from_dict(content)
        self._add_one(statement, 'skipped_content', content,
                      self._skipped_content_keys)

    @_prepared_statement('SELECT * FROM skipped_content WHERE ' +
                         ' AND '.join(map('%s = ?'.__mod__, HASH_ALGORITHMS)))
    def skipped_content_get_from_pk(
            self, content_hashes: Dict[str, bytes], *, statement
            ) -> Optional[Row]:
        rows = list(self._execute_with_retries(
            statement, [content_hashes[algo] or self._magic_null_pk
                        for algo in HASH_ALGORITHMS]))
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
            self, main_algo: str, content: Content) -> None:
        assert content.get_hash(main_algo) is not None
        query = ('INSERT INTO skipped_content_by_{algo} ({cols}) '
                 'VALUES ({values})').format(
                algo=main_algo, cols=', '.join(self._content_pk),
                values=', '.join('%s' for _ in self._content_pk))
        self._execute_with_retries(
            query, [content.get_hash(algo) or self._magic_null_pk
                    for algo in self._content_pk])

    ##########################
    # 'revision' table
    ##########################

    _revision_keys = [
        'id', 'date', 'committer_date', 'type', 'directory', 'message',
        'author', 'committer',
        'synthetic', 'metadata']

    @_prepared_exists_statement('revision')
    def revision_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement('revision', _revision_keys)
    def revision_add_one(self, revision: Dict[str, Any], *, statement) -> None:
        self._add_one(statement, 'revision', revision, self._revision_keys)

    @_prepared_statement('SELECT id FROM revision WHERE id IN ?')
    def revision_get_ids(self, revision_ids, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [revision_ids])

    @_prepared_statement('SELECT * FROM revision WHERE id IN ?')
    def revision_get(self, revision_ids, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [revision_ids])

    @_prepared_statement('SELECT * FROM revision WHERE token(id) > ? LIMIT 1')
    def revision_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'revision_parent' table
    ##########################

    _revision_parent_keys = ['id', 'parent_rank', 'parent_id']

    @_prepared_insert_statement('revision_parent', _revision_parent_keys)
    def revision_parent_add_one(
            self, id_: Sha1Git, parent_rank: int, parent_id: Sha1Git, *,
            statement) -> None:
        self._execute_with_retries(
            statement, [id_, parent_rank, parent_id])

    @_prepared_statement('SELECT parent_id FROM revision_parent WHERE id = ?')
    def revision_parent_get(
            self, revision_id: Sha1Git, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [revision_id])

    ##########################
    # 'release' table
    ##########################

    _release_keys = [
        'id', 'target', 'target_type', 'date', 'name', 'message', 'author',
        'synthetic']

    @_prepared_exists_statement('release')
    def release_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement('release', _release_keys)
    def release_add_one(self, release: Dict[str, Any], *, statement) -> None:
        self._add_one(statement, 'release', release, self._release_keys)

    @_prepared_statement('SELECT * FROM release WHERE id in ?')
    def release_get(self, release_ids: List[str], *, statement) -> None:
        return self._execute_with_retries(statement, [release_ids])

    @_prepared_statement('SELECT * FROM release WHERE token(id) > ? LIMIT 1')
    def release_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'directory' table
    ##########################

    _directory_keys = ['id']

    @_prepared_exists_statement('directory')
    def directory_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement('directory', _directory_keys)
    def directory_add_one(self, directory_id: Sha1Git, *, statement) -> None:
        """Called after all calls to directory_entry_add_one, to
        commit/finalize the directory."""
        self._execute_with_retries(statement, [directory_id])
        self._increment_counter('directory', 1)

    @_prepared_statement('SELECT * FROM directory WHERE token(id) > ? LIMIT 1')
    def directory_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'directory_entry' table
    ##########################

    _directory_entry_keys = ['directory_id', 'name', 'type', 'target', 'perms']

    @_prepared_insert_statement('directory_entry', _directory_entry_keys)
    def directory_entry_add_one(
            self, entry: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [entry[key] for key in self._directory_entry_keys])

    @_prepared_statement('SELECT * FROM directory_entry '
                         'WHERE directory_id IN ?')
    def directory_entry_get(self, directory_ids, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [directory_ids])

    ##########################
    # 'snapshot' table
    ##########################

    _snapshot_keys = ['id']

    @_prepared_exists_statement('snapshot')
    def snapshot_missing(self, ids: List[bytes], *, statement) -> List[bytes]:
        return self._missing(statement, ids)

    @_prepared_insert_statement('snapshot', _snapshot_keys)
    def snapshot_add_one(self, snapshot_id: Sha1Git, *, statement) -> None:
        self._execute_with_retries(statement, [snapshot_id])
        self._increment_counter('snapshot', 1)

    @_prepared_statement('SELECT * FROM snapshot '
                         'WHERE id = ?')
    def snapshot_get(self, snapshot_id: Sha1Git, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [snapshot_id])

    @_prepared_statement('SELECT * FROM snapshot WHERE token(id) > ? LIMIT 1')
    def snapshot_get_random(self, *, statement) -> Optional[Row]:
        return self._get_random_row(statement)

    ##########################
    # 'snapshot_branch' table
    ##########################

    _snapshot_branch_keys = ['snapshot_id', 'name', 'target_type', 'target']

    @_prepared_insert_statement('snapshot_branch', _snapshot_branch_keys)
    def snapshot_branch_add_one(
            self, branch: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [branch[key] for key in self._snapshot_branch_keys])

    @_prepared_statement('SELECT ascii_bins_count(target_type) AS counts '
                         'FROM snapshot_branch '
                         'WHERE snapshot_id = ? ')
    def snapshot_count_branches(
            self, snapshot_id: Sha1Git, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [snapshot_id])

    @_prepared_statement('SELECT * FROM snapshot_branch '
                         'WHERE snapshot_id = ? AND name >= ?'
                         'LIMIT ?')
    def snapshot_branch_get(
            self, snapshot_id: Sha1Git, from_: bytes, limit: int, *,
            statement) -> None:
        return self._execute_with_retries(
            statement, [snapshot_id, from_, limit])

    ##########################
    # 'origin' table
    ##########################

    origin_keys = ['sha1', 'url', 'type', 'next_visit_id']

    @_prepared_statement('INSERT INTO origin (sha1, url, next_visit_id) '
                         'VALUES (?, ?, 1) IF NOT EXISTS')
    def origin_add_one(self, origin: Origin, *, statement) -> None:
        self._execute_with_retries(
            statement, [hash_url(origin.url), origin.url])
        self._increment_counter('origin', 1)

    @_prepared_statement('SELECT * FROM origin WHERE sha1 = ?')
    def origin_get_by_sha1(self, sha1: bytes, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [sha1])

    def origin_get_by_url(self, url: str) -> ResultSet:
        return self.origin_get_by_sha1(hash_url(url))

    @_prepared_statement(
        f'SELECT token(sha1) AS tok, {", ".join(origin_keys)} '
        f'FROM origin WHERE token(sha1) >= ? LIMIT ?')
    def origin_list(
            self, start_token: int, limit: int, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [start_token, limit])

    @_prepared_statement('SELECT * FROM origin')
    def origin_iter_all(self, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [])

    @_prepared_statement('SELECT next_visit_id FROM origin WHERE sha1 = ?')
    def _origin_get_next_visit_id(
            self, origin_sha1: bytes, *, statement) -> int:
        rows = list(self._execute_with_retries(statement, [origin_sha1]))
        assert len(rows) == 1  # TODO: error handling
        return rows[0].next_visit_id

    @_prepared_statement('UPDATE origin SET next_visit_id=? '
                         'WHERE sha1 = ? IF next_visit_id=?')
    def origin_generate_unique_visit_id(
            self, origin_url: str, *, statement) -> int:
        origin_sha1 = hash_url(origin_url)
        next_id = self._origin_get_next_visit_id(origin_sha1)
        while True:
            res = list(self._execute_with_retries(
                statement, [next_id+1, origin_sha1, next_id]))
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
        'origin', 'visit', 'type', 'date', 'status', 'metadata', 'snapshot']
    _origin_visit_update_keys = [
        'type', 'date', 'status', 'metadata', 'snapshot']

    @_prepared_statement('SELECT * FROM origin_visit '
                         'WHERE origin = ? AND visit > ?')
    def _origin_visit_get_no_limit(
            self, origin_url: str, last_visit: int, *, statement) -> ResultSet:
        return self._execute_with_retries(statement, [origin_url, last_visit])

    @_prepared_statement('SELECT * FROM origin_visit '
                         'WHERE origin = ? AND visit > ? LIMIT ?')
    def _origin_visit_get_limit(
            self, origin_url: str, last_visit: int, limit: int, *, statement
            ) -> ResultSet:
        return self._execute_with_retries(
            statement, [origin_url, last_visit, limit])

    def origin_visit_get(
            self, origin_url: str, last_visit: Optional[int],
            limit: Optional[int]) -> ResultSet:
        if last_visit is None:
            last_visit = -1

        if limit is None:
            return self._origin_visit_get_no_limit(origin_url, last_visit)
        else:
            return self._origin_visit_get_limit(origin_url, last_visit, limit)

    def origin_visit_update(
            self, origin_url: str, visit_id: int, updates: Dict[str, Any]
            ) -> None:
        set_parts = []
        args: List[Any] = []
        for (column, value) in updates.items():
            set_parts.append(f'{column} = %s')
            if column == 'metadata':
                args.append(json.dumps(value))
            else:
                args.append(value)

        if not set_parts:
            return

        query = ('UPDATE origin_visit SET ' + ', '.join(set_parts) +
                 ' WHERE origin = %s AND visit = %s')
        self._execute_with_retries(
            query, args + [origin_url, visit_id])

    @_prepared_insert_statement('origin_visit', _origin_visit_keys)
    def origin_visit_add_one(
            self, visit: OriginVisit, *, statement) -> None:
        self._add_one(statement, 'origin_visit', visit,
                      self._origin_visit_keys)

    @_prepared_statement(
        'UPDATE origin_visit SET ' +
        ', '.join('%s = ?' % key for key in _origin_visit_update_keys) +
        ' WHERE origin = ? AND visit = ?')
    def origin_visit_upsert(
            self, visit: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement,
            [visit.get(key) for key in self._origin_visit_update_keys]
            + [visit['origin'], visit['visit']])
        # TODO:  check if there is already one
        self._increment_counter('origin_visit', 1)

    @_prepared_statement('SELECT * FROM origin_visit '
                         'WHERE origin = ? AND visit = ?')
    def origin_visit_get_one(
            self, origin_url: str, visit_id: int, *,
            statement) -> Optional[Row]:
        # TODO: error handling
        rows = list(self._execute_with_retries(
            statement, [origin_url, visit_id]))
        if rows:
            return rows[0]
        else:
            return None

    @_prepared_statement('SELECT * FROM origin_visit '
                         'WHERE origin = ?')
    def origin_visit_get_all(self, origin_url: str, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [origin_url])

    @_prepared_statement('SELECT * FROM origin_visit WHERE origin = ?')
    def origin_visit_get_latest(
            self, origin: str, allowed_statuses: Optional[Iterable[str]],
            require_snapshot: bool, *, statement) -> Optional[Row]:
        # TODO: do the ordering and filtering in Cassandra
        rows = list(self._execute_with_retries(statement, [origin]))

        rows.sort(key=lambda row: (row.date, row.visit), reverse=True)

        for row in rows:
            if require_snapshot and row.snapshot is None:
                continue
            if allowed_statuses is not None \
                    and row.status not in allowed_statuses:
                continue
            if row.snapshot is not None and \
                    self.snapshot_missing([row.snapshot]):
                raise ValueError('visit references unknown snapshot')
            return row
        else:
            return None

    @_prepared_statement('SELECT * FROM origin_visit WHERE token(origin) >= ?')
    def _origin_visit_iter_from(
            self, min_token: int, *, statement) -> Generator[Row, None, None]:
        yield from self._execute_with_retries(statement, [min_token])

    @_prepared_statement('SELECT * FROM origin_visit WHERE token(origin) < ?')
    def _origin_visit_iter_to(
            self, max_token: int, *, statement) -> Generator[Row, None, None]:
        yield from self._execute_with_retries(statement, [max_token])

    def origin_visit_iter(
            self, start_token: int) -> Generator[Row, None, None]:
        """Returns all origin visits in order from this token,
        and wraps around the token space."""
        yield from self._origin_visit_iter_from(start_token)
        yield from self._origin_visit_iter_to(start_token)

    ##########################
    # 'tool' table
    ##########################

    _tool_keys = ['id', 'name', 'version', 'configuration']

    @_prepared_insert_statement('tool_by_uuid', _tool_keys)
    def tool_by_uuid_add_one(self, tool: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [tool[key] for key in self._tool_keys])

    @_prepared_insert_statement('tool', _tool_keys)
    def tool_add_one(self, tool: Dict[str, Any], *, statement) -> None:
        self._execute_with_retries(
            statement, [tool[key] for key in self._tool_keys])
        self._increment_counter('tool', 1)

    @_prepared_statement('SELECT id FROM tool '
                         'WHERE name = ? AND version = ? '
                         'AND configuration = ?')
    def tool_get_one_uuid(
            self, name: str, version: str, configuration: Dict[str, Any], *,
            statement) -> Optional[str]:
        rows = list(self._execute_with_retries(
            statement, [name, version, configuration]))
        if rows:
            assert len(rows) == 1
            return rows[0].id
        else:
            return None

    ##########################
    # Miscellaneous
    ##########################

    @_prepared_statement('SELECT uuid() FROM revision LIMIT 1;')
    def check_read(self, *, statement):
        self._execute_with_retries(statement, [])

    @_prepared_statement('SELECT object_type, count FROM object_count '
                         'WHERE partition_key=0')
    def stat_counters(self, *, statement) -> ResultSet:
        return self._execute_with_retries(
            statement, [])
