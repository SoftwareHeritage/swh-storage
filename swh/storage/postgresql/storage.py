# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
from collections import defaultdict
import contextlib
from contextlib import contextmanager
import datetime
import functools
import itertools
import logging
import operator
from typing import (
    Any,
    Callable,
    Counter,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import attr
import psycopg2
import psycopg2.errors
import psycopg2.pool

from swh.core.api.serializers import msgpack_dumps, msgpack_loads
from swh.core.db.common import db_transaction as _db_transaction
from swh.core.db.common import db_transaction_generator as _db_transaction_generator
from swh.core.db.db_utils import swh_db_flavor, swh_db_version
from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_bytes, hash_to_hex
from swh.model.model import (
    SHA1_SIZE,
    Content,
    Directory,
    DirectoryEntry,
    ExtID,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Release,
    Revision,
    Sha1,
    Sha1Git,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID, ObjectType
from swh.storage.exc import (
    HashCollision,
    QueryTimeout,
    StorageArgumentException,
    StorageDBError,
    UnknownMetadataAuthority,
    UnknownMetadataFetcher,
)
from swh.storage.interface import (
    VISIT_STATUSES,
    HashDict,
    ListOrder,
    ObjectReference,
    OriginVisitWithStatuses,
    PagedResult,
    PartialBranches,
    SnapshotBranchByNameResponse,
)
from swh.storage.objstorage import ObjStorage
from swh.storage.utils import (
    extract_collision_hash,
    get_partition_bounds_bytes,
    map_optional,
    now,
)
from swh.storage.writer import JournalWriter

from . import converters
from .db import Db

logger = logging.getLogger(__name__)

# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000

EMPTY_SNAPSHOT_ID = hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e")
"""Identifier for the empty snapshot"""


VALIDATION_EXCEPTIONS = (
    KeyError,
    TypeError,
    ValueError,
    psycopg2.errors.CheckViolation,
    psycopg2.errors.IntegrityError,
    psycopg2.errors.InvalidTextRepresentation,
    psycopg2.errors.NotNullViolation,
    psycopg2.errors.NumericValueOutOfRange,
    psycopg2.errors.UndefinedFunction,  # (raised on wrong argument typs)
    psycopg2.errors.ProgramLimitExceeded,  # typically person_name_idx
)
"""Exceptions raised by postgresql when validation of the arguments
failed."""


@contextlib.contextmanager
def convert_validation_exceptions():
    """Catches postgresql errors related to invalid arguments, and
    re-raises a StorageArgumentException."""
    try:
        yield
    except psycopg2.errors.UniqueViolation:
        # This only happens because of concurrent insertions, but it is
        # a subclass of IntegrityError; so we need to catch and reraise it
        # before the next clause converts it to StorageArgumentException.
        raise
    except VALIDATION_EXCEPTIONS as e:
        raise StorageArgumentException(str(e))


def db_transaction_generator(*args, **kwargs):
    def decorator(meth):
        meth = _db_transaction_generator(*args, **kwargs)(meth)

        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            try:
                yield from meth(self, *args, **kwargs)
            except psycopg2.errors.QueryCanceled as e:
                raise QueryTimeout(*e.args)

        return _meth

    return decorator


def db_transaction(*args, **kwargs):
    def decorator(meth):
        meth = _db_transaction(*args, **kwargs)(meth)

        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            try:
                return meth(self, *args, **kwargs)
            except psycopg2.errors.QueryCanceled as e:
                raise QueryTimeout(*e.args)

        return _meth

    return decorator


TRow = TypeVar("TRow", bound=Tuple)
TResult = TypeVar("TResult")


def _get_paginated_sha1_partition(
    cur,
    partition_id: int,
    nb_partitions: int,
    page_token: Optional[str],
    limit: int,
    get_range: Callable[[bytes, bytes, int, Any], Iterator[TRow]],
    convert: Callable[[TRow], TResult],
    get_id: Optional[Callable[[TResult], Any]] = None,
) -> PagedResult[TResult]:
    """Implements the bulk of ``content_get_partition``, ``directory_get_partition``,
    ...:

    1. computes range bounds
    2. applies pagination token
    3. converts to final objects
    4. extracts next pagination token
    """
    if limit is None:
        raise StorageArgumentException("limit should not be None")
    if get_id is None:

        def get_id(obj: TResult):
            return obj.id  # type: ignore[attr-defined]

    assert get_id is not None  # to please mypy

    (start, end) = get_partition_bounds_bytes(partition_id, nb_partitions, SHA1_SIZE)
    if page_token:
        start = hash_to_bytes(page_token)
    if end is None:
        end = b"\xff" * SHA1_SIZE

    next_page_token: Optional[str] = None
    results = []
    for counter, row in enumerate(get_range(start, end, limit + 1, cur)):
        result = convert(row)
        if counter >= limit:
            # take the last content for the next page starting from this
            next_page_token = hash_to_hex(get_id(result))
            break
        results.append(result)

    assert len(results) <= limit
    return PagedResult(results=results, next_page_token=next_page_token)


class Storage:
    """SWH storage datastore proxy, encompassing DB and object storage"""

    current_version: int = 193

    def __init__(
        self,
        db: Union[str, Db],
        objstorage: Optional[Dict] = None,
        min_pool_conns: int = 1,
        max_pool_conns: int = 10,
        journal_writer: Optional[Dict[str, Any]] = None,
        query_options: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        """Instantiate a storage instance backed by a PostgreSQL database and an
        objstorage.

        When ``db`` is passed as a connection string, then this module automatically
        manages a connection pool between ``min_pool_conns`` and ``max_pool_conns``.
        When ``db`` is an explicit psycopg2 connection, then ``min_pool_conns`` and
        ``max_pool_conns`` are ignored and the connection is used directly.

        Args:
            db: either a libpq connection string, or a psycopg2 connection
            objstorage: configuration for the backend :class:`ObjStorage`; if unset,
               use a NoopObjStorage
            min_pool_conns: min number of connections in the psycopg2 pool
            max_pool_conns: max number of connections in the psycopg2 pool
            journal_writer: configuration for the :class:`JournalWriter`
            query_options: configuration for the sql connections; keys of the dict are
               the method names decorated with :func:`db_transaction` or
               :func:`db_transaction_generator` (eg. :func:`content_find`), and values
               are dicts (config_name, config_value) used to configure the sql
               connection for the method_name. For example, using::

                  {"content_get": {"statement_timeout": 5000}}

               will override the default statement timeout for the :func:`content_get`
               endpoint from 500ms to 5000ms.

               See :mod:`swh.core.db.common` for more details.

        """

        self._db: Optional[Db]

        try:
            if isinstance(db, psycopg2.extensions.connection):
                self._pool = None
                self._db = Db(db)

                # See comment below
                self._db.cursor().execute("SET TIME ZONE 'UTC'")
            else:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    min_pool_conns, max_pool_conns, db
                )
                self._db = None
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)
        self.journal_writer = JournalWriter(journal_writer)
        self.objstorage = ObjStorage(self, objstorage)
        self.query_options = query_options
        self._flavor: Optional[str] = None

    ##########################
    # Utilities
    ##########################

    def get_db(self):
        if self._db:
            return self._db
        else:
            db = Db.from_pool(self._pool)

            # Workaround for psycopg2 < 2.9.0 not handling fractional timezones,
            # which may happen on old revision/release dates on systems configured
            # with non-UTC timezones.
            # https://www.psycopg.org/docs/usage.html#time-zones-handling
            db.cursor().execute("SET TIME ZONE 'UTC'")

            return db

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()

    @contextmanager
    def db(self):
        db = None
        try:
            db = self.get_db()
            yield db
        finally:
            if db:
                self.put_db(db)

    @db_transaction()
    def get_flavor(self, *, db: Db, cur=None) -> str:
        flavor = swh_db_flavor(db.conn.dsn)
        assert flavor is not None
        return flavor

    @property
    def flavor(self) -> str:
        if self._flavor is None:
            self._flavor = self.get_flavor()
        assert self._flavor is not None
        return self._flavor

    @db_transaction()
    def check_config(self, *, check_write: bool, db: Db, cur=None) -> bool:
        if not self.objstorage.check_config(check_write=check_write):
            return False

        dbversion = swh_db_version(db.conn.dsn)
        if dbversion != self.current_version:
            logger.warning(
                "database dbversion (%s) != %s current_version (%s)",
                dbversion,
                __name__,
                self.current_version,
            )
            return False

        # Check permissions on one of the tables
        check = "INSERT" if check_write else "SELECT"

        cur.execute("select has_table_privilege(current_user, 'content', %s)", (check,))
        return cur.fetchone()[0]

    ##########################
    # Content
    ##########################

    def _content_unique_key(self, hash, db):
        """Given a hash (tuple or dict), return a unique key from the
        aggregation of keys.

        """
        keys = db.content_hash_keys
        if isinstance(hash, tuple):
            return hash
        return tuple([hash[k] for k in keys])

    def _content_add_metadata(self, db, cur, content):
        """Add content to the postgresql database but not the object storage."""
        # create temporary table for metadata injection
        db.mktemp("content", cur)

        db.copy_to(
            (c.to_dict() for c in content), "tmp_content", db.content_add_keys, cur
        )

        # move metadata in place
        try:
            db.content_add_from_temp(cur)
        except psycopg2.IntegrityError as e:
            if e.diag.sqlstate == "23505" and e.diag.table_name == "content":
                message_detail = e.diag.message_detail
                if message_detail:
                    hash_name, hash_id = extract_collision_hash(message_detail)
                    collision_contents_hashes = [
                        c.hashes() for c in content if c.get_hash(hash_name) == hash_id
                    ]
                else:
                    constraint_to_hash_name = {
                        "content_pkey": "sha1",
                        "content_sha1_git_idx": "sha1_git",
                        "content_sha256_idx": "sha256",
                    }
                    hash_name = constraint_to_hash_name.get(e.diag.constraint_name)
                    hash_id = None
                    collision_contents_hashes = None

                raise HashCollision(
                    hash_name, hash_id, collision_contents_hashes
                ) from None
            else:
                raise

    def content_add(self, content: List[Content]) -> Dict[str, int]:
        ctime = now()

        contents = [attr.evolve(c, ctime=ctime) for c in content]

        # Must add to the objstorage before the DB and journal. Otherwise:
        # 1. in case of a crash the DB may "believe" we have the content, but
        #    we didn't have time to write to the objstorage before the crash
        # 2. the objstorage mirroring, which reads from the journal, may attempt to
        #    read from the objstorage before we finished writing it
        objstorage_summary = self.objstorage.content_add(contents)

        with self.db() as db:
            with db.transaction() as cur:
                missing = set(
                    self.content_missing(
                        [c.to_dict() for c in contents],
                        key_hash="sha1_git",
                        db=db,
                        cur=cur,
                    )
                )
                contents = [c for c in contents if c.sha1_git in missing]

                self.journal_writer.content_add(contents)
                self._content_add_metadata(db, cur, contents)

        return {
            "content:add": len(contents),
            "content:add:bytes": objstorage_summary["content:add:bytes"],
        }

    @db_transaction()
    def content_update(
        self, contents: List[Dict[str, Any]], keys: List[str] = [], *, db: Db, cur=None
    ) -> None:
        # TODO: Add a check on input keys. How to properly implement
        # this? We don't know yet the new columns.
        self.journal_writer.content_update(contents)

        db.mktemp("content", cur)
        select_keys = list(set(db.content_get_metadata_keys).union(set(keys)))
        with convert_validation_exceptions():
            db.copy_to(contents, "tmp_content", select_keys, cur)
            db.content_update_from_temp(keys_to_update=keys, cur=cur)

    @db_transaction()
    def content_add_metadata(
        self, content: List[Content], *, db: Db, cur=None
    ) -> Dict[str, int]:
        missing = set(
            self.content_missing(
                [c.to_dict() for c in content],
                key_hash="sha1_git",
                db=db,
                cur=cur,
            )
        )
        contents = [c for c in content if c.sha1_git in missing]

        self.journal_writer.content_add_metadata(contents)
        self._content_add_metadata(db, cur, contents)

        return {
            "content:add": len(contents),
        }

    def content_get_data(self, content: Union[HashDict, Sha1]) -> Optional[bytes]:
        # FIXME: Make this method support slicing the `data`
        return self.objstorage.content_get(content)

    @db_transaction()
    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[Content]:
        return _get_paginated_sha1_partition(
            cur,
            partition_id,
            nb_partitions,
            page_token,
            limit,
            db.content_get_range,
            lambda row: Content(**dict(zip(db.content_get_metadata_keys, row))),
            lambda row: row.sha1,
        )

    @db_transaction(statement_timeout=500)
    def content_get(
        self, contents: List[bytes], algo: str = "sha1", *, db: Db, cur=None
    ) -> List[Optional[Content]]:
        contents_by_hash: Dict[bytes, Optional[Content]] = {}
        if algo not in DEFAULT_ALGORITHMS:
            raise StorageArgumentException(
                "algo should be one of {','.join(DEFAULT_ALGORITHMS)}"
            )

        rows = db.content_get_metadata_from_hashes(contents, algo, cur)
        key = operator.attrgetter(algo)

        for row in rows:
            row_d = dict(zip(db.content_get_metadata_keys, row))
            content = Content(**row_d)
            contents_by_hash[key(content)] = content

        return [contents_by_hash.get(sha1) for sha1 in contents]

    @db_transaction_generator()
    def content_missing(
        self,
        contents: List[HashDict],
        key_hash: str = "sha1",
        *,
        db: Db,
        cur=None,
    ) -> Iterable[bytes]:
        if key_hash not in DEFAULT_ALGORITHMS:
            raise StorageArgumentException(
                "key_hash should be one of {','.join(DEFAULT_ALGORITHMS)}"
            )

        keys = db.content_hash_keys
        key_hash_idx = keys.index(key_hash)

        for obj in db.content_missing_from_list(contents, cur):
            yield obj[key_hash_idx]

    @db_transaction_generator()
    def content_missing_per_sha1(
        self, contents: List[bytes], *, db: Db, cur=None
    ) -> Iterable[bytes]:
        for obj in db.content_missing_per_sha1(contents, cur):
            yield obj[0]

    @db_transaction_generator()
    def content_missing_per_sha1_git(
        self, contents: List[bytes], *, db: Db, cur=None
    ) -> Iterable[Sha1Git]:
        for obj in db.content_missing_per_sha1_git(contents, cur):
            yield obj[0]

    @db_transaction()
    def content_find(self, content: HashDict, *, db: Db, cur=None) -> List[Content]:
        if not set(content).intersection(DEFAULT_ALGORITHMS):
            raise StorageArgumentException(
                "content keys must contain at least one "
                f"of: {', '.join(sorted(DEFAULT_ALGORITHMS))}"
            )

        rows = db.content_find(
            sha1=content.get("sha1"),
            sha1_git=content.get("sha1_git"),
            sha256=content.get("sha256"),
            blake2s256=content.get("blake2s256"),
            cur=cur,
        )
        contents = []
        for row in rows:
            row_d = dict(zip(db.content_find_cols, row))
            contents.append(Content(**row_d))
        return contents

    @db_transaction()
    def content_get_random(self, *, db: Db, cur=None) -> Sha1Git:
        return db.content_get_random(cur)

    ##########################
    # SkippedContent
    ##########################

    @staticmethod
    def _skipped_content_normalize(d):
        d = d.copy()

        if d.get("status") is None:
            d["status"] = "absent"

        if d.get("length") is None:
            d["length"] = -1

        return d

    def _skipped_content_add_metadata(self, db, cur, content: List[SkippedContent]):
        origin_ids = db.origin_id_get_by_url([cont.origin for cont in content], cur=cur)
        content = [
            attr.evolve(c, origin=origin_id)
            for (c, origin_id) in zip(content, origin_ids)
        ]
        db.mktemp("skipped_content", cur)
        db.copy_to(
            [c.to_dict() for c in content],
            "tmp_skipped_content",
            db.skipped_content_keys,
            cur,
        )

        # move metadata in place
        db.skipped_content_add_from_temp(cur)

    @db_transaction()
    def skipped_content_add(
        self, content: List[SkippedContent], *, db: Db, cur=None
    ) -> Dict[str, int]:
        ctime = now()
        content = [attr.evolve(c, ctime=ctime) for c in content]

        missing_contents = self.skipped_content_missing(
            (c.to_dict() for c in content),
            db=db,
            cur=cur,
        )
        content = [
            c
            for c in content
            if any(
                all(
                    c.get_hash(algo) == missing_content.get(algo)
                    for algo in DEFAULT_ALGORITHMS
                )
                for missing_content in missing_contents
            )
        ]

        self.journal_writer.skipped_content_add(content)
        self._skipped_content_add_metadata(db, cur, content)

        return {
            "skipped_content:add": len(content),
        }

    @db_transaction()
    def skipped_content_find(
        self, content: HashDict, *, db: Db, cur=None
    ) -> List[SkippedContent]:
        if not set(content).intersection(DEFAULT_ALGORITHMS):
            raise StorageArgumentException(
                "content keys must contain at least one "
                f"of: {', '.join(sorted(DEFAULT_ALGORITHMS))}"
            )

        rows = db.skipped_content_find(
            sha1=content.get("sha1"),
            sha1_git=content.get("sha1_git"),
            sha256=content.get("sha256"),
            blake2s256=content.get("blake2s256"),
            cur=cur,
        )
        skipped_contents = []
        for row in rows:
            row_d = dict(zip(db.skipped_content_find_cols, row))
            skipped_contents.append(SkippedContent(**row_d))
        return skipped_contents

    @db_transaction_generator()
    def skipped_content_missing(
        self, contents: List[Dict[str, Any]], *, db: Db, cur=None
    ) -> Iterable[Dict[str, Any]]:
        contents = list(contents)
        for content in db.skipped_content_missing(contents, cur):
            yield {
                algo: hash
                for (algo, hash) in zip(db.content_hash_keys, content)
                if hash
            }

    ##########################
    # Directory
    ##########################

    @db_transaction()
    def directory_add(
        self, directories: List[Directory], *, db: Db, cur=None
    ) -> Dict[str, int]:
        summary = {"directory:add": 0}

        dirs = set()
        dir_entries: Dict[str, defaultdict] = {
            "file": defaultdict(list),
            "dir": defaultdict(list),
            "rev": defaultdict(list),
        }

        for cur_dir in directories:
            dir_id = cur_dir.id
            dirs.add(dir_id)
            for src_entry in cur_dir.entries:
                entry = src_entry.to_dict()
                entry["dir_id"] = dir_id
                dir_entries[entry["type"]][dir_id].append(entry)

        dirs_missing = set(self.directory_missing(dirs, db=db, cur=cur))
        if not dirs_missing:
            return summary

        self.journal_writer.directory_add(
            dir_ for dir_ in directories if dir_.id in dirs_missing
        )

        # Copy directory metadata
        dirs_missing_dict = (
            {"id": dir_.id, "raw_manifest": dir_.raw_manifest}
            for dir_ in directories
            if dir_.id in dirs_missing
        )
        db.mktemp("directory", cur)
        db.copy_to(dirs_missing_dict, "tmp_directory", ["id", "raw_manifest"], cur)

        # Copy entries
        for entry_type, entry_list in dir_entries.items():
            entries = itertools.chain.from_iterable(
                entries_for_dir
                for dir_id, entries_for_dir in entry_list.items()
                if dir_id in dirs_missing
            )

            db.mktemp_dir_entry(entry_type)

            db.copy_to(
                entries,
                "tmp_directory_entry_%s" % entry_type,
                ["target", "name", "perms", "dir_id"],
                cur,
            )

        # Do the final copy
        db.directory_add_from_temp(cur)
        summary["directory:add"] = len(dirs_missing)

        return summary

    @db_transaction_generator()
    def directory_missing(
        self, directories: List[Sha1Git], *, db: Db, cur=None
    ) -> Iterable[Sha1Git]:
        for obj in db.directory_missing_from_list(directories, cur):
            yield obj[0]

    @db_transaction_generator(statement_timeout=20000)
    def directory_ls(
        self, directory: Sha1Git, recursive: bool = False, *, db: Db, cur=None
    ) -> Iterable[Dict[str, Any]]:
        if recursive:
            res_gen = db.directory_walk(directory, cur=cur)
        else:
            res_gen = db.directory_walk_one(directory, cur=cur)

        for line in res_gen:
            yield dict(zip(db.directory_ls_cols, line))

    @db_transaction(statement_timeout=4000)
    def directory_entry_get_by_path(
        self, directory: Sha1Git, paths: List[bytes], *, db: Db, cur=None
    ) -> Optional[Dict[str, Any]]:
        res = db.directory_entry_get_by_path(directory, paths, cur)
        return dict(zip(db.directory_ls_cols, res)) if res else None

    @db_transaction()
    def directory_get_random(self, *, db: Db, cur=None) -> Sha1Git:
        return db.directory_get_random(cur)

    @db_transaction()
    def directory_get_entries(
        self,
        directory_id: Sha1Git,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> Optional[PagedResult[DirectoryEntry]]:
        if list(self.directory_missing([directory_id], db=db, cur=cur)):
            return None

        if page_token is not None:
            raise StorageArgumentException("Unsupported page token")

        # TODO: actually paginate
        rows = db.directory_get_entries(directory_id, cur=cur)
        return PagedResult(
            results=[
                DirectoryEntry(**dict(zip(db.directory_get_entries_cols, row)))
                for row in rows
            ],
            next_page_token=None,
        )

    @db_transaction()
    def directory_get_raw_manifest(
        self, directory_ids: List[Sha1Git], *, db: Db, cur=None
    ) -> Dict[Sha1Git, Optional[bytes]]:
        return dict(db.directory_get_raw_manifest(directory_ids, cur=cur))

    @db_transaction()
    def directory_get_id_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[Sha1Git]:
        return _get_paginated_sha1_partition(
            cur,
            partition_id,
            nb_partitions,
            page_token,
            limit,
            db.directory_get_id_range,
            operator.itemgetter(0),
            lambda id_: id_,
        )

    ##########################
    # Revision
    ##########################

    @db_transaction()
    def revision_add(
        self, revisions: List[Revision], *, db: Db, cur=None
    ) -> Dict[str, int]:
        summary = {"revision:add": 0}

        revisions_missing = set(
            self.revision_missing(
                set(revision.id for revision in revisions), db=db, cur=cur
            )
        )

        if not revisions_missing:
            return summary

        db.mktemp_revision(cur)

        revisions_filtered = [
            revision for revision in revisions if revision.id in revisions_missing
        ]

        self.journal_writer.revision_add(revisions_filtered)

        db_revisions_filtered = list(map(converters.revision_to_db, revisions_filtered))

        parents_filtered: List[Dict[str, Any]] = []

        with convert_validation_exceptions():
            db.copy_to(
                db_revisions_filtered,
                "tmp_revision",
                db.revision_add_cols,
                cur,
                lambda rev: parents_filtered.extend(rev["parents"]),
            )

            db.revision_add_from_temp(cur)

            db.copy_to(
                parents_filtered,
                "revision_history",
                ["id", "parent_id", "parent_rank"],
                cur,
            )

        return {"revision:add": len(revisions_missing)}

    @db_transaction_generator()
    def revision_missing(
        self, revisions: List[Sha1Git], *, db: Db, cur=None
    ) -> Iterable[Sha1Git]:
        if not revisions:
            return None

        for obj in db.revision_missing_from_list(revisions, cur):
            yield obj[0]

    @db_transaction()
    def revision_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[Revision]:
        return _get_paginated_sha1_partition(
            cur,
            partition_id,
            nb_partitions,
            page_token,
            limit,
            db.revision_get_range,
            lambda row: converters.db_to_revision(dict(zip(db.revision_get_cols, row))),
        )

    @db_transaction(statement_timeout=2000)
    def revision_get(
        self,
        revision_ids: List[Sha1Git],
        ignore_displayname: bool = False,
        *,
        db: Db,
        cur=None,
    ) -> List[Optional[Revision]]:
        revisions = []
        for line in db.revision_get_from_list(revision_ids, ignore_displayname, cur):
            revision = converters.db_to_optional_revision(
                dict(zip(db.revision_get_cols, line))
            )
            revisions.append(revision)

        return revisions

    @db_transaction_generator(statement_timeout=2000)
    def revision_log(
        self,
        revisions: List[Sha1Git],
        ignore_displayname: bool = False,
        limit: Optional[int] = None,
        *,
        db: Db,
        cur=None,
    ) -> Iterable[Optional[Dict[str, Any]]]:
        for line in db.revision_log(
            revisions, ignore_displayname=ignore_displayname, limit=limit, cur=cur
        ):
            data = converters.db_to_revision(dict(zip(db.revision_get_cols, line)))
            if not data:
                yield None
                continue
            yield data.to_dict()

    @db_transaction_generator(statement_timeout=2000)
    def revision_shortlog(
        self, revisions: List[Sha1Git], limit: Optional[int] = None, *, db: Db, cur=None
    ) -> Iterable[Optional[Tuple[Sha1Git, Tuple[Sha1Git, ...]]]]:
        yield from db.revision_shortlog(revisions, limit, cur)

    @db_transaction()
    def revision_get_random(self, *, db: Db, cur=None) -> Sha1Git:
        return db.revision_get_random(cur)

    ##########################
    # ExtID
    ##########################

    @db_transaction()
    def extid_get_from_extid(
        self,
        id_type: str,
        ids: List[bytes],
        version: Optional[int] = None,
        *,
        db: Db,
        cur=None,
    ) -> List[ExtID]:
        extids = []
        for row in db.extid_get_from_extid_list(id_type, ids, version=version, cur=cur):
            if row[0] is not None:
                extids.append(converters.db_to_extid(dict(zip(db.extid_cols, row))))
        return extids

    @db_transaction()
    def extid_get_from_target(
        self,
        target_type: ObjectType,
        ids: List[Sha1Git],
        extid_type: Optional[str] = None,
        extid_version: Optional[int] = None,
        *,
        db: Db,
        cur=None,
    ) -> List[ExtID]:
        extids = []
        if (extid_version is not None and extid_type is None) or (
            extid_version is None and extid_type is not None
        ):
            raise ValueError("You must provide both extid_type and extid_version")

        for row in db.extid_get_from_swhid_list(
            target_type.value,
            ids,
            extid_version=extid_version,
            extid_type=extid_type,
            cur=cur,
        ):
            if row[0] is not None:
                extids.append(converters.db_to_extid(dict(zip(db.extid_cols, row))))
        return extids

    @db_transaction()
    def extid_add(self, ids: List[ExtID], *, db: Db, cur=None) -> Dict[str, int]:
        extid = [
            {
                "extid": extid.extid,
                "extid_type": extid.extid_type,
                "extid_version": getattr(extid, "extid_version", 0),
                "target": extid.target.object_id,
                "target_type": extid.target.object_type.name.lower(),  # arghh
            }
            for extid in ids
        ]
        db.mktemp("extid", cur)

        self.journal_writer.extid_add(ids)

        db.copy_to(extid, "tmp_extid", db.extid_cols, cur)

        # move metadata in place
        db.extid_add_from_temp(cur)

        return {"extid:add": len(extid)}

    ##########################
    # Release
    ##########################

    @db_transaction()
    def release_add(
        self, releases: List[Release], *, db: Db, cur=None
    ) -> Dict[str, int]:
        summary = {"release:add": 0}

        release_ids = set(release.id for release in releases)
        releases_missing = set(self.release_missing(release_ids, db=db, cur=cur))

        if not releases_missing:
            return summary

        db.mktemp_release(cur)

        releases_filtered = [
            release for release in releases if release.id in releases_missing
        ]

        self.journal_writer.release_add(releases_filtered)

        db_releases_filtered = list(map(converters.release_to_db, releases_filtered))

        with convert_validation_exceptions():
            db.copy_to(db_releases_filtered, "tmp_release", db.release_add_cols, cur)

            db.release_add_from_temp(cur)

        return {"release:add": len(releases_missing)}

    @db_transaction_generator()
    def release_missing(
        self, releases: List[Sha1Git], *, db: Db, cur=None
    ) -> Iterable[Sha1Git]:
        if not releases:
            return

        for obj in db.release_missing_from_list(releases, cur):
            yield obj[0]

    @db_transaction(statement_timeout=1000)
    def release_get(
        self,
        releases: List[Sha1Git],
        ignore_displayname: bool = False,
        *,
        db: Db,
        cur=None,
    ) -> List[Optional[Release]]:
        rels = []
        for release in db.release_get_from_list(releases, ignore_displayname, cur):
            rel = converters.db_to_optional_release(
                dict(zip(db.release_get_cols, release))
            )
            rels.append(rel)
        return rels

    @db_transaction()
    def release_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[Release]:
        return _get_paginated_sha1_partition(
            cur,
            partition_id,
            nb_partitions,
            page_token,
            limit,
            db.release_get_range,
            lambda row: converters.db_to_release(dict(zip(db.release_get_cols, row))),
        )

    @db_transaction()
    def release_get_random(self, *, db: Db, cur=None) -> Sha1Git:
        return db.release_get_random(cur)

    ##########################
    # Snapshot
    ##########################

    @db_transaction()
    def snapshot_add(
        self, snapshots: List[Snapshot], *, db: Db, cur=None
    ) -> Dict[str, int]:
        created_temp_table = False

        count = 0
        for snapshot in snapshots:
            if not db.snapshot_exists(snapshot.id, cur):
                if not created_temp_table:
                    db.mktemp_snapshot_branch(cur)
                    created_temp_table = True

                with convert_validation_exceptions():
                    db.copy_to(
                        (
                            {
                                "name": name,
                                "target": info.target if info else None,
                                "target_type": (
                                    info.target_type.value if info else None
                                ),
                            }
                            for name, info in snapshot.branches.items()
                        ),
                        "tmp_snapshot_branch",
                        ["name", "target", "target_type"],
                        cur,
                    )

                self.journal_writer.snapshot_add([snapshot])

                db.snapshot_add(snapshot.id, cur)
                count += 1

        return {"snapshot:add": count}

    @db_transaction_generator()
    def snapshot_missing(
        self, snapshots: List[Sha1Git], *, db: Db, cur=None
    ) -> Iterable[Sha1Git]:
        for obj in db.snapshot_missing_from_list(snapshots, cur):
            yield obj[0]

    @db_transaction(statement_timeout=2000)
    def snapshot_get(
        self, snapshot_id: Sha1Git, *, db: Db, cur=None
    ) -> Optional[Dict[str, Any]]:
        d = self.snapshot_get_branches(snapshot_id)
        if d is None:
            return d
        return {
            "id": d["id"],
            "branches": {
                name: branch.to_dict() if branch else None
                for (name, branch) in d["branches"].items()
            },
            "next_branch": d["next_branch"],
        }

    @db_transaction()
    def snapshot_get_id_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        page_token: Optional[str] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[Sha1Git]:
        return _get_paginated_sha1_partition(
            cur,
            partition_id,
            nb_partitions,
            page_token,
            limit,
            db.snapshot_get_id_range,
            operator.itemgetter(0),
            lambda id_: id_,
        )

    @db_transaction(statement_timeout=2000)
    def snapshot_count_branches(
        self,
        snapshot_id: Sha1Git,
        branch_name_exclude_prefix: Optional[bytes] = None,
        *,
        db: Db,
        cur=None,
    ) -> Optional[Dict[Optional[str], int]]:
        return dict(
            [
                bc
                for bc in db.snapshot_count_branches(
                    snapshot_id,
                    branch_name_exclude_prefix,
                    cur,
                )
            ]
        )

    @db_transaction(statement_timeout=2000)
    def snapshot_get_branches(
        self,
        snapshot_id: Sha1Git,
        branches_from: bytes = b"",
        branches_count: int = 1000,
        target_types: Optional[List[str]] = None,
        branch_name_include_substring: Optional[bytes] = None,
        branch_name_exclude_prefix: Optional[bytes] = None,
        *,
        db: Db,
        cur=None,
    ) -> Optional[PartialBranches]:
        if snapshot_id == EMPTY_SNAPSHOT_ID:
            return PartialBranches(
                id=snapshot_id,
                branches={},
                next_branch=None,
            )

        if list(self.snapshot_missing([snapshot_id])):
            return None

        branches = {}
        next_branch = None

        fetched_branches = list(
            db.snapshot_get_by_id(
                snapshot_id,
                branches_from=branches_from,
                # the underlying SQL query can be quite expensive to execute for small
                # branches_count value, so we ensure a minimum branches limit of 10 for
                # optimal performances
                branches_count=max(branches_count + 1, 10),
                target_types=target_types,
                branch_name_include_substring=branch_name_include_substring,
                branch_name_exclude_prefix=branch_name_exclude_prefix,
                cur=cur,
            )
        )
        for row in fetched_branches[:branches_count]:
            branch_d = dict(zip(db.snapshot_get_cols, row))
            del branch_d["snapshot_id"]
            name = branch_d.pop("name")
            if branch_d["target"] is None and branch_d["target_type"] is None:
                branch = None
            else:
                assert branch_d["target_type"] is not None
                branch = SnapshotBranch(
                    target=branch_d["target"],
                    target_type=SnapshotTargetType(branch_d["target_type"]),
                )
            branches[name] = branch

        if len(fetched_branches) > branches_count:
            next_branch = dict(
                zip(db.snapshot_get_cols, fetched_branches[branches_count])
            )["name"]

        return PartialBranches(
            id=snapshot_id,
            branches=branches,
            next_branch=next_branch,
        )

    @db_transaction()
    def snapshot_get_random(self, *, db: Db, cur=None) -> Sha1Git:
        return db.snapshot_get_random(cur)

    @db_transaction(statement_timeout=2000)
    def snapshot_branch_get_by_name(
        self,
        snapshot_id: Sha1Git,
        branch_name: bytes,
        db: Db,
        cur=None,
        follow_alias_chain: bool = True,
        max_alias_chain_length: int = 100,
    ) -> Optional[SnapshotBranchByNameResponse]:
        if list(self.snapshot_missing([snapshot_id])):
            return None

        if snapshot_id == EMPTY_SNAPSHOT_ID:
            return SnapshotBranchByNameResponse(
                branch_found=False,
                target=None,
                aliases_followed=[],
            )

        cols_to_fetch = ["target", "target_type"]
        resolve_chain: List[bytes] = []
        while True:
            branch = db.snapshot_branch_get_by_name(
                cols_to_fetch=cols_to_fetch,
                snapshot_id=snapshot_id,
                branch_name=branch_name,
                cur=cur,
            )
            if branch is None:
                # target branch is None, there could be items in aliases_followed
                target = None
                break
            branch_d = dict(zip(cols_to_fetch, branch))
            resolve_chain.append(branch_name)
            if (
                branch_d["target_type"] != SnapshotTargetType.ALIAS.value
                or not follow_alias_chain
            ):
                # first non alias branch or the first branch when follow_alias_chain is False
                target = (
                    SnapshotBranch(
                        target=branch_d["target"],
                        target_type=SnapshotTargetType(branch_d["target_type"]),
                    )
                    if branch_d["target"]
                    else None
                )
                break
            elif (
                # Circular reference
                resolve_chain.count(branch_name) > 1
                # Too many re-directs
                or len(resolve_chain) >= max_alias_chain_length
            ):
                target = None
                break
            # Branch has a non-None target with type alias
            branch_name = branch_d["target"]

        return SnapshotBranchByNameResponse(
            # resolve_chian has items, brach_found must be True
            branch_found=bool(resolve_chain),
            target=target,
            aliases_followed=resolve_chain,
        )

    ##########################
    # OriginVisit and OriginVisitStatus
    ##########################

    @db_transaction()
    def origin_visit_add(
        self, visits: List[OriginVisit], *, db: Db, cur=None
    ) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get([visit.origin], db=db, cur=cur)[0]
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        for visit in visits:
            if visit.visit:
                self.journal_writer.origin_visit_add([visit])
                db.origin_visit_add_with_id(visit, cur=cur)
            else:
                # visit_id is not given, it needs to be set by the db
                with convert_validation_exceptions():
                    visit_id = db.origin_visit_add(
                        visit.origin, visit.date, visit.type, cur=cur
                    )
                visit = attr.evolve(visit, visit=visit_id)
                # Forced to write in the journal after the db (since its the db
                # call that set the visit id)
                self.journal_writer.origin_visit_add([visit])
                # In this case, we also want to create the initial OVS object
                visit_status = OriginVisitStatus(
                    origin=visit.origin,
                    visit=visit_id,
                    date=visit.date,
                    type=visit.type,
                    status="created",
                    snapshot=None,
                )
                self._origin_visit_status_add(visit_status, db=db, cur=cur)
            assert visit.visit is not None
            all_visits.append(visit)

        return all_visits

    def _origin_visit_status_add(
        self, visit_status: OriginVisitStatus, db, cur
    ) -> None:
        """Add an origin visit status"""
        self.journal_writer.origin_visit_status_add([visit_status])
        db.origin_visit_status_add(visit_status, cur=cur)

    @db_transaction()
    def origin_visit_status_add(
        self,
        visit_statuses: List[OriginVisitStatus],
        *,
        db: Db,
        cur=None,
    ) -> Dict[str, int]:
        visit_statuses_ = []

        # First round to check existence (fail early if any is ko)
        for visit_status in visit_statuses:
            origin_url = self.origin_get([visit_status.origin], db=db, cur=cur)[0]
            if not origin_url:
                raise StorageArgumentException(f"Unknown origin {visit_status.origin}")

            if visit_status.type is None:
                origin_visit = self.origin_visit_get_by(
                    visit_status.origin, visit_status.visit, db=db, cur=cur
                )
                if origin_visit is None:
                    raise StorageArgumentException(
                        f"Unknown origin visit {visit_status.visit} "
                        f"of origin {visit_status.origin}"
                    )

                origin_visit_status = attr.evolve(visit_status, type=origin_visit.type)
            else:
                origin_visit_status = visit_status

            visit_statuses_.append(origin_visit_status)

        for visit_status in visit_statuses_:
            self._origin_visit_status_add(visit_status, db, cur)
        return {"origin_visit_status:add": len(visit_statuses_)}

    @db_transaction()
    def origin_visit_status_get_latest(
        self,
        origin_url: str,
        visit: int,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        *,
        db: Db,
        cur=None,
    ) -> Optional[OriginVisitStatus]:
        if allowed_statuses and not set(allowed_statuses).intersection(VISIT_STATUSES):
            raise StorageArgumentException(
                f"Unknown allowed statuses {','.join(allowed_statuses)}, only "
                f"{','.join(VISIT_STATUSES)} authorized"
            )

        row_d = db.origin_visit_status_get_latest(
            origin_url, visit, allowed_statuses, require_snapshot, cur=cur
        )
        if not row_d:
            return None
        return OriginVisitStatus(**row_d)

    @db_transaction(statement_timeout=500)
    def origin_visit_get(
        self,
        origin: str,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[OriginVisit]:
        page_token = page_token or "0"
        if not isinstance(order, ListOrder):
            raise StorageArgumentException("order must be a ListOrder value")
        if not isinstance(page_token, str):
            raise StorageArgumentException("page_token must be a string.")

        next_page_token = None
        visit_from = int(page_token)
        visits: List[OriginVisit] = []
        extra_limit = limit + 1
        for row in db.origin_visit_get_range(
            origin, visit_from=visit_from, order=order, limit=extra_limit, cur=cur
        ):
            row_d = dict(zip(db.origin_visit_cols, row))
            visits.append(
                OriginVisit(
                    origin=row_d["origin"],
                    visit=row_d["visit"],
                    date=row_d["date"],
                    type=row_d["type"],
                )
            )

        assert len(visits) <= extra_limit

        if len(visits) == extra_limit:
            visits = visits[:limit]
            next_page_token = str(visits[-1].visit)

        return PagedResult(results=visits, next_page_token=next_page_token)

    @db_transaction(statement_timeout=2000)
    def origin_visit_get_with_statuses(
        self,
        origin: str,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[OriginVisitWithStatuses]:
        page_token = page_token or "0"
        if not isinstance(order, ListOrder):
            raise StorageArgumentException("order must be a ListOrder value")
        if not isinstance(page_token, str):
            raise StorageArgumentException("page_token must be a string.")

        # First get visits (plus one so we can use it as the next page token if any)
        visits_page = self.origin_visit_get(
            origin=origin,
            page_token=page_token,
            order=order,
            limit=limit,
            db=db,
            cur=cur,
        )

        visits = visits_page.results
        next_page_token = visits_page.next_page_token

        if visits:
            visit_from = min(visits[0].visit, visits[-1].visit)
            visit_to = max(visits[0].visit, visits[-1].visit)

            # Then, fetch all statuses associated to these visits
            visit_statuses: Dict[int, List[OriginVisitStatus]] = defaultdict(list)
            for row in db.origin_visit_status_get_all_in_range(
                origin,
                allowed_statuses,
                require_snapshot,
                visit_from=visit_from,
                visit_to=visit_to,
                cur=cur,
            ):
                row_d = dict(zip(db.origin_visit_status_cols, row))

                visit_statuses[row_d["visit"]].append(OriginVisitStatus(**row_d))

        results = [
            OriginVisitWithStatuses(visit=visit, statuses=visit_statuses[visit.visit])
            for visit in visits
        ]

        return PagedResult(results=results, next_page_token=next_page_token)

    @db_transaction(statement_timeout=2000)
    def origin_visit_find_by_date(
        self,
        origin: str,
        visit_date: datetime.datetime,
        type: Optional[str] = None,
        *,
        db: Db,
        cur=None,
    ) -> Optional[OriginVisit]:
        row_d = db.origin_visit_find_by_date(origin, visit_date, type=type, cur=cur)
        if not row_d:
            return None
        return OriginVisit(
            origin=row_d["origin"],
            visit=row_d["visit"],
            date=row_d["date"],
            type=row_d["type"],
        )

    @db_transaction(statement_timeout=500)
    def origin_visit_get_by(
        self, origin: str, visit: int, *, db: Db, cur=None
    ) -> Optional[OriginVisit]:
        row = db.origin_visit_get(origin, visit, cur)
        if row:
            row_d = dict(zip(db.origin_visit_get_cols, row))
            return OriginVisit(
                origin=row_d["origin"],
                visit=row_d["visit"],
                date=row_d["date"],
                type=row_d["type"],
            )
        return None

    @db_transaction(statement_timeout=4000)
    def origin_visit_get_latest(
        self,
        origin: str,
        type: Optional[str] = None,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        *,
        db: Db,
        cur=None,
    ) -> Optional[OriginVisit]:
        if allowed_statuses and not set(allowed_statuses).intersection(VISIT_STATUSES):
            raise StorageArgumentException(
                f"Unknown allowed statuses {','.join(allowed_statuses)}, only "
                f"{','.join(VISIT_STATUSES)} authorized"
            )

        row = db.origin_visit_get_latest(
            origin,
            type=type,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            cur=cur,
        )
        if row:
            row_d = dict(zip(db.origin_visit_get_cols, row))
            assert (
                row_d["visit"] is not None
            ), "origin_visit_status LEFT JOIN origin_visit returned NULL"
            visit = OriginVisit(
                origin=row_d["origin"],
                visit=row_d["visit"],
                date=row_d["date"],
                type=row_d["type"],
            )
            return visit
        return None

    @db_transaction(statement_timeout=500)
    def origin_visit_status_get(
        self,
        origin: str,
        visit: int,
        page_token: Optional[str] = None,
        order: ListOrder = ListOrder.ASC,
        limit: int = 10,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[OriginVisitStatus]:
        next_page_token = None
        date_from = None
        if page_token is not None:
            try:
                date_from = datetime.datetime.fromisoformat(page_token)
            except ValueError:
                raise StorageArgumentException(
                    "Invalid page_token argument to origin_visit_status_get."
                ) from None

        visit_statuses: List[OriginVisitStatus] = []
        # Take one more visit status so we can reuse it as the next page token if any
        for row in db.origin_visit_status_get_range(
            origin,
            visit,
            date_from=date_from,
            order=order,
            limit=limit + 1,
            cur=cur,
        ):
            row_d = dict(zip(db.origin_visit_status_cols, row))
            visit_statuses.append(OriginVisitStatus(**row_d))

        if len(visit_statuses) > limit:
            # last visit status date is the next page token
            next_page_token = str(visit_statuses[-1].date)
            # excluding that visit status from the result to respect the limit size
            visit_statuses = visit_statuses[:limit]

        return PagedResult(results=visit_statuses, next_page_token=next_page_token)

    @db_transaction()
    def origin_visit_status_get_random(
        self, type: str, *, db: Db, cur=None
    ) -> Optional[OriginVisitStatus]:
        row = db.origin_visit_get_random(type, cur)
        if row is not None:
            row_d = dict(zip(db.origin_visit_status_cols, row))
            return OriginVisitStatus(**row_d)
        return None

    ##########################
    # Origin
    ##########################

    @db_transaction(statement_timeout=1000)
    def origin_get(
        self, origins: List[str], *, db: Db, cur=None
    ) -> List[Optional[Origin]]:
        rows = db.origin_get_by_url(origins, cur)
        result: List[Optional[Origin]] = []
        for row in rows:
            origin_d = dict(zip(db.origin_cols, row))
            url = origin_d["url"]
            result.append(None if url is None else Origin(url=url))
        return result

    @db_transaction(statement_timeout=1000)
    def origin_get_by_sha1(
        self, sha1s: List[bytes], *, db: Db, cur=None
    ) -> List[Optional[Dict[str, Any]]]:
        return [
            dict(zip(db.origin_cols, row)) if row[0] else None
            for row in db.origin_get_by_sha1(sha1s, cur)
        ]

    @db_transaction_generator()
    def origin_get_range(self, origin_from=1, origin_count=100, *, db: Db, cur=None):
        for origin in db.origin_get_range(origin_from, origin_count, cur):
            yield dict(zip(db.origin_get_range_cols, origin))

    @db_transaction()
    def origin_list(
        self, page_token: Optional[str] = None, limit: int = 100, *, db: Db, cur=None
    ) -> PagedResult[Origin]:
        page_token = page_token or "0"
        if not isinstance(page_token, str):
            raise StorageArgumentException("page_token must be a string.")
        origin_from = int(page_token)
        next_page_token = None

        origins: List[Origin] = []
        # Take one more origin so we can reuse it as the next page token if any
        for row_d in self.origin_get_range(origin_from, limit + 1, db=db, cur=cur):
            origins.append(Origin(url=row_d["url"]))
            # keep the last_id for the pagination if needed
            last_id = row_d["id"]

        if len(origins) > limit:  # data left for subsequent call
            # last origin id is the next page token
            next_page_token = str(last_id)
            # excluding that origin from the result to respect the limit size
            origins = origins[:limit]

        assert len(origins) <= limit
        return PagedResult(results=origins, next_page_token=next_page_token)

    @db_transaction()
    def origin_search(
        self,
        url_pattern: str,
        page_token: Optional[str] = None,
        limit: int = 50,
        regexp: bool = False,
        with_visit: bool = False,
        visit_types: Optional[List[str]] = None,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[Origin]:
        next_page_token = None
        offset = int(page_token) if page_token else 0

        origins = []
        # Take one more origin so we can reuse it as the next page token if any
        for origin in db.origin_search(
            url_pattern, offset, limit + 1, regexp, with_visit, visit_types, cur
        ):
            row_d = dict(zip(db.origin_cols, origin))
            origins.append(Origin(url=row_d["url"]))

        if len(origins) > limit:
            # next offset
            next_page_token = str(offset + limit)
            # excluding that origin from the result to respect the limit size
            origins = origins[:limit]

        assert len(origins) <= limit

        return PagedResult(results=origins, next_page_token=next_page_token)

    @db_transaction()
    def origin_count(
        self,
        url_pattern: str,
        regexp: bool = False,
        with_visit: bool = False,
        *,
        db: Db,
        cur=None,
    ) -> int:
        return db.origin_count(url_pattern, regexp, with_visit, cur)

    @db_transaction()
    def origin_snapshot_get_all(
        self, origin_url: str, *, db: Db, cur=None
    ) -> List[Sha1Git]:
        return list(db.origin_snapshot_get_all(origin_url, cur))

    @db_transaction()
    def origin_add(self, origins: List[Origin], *, db: Db, cur=None) -> Dict[str, int]:
        urls = [o.url for o in origins]
        known_origins = set(url for (url,) in db.origin_get_by_url(urls, cur))
        # keep only one occurrence of each given origin while keeping the list
        # sorted as originally given
        to_add = sorted(set(urls) - known_origins, key=urls.index)

        self.journal_writer.origin_add([Origin(url=url) for url in to_add])
        added = 0
        for url in to_add:
            if db.origin_add(url, cur):
                added += 1
        return {"origin:add": added}

    ##########################
    # misc.
    ##########################

    @db_transaction(statement_timeout=2000)
    def object_find_by_sha1_git(
        self, ids: List[Sha1Git], *, db: Db, cur=None
    ) -> Dict[Sha1Git, List[Dict]]:
        ret: Dict[Sha1Git, List[Dict]] = {id: [] for id in ids}

        for retval in db.object_find_by_sha1_git(ids, cur=cur):
            if retval[1]:
                ret[retval[0]].append(
                    dict(zip(db.object_find_by_sha1_git_cols, retval))
                )

        return ret

    @db_transaction(statement_timeout=500)
    def stat_counters(self, *, db: Db, cur=None):
        return {k: v for (k, v) in db.stat_counters()}

    @db_transaction()
    def refresh_stat_counters(self, *, db: Db, cur=None):
        keys = [
            "content",
            "directory",
            "directory_entry_dir",
            "directory_entry_file",
            "directory_entry_rev",
            "origin",
            "origin_visit",
            "person",
            "release",
            "revision",
            "revision_history",
            "skipped_content",
            "snapshot",
        ]

        for key in keys:
            cur.execute("select * from swh_update_counter(%s)", (key,))

    ##########################
    # RawExtrinsicMetadata
    ##########################

    @db_transaction()
    def raw_extrinsic_metadata_add(
        self,
        metadata: List[RawExtrinsicMetadata],
        db,
        cur,
    ) -> Dict[str, int]:
        metadata = list(metadata)
        self.journal_writer.raw_extrinsic_metadata_add(metadata)
        counter = Counter[ExtendedObjectType]()
        for metadata_entry in metadata:
            authority_id = self._get_authority_id(metadata_entry.authority, db, cur)
            fetcher_id = self._get_fetcher_id(metadata_entry.fetcher, db, cur)

            db.raw_extrinsic_metadata_add(
                id=metadata_entry.id,
                type=metadata_entry.target.object_type.name.lower(),
                target=str(metadata_entry.target),
                discovery_date=metadata_entry.discovery_date,
                authority_id=authority_id,
                fetcher_id=fetcher_id,
                format=metadata_entry.format,
                metadata=metadata_entry.metadata,
                origin=metadata_entry.origin,
                visit=metadata_entry.visit,
                snapshot=map_optional(str, metadata_entry.snapshot),
                release=map_optional(str, metadata_entry.release),
                revision=map_optional(str, metadata_entry.revision),
                path=metadata_entry.path,
                directory=map_optional(str, metadata_entry.directory),
                cur=cur,
            )
            counter[metadata_entry.target.object_type] += 1

        return {
            f"{type.value}_metadata:add": count for (type, count) in counter.items()
        }

    @db_transaction()
    def raw_extrinsic_metadata_get(
        self,
        target: ExtendedSWHID,
        authority: MetadataAuthority,
        after: Optional[datetime.datetime] = None,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
        *,
        db: Db,
        cur=None,
    ) -> PagedResult[RawExtrinsicMetadata]:
        if page_token:
            (after_time, after_fetcher) = msgpack_loads(base64.b64decode(page_token))
            if after and after_time < after:
                raise StorageArgumentException(
                    "page_token is inconsistent with the value of 'after'."
                )
        else:
            after_time = after
            after_fetcher = None

        try:
            authority_id = self._get_authority_id(authority, db, cur)
        except UnknownMetadataAuthority:
            return PagedResult(
                next_page_token=None,
                results=[],
            )

        rows = db.raw_extrinsic_metadata_get(
            str(target),
            authority_id,
            after_time,
            after_fetcher,
            limit + 1,
            cur,
        )
        rows = [dict(zip(db.raw_extrinsic_metadata_get_cols, row)) for row in rows]
        results = []
        for row in rows:
            assert str(target) == row["raw_extrinsic_metadata.target"]
            results.append(converters.db_to_raw_extrinsic_metadata(row))

        if len(results) > limit:
            results.pop()
            assert len(results) == limit
            last_returned_row = rows[-2]  # rows[-1] corresponds to the popped result
            next_page_token: Optional[str] = base64.b64encode(
                msgpack_dumps(
                    (
                        last_returned_row["discovery_date"],
                        last_returned_row["metadata_fetcher.id"],
                    )
                )
            ).decode()
        else:
            next_page_token = None

        return PagedResult(
            next_page_token=next_page_token,
            results=results,
        )

    @db_transaction()
    def raw_extrinsic_metadata_get_by_ids(
        self,
        ids: List[Sha1Git],
        *,
        db: Db,
        cur=None,
    ) -> List[RawExtrinsicMetadata]:
        return [
            converters.db_to_raw_extrinsic_metadata(
                dict(zip(db.raw_extrinsic_metadata_get_cols, row))
            )
            for row in db.raw_extrinsic_metadata_get_by_ids(ids)
        ]

    ##########################
    # MetadataFetcher and MetadataAuthority
    ##########################

    @db_transaction()
    def metadata_fetcher_add(
        self, fetchers: List[MetadataFetcher], *, db: Db, cur=None
    ) -> Dict[str, int]:
        fetchers = list(fetchers)
        self.journal_writer.metadata_fetcher_add(fetchers)
        count = 0
        for fetcher in fetchers:
            db.metadata_fetcher_add(fetcher.name, fetcher.version, cur=cur)
            count += 1
        return {"metadata_fetcher:add": count}

    @db_transaction(statement_timeout=500)
    def metadata_fetcher_get(
        self, name: str, version: str, *, db: Db, cur=None
    ) -> Optional[MetadataFetcher]:
        row = db.metadata_fetcher_get(name, version, cur=cur)
        if not row:
            return None
        return MetadataFetcher.from_dict(dict(zip(db.metadata_fetcher_cols, row)))

    @db_transaction()
    def raw_extrinsic_metadata_get_authorities(
        self,
        target: ExtendedSWHID,
        *,
        db: Db,
        cur=None,
    ) -> List[MetadataAuthority]:
        return [
            MetadataAuthority(
                type=MetadataAuthorityType(authority_type), url=authority_url
            )
            for (
                authority_type,
                authority_url,
            ) in db.raw_extrinsic_metadata_get_authorities(str(target), cur)
        ]

    @db_transaction()
    def metadata_authority_add(
        self, authorities: List[MetadataAuthority], *, db: Db, cur=None
    ) -> Dict[str, int]:
        authorities = list(authorities)
        self.journal_writer.metadata_authority_add(authorities)
        count = 0
        for authority in authorities:
            db.metadata_authority_add(authority.type.value, authority.url, cur=cur)
            count += 1
        return {"metadata_authority:add": count}

    @db_transaction()
    def metadata_authority_get(
        self, type: MetadataAuthorityType, url: str, *, db: Db, cur=None
    ) -> Optional[MetadataAuthority]:
        row = db.metadata_authority_get(type.value, url, cur=cur)
        if not row:
            return None
        return MetadataAuthority.from_dict(dict(zip(db.metadata_authority_cols, row)))

    #########################
    # 'object_references' table
    #########################

    @db_transaction()
    def object_find_recent_references(
        self, target_swhid: ExtendedSWHID, limit: int, *, db: Db, cur=None
    ) -> List[ExtendedSWHID]:
        return [
            converters.db_to_object_reference_source(row)
            for row in db.object_references_get(
                target_type=target_swhid.object_type.name.lower(),
                target=target_swhid.object_id,
                limit=limit,
                cur=cur,
            )
        ]

    @db_transaction()
    def object_references_add(
        self, references: List[ObjectReference], *, db: Db, cur=None
    ) -> Dict[str, int]:
        to_add = list({converters.object_reference_to_db(ref) for ref in references})
        db.object_references_add(
            to_add,
            cur=cur,
        )

        return {"object_reference:add": len(to_add)}

    def clear_buffers(self, object_types: Sequence[str] = ()) -> None:
        """Do nothing"""
        return None

    def flush(self, object_types: Sequence[str] = ()) -> Dict[str, int]:
        return {}

    def _get_authority_id(self, authority: MetadataAuthority, db, cur):
        authority_id = db.metadata_authority_get_id(
            authority.type.value, authority.url, cur
        )
        if not authority_id:
            raise UnknownMetadataAuthority(str(authority))
        return authority_id

    def _get_fetcher_id(self, fetcher: MetadataFetcher, db, cur):
        fetcher_id = db.metadata_fetcher_get_id(fetcher.name, fetcher.version, cur)
        if not fetcher_id:
            raise UnknownMetadataFetcher(str(fetcher))
        return fetcher_id

    #########################
    # ObjectDeletionInterface
    #########################

    @db_transaction()
    def object_delete(
        self, swhids: List[ExtendedSWHID], *, db: Db, cur=None
    ) -> Dict[str, int]:
        """Delete objects from the storage

        All skipped content objects matching the given SWHID will be removed,
        including those who have the same SWHID due to hash collisions.

        Origin objects are removed alongside their associated origin visit and
        origin visit status objects.

        Args:
            swhids: list of SWHID of the objects to remove

        Returns:
            dict: number of objects removed. Details of each key:

            content:delete
                Number of content objects removed

            content:delete:bytes
                Sum of the removed contents’ data length

            skipped_content:delete
                Number of skipped content objects removed

            directory:delete
                Number of directory objects removed

            revision:delete
                Number of revision objects removed

            release:delete
                Number of release objects removed

            snapshot:delete
                Number of snapshot objects removed

            origin:delete
                Number of origin objects removed

            origin_visit:delete
                Number of origin visit objects removed

            origin_visit_status:delete
                Number of origin visit status objects removed

            ori_metadata:delete
                Number of raw extrinsic metadata objects targeting
                an origin that have been removed

            snp_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a snapshot that have been removed

            rev_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a revision that have been removed

            rel_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a release that have been removed

            dir_metadata:delete
                Number ef raw extrinsic metadata objects targeting
                a directory that have been removed

            cnt_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a content that have been removed

            emd_metadata:delete
                Number of raw extrinsic metadata objects targeting
                a raw extrinsic metadata object that have been removed
        """
        object_rows = [
            (swhid.object_type.name.lower(), swhid.object_id) for swhid in swhids
        ]
        return db.object_delete(object_rows, cur=cur)

    @db_transaction()
    def extid_delete_for_target(
        self, target_swhids: List[CoreSWHID], *, db: Db, cur=None
    ) -> Dict[str, int]:
        """Delete ExtID objects from the storage

        Args:
            target_swhids: list of SWHIDs targeted by the ExtID objects to remove

        Returns:
            Summary dict with the following keys and associated values:

                extid:delete: Number of ExtID objects removed
        """
        target_rows = [
            (swhid.object_type.name.lower(), swhid.object_id) for swhid in target_swhids
        ]
        return db.extid_delete_for_target(target_rows, cur=cur)
