# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
import itertools

from collections import defaultdict
from contextlib import contextmanager
from deprecated import deprecated
from typing import Any, Dict, Iterable, List, Optional

import attr
import psycopg2
import psycopg2.pool
import psycopg2.errors

from swh.core.api.serializers import msgpack_loads, msgpack_dumps
from swh.model.model import (
    Content,
    Directory,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Revision,
    Release,
    SkippedContent,
    Snapshot,
    SHA1_SIZE,
)
from swh.model.hashutil import DEFAULT_ALGORITHMS, hash_to_bytes, hash_to_hex
from swh.storage.objstorage import ObjStorage
from swh.storage.validate import VALIDATION_EXCEPTIONS
from swh.storage.utils import now

from . import converters
from .common import db_transaction_generator, db_transaction
from .db import Db
from .exc import StorageArgumentException, StorageDBError, HashCollision
from .algos import diff
from .metrics import timed, send_metric, process_metrics
from .utils import get_partition_bounds_bytes, extract_collision_hash
from .writer import JournalWriter


# Max block size of contents to return
BULK_BLOCK_CONTENT_LEN_MAX = 10000

EMPTY_SNAPSHOT_ID = hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e")
"""Identifier for the empty snapshot"""


VALIDATION_EXCEPTIONS = VALIDATION_EXCEPTIONS + [
    psycopg2.errors.CheckViolation,
    psycopg2.errors.IntegrityError,
    psycopg2.errors.InvalidTextRepresentation,
    psycopg2.errors.NotNullViolation,
    psycopg2.errors.NumericValueOutOfRange,
    psycopg2.errors.UndefinedFunction,  # (raised on wrong argument typs)
]
"""Exceptions raised by postgresql when validation of the arguments
failed."""


@contextlib.contextmanager
def convert_validation_exceptions():
    """Catches postgresql errors related to invalid arguments, and
    re-raises a StorageArgumentException."""
    try:
        yield
    except tuple(VALIDATION_EXCEPTIONS) as e:
        raise StorageArgumentException(str(e))


class Storage:
    """SWH storage proxy, encompassing DB and object storage

    """

    def __init__(
        self, db, objstorage, min_pool_conns=1, max_pool_conns=10, journal_writer=None
    ):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection
            obj_root: path to the root of the object storage

        """
        try:
            if isinstance(db, psycopg2.extensions.connection):
                self._pool = None
                self._db = Db(db)
            else:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    min_pool_conns, max_pool_conns, db
                )
                self._db = None
        except psycopg2.OperationalError as e:
            raise StorageDBError(e)

        self.journal_writer = JournalWriter(journal_writer)
        self.objstorage = ObjStorage(objstorage)

    def get_db(self):
        if self._db:
            return self._db
        else:
            return Db.from_pool(self._pool)

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

    @timed
    @db_transaction()
    def check_config(self, *, check_write, db=None, cur=None):

        if not self.objstorage.check_config(check_write=check_write):
            return False

        # Check permissions on one of the tables
        if check_write:
            check = "INSERT"
        else:
            check = "SELECT"

        cur.execute("select has_table_privilege(current_user, 'content', %s)", (check,))
        return cur.fetchone()[0]

    def _content_unique_key(self, hash, db):
        """Given a hash (tuple or dict), return a unique key from the
           aggregation of keys.

        """
        keys = db.content_hash_keys
        if isinstance(hash, tuple):
            return hash
        return tuple([hash[k] for k in keys])

    def _content_add_metadata(self, db, cur, content):
        """Add content to the postgresql database but not the object storage.
        """
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

    @timed
    @process_metrics
    def content_add(self, content: Iterable[Content]) -> Dict:
        ctime = now()

        contents = [attr.evolve(c, ctime=ctime) for c in content]

        objstorage_summary = self.objstorage.content_add(contents)

        with self.db() as db:
            with db.transaction() as cur:
                missing = list(
                    self.content_missing(
                        map(Content.to_dict, contents),
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

    @timed
    @db_transaction()
    def content_update(self, content, keys=[], db=None, cur=None):
        # TODO: Add a check on input keys. How to properly implement
        # this? We don't know yet the new columns.
        self.journal_writer.content_update(content)

        db.mktemp("content", cur)
        select_keys = list(set(db.content_get_metadata_keys).union(set(keys)))
        with convert_validation_exceptions():
            db.copy_to(content, "tmp_content", select_keys, cur)
            db.content_update_from_temp(keys_to_update=keys, cur=cur)

    @timed
    @process_metrics
    @db_transaction()
    def content_add_metadata(
        self, content: Iterable[Content], db=None, cur=None
    ) -> Dict:
        contents = list(content)
        missing = self.content_missing(
            (c.to_dict() for c in contents), key_hash="sha1_git", db=db, cur=cur,
        )
        contents = [c for c in contents if c.sha1_git in missing]

        self.journal_writer.content_add_metadata(contents)
        self._content_add_metadata(db, cur, contents)

        return {
            "content:add": len(contents),
        }

    @timed
    def content_get(self, content):
        # FIXME: Make this method support slicing the `data`.
        if len(content) > BULK_BLOCK_CONTENT_LEN_MAX:
            raise StorageArgumentException(
                "Send at maximum %s contents." % BULK_BLOCK_CONTENT_LEN_MAX
            )
        yield from self.objstorage.content_get(content)

    @timed
    @db_transaction()
    def content_get_range(self, start, end, limit=1000, db=None, cur=None):
        if limit is None:
            raise StorageArgumentException("limit should not be None")
        contents = []
        next_content = None
        for counter, content_row in enumerate(
            db.content_get_range(start, end, limit + 1, cur)
        ):
            content = dict(zip(db.content_get_metadata_keys, content_row))
            if counter >= limit:
                # take the last commit for the next page starting from this
                next_content = content["sha1"]
                break
            contents.append(content)
        return {
            "contents": contents,
            "next": next_content,
        }

    @timed
    def content_get_partition(
        self,
        partition_id: int,
        nb_partitions: int,
        limit: int = 1000,
        page_token: str = None,
    ):
        if limit is None:
            raise StorageArgumentException("limit should not be None")
        (start, end) = get_partition_bounds_bytes(
            partition_id, nb_partitions, SHA1_SIZE
        )
        if page_token:
            start = hash_to_bytes(page_token)
        if end is None:
            end = b"\xff" * SHA1_SIZE
        result = self.content_get_range(start, end, limit)
        result2 = {
            "contents": result["contents"],
            "next_page_token": None,
        }
        if result["next"]:
            result2["next_page_token"] = hash_to_hex(result["next"])
        return result2

    @timed
    @db_transaction(statement_timeout=500)
    def content_get_metadata(
        self, contents: List[bytes], db=None, cur=None
    ) -> Dict[bytes, List[Dict]]:
        result: Dict[bytes, List[Dict]] = {sha1: [] for sha1 in contents}
        for row in db.content_get_metadata_from_sha1s(contents, cur):
            content_meta = dict(zip(db.content_get_metadata_keys, row))
            result[content_meta["sha1"]].append(content_meta)
        return result

    @timed
    @db_transaction_generator()
    def content_missing(self, content, key_hash="sha1", db=None, cur=None):
        keys = db.content_hash_keys

        if key_hash not in keys:
            raise StorageArgumentException("key_hash should be one of %s" % keys)

        key_hash_idx = keys.index(key_hash)

        if not content:
            return

        for obj in db.content_missing_from_list(content, cur):
            yield obj[key_hash_idx]

    @timed
    @db_transaction_generator()
    def content_missing_per_sha1(self, contents, db=None, cur=None):
        for obj in db.content_missing_per_sha1(contents, cur):
            yield obj[0]

    @timed
    @db_transaction_generator()
    def content_missing_per_sha1_git(self, contents, db=None, cur=None):
        for obj in db.content_missing_per_sha1_git(contents, cur):
            yield obj[0]

    @timed
    @db_transaction()
    def content_find(self, content, db=None, cur=None):
        if not set(content).intersection(DEFAULT_ALGORITHMS):
            raise StorageArgumentException(
                "content keys must contain at least one of: "
                "sha1, sha1_git, sha256, blake2s256"
            )

        contents = db.content_find(
            sha1=content.get("sha1"),
            sha1_git=content.get("sha1_git"),
            sha256=content.get("sha256"),
            blake2s256=content.get("blake2s256"),
            cur=cur,
        )
        return [dict(zip(db.content_find_cols, content)) for content in contents]

    @timed
    @db_transaction()
    def content_get_random(self, db=None, cur=None):
        return db.content_get_random(cur)

    @staticmethod
    def _skipped_content_normalize(d):
        d = d.copy()

        if d.get("status") is None:
            d["status"] = "absent"

        if d.get("length") is None:
            d["length"] = -1

        return d

    @staticmethod
    def _skipped_content_validate(d):
        """Sanity checks on status / reason / length, that postgresql
        doesn't enforce."""
        if d["status"] != "absent":
            raise StorageArgumentException(
                "Invalid content status: {}".format(d["status"])
            )

        if d.get("reason") is None:
            raise StorageArgumentException(
                "Must provide a reason if content is absent."
            )

        if d["length"] < -1:
            raise StorageArgumentException("Content length must be positive or -1.")

    def _skipped_content_add_metadata(self, db, cur, content: Iterable[SkippedContent]):
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

    @timed
    @process_metrics
    @db_transaction()
    def skipped_content_add(
        self, content: Iterable[SkippedContent], db=None, cur=None
    ) -> Dict:
        ctime = now()
        content = [attr.evolve(c, ctime=ctime) for c in content]

        missing_contents = self.skipped_content_missing(
            (c.to_dict() for c in content), db=db, cur=cur,
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

    @timed
    @db_transaction_generator()
    def skipped_content_missing(self, contents, db=None, cur=None):
        contents = list(contents)
        for content in db.skipped_content_missing(contents, cur):
            yield dict(zip(db.content_hash_keys, content))

    @timed
    @process_metrics
    @db_transaction()
    def directory_add(
        self, directories: Iterable[Directory], db=None, cur=None
    ) -> Dict:
        directories = list(directories)
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

        # Copy directory ids
        dirs_missing_dict = ({"id": dir} for dir in dirs_missing)
        db.mktemp("directory", cur)
        db.copy_to(dirs_missing_dict, "tmp_directory", ["id"], cur)

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

    @timed
    @db_transaction_generator()
    def directory_missing(self, directories, db=None, cur=None):
        for obj in db.directory_missing_from_list(directories, cur):
            yield obj[0]

    @timed
    @db_transaction_generator(statement_timeout=20000)
    def directory_ls(self, directory, recursive=False, db=None, cur=None):
        if recursive:
            res_gen = db.directory_walk(directory, cur=cur)
        else:
            res_gen = db.directory_walk_one(directory, cur=cur)

        for line in res_gen:
            yield dict(zip(db.directory_ls_cols, line))

    @timed
    @db_transaction(statement_timeout=2000)
    def directory_entry_get_by_path(self, directory, paths, db=None, cur=None):
        res = db.directory_entry_get_by_path(directory, paths, cur)
        if res:
            return dict(zip(db.directory_ls_cols, res))

    @timed
    @db_transaction()
    def directory_get_random(self, db=None, cur=None):
        return db.directory_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def revision_add(self, revisions: Iterable[Revision], db=None, cur=None) -> Dict:
        revisions = list(revisions)
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

        revisions_filtered = list(map(converters.revision_to_db, revisions_filtered))

        parents_filtered: List[bytes] = []

        with convert_validation_exceptions():
            db.copy_to(
                revisions_filtered,
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

    @timed
    @db_transaction_generator()
    def revision_missing(self, revisions, db=None, cur=None):
        if not revisions:
            return

        for obj in db.revision_missing_from_list(revisions, cur):
            yield obj[0]

    @timed
    @db_transaction_generator(statement_timeout=1000)
    def revision_get(self, revisions, db=None, cur=None):
        for line in db.revision_get_from_list(revisions, cur):
            data = converters.db_to_revision(dict(zip(db.revision_get_cols, line)))
            if not data["type"]:
                yield None
                continue
            yield data

    @timed
    @db_transaction_generator(statement_timeout=2000)
    def revision_log(self, revisions, limit=None, db=None, cur=None):
        for line in db.revision_log(revisions, limit, cur):
            data = converters.db_to_revision(dict(zip(db.revision_get_cols, line)))
            if not data["type"]:
                yield None
                continue
            yield data

    @timed
    @db_transaction_generator(statement_timeout=2000)
    def revision_shortlog(self, revisions, limit=None, db=None, cur=None):

        yield from db.revision_shortlog(revisions, limit, cur)

    @timed
    @db_transaction()
    def revision_get_random(self, db=None, cur=None):
        return db.revision_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def release_add(self, releases: Iterable[Release], db=None, cur=None) -> Dict:
        releases = list(releases)
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

        releases_filtered = list(map(converters.release_to_db, releases_filtered))

        with convert_validation_exceptions():
            db.copy_to(releases_filtered, "tmp_release", db.release_add_cols, cur)

            db.release_add_from_temp(cur)

        return {"release:add": len(releases_missing)}

    @timed
    @db_transaction_generator()
    def release_missing(self, releases, db=None, cur=None):
        if not releases:
            return

        for obj in db.release_missing_from_list(releases, cur):
            yield obj[0]

    @timed
    @db_transaction_generator(statement_timeout=500)
    def release_get(self, releases, db=None, cur=None):
        for release in db.release_get_from_list(releases, cur):
            data = converters.db_to_release(dict(zip(db.release_get_cols, release)))
            yield data if data["target_type"] else None

    @timed
    @db_transaction()
    def release_get_random(self, db=None, cur=None):
        return db.release_get_random(cur)

    @timed
    @process_metrics
    @db_transaction()
    def snapshot_add(self, snapshots: Iterable[Snapshot], db=None, cur=None) -> Dict:
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

    @timed
    @db_transaction_generator()
    def snapshot_missing(self, snapshots, db=None, cur=None):
        for obj in db.snapshot_missing_from_list(snapshots, cur):
            yield obj[0]

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_get(self, snapshot_id, db=None, cur=None):

        return self.snapshot_get_branches(snapshot_id, db=db, cur=cur)

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_get_by_origin_visit(self, origin, visit, db=None, cur=None):
        snapshot_id = db.snapshot_get_by_origin_visit(origin, visit, cur)

        if snapshot_id:
            return self.snapshot_get(snapshot_id, db=db, cur=cur)

        return None

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_count_branches(self, snapshot_id, db=None, cur=None):
        return dict([bc for bc in db.snapshot_count_branches(snapshot_id, cur)])

    @timed
    @db_transaction(statement_timeout=2000)
    def snapshot_get_branches(
        self,
        snapshot_id,
        branches_from=b"",
        branches_count=1000,
        target_types=None,
        db=None,
        cur=None,
    ):
        if snapshot_id == EMPTY_SNAPSHOT_ID:
            return {
                "id": snapshot_id,
                "branches": {},
                "next_branch": None,
            }

        branches = {}
        next_branch = None

        fetched_branches = list(
            db.snapshot_get_by_id(
                snapshot_id,
                branches_from=branches_from,
                branches_count=branches_count + 1,
                target_types=target_types,
                cur=cur,
            )
        )
        for branch in fetched_branches[:branches_count]:
            branch = dict(zip(db.snapshot_get_cols, branch))
            del branch["snapshot_id"]
            name = branch.pop("name")
            if branch == {"target": None, "target_type": None}:
                branch = None
            branches[name] = branch

        if len(fetched_branches) > branches_count:
            branch = dict(zip(db.snapshot_get_cols, fetched_branches[-1]))
            next_branch = branch["name"]

        if branches:
            return {
                "id": snapshot_id,
                "branches": branches,
                "next_branch": next_branch,
            }

        return None

    @timed
    @db_transaction()
    def snapshot_get_random(self, db=None, cur=None):
        return db.snapshot_get_random(cur)

    @timed
    @db_transaction()
    def origin_visit_add(
        self, visits: Iterable[OriginVisit], db=None, cur=None
    ) -> Iterable[OriginVisit]:
        for visit in visits:
            origin = self.origin_get({"url": visit.origin}, db=db, cur=cur)
            if not origin:  # Cannot add a visit without an origin
                raise StorageArgumentException("Unknown origin %s", visit.origin)

        all_visits = []
        nb_visits = 0
        for visit in visits:
            nb_visits += 1
            if not visit.visit:
                with convert_validation_exceptions():
                    visit_id = db.origin_visit_add(
                        visit.origin, visit.date, visit.type, cur=cur
                    )
                visit = attr.evolve(visit, visit=visit_id)
            else:
                db.origin_visit_upsert(visit)
            assert visit.visit is not None
            all_visits.append(visit)
            # Forced to write after for the case when the visit has no id
            self.journal_writer.origin_visit_add([visit])
            visit_status = OriginVisitStatus(
                origin=visit.origin,
                visit=visit.visit,
                date=visit.date,
                status="created",
                snapshot=None,
            )
            self._origin_visit_status_add(visit_status, db=db, cur=cur)

        send_metric("origin_visit:add", count=nb_visits, method_name="origin_visit")
        return all_visits

    def _origin_visit_status_add(
        self, visit_status: OriginVisitStatus, db, cur
    ) -> None:
        """Add an origin visit status"""
        self.journal_writer.origin_visit_status_add([visit_status])
        db.origin_visit_status_add(visit_status, cur=cur)
        send_metric(
            "origin_visit_status:add", count=1, method_name="origin_visit_status"
        )

    @timed
    @db_transaction()
    def origin_visit_status_add(
        self, visit_statuses: Iterable[OriginVisitStatus], db=None, cur=None,
    ) -> None:
        # First round to check existence (fail early if any is ko)
        for visit_status in visit_statuses:
            origin_url = self.origin_get({"url": visit_status.origin}, db=db, cur=cur)
            if not origin_url:
                raise StorageArgumentException(f"Unknown origin {visit_status.origin}")

        for visit_status in visit_statuses:
            self._origin_visit_status_add(visit_status, db, cur)

    @timed
    @db_transaction()
    def origin_visit_status_get_latest(
        self,
        origin_url: str,
        visit: int,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        db=None,
        cur=None,
    ) -> Optional[OriginVisitStatus]:
        row = db.origin_visit_status_get_latest(
            origin_url, visit, allowed_statuses, require_snapshot, cur=cur
        )
        if not row:
            return None
        return OriginVisitStatus.from_dict(row)

    def _origin_visit_get_updated(
        self, origin: str, visit_id: int, db, cur
    ) -> Optional[Dict[str, Any]]:
        """Retrieve origin visit and latest origin visit status and merge them
        into an origin visit.

        """
        row_visit = db.origin_visit_get(origin, visit_id)
        if row_visit is None:
            return None
        visit = dict(zip(db.origin_visit_get_cols, row_visit))
        return self._origin_visit_apply_update(visit, db=db, cur=cur)

    def _origin_visit_apply_update(
        self, visit: Dict[str, Any], db, cur=None
    ) -> Dict[str, Any]:
        """Retrieve the latest visit status information for the origin visit.
        Then merge it with the visit and return it.

        """
        visit_status = db.origin_visit_status_get_latest(
            visit["origin"], visit["visit"], cur=cur
        )
        return self._origin_visit_merge(visit, visit_status)

    def _origin_visit_merge(
        self, visit: Dict[str, Any], visit_status: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge origin_visit and origin_visit_status together.

        """
        return OriginVisit.from_dict(
            {
                # default to the values in visit
                **visit,
                # override with the last update
                **visit_status,
                # visit['origin'] is the URL (via a join), while
                # visit_status['origin'] is only an id.
                "origin": visit["origin"],
                # but keep the date of the creation of the origin visit
                "date": visit["date"],
            }
        ).to_dict()

    @timed
    @db_transaction_generator(statement_timeout=500)
    def origin_visit_get(
        self,
        origin: str,
        last_visit: Optional[int] = None,
        limit: Optional[int] = None,
        order: str = "asc",
        db=None,
        cur=None,
    ) -> Iterable[Dict[str, Any]]:
        assert order in ["asc", "desc"]
        lines = db.origin_visit_get_all(
            origin, last_visit=last_visit, limit=limit, order=order, cur=cur
        )
        for line in lines:
            visit = dict(zip(db.origin_visit_get_cols, line))
            yield self._origin_visit_apply_update(visit, db)

    @timed
    @db_transaction(statement_timeout=500)
    def origin_visit_find_by_date(
        self, origin: str, visit_date: datetime.datetime, db=None, cur=None
    ) -> Optional[Dict[str, Any]]:
        visit = db.origin_visit_find_by_date(origin, visit_date, cur=cur)
        if visit:
            return self._origin_visit_apply_update(visit, db)
        return None

    @timed
    @db_transaction(statement_timeout=500)
    def origin_visit_get_by(
        self, origin: str, visit: int, db=None, cur=None
    ) -> Optional[Dict[str, Any]]:
        row = db.origin_visit_get(origin, visit, cur)
        if row:
            visit_dict = dict(zip(db.origin_visit_get_cols, row))
            return self._origin_visit_apply_update(visit_dict, db)
        return None

    @timed
    @db_transaction(statement_timeout=4000)
    def origin_visit_get_latest(
        self,
        origin: str,
        type: Optional[str] = None,
        allowed_statuses: Optional[List[str]] = None,
        require_snapshot: bool = False,
        db=None,
        cur=None,
    ) -> Optional[Dict[str, Any]]:
        row = db.origin_visit_get_latest(
            origin,
            type=type,
            allowed_statuses=allowed_statuses,
            require_snapshot=require_snapshot,
            cur=cur,
        )
        if row:
            visit = dict(zip(db.origin_visit_get_cols, row))
            return self._origin_visit_apply_update(visit, db)
        return None

    @timed
    @db_transaction()
    def origin_visit_get_random(
        self, type: str, db=None, cur=None
    ) -> Optional[Dict[str, Any]]:
        row = db.origin_visit_get_random(type, cur)
        if row:
            visit = dict(zip(db.origin_visit_get_cols, row))
            return self._origin_visit_apply_update(visit, db)
        return None

    @timed
    @db_transaction(statement_timeout=2000)
    def object_find_by_sha1_git(self, ids, db=None, cur=None):
        ret = {id: [] for id in ids}

        for retval in db.object_find_by_sha1_git(ids, cur=cur):
            if retval[1]:
                ret[retval[0]].append(
                    dict(zip(db.object_find_by_sha1_git_cols, retval))
                )

        return ret

    @timed
    @db_transaction(statement_timeout=500)
    def origin_get(self, origins, db=None, cur=None):
        if isinstance(origins, dict):
            # Old API
            return_single = True
            origins = [origins]
        elif len(origins) == 0:
            return []
        else:
            return_single = False

        origin_urls = [origin["url"] for origin in origins]
        results = db.origin_get_by_url(origin_urls, cur)

        results = [dict(zip(db.origin_cols, result)) for result in results]
        if return_single:
            assert len(results) == 1
            if results[0]["url"] is not None:
                return results[0]
            else:
                return None
        else:
            return [None if res["url"] is None else res for res in results]

    @timed
    @db_transaction_generator(statement_timeout=500)
    def origin_get_by_sha1(self, sha1s, db=None, cur=None):
        for line in db.origin_get_by_sha1(sha1s, cur):
            if line[0] is not None:
                yield dict(zip(db.origin_cols, line))
            else:
                yield None

    @timed
    @db_transaction_generator()
    def origin_get_range(self, origin_from=1, origin_count=100, db=None, cur=None):
        for origin in db.origin_get_range(origin_from, origin_count, cur):
            yield dict(zip(db.origin_get_range_cols, origin))

    @timed
    @db_transaction()
    def origin_list(
        self, page_token: Optional[str] = None, limit: int = 100, *, db=None, cur=None
    ) -> dict:
        page_token = page_token or "0"
        if not isinstance(page_token, str):
            raise StorageArgumentException("page_token must be a string.")
        origin_from = int(page_token)
        result: Dict[str, Any] = {
            "origins": [
                dict(zip(db.origin_get_range_cols, origin))
                for origin in db.origin_get_range(origin_from, limit, cur)
            ],
        }

        assert len(result["origins"]) <= limit
        if len(result["origins"]) == limit:
            result["next_page_token"] = str(result["origins"][limit - 1]["id"] + 1)

        for origin in result["origins"]:
            del origin["id"]

        return result

    @timed
    @db_transaction_generator()
    def origin_search(
        self,
        url_pattern,
        offset=0,
        limit=50,
        regexp=False,
        with_visit=False,
        db=None,
        cur=None,
    ):
        for origin in db.origin_search(
            url_pattern, offset, limit, regexp, with_visit, cur
        ):
            yield dict(zip(db.origin_cols, origin))

    @timed
    @db_transaction()
    def origin_count(
        self, url_pattern, regexp=False, with_visit=False, db=None, cur=None
    ):
        return db.origin_count(url_pattern, regexp, with_visit, cur)

    @timed
    @db_transaction()
    def origin_add(
        self, origins: Iterable[Origin], db=None, cur=None
    ) -> Dict[str, int]:
        urls = [o.url for o in origins]
        known_origins = set(url for (url,) in db.origin_get_by_url(urls, cur))
        # use lists here to keep origins sorted; some tests depend on this
        to_add = [url for url in urls if url not in known_origins]

        self.journal_writer.origin_add([Origin(url=url) for url in to_add])
        added = 0
        for url in to_add:
            if db.origin_add(url, cur):
                added += 1
        return {"origin:add": added}

    @deprecated("Use origin_add([origin]) instead")
    @timed
    @db_transaction()
    def origin_add_one(self, origin: Origin, db=None, cur=None) -> str:
        stats = self.origin_add([origin])
        if stats.get("origin:add", 0):
            send_metric("origin:add", count=1, method_name="origin_add_one")
        return origin.url

    @db_transaction(statement_timeout=500)
    def stat_counters(self, db=None, cur=None):
        return {k: v for (k, v) in db.stat_counters()}

    @db_transaction()
    def refresh_stat_counters(self, db=None, cur=None):
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

    @timed
    @db_transaction()
    def origin_metadata_add(
        self,
        origin_url: str,
        discovery_date: datetime.datetime,
        authority: Dict[str, Any],
        fetcher: Dict[str, Any],
        format: str,
        metadata: bytes,
        db=None,
        cur=None,
    ) -> None:
        authority_id = db.metadata_authority_get_id(
            authority["type"], authority["url"], cur
        )
        if not authority_id:
            raise StorageArgumentException(f"Unknown authority {authority}")
        fetcher_id = db.metadata_fetcher_get_id(
            fetcher["name"], fetcher["version"], cur
        )
        if not fetcher_id:
            raise StorageArgumentException(f"Unknown fetcher {fetcher}")
        try:
            db.origin_metadata_add(
                origin_url,
                discovery_date,
                authority_id,
                fetcher_id,
                format,
                metadata,
                cur,
            )
        except psycopg2.ProgrammingError as e:
            raise StorageArgumentException(*e.args)
        send_metric("origin_metadata:add", count=1, method_name="origin_metadata_add")

    @timed
    @db_transaction(statement_timeout=500)
    def origin_metadata_get(
        self,
        origin_url: str,
        authority: Dict[str, str],
        after: Optional[datetime.datetime] = None,
        page_token: Optional[bytes] = None,
        limit: int = 1000,
        db=None,
        cur=None,
    ) -> Dict[str, Any]:
        if page_token:
            (after_time, after_fetcher) = msgpack_loads(page_token)
            if after and after_time < after:
                raise StorageArgumentException(
                    "page_token is inconsistent with the value of 'after'."
                )
        else:
            after_time = after
            after_fetcher = None

        authority_id = db.metadata_authority_get_id(
            authority["type"], authority["url"], cur
        )
        if not authority_id:
            return {
                "next_page_token": None,
                "results": [],
            }

        rows = db.origin_metadata_get(
            origin_url, authority_id, after_time, after_fetcher, limit + 1, cur
        )
        rows = [dict(zip(db.origin_metadata_get_cols, row)) for row in rows]
        results = []
        for row in rows:
            row = row.copy()
            row.pop("metadata_fetcher.id")
            results.append(
                {
                    "origin_url": row.pop("origin.url"),
                    "authority": {
                        "type": row.pop("metadata_authority.type"),
                        "url": row.pop("metadata_authority.url"),
                    },
                    "fetcher": {
                        "name": row.pop("metadata_fetcher.name"),
                        "version": row.pop("metadata_fetcher.version"),
                    },
                    **row,
                }
            )

        if len(results) > limit:
            results.pop()
            assert len(results) == limit
            last_returned_row = rows[-2]  # rows[-1] corresponds to the popped result
            next_page_token: Optional[bytes] = msgpack_dumps(
                (
                    last_returned_row["discovery_date"],
                    last_returned_row["metadata_fetcher.id"],
                )
            )
        else:
            next_page_token = None

        return {
            "next_page_token": next_page_token,
            "results": results,
        }

    @timed
    @db_transaction()
    def metadata_fetcher_add(
        self, name: str, version: str, metadata: Dict[str, Any], db=None, cur=None
    ) -> None:
        db.metadata_fetcher_add(name, version, metadata)
        send_metric("metadata_fetcher:add", count=1, method_name="metadata_fetcher")

    @timed
    @db_transaction(statement_timeout=500)
    def metadata_fetcher_get(
        self, name: str, version: str, db=None, cur=None
    ) -> Optional[Dict[str, Any]]:
        row = db.metadata_fetcher_get(name, version, cur=cur)
        if not row:
            return None
        return dict(zip(db.metadata_fetcher_cols, row))

    @timed
    @db_transaction()
    def metadata_authority_add(
        self, type: str, url: str, metadata: Dict[str, Any], db=None, cur=None
    ) -> None:
        db.metadata_authority_add(type, url, metadata, cur)
        send_metric("metadata_authority:add", count=1, method_name="metadata_authority")

    @timed
    @db_transaction()
    def metadata_authority_get(
        self, type: str, url: str, db=None, cur=None
    ) -> Optional[Dict[str, Any]]:
        row = db.metadata_authority_get(type, url, cur=cur)
        if not row:
            return None
        return dict(zip(db.metadata_authority_cols, row))

    @timed
    def diff_directories(self, from_dir, to_dir, track_renaming=False):
        return diff.diff_directories(self, from_dir, to_dir, track_renaming)

    @timed
    def diff_revisions(self, from_rev, to_rev, track_renaming=False):
        return diff.diff_revisions(self, from_rev, to_rev, track_renaming)

    @timed
    def diff_revision(self, revision, track_renaming=False):
        return diff.diff_revision(self, revision, track_renaming)

    def clear_buffers(self, object_types: Optional[Iterable[str]] = None) -> None:
        """Do nothing

        """
        return None

    def flush(self, object_types: Optional[Iterable[str]] = None) -> Dict:
        return {}
