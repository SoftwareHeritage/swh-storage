# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Storage backfiller.

The backfiller goal is to produce back part or all of the objects
from a storage to the journal topics

Current implementation consists in the JournalBackfiller class.

It simply reads the objects from the storage and sends every object identifier back to
the journal.

"""

import logging
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Tuple, Union

from swh.core.db import BaseDb
from swh.model.model import (
    BaseModel,
    Directory,
    DirectoryEntry,
    ExtID,
    RawExtrinsicMetadata,
    Release,
    Revision,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import ExtendedObjectType
from swh.storage.postgresql.converters import (
    db_to_extid,
    db_to_raw_extrinsic_metadata,
    db_to_release,
    db_to_revision,
)
from swh.storage.replay import OBJECT_CONVERTERS
from swh.storage.writer import JournalWriter

logger = logging.getLogger(__name__)

PARTITION_KEY = {
    "content": "sha1",
    "skipped_content": "sha1",
    "directory": "id",
    "extid": "target",
    "metadata_authority": "type, url",
    "metadata_fetcher": "name, version",
    "raw_extrinsic_metadata": "target",
    "revision": "revision.id",
    "release": "release.id",
    "snapshot": "id",
    "origin": "id",
    "origin_visit": "origin_visit.origin",
    "origin_visit_status": "origin_visit_status.origin",
}

COLUMNS = {
    "content": [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "status",
        "ctime",
    ],
    "skipped_content": [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "ctime",
        "status",
        "reason",
    ],
    "directory": ["id", "dir_entries", "file_entries", "rev_entries", "raw_manifest"],
    "extid": ["extid_type", "extid", "extid_version", "target_type", "target"],
    "metadata_authority": ["type", "url"],
    "metadata_fetcher": ["name", "version"],
    "origin": ["url"],
    "origin_visit": [
        "visit",
        "type",
        ("origin.url", "origin"),
        "date",
    ],
    "origin_visit_status": [
        ("origin_visit_status.visit", "visit"),
        ("origin.url", "origin"),
        ("origin_visit_status.date", "date"),
        "type",
        "snapshot",
        "status",
        "metadata",
    ],
    "raw_extrinsic_metadata": [
        "raw_extrinsic_metadata.type",
        "raw_extrinsic_metadata.target",
        "metadata_authority.type",
        "metadata_authority.url",
        "metadata_fetcher.name",
        "metadata_fetcher.version",
        "discovery_date",
        "format",
        "raw_extrinsic_metadata.metadata",
        "origin",
        "visit",
        "snapshot",
        "release",
        "revision",
        "path",
        "directory",
    ],
    "revision": [
        ("revision.id", "id"),
        "date",
        "date_offset_bytes",
        "committer_date",
        "committer_date_offset_bytes",
        "type",
        "directory",
        "message",
        "synthetic",
        "metadata",
        "extra_headers",
        (
            "array(select parent_id::bytea from revision_history rh "
            "where rh.id = revision.id order by rh.parent_rank asc)",
            "parents",
        ),
        "raw_manifest",
        ("a.id", "author_id"),
        ("a.name", "author_name"),
        ("a.email", "author_email"),
        ("a.fullname", "author_fullname"),
        ("c.id", "committer_id"),
        ("c.name", "committer_name"),
        ("c.email", "committer_email"),
        ("c.fullname", "committer_fullname"),
    ],
    "release": [
        ("release.id", "id"),
        "date",
        "date_offset_bytes",
        "comment",
        ("release.name", "name"),
        "synthetic",
        "target",
        "target_type",
        ("a.id", "author_id"),
        ("a.name", "author_name"),
        ("a.email", "author_email"),
        ("a.fullname", "author_fullname"),
        "raw_manifest",
    ],
    "snapshot": ["id", "object_id"],
}


JOINS = {
    "release": ["person a on release.author=a.id"],
    "revision": [
        "person a on revision.author=a.id",
        "person c on revision.committer=c.id",
    ],
    "origin_visit": ["origin on origin_visit.origin=origin.id"],
    "origin_visit_status": [
        "origin on origin_visit_status.origin=origin.id",
    ],
    "raw_extrinsic_metadata": [
        "metadata_authority on "
        "raw_extrinsic_metadata.authority_id=metadata_authority.id",
        "metadata_fetcher on raw_extrinsic_metadata.fetcher_id=metadata_fetcher.id",
    ],
}

EXTRA_WHERE = {
    # hack to force the right index usage on table extid
    "extid": "target_type in ('revision', 'release', 'content', 'directory')"
}


def directory_converter(db: BaseDb, directory_d: Dict[str, Any]) -> Directory:
    """Convert directory from the flat representation to swh model
    compatible objects.

    """
    columns = ["target", "name", "perms"]
    query_template = """
    select %(columns)s
    from directory_entry_%(type)s
    where id in %%s
    """

    types = ["file", "dir", "rev"]

    entries = []
    with db.cursor() as cur:
        for type in types:
            ids = directory_d.pop("%s_entries" % type)
            if not ids:
                continue
            query = query_template % {
                "columns": ",".join(columns),
                "type": type,
            }
            cur.execute(query, (tuple(ids),))
            for row in cur:
                entry_d = dict(zip(columns, row))
                entry = DirectoryEntry(
                    name=entry_d["name"],
                    type=type,
                    target=entry_d["target"],
                    perms=entry_d["perms"],
                )
                entries.append(entry)

    (is_corrupt, dir_) = Directory.from_possibly_duplicated_entries(
        id=directory_d["id"],
        entries=tuple(entries),
        raw_manifest=directory_d["raw_manifest"],
    )
    if is_corrupt:
        logger.info("%s has duplicated entries", dir_.swhid())

    return dir_


def raw_extrinsic_metadata_converter(
    db: BaseDb, metadata: Dict[str, Any]
) -> RawExtrinsicMetadata:
    """Convert a raw extrinsic metadata from the flat representation to swh model
    compatible objects.

    """
    return db_to_raw_extrinsic_metadata(metadata)


def extid_converter(db: BaseDb, extid: Dict[str, Any]) -> ExtID:
    """Convert an extid from the flat representation to swh model
    compatible objects.

    """
    return db_to_extid(extid)


def revision_converter(db: BaseDb, revision_d: Dict[str, Any]) -> Revision:
    """Convert revision from the flat representation to swh model
    compatible objects.

    """
    return db_to_revision(revision_d)


def release_converter(db: BaseDb, release_d: Dict[str, Any]) -> Release:
    """Convert release from the flat representation to swh model
    compatible objects.

    """
    return db_to_release(release_d)


def snapshot_converter(db: BaseDb, snapshot_d: Dict[str, Any]) -> Snapshot:
    """Convert snapshot from the flat representation to swh model
    compatible objects.

    """
    columns = ["name", "target", "target_type"]
    query = """
    select %s
    from snapshot_branches sbs
    inner join snapshot_branch sb on sb.object_id=sbs.branch_id
    where sbs.snapshot_id=%%s
    """ % ", ".join(
        columns
    )
    with db.cursor() as cur:
        cur.execute(query, (snapshot_d["object_id"],))
        branches = {}
        for name, *row in cur:
            branch_d = dict(zip(columns[1:], row))
            if branch_d["target"] is not None and branch_d["target_type"] is not None:
                branch: Optional[SnapshotBranch] = SnapshotBranch(
                    target=branch_d["target"],
                    target_type=SnapshotTargetType(branch_d["target_type"]),
                )
            else:
                branch = None
            branches[name] = branch

    return Snapshot(
        id=snapshot_d["id"],
        branches=branches,
    )


CONVERTERS: Dict[str, Callable[[BaseDb, Dict[str, Any]], BaseModel]] = {
    "directory": directory_converter,
    "extid": extid_converter,
    "raw_extrinsic_metadata": raw_extrinsic_metadata_converter,
    "revision": revision_converter,
    "release": release_converter,
    "snapshot": snapshot_converter,
}


def object_to_offset(object_id, numbits):
    """Compute the index of the range containing object id, when dividing
       space into 2^numbits.

    Args:
        object_id (str): The hex representation of object_id
        numbits (int): Number of bits in which we divide input space

    Returns:
        The index of the range containing object id

    """
    q, r = divmod(numbits, 8)
    length = q + (r != 0)
    shift_bits = 8 - r if r else 0

    truncated_id = object_id[: length * 2]
    if len(truncated_id) < length * 2:
        truncated_id += "0" * (length * 2 - len(truncated_id))

    truncated_id_bytes = bytes.fromhex(truncated_id)
    return int.from_bytes(truncated_id_bytes, byteorder="big") >> shift_bits


def byte_ranges(
    numbits: int, start_object: Optional[str] = None, end_object: Optional[str] = None
) -> Iterator[Tuple[Optional[bytes], Optional[bytes]]]:
    """Generate start/end pairs of bytes spanning numbits bits and
       constrained by optional start_object and end_object.

    Args:
        numbits: Number of bits in which we divide input space
        start_object: Hex object id contained in the first range
                            returned
        end_object: Hex object id contained in the last range
                          returned

    Yields:
        2^numbits pairs of bytes

    """
    q, r = divmod(numbits, 8)
    length = q + (r != 0)
    shift_bits = 8 - r if r else 0

    def to_bytes(i):
        return int.to_bytes(i << shift_bits, length=length, byteorder="big")

    start_offset = 0
    end_offset = 1 << numbits

    if start_object is not None:
        start_offset = object_to_offset(start_object, numbits)
    if end_object is not None:
        end_offset = object_to_offset(end_object, numbits) + 1

    for start in range(start_offset, end_offset):
        end = start + 1

        if start == 0:
            yield None, to_bytes(end)
        elif end == 1 << numbits:
            yield to_bytes(start), None
        else:
            yield to_bytes(start), to_bytes(end)


def raw_extrinsic_metadata_target_ranges(
    start_object: Optional[str] = None, end_object: Optional[str] = None
) -> Iterator[Tuple[Optional[str], Optional[str]]]:
    """Generate ranges of values for the `target` attribute of `raw_extrinsic_metadata`
    objects.

    This generates one range for all values before the first SWHID (which would
    correspond to raw origin URLs), then a number of hex-based ranges for each
    known type of SWHID (2**12 ranges for directories, 2**8 ranges for all other
    types). Finally, it generates one extra range for values above all possible
    SWHIDs.
    """
    if start_object is None:
        start_object = ""

    swhid_target_types = sorted(type.value for type in ExtendedObjectType)

    first_swhid = f"swh:1:{swhid_target_types[0]}:"

    # Generate a range for url targets, if the starting object is before SWHIDs
    if start_object < first_swhid:
        yield start_object, (
            first_swhid
            if end_object is None or end_object >= first_swhid
            else end_object
        )

    if end_object is not None and end_object <= first_swhid:
        return

    # Prime the following loop, which uses the upper bound of the previous range
    # as lower bound, to account for potential targets between two valid types
    # of SWHIDs (even though they would eventually be rejected by the
    # RawExtrinsicMetadata parser, they /might/ exist...)
    end_swhid = first_swhid

    # Generate ranges for swhid targets
    for target_type in swhid_target_types:
        finished = False
        base_swhid = f"swh:1:{target_type}:"
        last_swhid = base_swhid + ("f" * 40)

        if start_object > last_swhid:
            continue

        # Generate 2**8 or 2**12 ranges
        for _, end in byte_ranges(12 if target_type == "dir" else 8):
            # Reuse previous upper bound
            start_swhid = end_swhid

            # Use last_swhid for this object type if on the last byte range
            end_swhid = (base_swhid + end.hex()) if end is not None else last_swhid

            # Ignore out of bounds ranges
            if start_object >= end_swhid:
                continue

            # Potentially clamp start of range to the first object requested
            start_swhid = max(start_swhid, start_object)

            # Handle ending the loop early if the last requested object id is in
            # the current range
            if end_object is not None and end_swhid >= end_object:
                end_swhid = end_object
                finished = True

            yield start_swhid, end_swhid

            if finished:
                return

    # Generate one final range for potential raw origin URLs after the last
    # valid SWHID
    start_swhid = max(start_object, end_swhid)
    yield start_swhid, end_object


def integer_ranges(
    start: str, end: str, block_size: int = 1000
) -> Iterator[Tuple[Optional[int], Optional[int]]]:
    for range_start in range(int(start), int(end), block_size):
        if range_start == 0:
            yield None, block_size
        elif range_start + block_size > int(end):
            yield range_start, int(end)
        else:
            yield range_start, range_start + block_size


RANGE_GENERATORS: Dict[
    str,
    Union[
        Callable[[str, str], Iterable[Tuple[Optional[str], Optional[str]]]],
        Callable[[str, str], Iterable[Tuple[Optional[bytes], Optional[bytes]]]],
        Callable[[str, str], Iterable[Tuple[Optional[int], Optional[int]]]],
    ],
] = {
    "content": lambda start, end: byte_ranges(24, start, end),
    "skipped_content": lambda start, end: [(None, None)],
    "directory": lambda start, end: byte_ranges(24, start, end),
    "extid": lambda start, end: byte_ranges(24, start, end),
    "revision": lambda start, end: byte_ranges(24, start, end),
    "release": lambda start, end: byte_ranges(16, start, end),
    "raw_extrinsic_metadata": raw_extrinsic_metadata_target_ranges,
    "snapshot": lambda start, end: byte_ranges(16, start, end),
    "origin": integer_ranges,
    "origin_visit": integer_ranges,
    "origin_visit_status": integer_ranges,
}


def compute_query(obj_type, start, end):
    columns = COLUMNS.get(obj_type)
    join_specs = JOINS.get(obj_type, [])
    join_clause = "\n".join("left join %s" % clause for clause in join_specs)
    additional_where = EXTRA_WHERE.get(obj_type)

    where = []
    where_args = []
    if start:
        where.append("%(keys)s >= %%s")
        where_args.append(start)
    if end:
        where.append("%(keys)s < %%s")
        where_args.append(end)

    if additional_where:
        where.append(additional_where)

    where_clause = ""
    if where:
        where_clause = ("where " + " and ".join(where)) % {
            "keys": "(%s)" % PARTITION_KEY[obj_type]
        }

    column_specs = []
    column_aliases = []
    for column in columns:
        if isinstance(column, str):
            column_specs.append(column)
            column_aliases.append(column)
        else:
            column_specs.append("%s as %s" % column)
            column_aliases.append(column[1])

    query = """
select %(columns)s
from %(table)s
%(join)s
%(where)s
    """ % {
        "columns": ",".join(column_specs),
        "table": obj_type,
        "join": join_clause,
        "where": where_clause,
    }

    return query, where_args, column_aliases


def fetch(db, obj_type, start, end):
    """Fetch all obj_type's identifiers from db.

    This opens one connection, stream objects and when done, close
    the connection.

    Args:
        db (BaseDb): Db connection object
        obj_type (str): Object type
        start (Union[bytes|Tuple]): Range start identifier
        end (Union[bytes|Tuple]): Range end identifier

    Raises:
        ValueError if obj_type is not supported

    Yields:
        Objects in the given range

    """
    query, where_args, column_aliases = compute_query(obj_type, start, end)
    converter = CONVERTERS.get(obj_type)
    with db.cursor() as cursor:
        logger.debug("Fetching data for table %s", obj_type)
        logger.debug("query: %s %s", query, where_args)
        cursor.execute(query, where_args)
        for row in cursor:
            record = dict(zip(column_aliases, row))
            if converter:
                record = converter(db, record)
            else:
                record = OBJECT_CONVERTERS[obj_type](record)

            logger.debug("record: %s", record)
            yield record


def _format_range_bound(bound):
    if isinstance(bound, bytes):
        return bound.hex()
    else:
        return str(bound)


MANDATORY_KEYS = ["storage", "journal_writer"]


class JournalBackfiller:
    """Class in charge of reading the storage's objects and sends those
    back to the journal's topics.

    This is designed to be run periodically.

    """

    def __init__(self, config=None):
        self.config = config
        self.check_config(config)
        self._db = None
        self.writer = JournalWriter({"cls": "kafka", **self.config["journal_writer"]})
        assert self.writer.journal is not None

    @property
    def db(self):
        if self._db is None:
            self._db = BaseDb.connect(self.config["storage"]["db"])
        return self._db

    def check_config(self, config):
        missing_keys = []
        for key in MANDATORY_KEYS:
            if not config.get(key):
                missing_keys.append(key)

        if missing_keys:
            raise ValueError(
                "Configuration error: The following keys must be"
                " provided: %s" % (",".join(missing_keys),)
            )

        if "cls" not in config["storage"] or config["storage"]["cls"] not in (
            "local",
            "postgresql",
        ):
            raise ValueError(
                "swh storage backfiller must be configured to use a local"
                " (PostgreSQL) storage"
            )

    def parse_arguments(self, object_type, start_object, end_object):
        """Parse arguments

        Raises:
            ValueError for unsupported object type
            ValueError if object ids are not parseable

        Returns:
            Parsed start and end object ids

        """
        if object_type not in COLUMNS:
            raise ValueError(
                "Object type %s is not supported. "
                "The only possible values are %s"
                % (object_type, ", ".join(sorted(COLUMNS.keys())))
            )

        if object_type in ["origin", "origin_visit", "origin_visit_status"]:
            start_object = start_object or "0"
            end_object = end_object or str(100_000_000)  # hard-coded limit

        return start_object, end_object

    def run(self, object_type, start_object, end_object, dry_run=False):
        """Reads storage's subscribed object types and send them to the
        journal's reading topic.

        """
        start_object, end_object = self.parse_arguments(
            object_type, start_object, end_object
        )

        for range_start, range_end in RANGE_GENERATORS[object_type](
            start_object, end_object
        ):
            logger.info(
                "Processing %s range %s to %s",
                object_type,
                _format_range_bound(range_start),
                _format_range_bound(range_end),
            )

            objects = fetch(self.db, object_type, start=range_start, end=range_end)

            if not dry_run:
                self.writer.write_additions(object_type, objects)
            else:
                # only consume the objects iterator to check for any potential
                # decoding/encoding errors
                for obj in objects:
                    pass


if __name__ == "__main__":
    print('Please use the "swh-journal backfiller run" command')
