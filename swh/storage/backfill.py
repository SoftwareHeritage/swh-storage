# Copyright (C) 2017-2020  The Software Heritage developers
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


from swh.core.db import BaseDb
from swh.journal.writer.kafka import KafkaJournalWriter
from swh.storage.converters import (
    db_to_raw_extrinsic_metadata,
    db_to_release,
    db_to_revision,
)
from swh.storage.replay import object_converter_fn


logger = logging.getLogger(__name__)

PARTITION_KEY = {
    "content": "sha1",
    "skipped_content": "sha1",
    "directory": "id",
    "metadata_authority": "type, url",
    "metadata_fetcher": "name, version",
    "raw_extrinsic_metadata": "id",
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
    "directory": ["id", "dir_entries", "file_entries", "rev_entries"],
    "metadata_authority": ["type", "url", "metadata",],
    "metadata_fetcher": ["name", "version", "metadata",],
    "raw_extrinsic_metadata": [
        "raw_extrinsic_metadata.type",
        "raw_extrinsic_metadata.id",
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
        "date_offset",
        "date_neg_utc_offset",
        "committer_date",
        "committer_date_offset",
        "committer_date_neg_utc_offset",
        "type",
        "directory",
        "message",
        "synthetic",
        "metadata",
        (
            "array(select parent_id::bytea from revision_history rh "
            "where rh.id = revision.id order by rh.parent_rank asc)",
            "parents",
        ),
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
        "date_offset",
        "date_neg_utc_offset",
        "comment",
        ("release.name", "name"),
        "synthetic",
        "target",
        "target_type",
        ("a.id", "author_id"),
        ("a.name", "author_name"),
        ("a.email", "author_email"),
        ("a.fullname", "author_fullname"),
    ],
    "snapshot": ["id", "object_id"],
    "origin": ["url"],
    "origin_visit": ["visit", "type", ("origin.url", "origin"), "date",],
    "origin_visit_status": [
        "visit",
        ("origin.url", "origin"),
        "date",
        "snapshot",
        "status",
        "metadata",
    ],
}


JOINS = {
    "release": ["person a on release.author=a.id"],
    "revision": [
        "person a on revision.author=a.id",
        "person c on revision.committer=c.id",
    ],
    "origin_visit": ["origin on origin_visit.origin=origin.id"],
    "origin_visit_status": ["origin on origin_visit_status.origin=origin.id"],
    "raw_extrinsic_metadata": [
        "metadata_authority on "
        "raw_extrinsic_metadata.authority_id=metadata_authority.id",
        "metadata_fetcher on raw_extrinsic_metadata.fetcher_id=metadata_fetcher.id",
    ],
}


def directory_converter(db, directory):
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
            ids = directory.pop("%s_entries" % type)
            if not ids:
                continue
            query = query_template % {
                "columns": ",".join(columns),
                "type": type,
            }
            cur.execute(query, (tuple(ids),))
            for row in cur:
                entry = dict(zip(columns, row))
                entry["type"] = type
                entries.append(entry)

    directory["entries"] = entries
    return directory


def raw_extrinsic_metadata_converter(db, metadata):
    """Convert revision from the flat representation to swh model
       compatible objects.

    """
    return db_to_raw_extrinsic_metadata(metadata).to_dict()


def revision_converter(db, revision):
    """Convert revision from the flat representation to swh model
       compatible objects.

    """
    return db_to_revision(revision)


def release_converter(db, release):
    """Convert release from the flat representation to swh model
       compatible objects.

    """
    return db_to_release(release)


def snapshot_converter(db, snapshot):
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
        cur.execute(query, (snapshot.pop("object_id"),))
        branches = {}
        for name, *row in cur:
            branch = dict(zip(columns[1:], row))
            if not branch["target"] and not branch["target_type"]:
                branch = None
            branches[name] = branch

    snapshot["branches"] = branches
    return snapshot


CONVERTERS = {
    "directory": directory_converter,
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


def byte_ranges(numbits, start_object=None, end_object=None):
    """Generate start/end pairs of bytes spanning numbits bits and
       constrained by optional start_object and end_object.

    Args:
        numbits (int): Number of bits in which we divide input space
        start_object (str): Hex object id contained in the first range
                            returned
        end_object (str): Hex object id contained in the last range
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


def integer_ranges(start, end, block_size=1000):
    for start in range(start, end, block_size):
        if start == 0:
            yield None, block_size
        elif start + block_size > end:
            yield start, end
        else:
            yield start, start + block_size


RANGE_GENERATORS = {
    "content": lambda start, end: byte_ranges(24, start, end),
    "skipped_content": lambda start, end: [(None, None)],
    "directory": lambda start, end: byte_ranges(24, start, end),
    "revision": lambda start, end: byte_ranges(24, start, end),
    "release": lambda start, end: byte_ranges(16, start, end),
    "snapshot": lambda start, end: byte_ranges(16, start, end),
    "origin": integer_ranges,
    "origin_visit": integer_ranges,
    "origin_visit_status": integer_ranges,
}


def compute_query(obj_type, start, end):
    columns = COLUMNS.get(obj_type)
    join_specs = JOINS.get(obj_type, [])
    join_clause = "\n".join("left join %s" % clause for clause in join_specs)

    where = []
    where_args = []
    if start:
        where.append("%(keys)s >= %%s")
        where_args.append(start)
    if end:
        where.append("%(keys)s < %%s")
        where_args.append(end)

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

            logger.debug("record: %s" % record)
            yield record


def _format_range_bound(bound):
    if isinstance(bound, bytes):
        return bound.hex()
    else:
        return str(bound)


MANDATORY_KEYS = ["brokers", "storage_dbconn", "prefix", "client_id"]


class JournalBackfiller:
    """Class in charge of reading the storage's objects and sends those
       back to the journal's topics.

       This is designed to be run periodically.

    """

    def __init__(self, config=None):
        self.config = config
        self.check_config(config)

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
                % (object_type, ", ".join(COLUMNS.keys()))
            )

        if object_type in ["origin", "origin_visit"]:
            if start_object:
                start_object = int(start_object)
            else:
                start_object = 0
            if end_object:
                end_object = int(end_object)
            else:
                end_object = 100 * 1000 * 1000  # hard-coded limit

        return start_object, end_object

    def run(self, object_type, start_object, end_object, dry_run=False):
        """Reads storage's subscribed object types and send them to the
           journal's reading topic.

        """
        start_object, end_object = self.parse_arguments(
            object_type, start_object, end_object
        )

        db = BaseDb.connect(self.config["storage_dbconn"])
        writer = KafkaJournalWriter(
            brokers=self.config["brokers"],
            prefix=self.config["prefix"],
            client_id=self.config["client_id"],
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

            for obj_d in fetch(db, object_type, start=range_start, end=range_end,):
                obj = object_converter_fn[object_type](obj_d)
                if dry_run:
                    continue
                writer.write_addition(object_type=object_type, object_=obj)

            writer.producer.flush()


if __name__ == "__main__":
    print('Please use the "swh-journal backfiller run" command')
