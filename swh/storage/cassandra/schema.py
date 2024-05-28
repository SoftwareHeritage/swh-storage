# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


CREATE_TABLES_QUERIES = {
    "microtimestamp": """
CREATE TYPE IF NOT EXISTS microtimestamp (
    seconds             bigint,
    microseconds        int,
);""",
    "microtimestamp_with_timezone": """
CREATE TYPE IF NOT EXISTS microtimestamp_with_timezone (
    timestamp           frozen<microtimestamp>,
    offset_bytes        blob,
);""",
    "person": """
CREATE TYPE IF NOT EXISTS person (
    fullname    blob,
    name        blob,
    email       blob
);""",
    "migration": """
CREATE TABLE IF NOT EXISTS migration (
    id                  ascii,
    dependencies        frozen<set<ascii>>,
    min_read_version    ascii,
    status              ascii,
    PRIMARY KEY ((id))
) WITH
    comment = 'Set of known database migrations'
    {table_options};""",
    "content": """
CREATE TABLE IF NOT EXISTS content (
    sha1          blob,
    sha1_git      blob,
    sha256        blob,
    blake2s256    blob,
    length        bigint,
    ctime         timestamp,
    status        ascii,
    PRIMARY KEY ((sha256), sha1, sha1_git, blake2s256)
) WITH
    comment = 'The content of a file, without a file name'
    {table_options};""",
    "skipped_content": """
CREATE TABLE IF NOT EXISTS skipped_content (
    sha1          blob,
    sha1_git      blob,
    sha256        blob,
    blake2s256    blob,
    length        bigint,
    ctime         timestamp,
    status        ascii,
    reason        text,
    origin        text,
    PRIMARY KEY ((sha1, sha1_git, sha256, blake2s256))
) WITH
    comment = 'A content that could not be archived by SWH for a given reason'
    {table_options};""",
    "revision": """
CREATE TABLE IF NOT EXISTS revision (
    id                              blob,
    date                            microtimestamp_with_timezone,
    committer_date                  microtimestamp_with_timezone,
    type                            ascii,
    directory                       blob,
    message                         blob,
    author                          person,
    committer                       person,
    synthetic                       boolean,
    metadata                        text,
    extra_headers                   frozen<list <list<blob>> >,
    raw_manifest                    blob,
    PRIMARY KEY ((id))
) WITH
    comment = 'Intermediary state of development of a project'
    {table_options};""",
    "revision_parent": """
CREATE TABLE IF NOT EXISTS revision_parent (
    id                     blob,
    parent_rank                     int,
    parent_id                       blob,
    PRIMARY KEY ((id), parent_rank)
) WITH
    comment = 'Ordered list of parents of a revision'
    {table_options};""",
    "release": """
CREATE TABLE IF NOT EXISTS release (
    id                              blob,
    target_type                     ascii,
    target                          blob,
    date                            microtimestamp_with_timezone,
    name                            blob,
    message                         blob,
    author                          person,
    synthetic                       boolean,
    raw_manifest                    blob,
    PRIMARY KEY ((id))
) WITH
    comment = 'Published versions of a software project'
    {table_options};""",
    "directory": """
CREATE TABLE IF NOT EXISTS directory (
    id              blob,
    raw_manifest                    blob,
    PRIMARY KEY ((id))
) WITH
    comment = 'Set of named references to other directories, files, and revisions'
    {table_options};""",
    "directory_entry": """
CREATE TABLE IF NOT EXISTS directory_entry (
    directory_id    blob,
    name            blob,
    target          blob,
    perms           int,
    type            ascii,
    PRIMARY KEY ((directory_id), name)
) WITH
    comment = 'Named reference from a directory to another entry (subdirectory, file, or revision)'
    {table_options};""",  # noqa: B950
    "snapshot": """
CREATE TABLE IF NOT EXISTS snapshot (
    id              blob,
    PRIMARY KEY ((id))
) WITH
    comment = 'State of an origin at a given time'
    {table_options};""",
    "snapshot_branch": """
CREATE TABLE IF NOT EXISTS snapshot_branch (
    snapshot_id     blob,
    name            blob,
    target_type     ascii,
    target          blob,
    PRIMARY KEY ((snapshot_id), name)
) WITH
    comment = 'Named referenced from a snapshot to an other object'
    {table_options};""",
    "origin_visit": """
CREATE TABLE IF NOT EXISTS origin_visit (
    origin          text,
    visit           bigint,
    date            timestamp,
    type            text,
    PRIMARY KEY ((origin), visit)
) WITH
    comment = 'Times where SWH looked at an origin for new content'
    {table_options};""",
    "origin_visit_status": """
CREATE TABLE IF NOT EXISTS origin_visit_status (
    origin          text,
    visit           bigint,
    date            timestamp,
    type            text,
    status          ascii,
    metadata        text,
    snapshot        blob,
    PRIMARY KEY ((origin), visit, date)
) WITH
    CLUSTERING ORDER BY (visit DESC, date DESC)
    AND comment = 'Updates to an origin visit'
    {table_options};""",  # 'WITH CLUSTERING ORDER BY' is optional with Cassandra 4,
    # but ScyllaDB needs it
    "origin": """
CREATE TABLE IF NOT EXISTS origin (
    sha1            blob,
    url             text,
    next_visit_id   int,
    PRIMARY KEY ((sha1))
) WITH
    comment = 'Software repositories'
    {table_options};""",
    "metadata_authority": """
CREATE TABLE IF NOT EXISTS metadata_authority (
    url             text,
    type            ascii,
    PRIMARY KEY ((url), type)
) WITH
    comment = 'External source of objects present in the "raw_extrinsic_metadata" table'
    {table_options};""",
    "metadata_fetcher": """
CREATE TABLE IF NOT EXISTS metadata_fetcher (
    name            ascii,
    version         ascii,
    PRIMARY KEY ((name), version)
) WITH
    comment = 'Software running on SWH that fetches objects present in the "raw_extrinsic_metadata" table'
    {table_options};""",  # noqa: B950
    # Cassandra handles null values for row properties as removals (tombstones), which
    # are never cleaned up as the values were never set. To avoid this issue, we instead
    # store a known invalid value of the proper type as a placeholder for null
    # properties: for strings and bytes: the empty value; for visit ids (integers): 0.
    # See comments for visit, snapshot, release, revision, path and directory fields
    "raw_extrinsic_metadata": """
CREATE TABLE IF NOT EXISTS raw_extrinsic_metadata (
    id              blob,
    type            text,
    target          text,
    authority_type  text,
    authority_url   text,
    discovery_date  timestamp,
    fetcher_name    ascii,
    fetcher_version ascii,
    format          ascii,
    metadata        blob,
    origin          text,
    visit           bigint,
    snapshot        text,
    release         text,
    revision        text,
    path            blob,
    directory       text,
    PRIMARY KEY ((target), authority_type, authority_url, discovery_date, id)
) WITH
    comment = 'Blobs of metadata obtained from external sources, that do not fit in the main data model'
    {table_options};""",  # noqa: B950
    "raw_extrinsic_metadata_by_id": """
CREATE TABLE IF NOT EXISTS raw_extrinsic_metadata_by_id (
    id              blob,
    target          text,
    authority_type  text,
    authority_url   text,
    PRIMARY KEY ((id))
) WITH
    comment = 'Secondary table to access "raw_extrinsic_metadata_by_id" from the swh:1:emd: pseudo-SWHID'
    {table_options};""",  # noqa: B950
    "extid": """
CREATE TABLE IF NOT EXISTS extid (
    extid_type      ascii,
    extid           blob,
    extid_version   smallint,
    target_type     ascii,
    target          blob,
    PRIMARY KEY ((extid_type, extid), extid_version, target_type, target)
) WITH
    comment = 'External identifiers for objects with SWHIDs'
    {table_options};""",
    "extid_by_target": """
CREATE TABLE IF NOT EXISTS extid_by_target (
    target_type     ascii,
    target          blob,
    target_token    bigint,
    PRIMARY KEY ((target_type, target), target_token)
) WITH
    comment = ''
    {table_options};""",
    "object_references": """
CREATE TABLE IF NOT EXISTS object_references (
    target_type     ascii,
    target          blob,
    source_type     ascii,
    source          blob,
    PRIMARY KEY ((target_type, target), source_type, source)
) WITH
    comment = 'Set of reverse-references, ie. set of sources pointing to a target. Not written to anymore, but still supported for reads'
    {table_options};""",  # noqa: B950
    "object_references_table": """
CREATE TABLE IF NOT EXISTS object_references_table (
    pk              int, -- always zero, puts everything in the same Cassandra partition
    name            ascii,
    year            int, -- ISO year
    week            int, -- ISO week
    start           date,
    end             date,
    PRIMARY KEY ((pk), name)
) WITH
    comment = 'Stores the list of object_references_* tables; which are dynamically created and dropped'
    {table_options};""",  # noqa: B950
}

CONTENT_INDEX_TEMPLATE = """
CREATE TABLE IF NOT EXISTS content_by_{main_algo} (
    {main_algo}   blob,
    target_token  bigint, -- value of token(pk) on the "primary" table
    PRIMARY KEY (({main_algo}), target_token)
) WITH
    comment = 'Secondary table, used for looking up "content" from a single hash'
    {{table_options}};"""

SKIPPED_CONTENT_INDEX_TEMPLATE = """
CREATE TABLE IF NOT EXISTS skipped_content_by_{main_algo} (
    {main_algo}   blob,
    target_token  bigint, -- value of token(pk) on the "primary" table
    PRIMARY KEY (({main_algo}), target_token)
) WITH
    comment = 'Secondary table, used for looking up "content" from a single hash'
    {{table_options}};"""

OBJECT_REFERENCES_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {keyspace}.{name} (
    target_type     ascii,
    target          blob,
    source_type     ascii,
    source          blob,
    PRIMARY KEY ((target_type, target), source_type, source)
) WITH
    comment = 'Time-based shard of the set of reverse-references (ie. set of sources pointing to a target)'
    {table_options};"""  # noqa: B950

TABLES = [
    "migration",
    "skipped_content",
    "content",
    "revision",
    "revision_parent",
    "release",
    "directory",
    "directory_entry",
    "snapshot",
    "snapshot_branch",
    "origin_visit",
    "origin",
    "raw_extrinsic_metadata",
    "raw_extrinsic_metadata_by_id",
    "origin_visit_status",
    "metadata_authority",
    "metadata_fetcher",
    "extid",
    "extid_by_target",
    "object_references",
    "object_references_table",
]


HASH_ALGORITHMS = ["sha1", "sha1_git", "sha256", "blake2s256"]

for main_algo in HASH_ALGORITHMS:
    TABLES.append(f"content_by_{main_algo}")
    CREATE_TABLES_QUERIES[f"content_by_{main_algo}"] = CONTENT_INDEX_TEMPLATE.format(
        main_algo=main_algo,
        other_algos=", ".join([algo for algo in HASH_ALGORITHMS if algo != main_algo]),
    )

    TABLES.append(f"skipped_content_by_{main_algo}")
    CREATE_TABLES_QUERIES[f"skipped_content_by_{main_algo}"] = (
        SKIPPED_CONTENT_INDEX_TEMPLATE.format(
            main_algo=main_algo,
            other_algos=", ".join(
                [algo for algo in HASH_ALGORITHMS if algo != main_algo]
            ),
        )
    )
