# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

_use_scylla = bool(os.environ.get("SWH_USE_SCYLLADB", ""))

UDF_LANGUAGE = "lua" if _use_scylla else "java"

if UDF_LANGUAGE == "java":
    # For Cassandra
    CREATE_TABLES_QUERIES = [
        """
        CREATE OR REPLACE FUNCTION ascii_bins_count_sfunc (
            state tuple<int, map<ascii, int>>, -- (nb_none, map<target_type, nb>)
            bin_name ascii
        )
        CALLED ON NULL INPUT
        RETURNS tuple<int, map<ascii, int>>
        LANGUAGE java AS
        $$
            if (bin_name == null) {
                state.setInt(0, state.getInt(0) + 1);
            }
            else {
                Map<String, Integer> counters = state.getMap(
                    1, String.class, Integer.class);
                Integer nb = counters.get(bin_name);
                if (nb == null) {
                    nb = 0;
                }
                counters.put(bin_name, nb + 1);
                state.setMap(1, counters, String.class, Integer.class);
            }
            return state;
        $$;""",
        """
        CREATE OR REPLACE AGGREGATE ascii_bins_count ( ascii )
        SFUNC ascii_bins_count_sfunc
        STYPE tuple<int, map<ascii, int>>
        INITCOND (0, {})
        ;""",
    ]
elif UDF_LANGUAGE == "lua":
    # For ScyllaDB
    # TODO: this is not implementable yet, because ScyllaDB does not support
    # user-defined aggregates. https://github.com/scylladb/scylla/issues/7201
    CREATE_TABLES_QUERIES = []
else:
    assert False, f"{UDF_LANGUAGE} must be 'lua' or 'java'"

CREATE_TABLES_QUERIES = [
    *CREATE_TABLES_QUERIES,
    """
CREATE TYPE IF NOT EXISTS microtimestamp (
    seconds             bigint,
    microseconds        int,
);""",
    """
CREATE TYPE IF NOT EXISTS microtimestamp_with_timezone (
    timestamp           frozen<microtimestamp>,
    offset_bytes        blob,
);""",
    """
CREATE TYPE IF NOT EXISTS person (
    fullname    blob,
    name        blob,
    email       blob
);""",
    """
CREATE TABLE IF NOT EXISTS content (
    sha1          blob,
    sha1_git      blob,
    sha256        blob,
    blake2s256    blob,
    length        bigint,
    ctime         timestamp,
    status        ascii,
    PRIMARY KEY ((sha256), sha1, sha1_git, blake2s256)
);""",
    """
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
);""",
    """
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
);""",
    """
CREATE TABLE IF NOT EXISTS revision_parent (
    id                     blob,
    parent_rank                     int,
    parent_id                       blob,
    PRIMARY KEY ((id), parent_rank)
);""",
    """
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
);""",
    """
CREATE TABLE IF NOT EXISTS directory (
    id              blob,
    raw_manifest                    blob,
    PRIMARY KEY ((id))
);""",
    """
CREATE TABLE IF NOT EXISTS directory_entry (
    directory_id    blob,
    name            blob,
    target          blob,
    perms           int,
    type            ascii,
    PRIMARY KEY ((directory_id), name)
);""",
    """
CREATE TABLE IF NOT EXISTS snapshot (
    id              blob,
    PRIMARY KEY ((id))
);""",
    """
CREATE TABLE IF NOT EXISTS snapshot_branch (
    snapshot_id     blob,
    name            blob,
    target_type     ascii,
    target          blob,
    PRIMARY KEY ((snapshot_id), name)
);""",
    """
CREATE TABLE IF NOT EXISTS origin_visit (
    origin          text,
    visit           bigint,
    date            timestamp,
    type            text,
    PRIMARY KEY ((origin), visit)
);""",
    """
CREATE TABLE IF NOT EXISTS origin_visit_status (
    origin          text,
    visit           bigint,
    date            timestamp,
    type            text,
    status          ascii,
    metadata        text,
    snapshot        blob,
    PRIMARY KEY ((origin), visit, date)
)
WITH CLUSTERING ORDER BY (visit DESC, date DESC)
;""",  # 'WITH CLUSTERING ORDER BY' is optional with Cassandra 4, but ScyllaDB needs it
    """
CREATE TABLE IF NOT EXISTS origin (
    sha1            blob,
    url             text,
    next_visit_id   int,
    PRIMARY KEY ((sha1))
);""",
    """
CREATE TABLE IF NOT EXISTS metadata_authority (
    url             text,
    type            ascii,
    PRIMARY KEY ((url), type)
);""",
    """
CREATE TABLE IF NOT EXISTS metadata_fetcher (
    name            ascii,
    version         ascii,
    PRIMARY KEY ((name), version)
);""",
    # Cassandra handles null values for row properties as removals (tombstones), which
    # are never cleaned up as the values were never set. To avoid this issue, we instead
    # store a known invalid value of the proper type as a placeholder for null
    # properties: for strings and bytes: the empty value; for visit ids (integers): 0.
    # See comments for visit, snapshot, release, revision, path and directory fields
    """
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
);""",
    """
CREATE TABLE IF NOT EXISTS raw_extrinsic_metadata_by_id (
    id              blob,
    target          text,
    authority_type  text,
    authority_url   text,
    PRIMARY KEY ((id))
);""",
    """
CREATE TABLE IF NOT EXISTS extid (
    extid_type      ascii,
    extid           blob,
    extid_version   smallint,
    target_type     ascii,
    target          blob,
    PRIMARY KEY ((extid_type, extid), extid_version, target_type, target)
);""",
    """
CREATE TABLE IF NOT EXISTS extid_by_target (
    target_type     ascii,
    target          blob,
    target_token    bigint,
    PRIMARY KEY ((target_type, target), target_token)
);""",
    """
CREATE TABLE IF NOT EXISTS object_references (
    target_type     ascii,
    target          blob,
    source_type     ascii,
    source          blob,
    PRIMARY KEY ((target_type, target), source_type, source)
);""",
]

CONTENT_INDEX_TEMPLATE = """
-- Secondary table, used for looking up "content" from a single hash
CREATE TABLE IF NOT EXISTS content_by_{main_algo} (
    {main_algo}   blob,
    target_token  bigint, -- value of token(pk) on the "primary" table
    PRIMARY KEY (({main_algo}), target_token)
);

-- Secondary table, used for looking up "skipped_content" from a single hash
CREATE TABLE IF NOT EXISTS skipped_content_by_{main_algo} (
    {main_algo}   blob,
    target_token  bigint, -- value of token(pk) on the "primary" table
    PRIMARY KEY (({main_algo}), target_token)
);
"""

TABLES = [
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
    "origin_visit_status",
    "metadata_authority",
    "metadata_fetcher",
    "extid",
    "extid_by_target",
    "object_references",
]

HASH_ALGORITHMS = ["sha1", "sha1_git", "sha256", "blake2s256"]

for main_algo in HASH_ALGORITHMS:
    CREATE_TABLES_QUERIES.extend(
        CONTENT_INDEX_TEMPLATE.format(
            main_algo=main_algo,
            other_algos=", ".join(
                [algo for algo in HASH_ALGORITHMS if algo != main_algo]
            ),
        ).split("\n\n")
    )

    TABLES.append("content_by_%s" % main_algo)
    TABLES.append("skipped_content_by_%s" % main_algo)
