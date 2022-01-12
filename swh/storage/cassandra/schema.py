# Copyright (C) 2019-2021  The Software Heritage developers
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
        -- creation time, i.e. time of (first) injection into the storage
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
        -- creation time, i.e. time of (first) injection into the storage
    status        ascii,
    reason        text,
    origin        text,
    PRIMARY KEY ((sha1, sha1_git, sha256, blake2s256))
);""",
    """
CREATE TABLE IF NOT EXISTS revision (
    id                              blob PRIMARY KEY,
    date                            microtimestamp_with_timezone,
    committer_date                  microtimestamp_with_timezone,
    type                            ascii,
    directory                       blob,  -- source code "root" directory
    message                         blob,
    author                          person,
    committer                       person,
    synthetic                       boolean,
        -- true iff revision has been created by Software Heritage
    metadata                        text,
        -- extra metadata as JSON(tarball checksums, etc...)
    extra_headers                   frozen<list <list<blob>> >,
        -- extra commit information as (tuple(key, value), ...)
    raw_manifest                    blob,
        -- NULL if the object can be rebuild from other cells and revision_parent.
);""",
    """
CREATE TABLE IF NOT EXISTS revision_parent (
    id                     blob,
    parent_rank                     int,
        -- parent position in merge commits, 0-based
    parent_id                       blob,
    PRIMARY KEY ((id), parent_rank)
);""",
    """
CREATE TABLE IF NOT EXISTS release
(
    id                              blob PRIMARY KEY,
    target_type                     ascii,
    target                          blob,
    date                            microtimestamp_with_timezone,
    name                            blob,
    message                         blob,
    author                          person,
    synthetic                       boolean,
        -- true iff release has been created by Software Heritage
    raw_manifest                    blob,
        -- NULL if the object can be rebuild from other cells
);""",
    """
CREATE TABLE IF NOT EXISTS directory (
    id              blob PRIMARY KEY,
    raw_manifest                    blob
        -- NULL if the object can be rebuild from (sorted) entries
);""",
    """
CREATE TABLE IF NOT EXISTS directory_entry (
    directory_id    blob,
    name            blob,  -- path name, relative to containing dir
    target          blob,
    perms           int,   -- unix-like permissions
    type            ascii, -- target type
    PRIMARY KEY ((directory_id), name)
);""",
    """
CREATE TABLE IF NOT EXISTS snapshot (
    id              blob PRIMARY KEY,
);""",
    """
-- For a given snapshot_id, branches are sorted by their name,
-- allowing easy pagination.
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
    sha1            blob PRIMARY KEY,
    url             text,
    next_visit_id   int,
        -- We need integer visit ids for compatibility with the pgsql
        -- storage, so we're using lightweight transactions with this trick:
        -- https://stackoverflow.com/a/29391877/539465
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
    """
CREATE TABLE IF NOT EXISTS raw_extrinsic_metadata (
    id              blob,

    type            text,
    target          text,

    -- metadata source
    authority_type  text,
    authority_url   text,
    discovery_date  timestamp,
    fetcher_name    ascii,
    fetcher_version ascii,

    -- metadata itself
    format          ascii,
    metadata        blob,

    -- context
    origin          text,
    visit           bigint,
    snapshot        text,
    release         text,
    revision        text,
    path            blob,
    directory       text,

    PRIMARY KEY ((target), authority_type, authority_url, discovery_date, id)

    -- An explanation is in order for this primary key:
    --
    -- Intuitively, the primary key should only be 'id', because two metadata
    -- entries are the same iff the id is the same; and 'id' is used for
    -- deduplication.
    --
    -- However, we also want to query by
    -- (target, authority_type, authority_url, discovery_date)
    -- The naive solution to this would be an extra table, to use as index;
    -- but it means 1. extra code to keep them in sync 2. overhead when writing
    -- 3. overhead + random reads (instead of linear) when reading.
    --
    -- Therefore, we use a single table for both, by adding the column
    -- we want to query with before the id.
    -- It solves both a) the query/order issues and b) the uniqueness issue because:
    --
    -- a) adding the id at the end of the primary key does not change the rows' order:
    --    for two different rows, id1 != id2, so
    --    (target1, ..., date1) < (target2, ..., date2)
    --    <=> (target1, ..., date1, id1) < (target2, ..., date2, id2)
    --
    -- b) the id is a hash of all the columns, so:
    --    rows are the same
    --    <=> id1 == id2
    --    <=> (target1, ..., date1, id1) == (target2, ..., date2, id2)
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
    target_token    bigint, -- value of token(pk) on the "primary" table
    PRIMARY KEY ((target_type, target), target_token)
);""",
]

CONTENT_INDEX_TEMPLATE = """
-- Secondary table, used for looking up "content" from a single hash
CREATE TABLE IF NOT EXISTS content_by_{main_algo} (
    {main_algo}   blob,
    target_token  bigint, -- value of token(pk) on the "primary" table
    PRIMARY KEY (({main_algo}), target_token)
);

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
