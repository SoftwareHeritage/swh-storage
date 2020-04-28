# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.storage.backfill import JournalBackfiller, compute_query, PARTITION_KEY


TEST_CONFIG = {
    "brokers": ["localhost"],
    "prefix": "swh.tmp_journal.new",
    "client_id": "swh.journal.client.test",
    "storage_dbconn": "service=swh-dev",
}


def test_config_ko_missing_mandatory_key():
    """Missing configuration key will make the initialization fail

    """
    for key in TEST_CONFIG.keys():
        config = TEST_CONFIG.copy()
        config.pop(key)

        with pytest.raises(ValueError) as e:
            JournalBackfiller(config)

        error = "Configuration error: The following keys must be" " provided: %s" % (
            ",".join([key]),
        )
        assert e.value.args[0] == error


def test_config_ko_unknown_object_type():
    """Parse arguments will fail if the object type is unknown

    """
    backfiller = JournalBackfiller(TEST_CONFIG)
    with pytest.raises(ValueError) as e:
        backfiller.parse_arguments("unknown-object-type", 1, 2)

    error = (
        "Object type unknown-object-type is not supported. "
        "The only possible values are %s" % (", ".join(PARTITION_KEY))
    )
    assert e.value.args[0] == error


def test_compute_query_content():
    query, where_args, column_aliases = compute_query("content", "\x000000", "\x000001")

    assert where_args == ["\x000000", "\x000001"]

    assert column_aliases == [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "status",
        "ctime",
    ]

    assert (
        query
        == """
select sha1,sha1_git,sha256,blake2s256,length,status,ctime
from content

where (sha1) >= %s and (sha1) < %s
    """
    )


def test_compute_query_skipped_content():
    query, where_args, column_aliases = compute_query("skipped_content", None, None)

    assert where_args == []

    assert column_aliases == [
        "sha1",
        "sha1_git",
        "sha256",
        "blake2s256",
        "length",
        "ctime",
        "status",
        "reason",
    ]

    assert (
        query
        == """
select sha1,sha1_git,sha256,blake2s256,length,ctime,status,reason
from skipped_content


    """
    )


def test_compute_query_origin_visit():
    query, where_args, column_aliases = compute_query("origin_visit", 1, 10)

    assert where_args == [1, 10]

    assert column_aliases == [
        "visit",
        "origin.type",
        "origin_visit.type",
        "url",
        "date",
        "snapshot",
        "status",
        "metadata",
    ]

    assert (
        query
        == """
select visit,origin.type,origin_visit.type,url,date,snapshot,status,metadata
from origin_visit
left join origin on origin_visit.origin=origin.id
where (origin_visit.origin) >= %s and (origin_visit.origin) < %s
    """
    )


def test_compute_query_release():
    query, where_args, column_aliases = compute_query("release", "\x000002", "\x000003")

    assert where_args == ["\x000002", "\x000003"]

    assert column_aliases == [
        "id",
        "date",
        "date_offset",
        "comment",
        "name",
        "synthetic",
        "date_neg_utc_offset",
        "target",
        "target_type",
        "author_id",
        "author_name",
        "author_email",
        "author_fullname",
    ]

    assert (
        query
        == """
select release.id as id,date,date_offset,comment,release.name as name,synthetic,date_neg_utc_offset,target,target_type,a.id as author_id,a.name as author_name,a.email as author_email,a.fullname as author_fullname
from release
left join person a on release.author=a.id
where (release.id) >= %s and (release.id) < %s
    """  # noqa
    )
