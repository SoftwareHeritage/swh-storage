# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import pytest

from swh.model.model import (
    ObjectType,
    Person,
    Release,
    Revision,
    RevisionType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import ExtendedSWHID
from swh.storage.postgresql import converters


@pytest.mark.parametrize(
    "model_date,db_date",
    [
        (
            None,
            {
                "timestamp": None,
                "offset": 0,
                "neg_utc_offset": None,
                "offset_bytes": None,
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1234567890,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            {
                "timestamp": "2009-02-13T23:31:30+00:00",
                "offset": 120,
                "neg_utc_offset": False,
                "offset_bytes": b"+0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1123456789,
                    microseconds=0,
                ),
                offset_bytes=b"-0000",
            ),
            {
                "timestamp": "2005-08-07T23:19:49+00:00",
                "offset": 0,
                "neg_utc_offset": True,
                "offset_bytes": b"-0000",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1234567890,
                    microseconds=0,
                ),
                offset_bytes=b"+0042",
            ),
            {
                "timestamp": "2009-02-13T23:31:30+00:00",
                "offset": 42,
                "neg_utc_offset": False,
                "offset_bytes": b"+0042",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1634366813,
                    microseconds=0,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "2021-10-16T06:46:53+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=0,
                    microseconds=0,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "1970-01-01T00:00:00+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=0,
                    microseconds=1,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "1970-01-01T00:00:00.000001+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=-1,
                    microseconds=0,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "1969-12-31T23:59:59+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=-1,
                    microseconds=1,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "1969-12-31T23:59:59.000001+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=-3600,
                    microseconds=0,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "1969-12-31T23:00:00+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=-3600,
                    microseconds=1,
                ),
                offset_bytes=b"-0200",
            ),
            {
                "timestamp": "1969-12-31T23:00:00.000001+00:00",
                "offset": -120,
                "neg_utc_offset": False,
                "offset_bytes": b"-0200",
            },
        ),
        (
            TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1234567890,
                    microseconds=0,
                ),
                offset_bytes=b"+200",
            ),
            {
                "timestamp": "2009-02-13T23:31:30+00:00",
                "offset": 120,
                "neg_utc_offset": False,
                "offset_bytes": b"+200",
            },
        ),
    ],
)
def test_date(model_date, db_date):
    assert converters.date_to_db(model_date) == db_date
    assert (
        converters.db_to_date(
            date=(
                None
                if db_date["timestamp"] is None
                else datetime.datetime.fromisoformat(db_date["timestamp"])
            ),
            offset_bytes=db_date["offset_bytes"],
        )
        == model_date
    )


def test_db_to_author():
    # when
    actual_author = converters.db_to_author(b"name <email> ", b"name", b"email")

    # then
    assert actual_author == Person.from_fullname(b"name <email> ")


def test_db_to_author_none():
    # when
    actual_author = converters.db_to_author(None, None, None)

    # then
    assert actual_author is None


def test_db_to_author_unparsed():
    author = converters.db_to_author(b"Fullname <email@example.com>", None, None)
    assert author == Person.from_fullname(b"Fullname <email@example.com>")


@pytest.mark.parametrize("tested_func", ["db_to_revision", "db_to_optional_revision"])
def test_db_to_revision(tested_func):
    # when
    actual_revision = getattr(converters, tested_func)(
        {
            "id": b"revision-id",
            "date": None,
            "date_offset": None,
            "date_neg_utc_offset": None,
            "date_offset_bytes": None,
            "committer_date": None,
            "committer_date_offset": None,
            "committer_date_neg_utc_offset": None,
            "committer_date_offset_bytes": None,
            "type": "git",
            "directory": b"dir-sha1",
            "message": b"commit message",
            "author_fullname": b"auth-name <auth-email>",
            "author_name": b"auth-name",
            "author_email": b"auth-email",
            "committer_fullname": b"comm-name <comm-email>",
            "committer_name": b"comm-name",
            "committer_email": b"comm-email",
            "metadata": {},
            "synthetic": False,
            "extra_headers": (),
            "raw_manifest": None,
            "parents": [b"123", b"456"],
        }
    )

    # then
    assert actual_revision == Revision(
        id=b"revision-id",
        author=Person(
            fullname=b"auth-name <auth-email>",
            name=b"auth-name",
            email=b"auth-email",
        ),
        date=None,
        committer=Person(
            fullname=b"comm-name <comm-email>",
            name=b"comm-name",
            email=b"comm-email",
        ),
        committer_date=None,
        type=RevisionType.GIT,
        directory=b"dir-sha1",
        message=b"commit message",
        metadata={},
        synthetic=False,
        extra_headers=(),
        parents=(b"123", b"456"),
    )


@pytest.mark.parametrize("tested_func", ["db_to_revision", "db_to_optional_revision"])
def test_db_to_revision_none(tested_func):
    # when
    row = {
        "id": b"revision-id",
        "date": None,
        "date_offset": None,
        "date_neg_utc_offset": None,
        "date_offset_bytes": None,
        "committer_date": None,
        "committer_date_offset": None,
        "committer_date_neg_utc_offset": None,
        "committer_date_offset_bytes": None,
        "type": None,
        "directory": None,
        "message": None,
        "author_fullname": None,
        "author_name": None,
        "author_email": None,
        "committer_fullname": None,
        "committer_name": None,
        "committer_email": None,
        "metadata": None,
        "synthetic": None,
        "extra_headers": None,
        "raw_manifest": None,
        "parents": [],
    }

    if tested_func == "db_to_revision":
        with pytest.raises(ValueError):
            converters.db_to_revision(row)
    else:
        assert converters.db_to_optional_revision(row) is None


@pytest.mark.parametrize("tested_func", ["db_to_release", "db_to_optional_release"])
def test_db_to_release(tested_func):
    # when
    actual_release = getattr(converters, tested_func)(
        {
            "id": b"release-id",
            "target": b"revision-id",
            "target_type": "revision",
            "date": None,
            "date_offset": None,
            "date_neg_utc_offset": None,
            "date_offset_bytes": None,
            "name": b"release-name",
            "comment": b"release comment",
            "synthetic": True,
            "author_fullname": b"auth-name <auth-email>",
            "author_name": b"auth-name",
            "author_email": b"auth-email",
            "raw_manifest": None,
        }
    )

    # then
    assert actual_release == Release(
        author=Person(
            fullname=b"auth-name <auth-email>",
            name=b"auth-name",
            email=b"auth-email",
        ),
        date=None,
        id=b"release-id",
        name=b"release-name",
        message=b"release comment",
        synthetic=True,
        target=b"revision-id",
        target_type=ObjectType.REVISION,
    )


@pytest.mark.parametrize("tested_func", ["db_to_release", "db_to_optional_release"])
def test_db_to_release_none(tested_func):
    row = {
        "id": b"release-id",
        "target": None,
        "target_type": None,
        "date": None,
        "date_offset": None,
        "date_neg_utc_offset": None,
        "date_offset_bytes": None,
        "name": None,
        "comment": None,
        "synthetic": None,
        "author_fullname": None,
        "author_name": None,
        "author_email": None,
        "raw_manifest": None,
    }

    if tested_func == "db_to_release":
        with pytest.raises(ValueError):
            converters.db_to_release(row)
    else:
        assert converters.db_to_optional_release(row) is None


def test_db_to_raw_extrinsic_metadata_raw_target():
    row = {
        "raw_extrinsic_metadata.target": "https://example.com/origin",
        "metadata_authority.type": "forge",
        "metadata_authority.url": "https://example.com",
        "metadata_fetcher.name": "swh.lister",
        "metadata_fetcher.version": "1.0.0",
        "discovery_date": datetime.datetime(
            2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "format": "text/plain",
        "raw_extrinsic_metadata.metadata": b"metadata",
        "origin": None,
        "visit": None,
        "snapshot": None,
        "release": None,
        "revision": None,
        "path": None,
        "directory": None,
    }

    with pytest.deprecated_call():
        computed_rem = converters.db_to_raw_extrinsic_metadata(row)

    assert computed_rem.target == ExtendedSWHID.from_string(
        "swh:1:ori:5a7439b0b93a5d230b6a67b8e7e0f7dc3c9f6c70"
    )
