# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import pytest

from swh.model.identifiers import ExtendedSWHID
from swh.model.model import (
    ObjectType,
    Person,
    Release,
    Revision,
    RevisionType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.storage.postgresql import converters


def test_date_to_db():
    date_to_db = converters.date_to_db
    assert date_to_db(None) == {"timestamp": None, "offset": 0, "neg_utc_offset": None}

    assert date_to_db(
        TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0,),
            offset=120,
            negative_utc=False,
        )
    ) == {
        "timestamp": "2009-02-13T23:31:30+00:00",
        "offset": 120,
        "neg_utc_offset": False,
    }

    assert date_to_db(
        TimestampWithTimezone(
            timestamp=Timestamp(seconds=1123456789, microseconds=0,),
            offset=0,
            negative_utc=True,
        )
    ) == {
        "timestamp": "2005-08-07T23:19:49+00:00",
        "offset": 0,
        "neg_utc_offset": True,
    }

    assert date_to_db(
        TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0,),
            offset=42,
            negative_utc=False,
        )
    ) == {
        "timestamp": "2009-02-13T23:31:30+00:00",
        "offset": 42,
        "neg_utc_offset": False,
    }

    assert date_to_db(
        TimestampWithTimezone(
            timestamp=Timestamp(seconds=1634366813, microseconds=0,),
            offset=-120,
            negative_utc=False,
        )
    ) == {
        "timestamp": "2021-10-16T06:46:53+00:00",
        "offset": -120,
        "neg_utc_offset": False,
    }


def test_db_to_author():
    # when
    actual_author = converters.db_to_author(b"fullname", b"name", b"email")

    # then
    assert actual_author == Person(fullname=b"fullname", name=b"name", email=b"email",)


def test_db_to_author_none():
    # when
    actual_author = converters.db_to_author(None, None, None)

    # then
    assert actual_author is None


def test_db_to_revision():
    # when
    actual_revision = converters.db_to_revision(
        {
            "id": b"revision-id",
            "date": None,
            "date_offset": None,
            "date_neg_utc_offset": None,
            "committer_date": None,
            "committer_date_offset": None,
            "committer_date_neg_utc_offset": None,
            "type": "git",
            "directory": b"dir-sha1",
            "message": b"commit message",
            "author_fullname": b"auth-fullname",
            "author_name": b"auth-name",
            "author_email": b"auth-email",
            "committer_fullname": b"comm-fullname",
            "committer_name": b"comm-name",
            "committer_email": b"comm-email",
            "metadata": {},
            "synthetic": False,
            "extra_headers": (),
            "parents": [b"123", b"456"],
        }
    )

    # then
    assert actual_revision == Revision(
        id=b"revision-id",
        author=Person(
            fullname=b"auth-fullname", name=b"auth-name", email=b"auth-email",
        ),
        date=None,
        committer=Person(
            fullname=b"comm-fullname", name=b"comm-name", email=b"comm-email",
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


def test_db_to_release():
    # when
    actual_release = converters.db_to_release(
        {
            "id": b"release-id",
            "target": b"revision-id",
            "target_type": "revision",
            "date": None,
            "date_offset": None,
            "date_neg_utc_offset": None,
            "name": b"release-name",
            "comment": b"release comment",
            "synthetic": True,
            "author_fullname": b"auth-fullname",
            "author_name": b"auth-name",
            "author_email": b"auth-email",
        }
    )

    # then
    assert actual_release == Release(
        author=Person(
            fullname=b"auth-fullname", name=b"auth-name", email=b"auth-email",
        ),
        date=None,
        id=b"release-id",
        name=b"release-name",
        message=b"release comment",
        synthetic=True,
        target=b"revision-id",
        target_type=ObjectType.REVISION,
    )


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
