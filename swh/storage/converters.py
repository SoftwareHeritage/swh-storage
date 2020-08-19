# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from typing import Any, Optional, Dict

from swh.core.utils import encode_with_unescape
from swh.model.identifiers import parse_swhid
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    ObjectType,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    RevisionType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.hashutil import MultiHash

from .utils import map_optional


DEFAULT_AUTHOR = {
    "fullname": None,
    "name": None,
    "email": None,
}

DEFAULT_DATE = {
    "timestamp": None,
    "offset": 0,
    "neg_utc_offset": None,
}


def author_to_db(author: Optional[Person]) -> Dict[str, Any]:
    """Convert a swh-model author to its DB representation.

    Args:
        author: a :mod:`swh.model` compatible author

    Returns:
        dict: a dictionary with three keys: author, fullname and email

    """
    if author is None:
        return DEFAULT_AUTHOR

    return author.to_dict()


def db_to_author(
    fullname: Optional[bytes], name: Optional[bytes], email: Optional[bytes]
) -> Optional[Person]:
    """Convert the DB representation of an author to a swh-model author.

    Args:
        fullname (bytes): the author's fullname
        name (bytes): the author's name
        email (bytes): the author's email

    Returns:
        a Person object, or None if 'fullname' is None.
    """
    if fullname is None:
        return None
    return Person(fullname=fullname, name=name, email=email,)


def db_to_git_headers(db_git_headers):
    ret = []
    for key, value in db_git_headers:
        ret.append([key.encode("utf-8"), encode_with_unescape(value)])
    return ret


def db_to_date(
    date: Optional[datetime.datetime], offset: int, neg_utc_offset: Optional[bool]
) -> Optional[TimestampWithTimezone]:
    """Convert the DB representation of a date to a swh-model compatible date.

    Args:
        date: a date pulled out of the database
        offset: an integer number of minutes representing an UTC offset
        neg_utc_offset: whether an utc offset is negative

    Returns:
        a TimestampWithTimezone, or None if the date is None.

    """

    if date is None:
        return None

    if neg_utc_offset is None:
        # For older versions of the database that were not migrated to schema v160
        neg_utc_offset = False

    return TimestampWithTimezone(
        timestamp=Timestamp(
            seconds=int(date.timestamp()), microseconds=date.microsecond,
        ),
        offset=offset,
        negative_utc=neg_utc_offset,
    )


def date_to_db(ts_with_tz: Optional[TimestampWithTimezone]) -> Dict[str, Any]:
    """Convert a swh-model date_offset to its DB representation.

    Args:
        ts_with_tz: a TimestampWithTimezone object

    Returns:
        dict: a dictionary with three keys:

            - timestamp: a date in ISO format
            - offset: the UTC offset in minutes
            - neg_utc_offset: a boolean indicating whether a null offset is
              negative or positive.

    """

    if ts_with_tz is None:
        return DEFAULT_DATE

    ts = ts_with_tz.timestamp

    timestamp = datetime.datetime.fromtimestamp(ts.seconds, datetime.timezone.utc)
    timestamp = timestamp.replace(microsecond=ts.microseconds)

    return {
        # PostgreSQL supports isoformatted timestamps
        "timestamp": timestamp.isoformat(),
        "offset": ts_with_tz.offset,
        "neg_utc_offset": ts_with_tz.negative_utc,
    }


def revision_to_db(revision: Revision) -> Dict[str, Any]:
    """Convert a swh-model revision to its database representation.
    """

    author = author_to_db(revision.author)
    date = date_to_db(revision.date)
    committer = author_to_db(revision.committer)
    committer_date = date_to_db(revision.committer_date)

    return {
        "id": revision.id,
        "author_fullname": author["fullname"],
        "author_name": author["name"],
        "author_email": author["email"],
        "date": date["timestamp"],
        "date_offset": date["offset"],
        "date_neg_utc_offset": date["neg_utc_offset"],
        "committer_fullname": committer["fullname"],
        "committer_name": committer["name"],
        "committer_email": committer["email"],
        "committer_date": committer_date["timestamp"],
        "committer_date_offset": committer_date["offset"],
        "committer_date_neg_utc_offset": committer_date["neg_utc_offset"],
        "type": revision.type.value,
        "directory": revision.directory,
        "message": revision.message,
        "metadata": None if revision.metadata is None else dict(revision.metadata),
        "synthetic": revision.synthetic,
        "extra_headers": revision.extra_headers,
        "parents": [
            {"id": revision.id, "parent_id": parent, "parent_rank": i,}
            for i, parent in enumerate(revision.parents)
        ],
    }


def db_to_revision(db_revision: Dict[str, Any]) -> Optional[Revision]:
    """Convert a database representation of a revision to its swh-model
    representation."""
    if db_revision["type"] is None:
        assert all(
            v is None for (k, v) in db_revision.items() if k not in ("id", "parents")
        )
        return None

    author = db_to_author(
        db_revision["author_fullname"],
        db_revision["author_name"],
        db_revision["author_email"],
    )
    date = db_to_date(
        db_revision["date"],
        db_revision["date_offset"],
        db_revision["date_neg_utc_offset"],
    )

    committer = db_to_author(
        db_revision["committer_fullname"],
        db_revision["committer_name"],
        db_revision["committer_email"],
    )
    committer_date = db_to_date(
        db_revision["committer_date"],
        db_revision["committer_date_offset"],
        db_revision["committer_date_neg_utc_offset"],
    )

    assert author, "author is None"
    assert committer, "committer is None"

    parents = []
    if "parents" in db_revision:
        for parent in db_revision["parents"]:
            if parent:
                parents.append(parent)

    metadata = db_revision["metadata"]
    extra_headers = db_revision["extra_headers"]
    if not extra_headers:
        if metadata and "extra_headers" in metadata:
            extra_headers = db_to_git_headers(metadata.pop("extra_headers"))
        else:
            # For older versions of the database that were not migrated to schema v161
            extra_headers = ()

    return Revision(
        id=db_revision["id"],
        author=author,
        date=date,
        committer=committer,
        committer_date=committer_date,
        type=RevisionType(db_revision["type"]),
        directory=db_revision["directory"],
        message=db_revision["message"],
        metadata=metadata,
        synthetic=db_revision["synthetic"],
        extra_headers=extra_headers,
        parents=tuple(parents),
    )


def release_to_db(release: Release) -> Dict[str, Any]:
    """Convert a swh-model release to its database representation.
    """
    author = author_to_db(release.author)
    date = date_to_db(release.date)

    return {
        "id": release.id,
        "author_fullname": author["fullname"],
        "author_name": author["name"],
        "author_email": author["email"],
        "date": date["timestamp"],
        "date_offset": date["offset"],
        "date_neg_utc_offset": date["neg_utc_offset"],
        "name": release.name,
        "target": release.target,
        "target_type": release.target_type.value,
        "comment": release.message,
        "synthetic": release.synthetic,
    }


def db_to_release(db_release: Dict[str, Any]) -> Optional[Release]:
    """Convert a database representation of a release to its swh-model
    representation.
    """
    if db_release["target_type"] is None:
        assert all(v is None for (k, v) in db_release.items() if k != "id")
        return None

    author = db_to_author(
        db_release["author_fullname"],
        db_release["author_name"],
        db_release["author_email"],
    )
    date = db_to_date(
        db_release["date"], db_release["date_offset"], db_release["date_neg_utc_offset"]
    )

    return Release(
        author=author,
        date=date,
        id=db_release["id"],
        name=db_release["name"],
        message=db_release["comment"],
        synthetic=db_release["synthetic"],
        target=db_release["target"],
        target_type=ObjectType(db_release["target_type"]),
    )


def db_to_raw_extrinsic_metadata(row) -> RawExtrinsicMetadata:
    type_ = MetadataTargetType(row["raw_extrinsic_metadata.type"])
    id_ = row["raw_extrinsic_metadata.id"]
    if type_ != MetadataTargetType.ORIGIN:
        id_ = parse_swhid(id_)
    return RawExtrinsicMetadata(
        type=type_,
        id=id_,
        authority=MetadataAuthority(
            type=MetadataAuthorityType(row["metadata_authority.type"]),
            url=row["metadata_authority.url"],
        ),
        fetcher=MetadataFetcher(
            name=row["metadata_fetcher.name"], version=row["metadata_fetcher.version"],
        ),
        discovery_date=row["discovery_date"],
        format=row["format"],
        metadata=row["raw_extrinsic_metadata.metadata"],
        origin=row["origin"],
        visit=row["visit"],
        snapshot=map_optional(parse_swhid, row["snapshot"]),
        release=map_optional(parse_swhid, row["release"]),
        revision=map_optional(parse_swhid, row["revision"]),
        path=row["path"],
        directory=map_optional(parse_swhid, row["directory"]),
    )


def origin_url_to_sha1(origin_url: str) -> bytes:
    """Convert an origin URL to a sha1. Encodes URL to utf-8."""
    return MultiHash.from_data(origin_url.encode("utf-8"), {"sha1"}).digest()["sha1"]
