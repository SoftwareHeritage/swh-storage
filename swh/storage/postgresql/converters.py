# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import math
from typing import Any, Dict, Optional, Tuple
import warnings

from swh.core.utils import encode_with_unescape
from swh.model.model import (
    ExtID,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    ObjectType,
    Origin,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    RevisionType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID
from swh.model.swhids import ExtendedObjectType as SwhidExtendedObjectType
from swh.model.swhids import ExtendedSWHID
from swh.model.swhids import ObjectType as SwhidObjectType
from swh.storage.interface import ObjectReference

from ..utils import map_optional

DEFAULT_AUTHOR = {
    "fullname": None,
    "name": None,
    "email": None,
}

DEFAULT_DATE = {
    "timestamp": None,
    "offset": 0,
    "neg_utc_offset": None,
    "offset_bytes": None,
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

    if name is None and email is None:
        # The fullname hasn't been parsed, try that again
        return Person.from_fullname(fullname)

    return Person(
        fullname=fullname,
        name=name,
        email=email,
    )


def db_to_git_headers(db_git_headers):
    ret = []
    for key, value in db_git_headers:
        ret.append([key.encode("utf-8"), encode_with_unescape(value)])
    return ret


def db_to_date(
    date: Optional[datetime.datetime],
    offset_bytes: bytes,
) -> Optional[TimestampWithTimezone]:
    """Convert the DB representation of a date to a swh-model compatible date.

    Args:
        date: a date pulled out of the database
        offset_bytes: a byte representation of the latter two, usually as "+HHMM"
          or "-HHMM"

    Returns:
        a TimestampWithTimezone, or None if the date is None.

    """

    if date is None:
        return None

    return TimestampWithTimezone(
        timestamp=Timestamp(
            # we use floor() instead of int() to round down, because of negative dates
            seconds=math.floor(date.timestamp()),
            microseconds=date.microsecond,
        ),
        offset_bytes=offset_bytes,
    )


def date_to_db(ts_with_tz: Optional[TimestampWithTimezone]) -> Dict[str, Any]:
    """Convert a swh-model date_offset to its DB representation.

    Args:
        ts_with_tz: a TimestampWithTimezone object

    Returns:
        dict: a dictionary with these keys:

            - timestamp: a date in ISO format
            - offset_bytes: a byte representation of the latter two, usually as "+HHMM"
              or "-HHMM"

    """

    if ts_with_tz is None:
        return DEFAULT_DATE

    ts = ts_with_tz.timestamp

    timestamp = datetime.datetime.fromtimestamp(ts.seconds, datetime.timezone.utc)
    timestamp = timestamp.replace(microsecond=ts.microseconds)

    return {
        # PostgreSQL supports isoformatted timestamps
        "timestamp": timestamp.isoformat(),
        "offset": ts_with_tz.offset_minutes(),
        "neg_utc_offset": ts_with_tz.offset_minutes() == 0
        and ts_with_tz.offset_bytes.startswith(b"-"),
        "offset_bytes": ts_with_tz.offset_bytes,
    }


def revision_to_db(revision: Revision) -> Dict[str, Any]:
    """Convert a swh-model revision to its database representation."""

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
        "date_offset_bytes": date["offset_bytes"],
        "committer_fullname": committer["fullname"],
        "committer_name": committer["name"],
        "committer_email": committer["email"],
        "committer_date": committer_date["timestamp"],
        "committer_date_offset": committer_date["offset"],
        "committer_date_neg_utc_offset": committer_date["neg_utc_offset"],
        "committer_date_offset_bytes": committer_date["offset_bytes"],
        "type": revision.type.value,
        "directory": revision.directory,
        "message": revision.message,
        "metadata": None if revision.metadata is None else dict(revision.metadata),
        "synthetic": revision.synthetic,
        "extra_headers": revision.extra_headers,
        "raw_manifest": revision.raw_manifest,
        "parents": [
            {
                "id": revision.id,
                "parent_id": parent,
                "parent_rank": i,
            }
            for i, parent in enumerate(revision.parents)
        ],
    }


def db_to_optional_revision(db_revision: Dict[str, Any]) -> Optional[Revision]:
    """Convert a database representation of a revision to its swh-model
    representation.
    This is similar to :func:`db_to_revision`, but this returns :const:`None`
    instead of crashing if the revision does not exist (returned by an outer join).
    """
    if db_revision["type"] is None:
        assert all(
            v is None for (k, v) in db_revision.items() if k not in ("id", "parents")
        )
        return None

    return db_to_revision(db_revision)


def db_to_revision(db_revision: Dict[str, Any]) -> Revision:
    """Convert a database representation of a revision to its swh-model
    representation. Raises ValueError if required values are None."""
    author = db_to_author(
        db_revision["author_fullname"],
        db_revision["author_name"],
        db_revision["author_email"],
    )
    date = db_to_date(
        db_revision["date"],
        db_revision["date_offset_bytes"],
    )

    committer = db_to_author(
        db_revision["committer_fullname"],
        db_revision["committer_name"],
        db_revision["committer_email"],
    )
    committer_date = db_to_date(
        db_revision["committer_date"],
        db_revision["committer_date_offset_bytes"],
    )

    assert (author is None) == (
        db_revision["author_fullname"] is None
    ), "author is unexpectedly None"
    assert (committer is None) == (
        db_revision["committer_fullname"] is None
    ), "committer is unexpectedly None"

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
        raw_manifest=db_revision["raw_manifest"],
    )


def release_to_db(release: Release) -> Dict[str, Any]:
    """Convert a swh-model release to its database representation."""
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
        "date_offset_bytes": date["offset_bytes"],
        "name": release.name,
        "target": release.target,
        "target_type": release.target_type.value,
        "comment": release.message,
        "synthetic": release.synthetic,
        "raw_manifest": release.raw_manifest,
    }


def db_to_optional_release(db_release: Dict[str, Any]) -> Optional[Release]:
    """Convert a database representation of a release to its swh-model
    representation.
    This is similar to :func:`db_to_release`, but this returns :const:`None`
    instead of crashing if the revision does not exist (returned by an outer join).
    """
    if db_release["target_type"] is None:
        assert all(v is None for (k, v) in db_release.items() if k != "id")
        return None

    return db_to_release(db_release)


def db_to_release(db_release: Dict[str, Any]) -> Release:
    """Convert a database representation of a release to its swh-model
    representation. Raises ValueError if required values are None.
    """
    author = db_to_author(
        db_release["author_fullname"],
        db_release["author_name"],
        db_release["author_email"],
    )
    date = db_to_date(
        db_release["date"],
        db_release["date_offset_bytes"],
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
        raw_manifest=db_release["raw_manifest"],
    )


def db_to_raw_extrinsic_metadata(row) -> RawExtrinsicMetadata:
    target = row["raw_extrinsic_metadata.target"]
    if not target.startswith("swh:1:"):
        warnings.warn(
            "Fetching raw_extrinsic_metadata row with URL target", DeprecationWarning
        )
        target = str(Origin(url=target).swhid())

    return RawExtrinsicMetadata(
        target=ExtendedSWHID.from_string(target),
        authority=MetadataAuthority(
            type=MetadataAuthorityType(row["metadata_authority.type"]),
            url=row["metadata_authority.url"],
        ),
        fetcher=MetadataFetcher(
            name=row["metadata_fetcher.name"],
            version=row["metadata_fetcher.version"],
        ),
        discovery_date=row["discovery_date"],
        format=row["format"],
        metadata=row["raw_extrinsic_metadata.metadata"],
        origin=row["origin"],
        visit=row["visit"],
        snapshot=map_optional(CoreSWHID.from_string, row["snapshot"]),
        release=map_optional(CoreSWHID.from_string, row["release"]),
        revision=map_optional(CoreSWHID.from_string, row["revision"]),
        path=row["path"],
        directory=map_optional(CoreSWHID.from_string, row["directory"]),
    )


def db_to_extid(row) -> ExtID:
    return ExtID(
        extid=row["extid"],
        extid_type=row["extid_type"],
        extid_version=row.get("extid_version", 0),
        target=CoreSWHID(
            object_id=row["target"],
            object_type=SwhidObjectType[row["target_type"].upper()],
        ),
    )


def object_reference_to_db(
    reference: ObjectReference,
) -> Tuple[str, bytes, str, bytes]:
    return (
        reference.source.object_type.name.lower(),
        reference.source.object_id,
        reference.target.object_type.name.lower(),
        reference.target.object_id,
    )


def db_to_object_reference_source(row) -> ExtendedSWHID:
    return ExtendedSWHID(
        object_id=row["source"],
        object_type=SwhidExtendedObjectType[row["source_type"].upper()],
    )
