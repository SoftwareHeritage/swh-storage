# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from typing import Optional, Dict

from swh.core.utils import encode_with_unescape
from swh.model import identifiers
from swh.model.identifiers import parse_swhid
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    RawExtrinsicMetadata,
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


def author_to_db(author):
    """Convert a swh-model author to its DB representation.

    Args:
        author: a :mod:`swh.model` compatible author

    Returns:
        dict: a dictionary with three keys: author, fullname and email

    """
    if author is None:
        return DEFAULT_AUTHOR

    return author


def db_to_author(
    fullname: Optional[bytes], name: Optional[bytes], email: Optional[bytes]
) -> Optional[Dict[str, Optional[bytes]]]:
    """Convert the DB representation of an author to a swh-model author.

    Args:
        fullname (bytes): the author's fullname
        name (bytes): the author's name
        email (bytes): the author's email

    Returns:
        a dictionary with three keys (fullname, name and email), or
        None if all the arguments are None.
    """
    if (fullname, name, email) == (None, None, None):
        return None
    return {
        "fullname": fullname,
        "name": name,
        "email": email,
    }


def db_to_git_headers(db_git_headers):
    ret = []
    for key, value in db_git_headers:
        ret.append([key.encode("utf-8"), encode_with_unescape(value)])
    return ret


def db_to_date(date, offset, neg_utc_offset):
    """Convert the DB representation of a date to a swh-model compatible date.

    Args:
        date (datetime.datetime): a date pulled out of the database
        offset (int): an integer number of minutes representing an UTC offset
        neg_utc_offset (boolean): whether an utc offset is negative

    Returns:
        dict: a dict with three keys:

            - timestamp: a timestamp from UTC
            - offset: the number of minutes since UTC
            - negative_utc: whether a null UTC offset is negative

    """

    if date is None:
        return None

    return {
        "timestamp": {
            "seconds": int(date.timestamp()),
            "microseconds": date.microsecond,
        },
        "offset": offset,
        "negative_utc": neg_utc_offset,
    }


def date_to_db(date_offset):
    """Convert a swh-model date_offset to its DB representation.

    Args:
        date_offset: a :mod:`swh.model` compatible date_offset

    Returns:
        dict: a dictionary with three keys:

            - timestamp: a date in ISO format
            - offset: the UTC offset in minutes
            - neg_utc_offset: a boolean indicating whether a null offset is
              negative or positive.

    """

    if date_offset is None:
        return DEFAULT_DATE

    normalized = identifiers.normalize_timestamp(date_offset)

    ts = normalized["timestamp"]
    seconds = ts.get("seconds", 0)
    microseconds = ts.get("microseconds", 0)

    timestamp = datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc)
    timestamp = timestamp.replace(microsecond=microseconds)

    return {
        # PostgreSQL supports isoformatted timestamps
        "timestamp": timestamp.isoformat(),
        "offset": normalized["offset"],
        "neg_utc_offset": normalized["negative_utc"],
    }


def revision_to_db(rev):
    """Convert a swh-model revision to its database representation.
    """

    revision = rev.to_dict()
    author = author_to_db(revision["author"])
    date = date_to_db(revision["date"])
    committer = author_to_db(revision["committer"])
    committer_date = date_to_db(revision["committer_date"])

    return {
        "id": revision["id"],
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
        "type": revision["type"],
        "directory": revision["directory"],
        "message": revision["message"],
        "metadata": revision["metadata"],
        "synthetic": revision["synthetic"],
        "extra_headers": revision["extra_headers"],
        "parents": [
            {"id": revision["id"], "parent_id": parent, "parent_rank": i,}
            for i, parent in enumerate(revision["parents"])
        ],
    }


def db_to_revision(db_revision):
    """Convert a database representation of a revision to its swh-model
    representation."""

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

    parents = []
    if "parents" in db_revision:
        for parent in db_revision["parents"]:
            if parent:
                parents.append(parent)

    metadata = db_revision["metadata"]
    extra_headers = db_revision.get("extra_headers", ())
    if not extra_headers and metadata and "extra_headers" in metadata:
        extra_headers = db_to_git_headers(metadata.pop("extra_headers"))

    ret = {
        "id": db_revision["id"],
        "author": author,
        "date": date,
        "committer": committer,
        "committer_date": committer_date,
        "type": db_revision["type"],
        "directory": db_revision["directory"],
        "message": db_revision["message"],
        "metadata": metadata,
        "synthetic": db_revision["synthetic"],
        "extra_headers": extra_headers,
        "parents": parents,
    }

    if "object_id" in db_revision:
        ret["object_id"] = db_revision["object_id"]

    return ret


def release_to_db(rel):
    """Convert a swh-model release to its database representation.
    """

    release = rel.to_dict()

    author = author_to_db(release["author"])
    date = date_to_db(release["date"])

    return {
        "id": release["id"],
        "author_fullname": author["fullname"],
        "author_name": author["name"],
        "author_email": author["email"],
        "date": date["timestamp"],
        "date_offset": date["offset"],
        "date_neg_utc_offset": date["neg_utc_offset"],
        "name": release["name"],
        "target": release["target"],
        "target_type": release["target_type"],
        "comment": release["message"],
        "synthetic": release["synthetic"],
    }


def db_to_release(db_release):
    """Convert a database representation of a release to its swh-model
    representation.
    """

    author = db_to_author(
        db_release["author_fullname"],
        db_release["author_name"],
        db_release["author_email"],
    )
    date = db_to_date(
        db_release["date"], db_release["date_offset"], db_release["date_neg_utc_offset"]
    )

    ret = {
        "author": author,
        "date": date,
        "id": db_release["id"],
        "name": db_release["name"],
        "message": db_release["comment"],
        "synthetic": db_release["synthetic"],
        "target": db_release["target"],
        "target_type": db_release["target_type"],
    }

    if "object_id" in db_release:
        ret["object_id"] = db_release["object_id"]

    return ret


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


def origin_url_to_sha1(origin_url):
    """Convert an origin URL to a sha1. Encodes URL to utf-8."""
    return MultiHash.from_data(origin_url.encode("utf-8"), {"sha1"}).digest()["sha1"]
