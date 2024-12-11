# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
import datetime
import json
from typing import Any, Dict, Literal, Tuple

import attr

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    ObjectType,
    OriginVisit,
    OriginVisitStatus,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    RevisionType,
    Sha1Git,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.storage.interface import ObjectReference

from ..utils import remove_keys
from .model import (
    ObjectReferenceRow,
    OriginVisitRow,
    OriginVisitStatusRow,
    RawExtrinsicMetadataRow,
    ReleaseRow,
    RevisionRow,
)


def _inflate_person(d: Dict[str, Any], prefix: Literal["author", "committer"]) -> None:
    """If present, pops ``{prefix}_fullname``, ``{prefix}_name``, and ``{prefix}_email``
    and uses them to build a :class:`Person` with key ``{prefix}``.

    This allows parsing the ``author`` and ``committer`` fields of :class:`Revision`
    and :class:`Release`.
    """
    if d[f"{prefix}_fullname"]:
        # recently written row
        d[prefix] = Person(
            fullname=d.pop(f"{prefix}_fullname"),
            name=d.pop(f"{prefix}_name"),
            email=d.pop(f"{prefix}_email"),
        )
    else:
        # legacy row, or no author/committer
        d.pop(f"{prefix}_fullname")
        d.pop(f"{prefix}_name")
        d.pop(f"{prefix}_email")


def _flatten_person(d: Dict[str, Any], prefix: Literal["author", "committer"]) -> None:
    """Adds ``{prefix}_fullname``, ``{prefix}_name``, and ``{prefix}_email`` to the dictionary
    from the :class:`Person` with key ``{prefix}``.

    This allows serializing the ``author`` and ``committer`` fields of :class:`Revision`
    and :class:`Release`.
    """
    if d[prefix] is None:
        d[f"{prefix}_fullname"] = None
        d[f"{prefix}_name"] = None
        d[f"{prefix}_email"] = None
    else:
        d[f"{prefix}_fullname"] = d[prefix].fullname
        d[f"{prefix}_name"] = d[prefix].name
        d[f"{prefix}_email"] = d[prefix].email


def _inflate_date(d: Dict[str, Any], prefix: Literal["date", "committer_date"]) -> None:
    """If present, pops ``{prefix}_seconds``, ``{prefix}_microseconds``, and
    ``{prefix}_offset_bytes`` and uses them to build a `TimestampWithTimezone`
    with key ``{prefix}``.

    This allows parsing the ``date`` and ``committer_date`` fields of :class:`Revision`
    and :class:`Release`.
    """
    if d[f"{prefix}_seconds"] is not None:
        # recently written row
        d[prefix] = TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=d.pop(f"{prefix}_seconds"),
                microseconds=d.pop(f"{prefix}_microseconds"),
            ),
            offset_bytes=d.pop(f"{prefix}_offset_bytes"),
        )
    else:
        # legacy row, or no date/committer_date
        d.pop(f"{prefix}_seconds")
        d.pop(f"{prefix}_microseconds")
        d.pop(f"{prefix}_offset_bytes")


def _flatten_date(d: Dict[str, Any], prefix: Literal["date", "committer_date"]) -> None:
    """Adds ``{prefix}_seconds``, ``{prefix}_microseconds``, and ``{prefix}_offset_bytes``
    to the dictionary from the :class:`TimestampWithTimezone` with key ``{prefix}``.

    This allows serializing the ``date`` and ``committer_date`` fields of :class:`Revision`
    and :class:`Release`.
    """
    if d[prefix] is None:
        d[f"{prefix}_seconds"] = None
        d[f"{prefix}_microseconds"] = None
        d[f"{prefix}_offset_bytes"] = None
    else:
        d[f"{prefix}_seconds"] = d[prefix].timestamp.seconds
        d[f"{prefix}_microseconds"] = d[prefix].timestamp.microseconds
        d[f"{prefix}_offset_bytes"] = d[prefix].offset_bytes


def revision_to_db(revision: Revision) -> RevisionRow:
    # we use a deepcopy of the dict because we do not want to recurse the
    # Model->dict conversion (to keep Timestamp & al. entities), BUT we do not
    # want to modify original metadata (embedded in the Model entity), so we
    # non-recursively convert it as a dict but make a deep copy.
    db_revision = deepcopy(attr.asdict(revision, recurse=False))
    metadata = revision.metadata
    extra_headers = revision.extra_headers
    if not extra_headers and metadata and "extra_headers" in metadata:
        extra_headers = db_revision["metadata"].pop("extra_headers")
    db_revision["metadata"] = json.dumps(
        dict(db_revision["metadata"]) if db_revision["metadata"] is not None else None
    )
    db_revision["extra_headers"] = extra_headers
    db_revision["type"] = db_revision["type"].value

    _flatten_person(db_revision, "author")
    _flatten_person(db_revision, "committer")
    _flatten_date(db_revision, "date")
    _flatten_date(db_revision, "committer_date")

    return RevisionRow(**remove_keys(db_revision, ("parents",)))


def revision_from_db(
    db_revision: RevisionRow, parents: Tuple[Sha1Git, ...]
) -> Revision:
    revision = db_revision.to_dict()
    metadata = json.loads(revision.pop("metadata", None))
    extra_headers = revision.pop("extra_headers", ())
    if not extra_headers and metadata and "extra_headers" in metadata:
        extra_headers = metadata.pop("extra_headers")
    if extra_headers is None:
        extra_headers = ()

    _inflate_person(revision, "author")
    _inflate_person(revision, "committer")
    _inflate_date(revision, "date")
    _inflate_date(revision, "committer_date")

    return Revision(
        parents=parents,
        type=RevisionType(revision.pop("type")),
        metadata=metadata,
        extra_headers=extra_headers,
        **revision,
    )


def release_to_db(release: Release) -> ReleaseRow:
    db_release = attr.asdict(release, recurse=False)
    db_release["target_type"] = db_release["target_type"].value

    _flatten_person(db_release, "author")
    _flatten_date(db_release, "date")

    return ReleaseRow(**remove_keys(db_release, ("metadata",)))


def release_from_db(db_release: ReleaseRow) -> Release:
    release = db_release.to_dict()

    _inflate_person(release, "author")
    _inflate_date(release, "date")

    return Release(
        target_type=ObjectType(release.pop("target_type")),
        **release,
    )


def row_to_content_hashes(row: ReleaseRow) -> Dict[str, bytes]:
    """Convert cassandra row to a content hashes"""
    hashes = {}
    for algo in DEFAULT_ALGORITHMS:
        hashes[algo] = getattr(row, algo)
    return hashes


def row_to_visit(row: OriginVisitRow) -> OriginVisit:
    """Format a row representing an origin_visit to an actual OriginVisit."""
    return OriginVisit(
        origin=row.origin,
        visit=row.visit,
        date=row.date.replace(tzinfo=datetime.timezone.utc),
        type=row.type,
    )


def row_to_visit_status(row: OriginVisitStatusRow) -> OriginVisitStatus:
    """Format a row representing a visit_status to an actual OriginVisitStatus."""
    return OriginVisitStatus.from_dict(
        {
            **row.to_dict(),
            "date": row.date.replace(tzinfo=datetime.timezone.utc),
            "metadata": (json.loads(row.metadata) if row.metadata else None),
        }
    )


def visit_status_to_row(status: OriginVisitStatus) -> OriginVisitStatusRow:
    d = status.to_dict()
    return OriginVisitStatusRow.from_dict({**d, "metadata": json.dumps(d["metadata"])})


def row_to_raw_extrinsic_metadata(row: RawExtrinsicMetadataRow) -> RawExtrinsicMetadata:
    discovery_date = row.discovery_date.replace(tzinfo=datetime.timezone.utc)

    return RawExtrinsicMetadata(
        target=ExtendedSWHID.from_string(row.target),
        authority=MetadataAuthority(
            type=MetadataAuthorityType(row.authority_type),
            url=row.authority_url,
        ),
        fetcher=MetadataFetcher(
            name=row.fetcher_name,
            version=row.fetcher_version,
        ),
        discovery_date=discovery_date,
        format=row.format,
        metadata=row.metadata,
        origin=row.origin or None,  # to account for "" conversion to None
        visit=row.visit if row.visit and row.visit != 0 else None,
        snapshot=CoreSWHID.from_string(row.snapshot) if row.snapshot else None,
        release=CoreSWHID.from_string(row.release) if row.release else None,
        revision=CoreSWHID.from_string(row.revision) if row.revision else None,
        path=row.path or None,  # to account for b"" conversion to None
        directory=CoreSWHID.from_string(row.directory) if row.directory else None,
    )


def object_reference_to_row(object_reference: ObjectReference) -> ObjectReferenceRow:
    return ObjectReferenceRow(
        source_type=object_reference.source.object_type.value,
        source=object_reference.source.object_id,
        target_type=object_reference.target.object_type.value,
        target=object_reference.target.object_id,
    )


def row_to_object_reference(row: ObjectReferenceRow) -> ObjectReference:
    return ObjectReference(
        source=ExtendedSWHID(
            object_type=ExtendedObjectType(row.source_type), object_id=row.source
        ),
        target=ExtendedSWHID(
            object_type=ExtendedObjectType(row.target_type), object_id=row.target
        ),
    )
