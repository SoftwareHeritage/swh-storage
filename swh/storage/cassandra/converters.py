# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy
import datetime
import json
from typing import Dict, Tuple

import attr

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import (
    ObjectType,
    OriginVisit,
    OriginVisitStatus,
    Release,
    Revision,
    RevisionType,
    Sha1Git,
)

from .common import remove_keys
from .model import OriginVisitRow, OriginVisitStatusRow, ReleaseRow, RevisionRow


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
    return ReleaseRow(**remove_keys(db_release, ("metadata",)))


def release_from_db(db_release: ReleaseRow) -> Release:
    release = db_release.to_dict()
    return Release(target_type=ObjectType(release.pop("target_type")), **release,)


def row_to_content_hashes(row: ReleaseRow) -> Dict[str, bytes]:
    """Convert cassandra row to a content hashes

    """
    hashes = {}
    for algo in DEFAULT_ALGORITHMS:
        hashes[algo] = getattr(row, algo)
    return hashes


def row_to_visit(row: OriginVisitRow) -> OriginVisit:
    """Format a row representing an origin_visit to an actual OriginVisit.

    """
    return OriginVisit(
        origin=row.origin,
        visit=row.visit,
        date=row.date.replace(tzinfo=datetime.timezone.utc),
        type=row.type,
    )


def row_to_visit_status(row: OriginVisitStatusRow) -> OriginVisitStatus:
    """Format a row representing a visit_status to an actual OriginVisitStatus.

    """
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
