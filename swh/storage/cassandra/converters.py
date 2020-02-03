# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
from typing import Any, Dict

import attr

from swh.model.model import (
    RevisionType, ObjectType, Revision, Release,
)


from ..converters import git_headers_to_db, db_to_git_headers


def revision_to_db(revision: Dict[str, Any]) -> Revision:
    metadata = revision.get('metadata')
    if metadata and 'extra_headers' in metadata:
        extra_headers = git_headers_to_db(
            metadata['extra_headers'])
        revision = {
            **revision,
            'metadata': {
                **metadata,
                'extra_headers': extra_headers
            }
        }

    rev = Revision.from_dict(revision)
    rev = attr.evolve(
        rev,
        type=rev.type.value,
        metadata=json.dumps(rev.metadata),
    )

    return rev


def revision_from_db(revision) -> Revision:
    metadata = json.loads(revision.metadata)
    if metadata and 'extra_headers' in metadata:
        extra_headers = db_to_git_headers(
            metadata['extra_headers'])
        metadata['extra_headers'] = extra_headers
    rev = attr.evolve(
        revision,
        type=RevisionType(revision.type),
        metadata=metadata,
    )

    return rev


def release_to_db(release: Dict[str, Any]) -> Release:
    rel = Release.from_dict(release)
    rel = attr.evolve(
        rel,
        target_type=rel.target_type.value,
    )
    return rel


def release_from_db(release: Release) -> Release:
    release = attr.evolve(
        release,
        target_type=ObjectType(release.target_type),
    )
    return release
