# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

import attr

from copy import deepcopy
from typing import Dict

from swh.model.model import (
    RevisionType, ObjectType, Revision, Release,
)
from swh.model.hashutil import DEFAULT_ALGORITHMS

from ..converters import git_headers_to_db, db_to_git_headers
from .common import Row


class CassObject(dict):
    __getattr__ = dict.__getitem__


def revision_to_db(revision: Revision) -> CassObject:
    # we use a deepcopy of the dict because we do not want to recurse the
    # Model->dict conversion (to keep Timestamp & al. entities), BUT we do not
    # want to modify original metadata (embedded in the Model entity), so we
    # non-recursively convert it as a dict but make a deep copy.
    db_revision = CassObject(deepcopy(attr.asdict(revision, recurse=False)))
    metadata = revision.metadata
    if metadata and 'extra_headers' in metadata:
        db_revision['metadata']['extra_headers'] = git_headers_to_db(
            metadata['extra_headers'])
    db_revision['metadata'] = json.dumps(db_revision['metadata'])
    db_revision['type'] = db_revision['type'].value
    return db_revision


def revision_from_db(**kwargs) -> Revision:
    kwargs['metadata'] = metadata = json.loads(kwargs['metadata'])
    if metadata and 'extra_headers' in metadata:
        extra_headers = db_to_git_headers(
            metadata['extra_headers'])
        metadata['extra_headers'] = extra_headers
    kwargs['type'] = RevisionType(kwargs['type'])
    return Revision(**kwargs)


def release_to_db(release: Release) -> CassObject:
    db_release = CassObject(attr.asdict(release, recurse=False))
    db_release['target_type'] = release.target_type.value
    return db_release


def release_from_db(**kwargs) -> Release:
    kwargs['target_type'] = ObjectType(kwargs['target_type'])
    return Release(**kwargs)


def row_to_content_hashes(row: Row) -> Dict[str, bytes]:
    """Convert cassandra row to a content hashes

    """
    hashes = {}
    for algo in DEFAULT_ALGORITHMS:
        hashes[algo] = getattr(row, algo)
    return hashes
