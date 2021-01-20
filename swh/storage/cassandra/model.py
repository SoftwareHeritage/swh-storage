# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Classes representing tables in the Cassandra database.

They are very close to classes found in swh.model.model, but most of
them are subtly different:

* Large objects are split into other classes (eg. RevisionRow has no
  'parents' field, because parents are stored in a different table,
  represented by RevisionParentRow)
* They have a "cols" field, which returns the list of column names
  of the table
* They only use types that map directly to Cassandra's schema (ie. no enums)

Therefore, this model doesn't reuse swh.model.model, except for types
that can be mapped to UDTs (Person and TimestampWithTimezone).
"""

import dataclasses
import datetime
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar

from swh.model.model import Person, TimestampWithTimezone

MAGIC_NULL_PK = b"<null>"
"""
NULLs (or all-empty blobs) are not allowed in primary keys; instead we use a
special value that can't possibly be a valid hash.
"""


T = TypeVar("T", bound="BaseRow")


class BaseRow:
    TABLE: ClassVar[str]
    PARTITION_KEY: ClassVar[Tuple[str, ...]]
    CLUSTERING_KEY: ClassVar[Tuple[str, ...]] = ()

    @classmethod
    def from_dict(cls: Type[T], d: Dict[str, Any]) -> T:
        return cls(**d)  # type: ignore

    @classmethod
    def cols(cls) -> List[str]:
        return [field.name for field in dataclasses.fields(cls)]

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ContentRow(BaseRow):
    TABLE = "content"
    PARTITION_KEY = ("sha1", "sha1_git", "sha256", "blake2s256")

    sha1: bytes
    sha1_git: bytes
    sha256: bytes
    blake2s256: bytes
    length: int
    ctime: datetime.datetime
    status: str


@dataclasses.dataclass
class SkippedContentRow(BaseRow):
    TABLE = "skipped_content"
    PARTITION_KEY = ("sha1", "sha1_git", "sha256", "blake2s256")

    sha1: Optional[bytes]
    sha1_git: Optional[bytes]
    sha256: Optional[bytes]
    blake2s256: Optional[bytes]
    length: Optional[int]
    ctime: Optional[datetime.datetime]
    status: str
    reason: str
    origin: str

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SkippedContentRow":
        d = d.copy()
        for k in ("sha1", "sha1_git", "sha256", "blake2s256"):
            if d[k] == MAGIC_NULL_PK:
                d[k] = None
        return super().from_dict(d)


@dataclasses.dataclass
class DirectoryRow(BaseRow):
    TABLE = "directory"
    PARTITION_KEY = ("id",)

    id: bytes


@dataclasses.dataclass
class DirectoryEntryRow(BaseRow):
    TABLE = "directory_entry"
    PARTITION_KEY = ("directory_id",)
    CLUSTERING_KEY = ("name",)

    directory_id: bytes
    name: bytes
    target: bytes
    perms: int
    type: str


@dataclasses.dataclass
class RevisionRow(BaseRow):
    TABLE = "revision"
    PARTITION_KEY = ("id",)

    id: bytes
    date: Optional[TimestampWithTimezone]
    committer_date: Optional[TimestampWithTimezone]
    type: str
    directory: bytes
    message: bytes
    author: Person
    committer: Person
    synthetic: bool
    metadata: str
    extra_headers: dict


@dataclasses.dataclass
class RevisionParentRow(BaseRow):
    TABLE = "revision_parent"
    PARTITION_KEY = ("id",)
    CLUSTERING_KEY = ("parent_rank",)

    id: bytes
    parent_rank: int
    parent_id: bytes


@dataclasses.dataclass
class ReleaseRow(BaseRow):
    TABLE = "release"
    PARTITION_KEY = ("id",)

    id: bytes
    target_type: str
    target: bytes
    date: TimestampWithTimezone
    name: bytes
    message: bytes
    author: Person
    synthetic: bool


@dataclasses.dataclass
class SnapshotRow(BaseRow):
    TABLE = "snapshot"
    PARTITION_KEY = ("id",)

    id: bytes


@dataclasses.dataclass
class SnapshotBranchRow(BaseRow):
    TABLE = "snapshot_branch"
    PARTITION_KEY = ("snapshot_id",)
    CLUSTERING_KEY = ("name",)

    snapshot_id: bytes
    name: bytes
    target_type: Optional[str]
    target: Optional[bytes]


@dataclasses.dataclass
class OriginVisitRow(BaseRow):
    TABLE = "origin_visit"
    PARTITION_KEY = ("origin",)
    CLUSTERING_KEY = ("visit",)

    origin: str
    visit: int
    date: datetime.datetime
    type: str


@dataclasses.dataclass
class OriginVisitStatusRow(BaseRow):
    TABLE = "origin_visit_status"
    PARTITION_KEY = ("origin",)
    CLUSTERING_KEY = ("visit", "date")

    origin: str
    visit: int
    date: datetime.datetime
    type: str
    status: str
    metadata: str
    snapshot: bytes

    @classmethod
    def from_dict(cls: Type[T], d: Dict[str, Any]) -> T:
        return cls(**d)  # type: ignore


@dataclasses.dataclass
class OriginRow(BaseRow):
    TABLE = "origin"
    PARTITION_KEY = ("sha1",)

    sha1: bytes
    url: str
    next_visit_id: int


@dataclasses.dataclass
class MetadataAuthorityRow(BaseRow):
    TABLE = "metadata_authority"
    PARTITION_KEY = ("url",)
    CLUSTERING_KEY = ("type",)

    url: str
    type: str
    metadata: str


@dataclasses.dataclass
class MetadataFetcherRow(BaseRow):
    TABLE = "metadata_fetcher"
    PARTITION_KEY = ("name",)
    CLUSTERING_KEY = ("version",)

    name: str
    version: str
    metadata: str


@dataclasses.dataclass
class RawExtrinsicMetadataRow(BaseRow):
    TABLE = "raw_extrinsic_metadata"
    PARTITION_KEY = ("target",)
    CLUSTERING_KEY = (
        "authority_type",
        "authority_url",
        "discovery_date",
        "fetcher_name",
        "fetcher_version",
    )

    type: str
    target: str

    authority_type: str
    authority_url: str
    discovery_date: datetime.datetime
    fetcher_name: str
    fetcher_version: str

    format: str
    metadata: bytes

    origin: Optional[str]
    visit: Optional[int]
    snapshot: Optional[str]
    release: Optional[str]
    revision: Optional[str]
    path: Optional[bytes]
    directory: Optional[str]


@dataclasses.dataclass
class ObjectCountRow(BaseRow):
    TABLE = "object_count"
    PARTITION_KEY = ("partition_key",)
    CLUSTERING_KEY = ("object_type",)

    partition_key: int
    object_type: str
    count: int
