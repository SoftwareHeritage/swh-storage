# Copyright (C) 2020-2024  The Software Heritage developers
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
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

from swh.model.model import Person, TimestampWithTimezone

MAGIC_NULL_PK = b"<null>"
"""
NULLs (or all-empty blobs) are not allowed in primary keys; instead we use a
special value that can't possibly be a valid hash.
"""


T = TypeVar("T", bound="BaseRow")


def content_index_table_name(algo: str, skipped_content: bool) -> str:
    """Given an algorithm name, returns the name of one of the 'content_by_*'
    and 'skipped_content_by_*' tables that serve as index for the 'content'
    and 'skipped_content' tables based on this algorithm's hashes.

    For now it is a simple substitution, but future versions may append a version
    number to it, if needed for schema updates."""
    if skipped_content:
        return f"skipped_content_by_{algo}"
    else:
        return f"content_by_{algo}"


class BaseRow:
    TABLE: ClassVar[str]
    PARTITION_KEY: ClassVar[Tuple[str, ...]]
    CLUSTERING_KEY: ClassVar[Tuple[str, ...]] = ()

    @classmethod
    def from_dict(cls: Type[T], d: Dict[str, Any]) -> T:
        return cls(**d)

    @classmethod
    def cols(cls) -> List[str]:
        return [
            field.name for field in dataclasses.fields(cast("DataclassInstance", cls))
        ]

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(cast("DataclassInstance", self))


@dataclasses.dataclass
class ContentRow(BaseRow):
    TABLE = "content"
    PARTITION_KEY: ClassVar[Tuple[str, ...]] = ("sha256",)
    CLUSTERING_KEY = (
        "sha1",
        "sha1_git",
        "blake2s256",
    )

    sha1: bytes
    sha1_git: bytes
    sha256: bytes
    blake2s256: bytes
    length: int
    ctime: Optional[datetime.datetime]
    """creation time, i.e. time of (first) injection into the storage"""
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
    """creation time, i.e. time of (first) injection into the storage"""
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
    raw_manifest: Optional[bytes]
    """NULL if the object can be rebuild from (sorted) entries"""


@dataclasses.dataclass
class DirectoryEntryRow(BaseRow):
    TABLE = "directory_entry"
    PARTITION_KEY = ("directory_id",)
    CLUSTERING_KEY = ("name",)

    directory_id: bytes
    name: bytes
    """path name, relative to containing dir"""
    target: bytes
    perms: int
    """unix-like permissions"""
    type: str
    """target type"""


@dataclasses.dataclass
class RevisionRow(BaseRow):
    TABLE = "revision"
    PARTITION_KEY = ("id",)

    id: bytes
    date: Optional[TimestampWithTimezone]
    committer_date: Optional[TimestampWithTimezone]
    type: str
    directory: bytes
    """source code "root" directory"""
    message: bytes
    author: Person
    committer: Person
    synthetic: bool
    """true iff revision has been created by Software Heritage"""
    metadata: str
    """extra metadata as JSON(tarball checksums, etc...)"""
    extra_headers: dict
    """extra commit information as (tuple(key, value), ...)"""
    raw_manifest: Optional[bytes]
    """NULL if the object can be rebuild from other cells and revision_parent."""


@dataclasses.dataclass
class RevisionParentRow(BaseRow):
    TABLE = "revision_parent"
    PARTITION_KEY = ("id",)
    CLUSTERING_KEY = ("parent_rank",)

    id: bytes
    parent_rank: int
    """parent position in merge commits, 0-based"""
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
    """true iff release has been created by Software Heritage"""
    raw_manifest: Optional[bytes]
    """NULL if the object can be rebuild from other cells"""


@dataclasses.dataclass
class SnapshotRow(BaseRow):
    TABLE = "snapshot"
    PARTITION_KEY = ("id",)

    id: bytes


@dataclasses.dataclass
class SnapshotBranchRow(BaseRow):
    """
    For a given snapshot_id, branches are sorted by their name,
    allowing easy pagination.
    """

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
        return cls(**d)


@dataclasses.dataclass
class OriginRow(BaseRow):
    TABLE = "origin"
    PARTITION_KEY = ("sha1",)

    sha1: bytes
    url: str
    next_visit_id: int
    """
    We need integer visit ids for compatibility with the pgsql
    storage, so we're using lightweight transactions with this trick:
    https://stackoverflow.com/a/29391877/539465
    """


@dataclasses.dataclass
class MetadataAuthorityRow(BaseRow):
    TABLE = "metadata_authority"
    PARTITION_KEY = ("url",)
    CLUSTERING_KEY = ("type",)

    url: str
    type: str


@dataclasses.dataclass
class MetadataFetcherRow(BaseRow):
    TABLE = "metadata_fetcher"
    PARTITION_KEY = ("name",)
    CLUSTERING_KEY = ("version",)

    name: str
    version: str


@dataclasses.dataclass
class RawExtrinsicMetadataRow(BaseRow):
    """
    An explanation is in order for the primary key:

    Intuitively, the primary key should only be 'id', because two metadata
    entries are the same iff the id is the same; and 'id' is used for
    deduplication.

    However, we also want to query by
    (target, authority_type, authority_url, discovery_date)
    The naive solution to this would be an extra table, to use as index;
    but it means 1. extra code to keep them in sync 2. overhead when writing
    3. overhead + random reads (instead of linear) when reading.

    Therefore, we use a single table for both, by adding the column
    we want to query with before the id.
    It solves both a) the query/order issues and b) the uniqueness issue because:

    a) adding the id at the end of the primary key does not change the rows' order:
       for two different rows, id1 != id2, so
       (target1, ..., date1) < (target2, ..., date2)
       <=> (target1, ..., date1, id1) < (target2, ..., date2, id2)

    b) the id is a hash of all the columns, so:
       rows are the same
       <=> id1 == id2
       <=> (target1, ..., date1, id1) == (target2, ..., date2, id2)
    """

    TABLE = "raw_extrinsic_metadata"
    PARTITION_KEY = ("target",)
    CLUSTERING_KEY = (
        "authority_type",
        "authority_url",
        "discovery_date",
        "id",
    )

    id: bytes

    type: str
    target: str

    # metadata source:
    authority_type: str
    authority_url: str
    discovery_date: datetime.datetime
    fetcher_name: str
    fetcher_version: str

    # metadata itself:
    format: str
    metadata: bytes

    # context:

    # The following keys are kept optional but extra effort is made to avoid setting
    # those None values to null in cassandra. Otherwise, that would end up churning on
    # cleaning up (all the time)
    origin: Optional[str]
    visit: Optional[int]
    snapshot: Optional[str]
    release: Optional[str]
    revision: Optional[str]
    path: Optional[bytes]
    directory: Optional[str]


@dataclasses.dataclass
class RawExtrinsicMetadataByIdRow(BaseRow):
    TABLE = "raw_extrinsic_metadata_by_id"
    PARTITION_KEY = ("id",)
    CLUSTERING_KEY = ()

    id: bytes
    target: str
    authority_type: str
    authority_url: str


@dataclasses.dataclass
class ObjectCountRow(BaseRow):
    TABLE = "object_count"
    PARTITION_KEY = ("partition_key",)
    CLUSTERING_KEY = ("object_type",)

    partition_key: int
    object_type: str
    count: int


@dataclasses.dataclass
class ExtIDRow(BaseRow):
    TABLE = "extid"
    PARTITION_KEY = ("extid_type", "extid")
    CLUSTERING_KEY = ("extid_version", "target_type", "target")

    extid_type: str
    extid: bytes
    extid_version: int
    target_type: str
    target: bytes


@dataclasses.dataclass
class ExtIDByTargetRow(BaseRow):
    TABLE = "extid_by_target"
    PARTITION_KEY = ("target_type", "target")
    CLUSTERING_KEY = ("target_token",)

    target_type: str
    target: bytes
    target_token: int
    """value of token(pk) on the "primary" table"""


@dataclasses.dataclass(frozen=True)
class ObjectReferenceRow(BaseRow):
    TABLE = "object_references"
    PARTITION_KEY = ("target_type", "target")
    CLUSTERING_KEY = ("source_type", "source")

    target_type: str
    target: bytes
    source_type: str
    source: bytes
