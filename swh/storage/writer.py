# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Iterable, Optional

from swh.model.model import (
    Content,
    Directory,
    ExtID,
    MetadataAuthority,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)

try:
    from swh.journal.writer import JournalWriterInterface, get_journal_writer
    from swh.journal.writer.interface import ValueProtocol
except ImportError:
    get_journal_writer = None  # type: ignore
    # mypy limitation, see https://github.com/python/mypy/issues/1153


def model_object_dict_sanitizer(
    object_type: str, object_dict: Dict[str, Any]
) -> Dict[str, str]:
    object_dict = object_dict.copy()
    if object_type == "content":
        object_dict.pop("data", None)
    return object_dict


class JournalWriter:
    """Journal writer storage collaborator. It's in charge of adding objects to
    the journal.

    """

    def __init__(self, journal_writer: Optional[Dict[str, Any]]):
        self.journal: Optional[JournalWriterInterface] = None
        if journal_writer:
            if get_journal_writer is None:
                raise EnvironmentError(
                    "You need the swh.journal package to use the "
                    "journal_writer feature"
                )
            self.journal = get_journal_writer(
                value_sanitizer=model_object_dict_sanitizer, **journal_writer
            )

    def write_addition(self, object_type: str, object_: ValueProtocol) -> None:
        if self.journal:
            self.journal.write_addition(object_type, object_)

    def write_additions(
        self, object_type: str, objects: Iterable[ValueProtocol]
    ) -> None:
        if self.journal:
            self.journal.write_additions(object_type, objects)

    def content_add(self, contents: Iterable[Content]) -> None:
        """Add contents to the journal. Drop the data field if provided."""
        contents = [item.evolve(data=None, get_data=None) for item in contents]
        self.write_additions("content", contents)

    def content_update(self, contents: Iterable[Dict[str, Any]]) -> None:
        if self.journal:
            raise NotImplementedError("content_update is not supported by the journal.")

    def content_add_metadata(self, contents: Iterable[Content]) -> None:
        self.content_add(contents)

    def skipped_content_add(self, contents: Iterable[SkippedContent]) -> None:
        self.write_additions("skipped_content", contents)

    def directory_add(self, directories: Iterable[Directory]) -> None:
        self.write_additions("directory", directories)

    def revision_add(self, revisions: Iterable[Revision]) -> None:
        self.write_additions("revision", revisions)

    def release_add(self, releases: Iterable[Release]) -> None:
        self.write_additions("release", releases)

    def snapshot_add(self, snapshots: Iterable[Snapshot]) -> None:
        self.write_additions("snapshot", snapshots)

    def origin_visit_add(self, visits: Iterable[OriginVisit]) -> None:
        self.write_additions("origin_visit", visits)

    def origin_visit_status_add(
        self, visit_statuses: Iterable[OriginVisitStatus]
    ) -> None:
        self.write_additions("origin_visit_status", visit_statuses)

    def origin_add(self, origins: Iterable[Origin]) -> None:
        self.write_additions("origin", origins)

    def raw_extrinsic_metadata_add(
        self, metadata: Iterable[RawExtrinsicMetadata]
    ) -> None:
        self.write_additions("raw_extrinsic_metadata", metadata)

    def metadata_fetcher_add(self, fetchers: Iterable[MetadataFetcher]) -> None:
        self.write_additions("metadata_fetcher", fetchers)

    def metadata_authority_add(self, authorities: Iterable[MetadataAuthority]) -> None:
        self.write_additions("metadata_authority", authorities)

    def extid_add(self, extids: Iterable[ExtID]) -> None:
        self.write_additions("extid", extids)
