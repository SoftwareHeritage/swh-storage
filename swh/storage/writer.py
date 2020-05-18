# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable

from attr import evolve

from swh.model.model import (
    Origin,
    OriginVisit,
    Snapshot,
    Directory,
    Revision,
    Release,
    Content,
    SkippedContent,
)

try:
    from swh.journal.writer import get_journal_writer
except ImportError:
    get_journal_writer = None  # type: ignore
    # mypy limitation, see https://github.com/python/mypy/issues/1153


class JournalWriter:
    """Journal writer storage collaborator. It's in charge of adding objects to
    the journal.

    """

    def __init__(self, journal_writer):
        if journal_writer:
            if get_journal_writer is None:
                raise EnvironmentError(
                    "You need the swh.journal package to use the "
                    "journal_writer feature"
                )
            self.journal = get_journal_writer(**journal_writer)
        else:
            self.journal = None

    def write_additions(self, obj_type, values) -> None:
        if self.journal:
            self.journal.write_additions(obj_type, values)

    def content_add(self, contents: Iterable[Content]) -> None:
        """Add contents to the journal. Drop the data field if provided.

        """
        contents = [evolve(item, data=None) for item in contents]
        self.write_additions("content", contents)

    def content_update(self, contents: Iterable[Content]) -> None:
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

    def origin_visit_update(self, visits: Iterable[OriginVisit]) -> None:
        self.write_additions("origin_visit", visits)

    def origin_visit_upsert(self, visits: Iterable[OriginVisit]) -> None:
        self.write_additions("origin_visit", visits)

    def origin_add(self, origins: Iterable[Origin]) -> None:
        self.write_additions("origin", origins)
