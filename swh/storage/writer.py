# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable, Union
from swh.model.model import (
    Origin, OriginVisit, Snapshot, Directory, Revision, Release, Content
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
                    'You need the swh.journal package to use the '
                    'journal_writer feature')
            self.journal = get_journal_writer(**journal_writer)
        else:
            self.journal = None

    def content_add(self, contents: Iterable[Content]) -> None:
        """Add contents to the journal. Drop the data field if provided.

        """
        if not self.journal:
            return
        for item in contents:
            content = item.to_dict()
            if 'data' in content:
                del content['data']
            self.journal.write_addition('content', content)

    def content_update(self, contents: Iterable[Content]) -> None:
        if not self.journal:
            return
        raise NotImplementedError(
            'content_update is not yet supported with a journal writer.')

    def content_add_metadata(
            self, contents: Iterable[Content]) -> None:
        return self.content_add(contents)

    def skipped_content_add(
            self, contents: Iterable[Content]) -> None:
        return self.content_add(contents)

    def directory_add(self, directories: Iterable[Directory]) -> None:
        if not self.journal:
            return
        self.journal.write_additions('directory', directories)

    def revision_add(self, revisions: Iterable[Revision]) -> None:
        if not self.journal:
            return
        self.journal.write_additions('revision', revisions)

    def release_add(self, releases: Iterable[Release]) -> None:
        if not self.journal:
            return
        self.journal.write_additions('release', releases)

    def snapshot_add(
            self, snapshots: Union[Iterable[Snapshot], Snapshot]) -> None:
        if not self.journal:
            return
        snaps = snapshots if isinstance(snapshots, list) else [snapshots]
        self.journal.write_additions('snapshot', snaps)

    def origin_visit_add(self, visit: OriginVisit):
        if not self.journal:
            return
        self.journal.write_addition('origin_visit', visit)

    def origin_visit_update(self, visit: OriginVisit):
        if not self.journal:
            return
        self.journal.write_update('origin_visit', visit)

    def origin_visit_upsert(self, visits: Iterable[OriginVisit]):
        if not self.journal:
            return
        self.journal.write_additions('origin_visit', visits)

    def origin_add_one(self, origin: Origin):
        if not self.journal:
            return
        self.journal.write_addition('origin', origin)
