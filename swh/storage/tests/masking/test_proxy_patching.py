# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import attr

from swh.core.api.classes import stream_results
from swh.model.model import Person
from swh.storage.proxies.masking.db import MaskingAdmin
from swh.storage.tests.storage_data import StorageData


def test_revision_get(swh_storage, masking_admin: MaskingAdmin):
    revision = StorageData.revision
    revision2 = StorageData.revision2
    revision3 = StorageData.revision4  # StorageData.rev3 has same author/committer as 2

    assert (
        revision.author is not None
        and revision.author.email is not None
        and revision2.committer is not None
        and revision2.committer.email is not None
    )  # for mypy

    swh_storage.revision_add([revision, revision2, revision3])
    assert swh_storage.revision_get([revision.id, revision2.id, revision3.id]) == [
        revision,
        revision2,
        revision3,
    ]

    display_name1 = b"New Author <new-author1@example.org>"
    display_name2 = b"New Author <new-author2@example.org>"
    masking_admin.set_display_name(revision.author.email, display_name1)
    masking_admin.set_display_name(revision2.committer.email, display_name2)

    assert swh_storage.revision_get([revision.id, revision2.id, revision3.id]) == [
        attr.evolve(revision, author=Person.from_fullname(display_name1)),
        attr.evolve(revision2, committer=Person.from_fullname(display_name2)),
        revision3,
    ]


def test_revision_get_none_author(swh_storage, masking_admin: MaskingAdmin):
    revision = attr.evolve(
        StorageData.revision,
        author=None,
        date=None,
        committer=None,
        committer_date=None,
    )
    revision2 = StorageData.revision2
    assert revision2.author is not None and revision2.committer is not None  # for mypy
    revision2 = attr.evolve(
        revision2,
        author=attr.evolve(revision2.author, email=None),
        committer=attr.evolve(revision2.committer, email=None),
    )

    swh_storage.revision_add([revision, revision2])
    assert swh_storage.revision_get([revision.id, revision2.id]) == [
        revision,
        revision2,
    ]

    display_name = b"New Author <new-author@example.org>"
    masking_admin.set_display_name(b"old-author@example.org", display_name)

    assert swh_storage.revision_get([revision.id, revision2.id]) == [
        revision,
        revision2,
    ]


def test_revision_log(swh_storage, masking_admin: MaskingAdmin):
    revision = StorageData.revision
    revision2 = StorageData.revision2
    revision3 = StorageData.revision3
    revision4 = StorageData.revision4

    assert (
        revision.author is not None
        and revision.author.email is not None
        and revision2.committer is not None
        and revision2.committer.email is not None
    )  # for mypy

    swh_storage.revision_add([revision, revision2, revision3, revision4])
    assert list(swh_storage.revision_log([revision3.id, revision4.id])) == [
        revision3.to_dict(),
        revision.to_dict(),
        revision2.to_dict(),
        revision4.to_dict(),
    ]

    display_name1 = b"New Author <new-author1@example.org>"
    display_name2 = b"New Author <new-author2@example.org>"
    masking_admin.set_display_name(revision.author.email, display_name1)
    masking_admin.set_display_name(revision2.committer.email, display_name2)

    assert list(swh_storage.revision_log([revision3.id, revision4.id])) == [
        attr.evolve(revision3, committer=Person.from_fullname(display_name2)).to_dict(),
        attr.evolve(revision, author=Person.from_fullname(display_name1)).to_dict(),
        attr.evolve(revision2, committer=Person.from_fullname(display_name2)).to_dict(),
        revision4.to_dict(),
    ]


def test_revision_log_none_author(swh_storage, masking_admin: MaskingAdmin):
    revision = attr.evolve(
        StorageData.revision,
        author=None,
        date=None,
        committer=None,
        committer_date=None,
    )
    revision2 = StorageData.revision2
    revision3 = StorageData.revision3
    assert revision2.author is not None and revision2.committer is not None  # for mypy
    revision2 = attr.evolve(
        revision2,
        author=attr.evolve(revision2.author, email=None),
        committer=attr.evolve(revision2.committer, email=None),
    )

    swh_storage.revision_add([revision, revision2, revision3])
    assert list(swh_storage.revision_log([revision3.id])) == [
        revision3.to_dict(),
        revision.to_dict(),
        revision2.to_dict(),
    ]

    display_name = b"New Author <new-author@example.org>"
    masking_admin.set_display_name(b"old-author@example.org", display_name)

    assert list(swh_storage.revision_log([revision3.id])) == [
        revision3.to_dict(),
        revision.to_dict(),
        revision2.to_dict(),
    ]


def test_revision_get_partition(swh_storage, masking_admin: MaskingAdmin):
    revision = attr.evolve(StorageData.revision, metadata=None)
    revision2 = StorageData.revision2
    revision3 = StorageData.revision4  # StorageData.rev3 has same author/committer as 2

    assert (
        revision.author is not None
        and revision.author.email is not None
        and revision2.committer is not None
        and revision2.committer.email is not None
    )  # for mypy

    swh_storage.revision_add([revision, revision2, revision3])
    assert set(
        stream_results(
            swh_storage.revision_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        revision,
        revision2,
        revision3,
    }

    display_name1 = b"New Author <new-author1@example.org>"
    display_name2 = b"New Author <new-author2@example.org>"
    masking_admin.set_display_name(revision.author.email, display_name1)
    masking_admin.set_display_name(revision2.committer.email, display_name2)

    assert set(
        stream_results(
            swh_storage.revision_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        attr.evolve(revision, author=Person.from_fullname(display_name1)),
        attr.evolve(revision2, committer=Person.from_fullname(display_name2)),
        revision3,
    }


def test_revision_get_partition_none_author(swh_storage, masking_admin: MaskingAdmin):
    revision = attr.evolve(
        StorageData.revision,
        author=None,
        date=None,
        committer=None,
        committer_date=None,
        metadata=None,
    )
    revision2 = StorageData.revision2
    assert revision2.author is not None and revision2.committer is not None  # for mypy
    revision2 = attr.evolve(
        revision2,
        author=attr.evolve(revision2.author, email=None),
        committer=attr.evolve(revision2.committer, email=None),
    )

    swh_storage.revision_add([revision, revision2])
    assert set(
        stream_results(
            swh_storage.revision_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        revision,
        revision2,
    }

    display_name = b"New Author <new-author@example.org>"
    masking_admin.set_display_name(b"old-author@example.org", display_name)

    assert set(
        stream_results(
            swh_storage.revision_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        revision,
        revision2,
    }


############


def test_release_get(swh_storage, masking_admin: MaskingAdmin):
    release = StorageData.release
    release2 = StorageData.release2

    assert release.author is not None and release.author.email is not None  # for mypy

    swh_storage.release_add([release, release2])
    assert swh_storage.release_get([release.id, release2.id]) == [
        release,
        release2,
    ]

    display_name1 = b"New Author <new-author1@example.org>"
    masking_admin.set_display_name(release.author.email, display_name1)

    assert swh_storage.release_get([release.id, release2.id]) == [
        attr.evolve(release, author=Person.from_fullname(display_name1)),
        release2,
    ]


def test_release_get_none_author(swh_storage, masking_admin: MaskingAdmin):
    release = attr.evolve(
        StorageData.release,
        author=None,
        date=None,
    )
    release2 = StorageData.release2
    assert release2.author is not None  # for mypy
    release2 = attr.evolve(
        release2,
        author=attr.evolve(release2.author, email=None),
    )

    swh_storage.release_add([release, release2])
    assert swh_storage.release_get([release.id, release2.id]) == [
        release,
        release2,
    ]

    display_name = b"New Author <new-author@example.org>"
    masking_admin.set_display_name(b"old-author@example.org", display_name)

    assert swh_storage.release_get([release.id, release2.id]) == [
        release,
        release2,
    ]


def test_release_get_partition(swh_storage, masking_admin: MaskingAdmin):
    release = attr.evolve(StorageData.release, metadata=None)
    release2 = StorageData.release2

    assert release.author is not None and release.author.email is not None  # for mypy

    swh_storage.release_add([release, release2])
    assert set(
        stream_results(
            swh_storage.release_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        release,
        release2,
    }

    display_name1 = b"New Author <new-author1@example.org>"
    masking_admin.set_display_name(release.author.email, display_name1)

    assert set(
        stream_results(
            swh_storage.release_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        attr.evolve(release, author=Person.from_fullname(display_name1)),
        release2,
    }


def test_release_get_partition_none_author(swh_storage, masking_admin: MaskingAdmin):
    release = attr.evolve(
        StorageData.release,
        author=None,
        date=None,
        metadata=None,
    )

    swh_storage.release_add([release])
    assert set(
        stream_results(
            swh_storage.release_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        release,
    }

    display_name = b"New Author <new-author@example.org>"
    masking_admin.set_display_name(b"old-author@example.org", display_name)

    assert set(
        stream_results(
            swh_storage.release_get_partition, partition_id=0, nb_partitions=1
        )
    ) == {
        release,
    }


def test_set_display_names(swh_storage, masking_admin: MaskingAdmin):
    def get_all():
        cur = masking_admin.cursor()
        cur.execute("SELECT * FROM display_name")
        return set(cur.fetchall())

    display_names = {
        (
            f"Author {i} <author{i}@example.org>".encode(),
            f"New Author {i} <new-author{i}example.org>".encode(),
        )
        for i in range(10)
    }

    masking_admin.set_display_names(display_names)
    assert get_all() == display_names

    # doing it again with clear=True should not change anything
    masking_admin.set_display_names(display_names, clear=True)
    assert get_all() == display_names

    # doing it again should not change anything
    masking_admin.set_display_names(display_names)
    assert get_all() == display_names

    more_display_names = {
        (
            f"Person {i} <person{i}@example.org>".encode(),
            f"New Person {i} <new-person{i}example.org>".encode(),
        )
        for i in range(10)
    }

    masking_admin.set_display_names(more_display_names)
    assert get_all() == (display_names | more_display_names)

    masking_admin.set_display_names(more_display_names)
    assert get_all() == (display_names | more_display_names)

    # with clear=True, should only get the inserted objects
    masking_admin.set_display_names(display_names, clear=True)
    assert get_all() == display_names
