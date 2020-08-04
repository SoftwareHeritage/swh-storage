# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import attr

from typing import Tuple

from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model import from_disk
from swh.model.identifiers import parse_swhid
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    ObjectType,
    Origin,
    OriginVisit,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    RevisionType,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    TargetType,
    Timestamp,
    TimestampWithTimezone,
)


class StorageData:
    """Data model objects to use within tests.

    """

    content = Content(
        data=b"42\n",
        length=3,
        sha1=hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4689"),
        sha1_git=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        sha256=hash_to_bytes(
            "673650f936cb3b0a2f93ce09d81be10748b1b203c19e8176b4eefc1964a0cf3a"
        ),
        blake2s256=hash_to_bytes(
            "d5fe1939576527e42cfd76a9455a2432fe7f56669564577dd93c4280e76d661d"
        ),
        status="visible",
    )
    content2 = Content(
        data=b"4242\n",
        length=5,
        sha1=hash_to_bytes("61c2b3a30496d329e21af70dd2d7e097046d07b7"),
        sha1_git=hash_to_bytes("36fade77193cb6d2bd826161a0979d64c28ab4fa"),
        sha256=hash_to_bytes(
            "859f0b154fdb2d630f45e1ecae4a862915435e663248bb8461d914696fc047cd"
        ),
        blake2s256=hash_to_bytes(
            "849c20fad132b7c2d62c15de310adfe87be94a379941bed295e8141c6219810d"
        ),
        status="visible",
    )
    content3 = Content(
        data=b"424242\n",
        length=7,
        sha1=hash_to_bytes("3e21cc4942a4234c9e5edd8a9cacd1670fe59f13"),
        sha1_git=hash_to_bytes("c932c7649c6dfa4b82327d121215116909eb3bea"),
        sha256=hash_to_bytes(
            "92fb72daf8c6818288a35137b72155f507e5de8d892712ab96277aaed8cf8a36"
        ),
        blake2s256=hash_to_bytes(
            "76d0346f44e5a27f6bafdd9c2befd304aff83780f93121d801ab6a1d4769db11"
        ),
        status="visible",
        ctime=datetime.datetime(2019, 12, 1, tzinfo=datetime.timezone.utc),
    )
    contents: Tuple[Content, ...] = (content, content2, content3)

    skipped_content = SkippedContent(
        length=1024 * 1024 * 200,
        sha1_git=hash_to_bytes("33e45d56f88993aae6a0198013efa80716fd8920"),
        sha1=hash_to_bytes("43e45d56f88993aae6a0198013efa80716fd8920"),
        sha256=hash_to_bytes(
            "7bbd052ab054ef222c1c87be60cd191addedd24cc882d1f5f7f7be61dc61bb3a"
        ),
        blake2s256=hash_to_bytes(
            "ade18b1adecb33f891ca36664da676e12c772cc193778aac9a137b8dc5834b9b"
        ),
        reason="Content too long",
        status="absent",
        origin="file:///dev/zero",
    )
    skipped_content2 = SkippedContent(
        length=1024 * 1024 * 300,
        sha1_git=hash_to_bytes("44e45d56f88993aae6a0198013efa80716fd8921"),
        sha1=hash_to_bytes("54e45d56f88993aae6a0198013efa80716fd8920"),
        sha256=hash_to_bytes(
            "8cbd052ab054ef222c1c87be60cd191addedd24cc882d1f5f7f7be61dc61bb3a"
        ),
        blake2s256=hash_to_bytes(
            "9ce18b1adecb33f891ca36664da676e12c772cc193778aac9a137b8dc5834b9b"
        ),
        reason="Content too long",
        status="absent",
    )
    skipped_contents: Tuple[SkippedContent, ...] = (skipped_content, skipped_content2)

    directory5 = Directory(entries=())
    directory = Directory(
        id=hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),
        entries=tuple(
            [
                DirectoryEntry(
                    name=b"foo",
                    type="file",
                    target=content.sha1_git,
                    perms=from_disk.DentryPerms.content,
                ),
                DirectoryEntry(
                    name=b"bar\xc3",
                    type="dir",
                    target=directory5.id,
                    perms=from_disk.DentryPerms.directory,
                ),
            ],
        ),
    )
    directory2 = Directory(
        id=hash_to_bytes("8505808532953da7d2581741f01b29c04b1cb9ab"),
        entries=tuple(
            [
                DirectoryEntry(
                    name=b"oof",
                    type="file",
                    target=content2.sha1_git,
                    perms=from_disk.DentryPerms.content,
                )
            ],
        ),
    )
    directory3 = Directory(
        id=hash_to_bytes("4ea8c6b2f54445e5dd1a9d5bb2afd875d66f3150"),
        entries=tuple(
            [
                DirectoryEntry(
                    name=b"foo",
                    type="file",
                    target=content.sha1_git,
                    perms=from_disk.DentryPerms.content,
                ),
                DirectoryEntry(
                    name=b"subdir",
                    type="dir",
                    target=directory.id,
                    perms=from_disk.DentryPerms.directory,
                ),
                DirectoryEntry(
                    name=b"hello",
                    type="file",
                    target=content2.sha1_git,
                    perms=from_disk.DentryPerms.content,
                ),
            ],
        ),
    )
    directory4 = Directory(
        id=hash_to_bytes("377aa5fcd944fbabf502dbfda55cd14d33c8c3c6"),
        entries=tuple(
            [
                DirectoryEntry(
                    name=b"subdir1",
                    type="dir",
                    target=directory3.id,
                    perms=from_disk.DentryPerms.directory,
                )
            ],
        ),
    )
    directories: Tuple[Directory, ...] = (
        directory2,
        directory,
        directory3,
        directory4,
        directory5,
    )

    revision = Revision(
        id=hash_to_bytes("066b1b62dbfa033362092af468bf6cfabec230e7"),
        message=b"hello",
        author=Person(
            name=b"Nicolas Dandrimont",
            email=b"nicolas@example.com",
            fullname=b"Nicolas Dandrimont <nicolas@example.com> ",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0),
            offset=120,
            negative_utc=False,
        ),
        committer=Person(
            name=b"St\xc3fano Zacchiroli",
            email=b"stefano@example.com",
            fullname=b"St\xc3fano Zacchiroli <stefano@example.com>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1123456789, microseconds=0),
            offset=120,
            negative_utc=False,
        ),
        parents=(),
        type=RevisionType.GIT,
        directory=directory.id,
        metadata={
            "checksums": {"sha1": "tarball-sha1", "sha256": "tarball-sha256",},
            "signed-off-by": "some-dude",
        },
        extra_headers=(
            (b"gpgsig", b"test123"),
            (b"mergetag", b"foo\\bar"),
            (b"mergetag", b"\x22\xaf\x89\x80\x01\x00"),
        ),
        synthetic=True,
    )
    revision2 = Revision(
        id=hash_to_bytes("df7a6f6a99671fb7f7343641aff983a314ef6161"),
        message=b"hello again",
        author=Person(
            name=b"Roberto Dicosmo",
            email=b"roberto@example.com",
            fullname=b"Roberto Dicosmo <roberto@example.com>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567843, microseconds=220000,),
            offset=-720,
            negative_utc=False,
        ),
        committer=Person(
            name=b"tony", email=b"ar@dumont.fr", fullname=b"tony <ar@dumont.fr>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1123456789, microseconds=220000,),
            offset=0,
            negative_utc=False,
        ),
        parents=tuple([revision.id]),
        type=RevisionType.GIT,
        directory=directory2.id,
        metadata=None,
        extra_headers=(),
        synthetic=False,
    )
    revision3 = Revision(
        id=hash_to_bytes("2cbd7bb22c653bbb23a29657852a50a01b591d46"),
        message=b"a simple revision with no parents this time",
        author=Person(
            name=b"Roberto Dicosmo",
            email=b"roberto@example.com",
            fullname=b"Roberto Dicosmo <roberto@example.com>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567843, microseconds=220000,),
            offset=-720,
            negative_utc=False,
        ),
        committer=Person(
            name=b"tony", email=b"ar@dumont.fr", fullname=b"tony <ar@dumont.fr>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1127351742, microseconds=220000,),
            offset=0,
            negative_utc=False,
        ),
        parents=tuple([revision.id, revision2.id]),
        type=RevisionType.GIT,
        directory=directory2.id,
        metadata=None,
        extra_headers=(),
        synthetic=True,
    )
    revision4 = Revision(
        id=hash_to_bytes("88cd5126fc958ed70089d5340441a1c2477bcc20"),
        message=b"parent of self.revision2",
        author=Person(
            name=b"me", email=b"me@soft.heri", fullname=b"me <me@soft.heri>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567843, microseconds=220000,),
            offset=-720,
            negative_utc=False,
        ),
        committer=Person(
            name=b"committer-dude",
            email=b"committer@dude.com",
            fullname=b"committer-dude <committer@dude.com>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1244567843, microseconds=220000,),
            offset=-720,
            negative_utc=False,
        ),
        parents=tuple([revision3.id]),
        type=RevisionType.GIT,
        directory=directory.id,
        metadata=None,
        extra_headers=(),
        synthetic=False,
    )
    revisions: Tuple[Revision, ...] = (revision, revision2, revision3, revision4)

    origins: Tuple[Origin, ...] = (
        Origin(url="https://github.com/user1/repo1"),
        Origin(url="https://github.com/user2/repo1"),
        Origin(url="https://github.com/user3/repo1"),
        Origin(url="https://gitlab.com/user1/repo1"),
        Origin(url="https://gitlab.com/user2/repo1"),
        Origin(url="https://forge.softwareheritage.org/source/repo1"),
    )
    origin, origin2 = origins[:2]

    metadata_authority = MetadataAuthority(
        type=MetadataAuthorityType.DEPOSIT_CLIENT,
        url="http://hal.inria.example.com/",
        metadata={"location": "France"},
    )
    metadata_authority2 = MetadataAuthority(
        type=MetadataAuthorityType.REGISTRY,
        url="http://wikidata.example.com/",
        metadata={},
    )
    authorities: Tuple[MetadataAuthority, ...] = (
        metadata_authority,
        metadata_authority2,
    )

    metadata_fetcher = MetadataFetcher(
        name="swh-deposit", version="0.0.1", metadata={"sword_version": "2"},
    )
    metadata_fetcher2 = MetadataFetcher(
        name="swh-example", version="0.0.1", metadata={},
    )
    fetchers: Tuple[MetadataFetcher, ...] = (metadata_fetcher, metadata_fetcher2)

    date_visit1 = datetime.datetime(2015, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
    date_visit2 = datetime.datetime(2017, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
    date_visit3 = datetime.datetime(2018, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)

    type_visit1 = "git"
    type_visit2 = "hg"
    type_visit3 = "deb"

    origin_visit = OriginVisit(
        origin=origin.url, visit=1, date=date_visit1, type=type_visit1,
    )
    origin_visit2 = OriginVisit(
        origin=origin.url, visit=2, date=date_visit2, type=type_visit1,
    )
    origin_visit3 = OriginVisit(
        origin=origin2.url, visit=1, date=date_visit1, type=type_visit2,
    )
    origin_visits: Tuple[OriginVisit, ...] = (
        origin_visit,
        origin_visit2,
        origin_visit3,
    )

    release = Release(
        id=hash_to_bytes("a673e617fcc6234e29b2cad06b8245f96c415c61"),
        name=b"v0.0.1",
        author=Person(
            name=b"olasd", email=b"nic@olasd.fr", fullname=b"olasd <nic@olasd.fr>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0),
            offset=42,
            negative_utc=False,
        ),
        target=revision.id,
        target_type=ObjectType.REVISION,
        message=b"synthetic release",
        synthetic=True,
    )
    release2 = Release(
        id=hash_to_bytes("6902bd4c82b7d19a421d224aedab2b74197e420d"),
        name=b"v0.0.2",
        author=Person(
            name=b"tony", email=b"ar@dumont.fr", fullname=b"tony <ar@dumont.fr>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1634366813, microseconds=0),
            offset=-120,
            negative_utc=False,
        ),
        target=revision2.id,
        target_type=ObjectType.REVISION,
        message=b"v0.0.2\nMisc performance improvements + bug fixes",
        synthetic=False,
    )
    release3 = Release(
        id=hash_to_bytes("3e9050196aa288264f2a9d279d6abab8b158448b"),
        name=b"v0.0.2",
        author=Person(
            name=b"tony",
            email=b"tony@ardumont.fr",
            fullname=b"tony <tony@ardumont.fr>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1634366813, microseconds=0),
            offset=-120,
            negative_utc=False,
        ),
        target=revision3.id,
        target_type=ObjectType.REVISION,
        message=b"yet another synthetic release",
        synthetic=True,
    )

    releases: Tuple[Release, ...] = (release, release2, release3)

    snapshot = Snapshot(
        id=hash_to_bytes("409ee1ff3f10d166714bc90581debfd0446dda57"),
        branches={
            b"master": SnapshotBranch(
                target=revision.id, target_type=TargetType.REVISION,
            ),
        },
    )
    empty_snapshot = Snapshot(
        id=hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"), branches={},
    )
    complete_snapshot = Snapshot(
        id=hash_to_bytes("a56ce2d81c190023bb99a3a36279307522cb85f6"),
        branches={
            b"directory": SnapshotBranch(
                target=directory.id, target_type=TargetType.DIRECTORY,
            ),
            b"directory2": SnapshotBranch(
                target=directory2.id, target_type=TargetType.DIRECTORY,
            ),
            b"content": SnapshotBranch(
                target=content.sha1_git, target_type=TargetType.CONTENT,
            ),
            b"alias": SnapshotBranch(target=b"revision", target_type=TargetType.ALIAS,),
            b"revision": SnapshotBranch(
                target=revision.id, target_type=TargetType.REVISION,
            ),
            b"release": SnapshotBranch(
                target=release.id, target_type=TargetType.RELEASE,
            ),
            b"snapshot": SnapshotBranch(
                target=empty_snapshot.id, target_type=TargetType.SNAPSHOT,
            ),
            b"dangling": None,
        },
    )

    snapshots: Tuple[Snapshot, ...] = (snapshot, empty_snapshot, complete_snapshot)

    content_metadata1 = RawExtrinsicMetadata(
        type=MetadataTargetType.CONTENT,
        id=parse_swhid(f"swh:1:cnt:{hash_to_hex(content.sha1_git)}"),
        origin=origin.url,
        discovery_date=datetime.datetime(
            2015, 1, 1, 21, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority, metadata=None),
        fetcher=attr.evolve(metadata_fetcher, metadata=None),
        format="json",
        metadata=b'{"foo": "bar"}',
    )
    content_metadata2 = RawExtrinsicMetadata(
        type=MetadataTargetType.CONTENT,
        id=parse_swhid(f"swh:1:cnt:{hash_to_hex(content.sha1_git)}"),
        origin=origin2.url,
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority, metadata=None),
        fetcher=attr.evolve(metadata_fetcher, metadata=None),
        format="yaml",
        metadata=b"foo: bar",
    )
    content_metadata3 = RawExtrinsicMetadata(
        type=MetadataTargetType.CONTENT,
        id=parse_swhid(f"swh:1:cnt:{hash_to_hex(content.sha1_git)}"),
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority2, metadata=None),
        fetcher=attr.evolve(metadata_fetcher2, metadata=None),
        format="yaml",
        metadata=b"foo: bar",
        origin=origin.url,
        visit=42,
        snapshot=parse_swhid(f"swh:1:snp:{hash_to_hex(snapshot.id)}"),
        release=parse_swhid(f"swh:1:rel:{hash_to_hex(release.id)}"),
        revision=parse_swhid(f"swh:1:rev:{hash_to_hex(revision.id)}"),
        directory=parse_swhid(f"swh:1:dir:{hash_to_hex(directory.id)}"),
        path=b"/foo/bar",
    )

    content_metadata: Tuple[RawExtrinsicMetadata, ...] = (
        content_metadata1,
        content_metadata2,
        content_metadata3,
    )

    origin_metadata1 = RawExtrinsicMetadata(
        type=MetadataTargetType.ORIGIN,
        id=origin.url,
        discovery_date=datetime.datetime(
            2015, 1, 1, 21, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority, metadata=None),
        fetcher=attr.evolve(metadata_fetcher, metadata=None),
        format="json",
        metadata=b'{"foo": "bar"}',
    )
    origin_metadata2 = RawExtrinsicMetadata(
        type=MetadataTargetType.ORIGIN,
        id=origin.url,
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority, metadata=None),
        fetcher=attr.evolve(metadata_fetcher, metadata=None),
        format="yaml",
        metadata=b"foo: bar",
    )
    origin_metadata3 = RawExtrinsicMetadata(
        type=MetadataTargetType.ORIGIN,
        id=origin.url,
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority2, metadata=None),
        fetcher=attr.evolve(metadata_fetcher2, metadata=None),
        format="yaml",
        metadata=b"foo: bar",
    )

    origin_metadata: Tuple[RawExtrinsicMetadata, ...] = (
        origin_metadata1,
        origin_metadata2,
        origin_metadata3,
    )
