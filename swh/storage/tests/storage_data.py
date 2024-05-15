# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Tuple

import attr

from swh.model import from_disk
from swh.model.hashutil import hash_to_bytes
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ExtID,
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    ObjectType,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Person,
    RawExtrinsicMetadata,
    Release,
    Revision,
    RevisionType,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.model.swhids import ObjectType as SwhidObjectType


class StorageData:
    """Data model objects to use within tests."""

    content = Content(
        data=b"42\n",
        length=3,
        sha1=hash_to_bytes("34973274ccef6ab4dfaaf86599792fa9c3fe4689"),
        sha1_git=hash_to_bytes("d81cc0710eb6cf9efd5b920a8453e1e07157b6cd"),
        sha256=hash_to_bytes(
            "084c799cd551dd1d8d5c5f9a5d593b2e931f5e36122ee5c793c1d08a19839cc0"
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

    directory5 = Directory(
        id=hash_to_bytes("4b825dc642cb6eb9a060e54bf8d69288fbee4904"),
        entries=(),
    )
    directory = Directory(
        id=hash_to_bytes("5256e856a0a0898966d6ba14feb4388b8b82d302"),
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
        id=hash_to_bytes("13089e6e544f78df7c9a40a3059050d10dee686a"),
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
        id=hash_to_bytes("cd5dfd9c09d9e99ed123bc7937a0d5fddc3cd531"),
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

    directory6 = Directory(
        id=hash_to_bytes("afa0105cfcaa14fdbacee344e96659170bb1bda5"),
        entries=tuple(
            [
                DirectoryEntry(
                    name=b"foo",
                    type="file",
                    target=b"\x00" * 20,
                    perms=from_disk.DentryPerms.content,
                ),
                DirectoryEntry(
                    name=b"bar",
                    type="dir",
                    target=b"\x01" * 20,
                    perms=from_disk.DentryPerms.directory,
                ),
            ],
        ),
        raw_manifest=(
            b"tree 61\x00"
            b"100644 foo\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # noqa
            b"40000 bar\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01"  # noqa
        ),
    )

    directory7 = Directory(
        id=hash_to_bytes("a720da7e7ba5af69498f884f91ca7f4ed16997be"),
        entries=tuple(
            [
                DirectoryEntry(
                    name=b"subdir1",
                    type="dir",
                    target=directory5.id,
                    perms=from_disk.DentryPerms.directory,
                ),
                DirectoryEntry(
                    name=b"subdir2",
                    type="dir",
                    target=directory5.id,
                    perms=from_disk.DentryPerms.directory,
                ),
                DirectoryEntry(
                    name=b"subdir3",
                    type="dir",
                    target=directory4.id,
                    perms=from_disk.DentryPerms.directory,
                ),
            ],
        ),
    )

    directories: Tuple[Directory, ...] = (
        directory2,
        directory,
        directory3,
        directory4,
        directory5,
        directory6,
        directory7,
    )

    revision = Revision(
        id=hash_to_bytes("01a7114f36fddd5ef2511b2cadda237a68adbb12"),
        message=b"hello",
        author=Person(
            name=b"Nicolas Dandrimont",
            email=b"nicolas@example.com",
            fullname=b"Nicolas Dandrimont <nicolas@example.com> ",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0),
            offset_bytes=b"+0200",
        ),
        committer=Person(
            name=b"St\xc3fano Zacchiroli",
            email=b"stefano@example.com",
            fullname=b"St\xc3fano Zacchiroli <stefano@example.com>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1123456789, microseconds=0),
            offset_bytes=b"+0200",
        ),
        parents=(),
        type=RevisionType.GIT,
        directory=directory.id,
        metadata={
            "checksums": {
                "sha1": "tarball-sha1",
                "sha256": "tarball-sha256",
            },
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
        id=hash_to_bytes("a646dd94c912829659b22a1e7e143d2fa5ebde1b"),
        message=b"hello again",
        author=Person(
            name=b"Roberto Dicosmo",
            email=b"roberto@example.com",
            fullname=b"Roberto Dicosmo <roberto@example.com>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        committer=Person(
            name=b"tony",
            email=b"ar@dumont.fr",
            fullname=b"tony <ar@dumont.fr>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1123456789,
                microseconds=220000,
            ),
            offset_bytes=b"+0000",
        ),
        parents=tuple([revision.id]),
        type=RevisionType.GIT,
        directory=directory2.id,
        metadata=None,
        extra_headers=(),
        synthetic=False,
    )
    revision3 = Revision(
        id=hash_to_bytes("beb2844dff30658e27573cb46eb55980e974b391"),
        message=b"a simple revision with no parents this time",
        author=Person(
            name=b"Roberto Dicosmo",
            email=b"roberto@example.com",
            fullname=b"Roberto Dicosmo <roberto@example.com>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        committer=Person(
            name=b"tony",
            email=b"ar@dumont.fr",
            fullname=b"tony <ar@dumont.fr>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1127351742,
                microseconds=220000,
            ),
            offset_bytes=b"+0000",
        ),
        parents=tuple([revision.id, revision2.id]),
        type=RevisionType.GIT,
        directory=directory2.id,
        metadata=None,
        extra_headers=(),
        synthetic=True,
    )
    revision4 = Revision(
        id=hash_to_bytes("ae860aec43700c7f5a295e2ef47e2ae41b535dfe"),
        message=b"parent of self.revision2",
        author=Person(
            name=b"me",
            email=b"me@soft.heri",
            fullname=b"me <me@soft.heri>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        committer=Person(
            name=b"committer-dude",
            email=b"committer@dude.com",
            fullname=b"committer-dude <committer@dude.com>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1244567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        parents=tuple([revision3.id]),
        type=RevisionType.GIT,
        directory=directory.id,
        metadata=None,
        extra_headers=(),
        synthetic=False,
    )
    git_revisions: Tuple[Revision, ...] = (revision, revision2, revision3, revision4)

    hg_revision = Revision(
        id=hash_to_bytes("951c9503541e7beaf002d7aebf2abd1629084c68"),
        message=b"hello",
        author=Person(
            name=b"Nicolas Dandrimont",
            email=b"nicolas@example.com",
            fullname=b"Nicolas Dandrimont <nicolas@example.com> ",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0),
            offset_bytes=b"+0200",
        ),
        committer=Person(
            name=b"St\xc3fano Zacchiroli",
            email=b"stefano@example.com",
            fullname=b"St\xc3fano Zacchiroli <stefano@example.com>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1123456789, microseconds=0),
            offset_bytes=b"+0200",
        ),
        parents=(),
        type=RevisionType.MERCURIAL,
        directory=directory.id,
        metadata={
            "checksums": {
                "sha1": "tarball-sha1",
                "sha256": "tarball-sha256",
            },
            "signed-off-by": "some-dude",
            "node": "a316dfb434af2b451c1f393496b7eaeda343f543",
        },
        extra_headers=(),
        synthetic=True,
    )
    hg_revision2 = Revision(
        id=hash_to_bytes("df4afb063236300eb13b96a0d7fff03f7b7cbbaf"),
        message=b"hello again",
        author=Person(
            name=b"Roberto Dicosmo",
            email=b"roberto@example.com",
            fullname=b"Roberto Dicosmo <roberto@example.com>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        committer=Person(
            name=b"tony",
            email=b"ar@dumont.fr",
            fullname=b"tony <ar@dumont.fr>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1123456789,
                microseconds=220000,
            ),
            offset_bytes=b"+0000",
        ),
        parents=tuple([hg_revision.id]),
        type=RevisionType.MERCURIAL,
        directory=directory2.id,
        metadata=None,
        extra_headers=(
            (b"node", hash_to_bytes("fa1b7c84a9b40605b67653700f268349a6d6aca1")),
        ),
        synthetic=False,
    )
    hg_revision3 = Revision(
        id=hash_to_bytes("84d8e7081b47ebb88cad9fa1f25de5f330872a37"),
        message=b"a simple revision with no parents this time",
        author=Person(
            name=b"Roberto Dicosmo",
            email=b"roberto@example.com",
            fullname=b"Roberto Dicosmo <roberto@example.com>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        committer=Person(
            name=b"tony",
            email=b"ar@dumont.fr",
            fullname=b"tony <ar@dumont.fr>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1127351742,
                microseconds=220000,
            ),
            offset_bytes=b"+0000",
        ),
        parents=tuple([hg_revision.id, hg_revision2.id]),
        type=RevisionType.MERCURIAL,
        directory=directory2.id,
        metadata=None,
        extra_headers=(
            (b"node", hash_to_bytes("7f294a01c49065a90b3fe8b4ad49f08ce9656ef6")),
        ),
        synthetic=True,
    )
    hg_revision4 = Revision(
        id=hash_to_bytes("4683324ba26dfe941a72cc7552e86eaaf7c27fe3"),
        message=b"parent of self.revision2",
        author=Person(
            name=b"me",
            email=b"me@soft.heri",
            fullname=b"me <me@soft.heri>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        committer=Person(
            name=b"committer-dude",
            email=b"committer@dude.com",
            fullname=b"committer-dude <committer@dude.com>",
        ),
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1244567843,
                microseconds=220000,
            ),
            offset_bytes=b"-1200",
        ),
        parents=tuple([hg_revision3.id]),
        type=RevisionType.MERCURIAL,
        directory=directory.id,
        metadata=None,
        extra_headers=(
            (b"node", hash_to_bytes("f4160af0485c85823d9e829bae2c00b00a2e6297")),
        ),
        synthetic=False,
    )
    hg_revisions: Tuple[Revision, ...] = (
        hg_revision,
        hg_revision2,
        hg_revision3,
        hg_revision4,
    )
    revisions: Tuple[Revision, ...] = git_revisions + hg_revisions

    origins: Tuple[Origin, ...] = (
        Origin(url="https://github.com/user1/repo1"),
        Origin(url="https://github.com/user2/repo1"),
        Origin(url="https://github.com/user3/repo1"),
        Origin(url="https://gitlab.com/user1/repo1"),
        Origin(url="https://gitlab.com/user2/repo1"),
        Origin(url="https://forge.softwareheritage.org/source/repo1"),
        Origin(url="https://example.рф/🏛️.txt"),
    )
    origin, origin2 = origins[:2]

    metadata_authority = MetadataAuthority(
        type=MetadataAuthorityType.DEPOSIT_CLIENT,
        url="http://hal.inria.example.com/",
    )
    metadata_authority2 = MetadataAuthority(
        type=MetadataAuthorityType.REGISTRY,
        url="http://wikidata.example.com/",
    )
    authorities: Tuple[MetadataAuthority, ...] = (
        metadata_authority,
        metadata_authority2,
    )

    metadata_fetcher = MetadataFetcher(
        name="swh-deposit",
        version="0.0.1",
    )
    metadata_fetcher2 = MetadataFetcher(
        name="swh-example",
        version="0.0.1",
    )
    fetchers: Tuple[MetadataFetcher, ...] = (metadata_fetcher, metadata_fetcher2)

    date_visit1 = datetime.datetime(2015, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
    date_visit2 = datetime.datetime(2017, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)
    date_visit3 = datetime.datetime(2018, 1, 1, 23, 0, 0, tzinfo=datetime.timezone.utc)

    type_visit1 = "git"
    type_visit2 = "hg"
    type_visit3 = "deb"

    origin_visit = OriginVisit(
        origin=origin.url,
        visit=1,
        date=date_visit1,
        type=type_visit1,
    )
    origin_visit2 = OriginVisit(
        origin=origin.url,
        visit=2,
        date=date_visit2,
        type=type_visit1,
    )
    origin_visit3 = OriginVisit(
        origin=origin2.url,
        visit=1,
        date=date_visit1,
        type=type_visit2,
    )
    origin_visits: Tuple[OriginVisit, ...] = (
        origin_visit,
        origin_visit2,
        origin_visit3,
    )

    origin_visit_status = OriginVisitStatus(
        origin=origin.url,
        visit=1,
        date=date_visit1,
        type=type_visit1,
        status="created",
        snapshot=None,
    )
    origin_visit2_status = OriginVisitStatus(
        origin=origin.url,
        visit=2,
        date=date_visit2,
        type=type_visit1,
        status="created",
        snapshot=None,
    )
    origin_visit3_status = OriginVisitStatus(
        origin=origin2.url,
        visit=1,
        date=date_visit1,
        type=type_visit2,
        status="created",
        snapshot=None,
    )
    origin_visit_statuses: Tuple[OriginVisitStatus, ...] = (
        origin_visit_status,
        origin_visit2_status,
        origin_visit3_status,
    )

    release = Release(
        id=hash_to_bytes("f7f222093a18ec60d781070abec4a630c850b837"),
        name=b"v0.0.1",
        author=Person(
            name=b"olasd",
            email=b"nic@olasd.fr",
            fullname=b"olasd <nic@olasd.fr>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1234567890, microseconds=0),
            offset_bytes=b"+0042",
        ),
        target=revision.id,
        target_type=ObjectType.REVISION,
        message=b"synthetic release",
        synthetic=True,
    )
    release2 = Release(
        id=hash_to_bytes("db81a26783a3f4a9db07b4759ffc37621f159bb2"),
        name=b"v0.0.2",
        author=Person(
            name=b"tony",
            email=b"ar@dumont.fr",
            fullname=b"tony <ar@dumont.fr>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1634366813, microseconds=0),
            offset_bytes=b"-0200",
        ),
        target=revision2.id,
        target_type=ObjectType.REVISION,
        message=b"v0.0.2\nMisc performance improvements + bug fixes",
        synthetic=False,
    )
    release3 = Release(
        id=hash_to_bytes("1c5d42e603ce2eea44917fadca76c78bad76aeb9"),
        name=b"v0.0.2",
        author=Person(
            name=b"tony",
            email=b"tony@ardumont.fr",
            fullname=b"tony <tony@ardumont.fr>",
        ),
        date=TimestampWithTimezone(
            timestamp=Timestamp(seconds=1634366813, microseconds=0),
            offset_bytes=b"-0200",
        ),
        target=revision3.id,
        target_type=ObjectType.REVISION,
        message=b"yet another synthetic release",
        synthetic=True,
    )

    releases: Tuple[Release, ...] = (release, release2, release3)

    snapshot = Snapshot(
        id=hash_to_bytes("9b922e6d8d5b803c1582aabe5525b7b91150788e"),
        branches={
            b"master": SnapshotBranch(
                target=revision.id,
                target_type=SnapshotTargetType.REVISION,
            ),
        },
    )
    empty_snapshot = Snapshot(
        id=hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"),
        branches={},
    )
    complete_snapshot = Snapshot(
        id=hash_to_bytes("db99fda25b43dc5cd90625ee4b0744751799c917"),
        branches={
            b"directory": SnapshotBranch(
                target=directory.id,
                target_type=SnapshotTargetType.DIRECTORY,
            ),
            b"directory2": SnapshotBranch(
                target=directory2.id,
                target_type=SnapshotTargetType.DIRECTORY,
            ),
            b"content": SnapshotBranch(
                target=content.sha1_git,
                target_type=SnapshotTargetType.CONTENT,
            ),
            b"alias": SnapshotBranch(
                target=b"revision",
                target_type=SnapshotTargetType.ALIAS,
            ),
            b"revision": SnapshotBranch(
                target=revision.id,
                target_type=SnapshotTargetType.REVISION,
            ),
            b"release": SnapshotBranch(
                target=release.id,
                target_type=SnapshotTargetType.RELEASE,
            ),
            b"snapshot": SnapshotBranch(
                target=empty_snapshot.id,
                target_type=SnapshotTargetType.SNAPSHOT,
            ),
            b"dangling": None,
        },
    )

    snapshots: Tuple[Snapshot, ...] = (snapshot, empty_snapshot, complete_snapshot)

    content_metadata1 = RawExtrinsicMetadata(
        target=ExtendedSWHID(
            object_type=ExtendedObjectType.CONTENT, object_id=content.sha1_git
        ),
        origin=origin.url,
        discovery_date=datetime.datetime(
            2015, 1, 1, 21, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=metadata_authority,
        fetcher=metadata_fetcher,
        format="json",
        metadata=b'{"foo": "bar"}',
    )
    content_metadata2 = RawExtrinsicMetadata(
        target=ExtendedSWHID(
            object_type=ExtendedObjectType.CONTENT, object_id=content.sha1_git
        ),
        origin=origin2.url,
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=metadata_authority,
        fetcher=metadata_fetcher,
        format="yaml",
        metadata=b"foo: bar",
    )
    content_metadata3 = RawExtrinsicMetadata(
        target=ExtendedSWHID(
            object_type=ExtendedObjectType.CONTENT, object_id=content.sha1_git
        ),
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority2, metadata=None),
        fetcher=attr.evolve(metadata_fetcher2, metadata=None),
        format="yaml",
        metadata=b"foo: bar",
        origin=origin.url,
        visit=42,
        snapshot=snapshot.swhid(),
        release=release.swhid(),
        revision=revision.swhid(),
        directory=directory.swhid(),
        path=b"/foo/bar",
    )

    content_metadata: Tuple[RawExtrinsicMetadata, ...] = (
        content_metadata1,
        content_metadata2,
        content_metadata3,
    )

    origin_metadata1 = RawExtrinsicMetadata(
        target=Origin(origin.url).swhid(),
        discovery_date=datetime.datetime(
            2015, 1, 1, 21, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority, metadata=None),
        fetcher=attr.evolve(metadata_fetcher, metadata=None),
        format="json",
        metadata=b'{"foo": "bar"}',
    )
    origin_metadata2 = RawExtrinsicMetadata(
        target=Origin(origin.url).swhid(),
        discovery_date=datetime.datetime(
            2017, 1, 1, 22, 0, 0, tzinfo=datetime.timezone.utc
        ),
        authority=attr.evolve(metadata_authority, metadata=None),
        fetcher=attr.evolve(metadata_fetcher, metadata=None),
        format="yaml",
        metadata=b"foo: bar",
    )
    origin_metadata3 = RawExtrinsicMetadata(
        target=Origin(origin.url).swhid(),
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

    extid1 = ExtID(
        target=CoreSWHID(object_type=SwhidObjectType.REVISION, object_id=revision.id),
        extid_type="git",
        extid=revision.id,
    )

    extid2 = ExtID(
        target=CoreSWHID(
            object_type=SwhidObjectType.REVISION, object_id=hg_revision.id
        ),
        extid_type="mercurial",
        extid=hash_to_bytes("a316dfb434af2b451c1f393496b7eaeda343f543"),
    )

    extid3 = ExtID(
        target=CoreSWHID(object_type=SwhidObjectType.DIRECTORY, object_id=directory.id),
        extid_type="directory",
        extid=b"something",
    )
    extid4 = ExtID(
        target=CoreSWHID(
            object_type=SwhidObjectType.DIRECTORY, object_id=directory2.id
        ),
        extid_type="directory",
        extid=b"something",
        extid_version=2,
    )

    extids: Tuple[ExtID, ...] = (
        extid1,
        extid2,
        extid3,
        extid4,
    )
