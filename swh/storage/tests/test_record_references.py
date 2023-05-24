#!/usr/bin/env python3

from collections import defaultdict
from typing import Set, Tuple

import pytest

from swh.model.model import OriginVisitStatus
from swh.model.swhids import ExtendedSWHID
from swh.storage import get_storage
from swh.storage.interface import StorageInterface


@pytest.fixture
def swh_storage(mocker):
    storage = get_storage(
        cls="pipeline",
        steps=[
            {"cls": "record_references"},
            {"cls": "memory"},
        ],
    )

    return mocker.Mock(wraps=storage)


def check_references_recorded(
    storage: StorageInterface, references: Set[Tuple[str, str]]
) -> None:
    expected = defaultdict(set)

    for source, target in references:
        expected[ExtendedSWHID.from_string(target)].add(
            ExtendedSWHID.from_string(source)
        )

    for target_swhid, expected_sources in expected.items():
        recorded_refs = storage.object_find_recent_references(
            target_swhid, limit=len(expected_sources) + 1
        )

        assert set(recorded_refs) == expected_sources


def test_record_references_adding_content(swh_storage, sample_data):
    stats = swh_storage.content_add(sample_data.contents)
    assert "object_reference:add" not in stats
    swh_storage.object_references_add.assert_not_called()


def test_record_references_adding_directories(swh_storage, sample_data):
    stats = swh_storage.directory_add(
        [
            sample_data.directory,
            sample_data.directory2,
            sample_data.directory3,
            sample_data.directory5,
        ]
    )

    assert stats["directory:add"] > 0
    assert stats["object_reference:add"] == 6

    check_references_recorded(
        swh_storage,
        {
            # directory → content
            (
                "swh:1:dir:5256e856a0a0898966d6ba14feb4388b8b82d302",
                "swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd",
            ),
            # directory → directory5
            (
                "swh:1:dir:5256e856a0a0898966d6ba14feb4388b8b82d302",
                "swh:1:dir:4b825dc642cb6eb9a060e54bf8d69288fbee4904",
            ),
            # directory2 → content2
            (
                "swh:1:dir:8505808532953da7d2581741f01b29c04b1cb9ab",
                "swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa",
            ),
            # directory3 → content
            (
                "swh:1:dir:13089e6e544f78df7c9a40a3059050d10dee686a",
                "swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd",
            ),
            # directory3 → directory
            (
                "swh:1:dir:13089e6e544f78df7c9a40a3059050d10dee686a",
                "swh:1:dir:5256e856a0a0898966d6ba14feb4388b8b82d302",
            ),
            # directory3 → content2
            (
                "swh:1:dir:13089e6e544f78df7c9a40a3059050d10dee686a",
                "swh:1:cnt:36fade77193cb6d2bd826161a0979d64c28ab4fa",
            ),
        },
    )


def test_record_references_adding_revisions(swh_storage, sample_data):
    stats = swh_storage.revision_add(sample_data.git_revisions)

    assert stats["revision:add"] > 0
    assert stats["object_reference:add"] == 8

    check_references_recorded(
        swh_storage,
        {
            # revision → directory
            (
                "swh:1:rev:01a7114f36fddd5ef2511b2cadda237a68adbb12",
                "swh:1:dir:5256e856a0a0898966d6ba14feb4388b8b82d302",
            ),
            # revision2 → directory2
            (
                "swh:1:rev:a646dd94c912829659b22a1e7e143d2fa5ebde1b",
                "swh:1:dir:8505808532953da7d2581741f01b29c04b1cb9ab",
            ),
            # revision2 → revision
            (
                "swh:1:rev:a646dd94c912829659b22a1e7e143d2fa5ebde1b",
                "swh:1:rev:01a7114f36fddd5ef2511b2cadda237a68adbb12",
            ),
            # revision3 → directory2
            (
                "swh:1:rev:beb2844dff30658e27573cb46eb55980e974b391",
                "swh:1:dir:8505808532953da7d2581741f01b29c04b1cb9ab",
            ),
            # revision3 → revision
            (
                "swh:1:rev:beb2844dff30658e27573cb46eb55980e974b391",
                "swh:1:rev:01a7114f36fddd5ef2511b2cadda237a68adbb12",
            ),
            # revision3 → revision2
            (
                "swh:1:rev:beb2844dff30658e27573cb46eb55980e974b391",
                "swh:1:rev:a646dd94c912829659b22a1e7e143d2fa5ebde1b",
            ),
            # revision4 → directory
            (
                "swh:1:rev:ae860aec43700c7f5a295e2ef47e2ae41b535dfe",
                "swh:1:dir:5256e856a0a0898966d6ba14feb4388b8b82d302",
            ),
            # revision4 → revision3
            (
                "swh:1:rev:ae860aec43700c7f5a295e2ef47e2ae41b535dfe",
                "swh:1:rev:beb2844dff30658e27573cb46eb55980e974b391",
            ),
        },
    )


def test_record_references_adding_releases(swh_storage, sample_data):
    stats = swh_storage.release_add(sample_data.releases)

    assert stats["release:add"] > 0
    assert stats["object_reference:add"] == 3

    check_references_recorded(
        swh_storage,
        {
            # release → revision
            (
                "swh:1:rel:f7f222093a18ec60d781070abec4a630c850b837",
                "swh:1:rev:01a7114f36fddd5ef2511b2cadda237a68adbb12",
            ),
            # release2 → revision2
            (
                "swh:1:rel:db81a26783a3f4a9db07b4759ffc37621f159bb2",
                "swh:1:rev:a646dd94c912829659b22a1e7e143d2fa5ebde1b",
            ),
            # release3 → revision3
            (
                "swh:1:rel:1c5d42e603ce2eea44917fadca76c78bad76aeb9",
                "swh:1:rev:beb2844dff30658e27573cb46eb55980e974b391",
            ),
        },
    )


def test_record_references_adding_snapshots(swh_storage, sample_data):
    stats = swh_storage.snapshot_add(sample_data.snapshots)
    assert stats["snapshot:add"] > 0
    assert stats["object_reference:add"] == 7

    check_references_recorded(
        swh_storage,
        {
            # snapshot → revision
            (
                "swh:1:snp:9b922e6d8d5b803c1582aabe5525b7b91150788e",
                "swh:1:rev:01a7114f36fddd5ef2511b2cadda237a68adbb12",
            ),
            # complete_snapshot → directory
            (
                "swh:1:snp:db99fda25b43dc5cd90625ee4b0744751799c917",
                "swh:1:dir:5256e856a0a0898966d6ba14feb4388b8b82d302",
            ),
            # complete_snapshot → directory2
            (
                "swh:1:snp:db99fda25b43dc5cd90625ee4b0744751799c917",
                "swh:1:dir:8505808532953da7d2581741f01b29c04b1cb9ab",
            ),
            # complete_snapshot → content
            (
                "swh:1:snp:db99fda25b43dc5cd90625ee4b0744751799c917",
                "swh:1:cnt:d81cc0710eb6cf9efd5b920a8453e1e07157b6cd",
            ),
            # complete_snapshot → revision
            (
                "swh:1:snp:db99fda25b43dc5cd90625ee4b0744751799c917",
                "swh:1:rev:01a7114f36fddd5ef2511b2cadda237a68adbb12",
            ),
            # complete_snapshot → release
            (
                "swh:1:snp:db99fda25b43dc5cd90625ee4b0744751799c917",
                "swh:1:rel:f7f222093a18ec60d781070abec4a630c850b837",
            ),
            # complete_snapshot → empty_snapshot
            (
                "swh:1:snp:db99fda25b43dc5cd90625ee4b0744751799c917",
                "swh:1:snp:1a8893e6a86f444e8be8e7bda6cb34fb1735a00e",
            ),
        },
    )


def test_record_references_adding_origin_visit_statuses(swh_storage, sample_data):
    swh_storage.origin_add([sample_data.origin])

    # The one in sample_data does not reference a snapshot
    origin_visit_status = OriginVisitStatus(
        origin=sample_data.origin.url,
        visit=1,
        date=sample_data.date_visit1,
        type=sample_data.type_visit1,
        status="created",
        snapshot=sample_data.snapshot.id,
    )
    stats = swh_storage.origin_visit_status_add(
        [origin_visit_status, sample_data.origin_visit2_status]
    )

    assert stats["origin_visit_status:add"] > 0
    assert stats["object_reference:add"] == 1

    check_references_recorded(
        swh_storage,
        {
            # origin → snapshot
            (
                "swh:1:ori:33abd4b4c5db79c7387673f71302750fd73e0645",
                "swh:1:snp:9b922e6d8d5b803c1582aabe5525b7b91150788e",
            ),
        },
    )
