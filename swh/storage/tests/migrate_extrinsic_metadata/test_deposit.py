# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# flake8: noqa
# because of long lines

import copy
import datetime
import json
from unittest.mock import MagicMock, Mock, call

from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    RawExtrinsicMetadata,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.storage.migrate_extrinsic_metadata import (
    DEPOSIT_COLS,
    cran_package_from_url,
    handle_row,
)

FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions",
    version="0.0.1",
)
SWH_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.REGISTRY,
    url="https://softwareheritage.org/",
    metadata={},
)
SWH_DEPOSIT_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.DEPOSIT_CLIENT,
    url="https://www.softwareheritage.org",
    metadata={},
)
HAL_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.DEPOSIT_CLIENT,
    url="https://hal.archives-ouvertes.fr/",
    metadata={},
)
INTEL_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.DEPOSIT_CLIENT,
    url="https://software.intel.com",
    metadata={},
)

DIRECTORY_ID = b"a" * 20
DIRECTORY_SWHID = ExtendedSWHID(
    object_type=ExtendedObjectType.DIRECTORY, object_id=DIRECTORY_ID
)


def get_mock_deposit_cur(row_dicts):
    rows = [tuple(d[key] for key in DEPOSIT_COLS) for d in row_dicts]
    deposit_cur = MagicMock()
    deposit_cur.__iter__.side_effect = [iter(rows)]
    return deposit_cur


def test_deposit_1():
    """Has a provider and xmlns, and the metadata is in the revision twice
    (at the root of the metadata dict, and in
    metadata->extrinsic->raw->origin_metadata)"""
    extrinsic_metadata = {
        "title": "Je suis GPL",
        "@xmlns": "http://www.w3.org/2005/Atom",
        "client": "swh",
        "codemeta:url": "https://forge.softwareheritage.org/source/jesuisgpl/",
        "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0",
        "codemeta:author": {
            "codemeta:name": "Stefano Zacchiroli",
            "codemeta:jobTitle": "Maintainer",
        },
        "codemeta:license": {
            "codemeta:url": "https://spdx.org/licenses/GPL-3.0-or-later.html",
            "codemeta:name": "GNU General Public License v3.0 or later",
        },
        # ...
    }
    original_artifacts = [
        {
            "length": 80880,
            "filename": "archive.zip",
            "checksums": {
                "sha1": "bad32a47a359e0e16ebdca2ad2dc6a771dac8f71",
                "sha256": "182b7ee3b7b5b550e83d3bcfed029bb2f625ee760ebfe9557d5fd072bd4e22e4",
            },
        }
    ]

    row = {
        "id": b"\x02#\x10\xdf\x16\xfd\x9eMO\x81\xfe6\xa1B\xe8-\xb9w\xc0\x1d",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2018, 1, 5, 0, 0, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2018, 1, 5, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"swh: Deposit 467 in collection swh",
        "metadata": {
            "client": "swh",
            "extrinsic": {
                "raw": {
                    "origin": {
                        "url": "https://www.softwareheritage.org/check-deposit-2020-03-11T11:07:18.424476",
                        "type": "deposit",
                    },
                    "branch_name": "master",
                    "origin_metadata": {
                        "tool": {
                            "name": "swh-deposit",
                            "version": "0.0.1",
                            "configuration": {"sword_version": 2},
                        },
                        "metadata": extrinsic_metadata,
                    },
                },
                "when": "2020-03-11T11:11:36.336283+00:00",
                "provider": "https://deposit.softwareheritage.org/1/private/467/meta/",
            },
            "original_artifact": original_artifacts,
            **extrinsic_metadata,
        },
    }

    origin_url = (
        "https://www.softwareheritage.org/check-deposit-2020-03-11T11:07:18.424476"
    )

    swhid = (
        f"swh:1:dir:ef04a768181417fbc5eef4243e2507915f24deea"
        f";origin={origin_url}"
        f";visit=swh:1:snp:14433c19dbb03ad57c86b58b53a800d6a0e32dd3"
        f";anchor=swh:1:rev:022310df16fd9e4d4f81fe36a142e82db977c01d"
        f";path=/"
    )

    deposit_rows = [
        {
            "deposit.id": 467,
            "deposit.external_id": "check-deposit-2020-03-11T11:07:18.424476",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2020, 3, 11, 11, 7, 18, 688410, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://www.softwareheritage.org",
            "deposit_collection.name": "swh",
            "auth_user.username": "swh",
        },
        {
            "deposit.id": 467,
            "deposit.external_id": "check-deposit-2020-03-11T11:07:18.424476",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2020, 3, 11, 11, 7, 18, 669428, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://www.softwareheritage.org",
            "deposit_collection.name": "swh",
            "auth_user.username": "swh",
        },
    ]

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 3, 11, 11, 7, 18, 688410, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_DEPOSIT_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:022310df16fd9e4d4f81fe36a142e82db977c01d"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 3, 11, 11, 11, 36, 336283, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:022310df16fd9e4d4f81fe36a142e82db977c01d"
                    ),
                ),
            ]
        ),
    ]


def test_deposit_2_without_xmlns():
    """Has a provider, no xmlns, and the metadata is only in
    metadata->extrinsic->raw->origin_metadata)"""
    extrinsic_metadata = {
        "{http://www.w3.org/2005/Atom}id": "hal-01243573",
        "{http://www.w3.org/2005/Atom}author": {
            "{http://www.w3.org/2005/Atom}name": "HAL",
            "{http://www.w3.org/2005/Atom}email": "hal@ccsd.cnrs.fr",
        },
        "{http://www.w3.org/2005/Atom}client": "hal",
        "{http://www.w3.org/2005/Atom}external_identifier": "hal-01243573",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}url": "https://hal-test.archives-ouvertes.fr/hal-01243573",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}name": "The assignment problem",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}author": {
            "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}name": "Morane Gruenpeter"
        },
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}version": 1,
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}identifier": "10.5281/zenodo.438684",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}dateCreated": "2017-11-16T14:54:23+01:00",
    }
    original_artifacts = [
        {
            "length": 208357,
            "filename": "archive.zip",
            "checksums": {
                "sha1": "fa0aec08e8a44ea144dba7ce366c8b5d66c14453",
                "sha256": "f53c05fe947e88ce83751a93bd522b1f88478ea2e7b984c07fc7a7c68128bf87",
            },
        }
    ]

    row = {
        "id": b"\x01\x16\xca\xb7\x19d\xd5\x9c\x85p\xb4\xc5r\x9b(\xbd\xd6<\x9bF",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2018, 1, 17, 12, 54, 0, 723882, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2018, 1, 17, 12, 54, 0, 723882, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"hal: Deposit 82 in collection hal",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "origin": {
                        "url": "https://hal.archives-ouvertes.fr/hal-01243573",
                        "type": "deposit",
                    },
                    "origin_metadata": {
                        "tool": {
                            "name": "swh-deposit",
                            "version": "0.0.1",
                            "configuration": {"sword_version": 2},
                        },
                        "metadata": extrinsic_metadata,
                        "provider": {
                            "metadata": {},
                            "provider_url": "https://hal.archives-ouvertes.fr/",
                            "provider_name": "hal",
                            "provider_type": "deposit_client",
                        },
                    },
                },
                "when": "2020-05-15T14:27:21.462270+00:00",
                "provider": "https://deposit.softwareheritage.org/1/private/82/meta/",
            },
            "original_artifact": original_artifacts,
        },
    }

    swhid = (
        "swh:1:dir:e04b2a7b8a8838da0693e9fd992a10d6fd211b50"
        ";origin=https://hal.archives-ouvertes.fr/hal-01243573"
        ";visit=swh:1:snp:abc9ae594245a740235b6c039f044352a5f723ec"
        ";anchor=swh:1:rev:0116cab71964d59c8570b4c5729b28bdd63c9b46"
        ";path=/"
    )

    deposit_rows = [
        {
            "deposit.id": 82,
            "deposit.external_id": "hal-01243573",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2018, 1, 17, 12, 54, 1, 533972, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
        {
            "deposit.id": 82,
            "deposit.external_id": "hal-01243573",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2018, 1, 17, 12, 54, 0, 413748, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
    ]

    origin_url = "https://hal.archives-ouvertes.fr/hal-01243573"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2018, 1, 17, 12, 54, 0, 413748, tzinfo=datetime.timezone.utc
                    ),
                    authority=HAL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json-with-expanded-namespaces",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:0116cab71964d59c8570b4c5729b28bdd63c9b46"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 5, 15, 14, 27, 21, 462270, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:0116cab71964d59c8570b4c5729b28bdd63c9b46"
                    ),
                ),
            ]
        ),
    ]


def test_deposit_2_with_xmlns():
    """Has a provider, xmlns, and the metadata is only in
    metadata->extrinsic->raw->origin_metadata)"""
    extrinsic_metadata = {
        "title": "Je suis GPL",
        "@xmlns": "http://www.w3.org/2005/Atom",
        "client": "swh",
        "codemeta:url": "https://forge.softwareheritage.org/source/jesuisgpl/",
        "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0",
        "codemeta:author": {
            "codemeta:name": "Stefano Zacchiroli",
            "codemeta:jobTitle": "Maintainer",
        },
        "codemeta:license": {
            "codemeta:url": "https://spdx.org/licenses/GPL-3.0-or-later.html",
            "codemeta:name": "GNU General Public License v3.0 or later",
        },
        "external_identifier": "je-suis-gpl",
        "codemeta:dateCreated": "2018-01-05",
    }
    original_artifacts = [
        {
            "length": 80880,
            "filename": "archive.zip",
            "checksums": {
                "sha1": "bad32a47a359e0e16ebdca2ad2dc6a771dac8f71",
                "sha256": "182b7ee3b7b5b550e83d3bcfed029bb2f625ee760ebfe9557d5fd072bd4e22e4",
            },
        }
    ]

    row = {
        "id": b'\x01"\x96nP\x93\x17\xae\xcejA\xd0\xf0\x88\xdas<\xc0\x9d\x0f',
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2018, 1, 5, 0, 0, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2018, 1, 5, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"swh: Deposit 687 in collection swh",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "origin": {
                        "url": "https://www.softwareheritage.org/check-deposit-2020-06-26T13:50:07.564420",
                        "type": "deposit",
                    },
                    "origin_metadata": {
                        "tool": {
                            "name": "swh-deposit",
                            "version": "0.0.1",
                            "configuration": {"sword_version": 2},
                        },
                        "metadata": extrinsic_metadata,
                        "provider": {
                            "metadata": {},
                            "provider_url": "https://www.softwareheritage.org",
                            "provider_name": "swh",
                            "provider_type": "deposit_client",
                        },
                    },
                },
                "when": "2020-06-26T13:50:22.640625+00:00",
                "provider": "https://deposit.softwareheritage.org/1/private/687/meta/",
            },
            "original_artifact": original_artifacts,
        },
    }

    swhid = (
        "swh:1:dir:ef04a768181417fbc5eef4243e2507915f24deea"
        ";origin=https://www.softwareheritage.org/check-deposit-2020-06-26T13:50:07.564420"
        ";visit=swh:1:snp:8fd469e280fb0724175c64906627f619143d5bdb"
        ";anchor=swh:1:rev:0122966e509317aece6a41d0f088da733cc09d0f"
        ";path=/"
    )
    deposit_rows = [
        {
            "deposit.id": 687,
            "deposit.external_id": "check-deposit-2020-06-26T13:50:07.564420",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2020, 6, 26, 13, 50, 8, 216113, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://www.softwareheritage.org",
            "deposit_collection.name": "swh",
            "auth_user.username": "swh",
        },
        {
            "deposit.id": 687,
            "deposit.external_id": "check-deposit-2020-06-26T13:50:07.564420",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2020, 6, 26, 13, 50, 8, 150498, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://www.softwareheritage.org",
            "deposit_collection.name": "swh",
            "auth_user.username": "swh",
        },
    ]

    origin_url = (
        "https://www.softwareheritage.org/check-deposit-2020-06-26T13:50:07.564420"
    )

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 6, 26, 13, 50, 8, 216113, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_DEPOSIT_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:0122966e509317aece6a41d0f088da733cc09d0f"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 6, 26, 13, 50, 22, 640625, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:0122966e509317aece6a41d0f088da733cc09d0f"
                    ),
                ),
            ]
        ),
    ]


def test_deposit_2_with_json_in_json_and_no_xmlns():
    """New formats introduced in https://forge.softwareheritage.org/D4105 ,
    where the raw metadata is itself JSONed inside the metadata JSON tree
    and https://forge.softwareheritage.org/D4065 where the @xmlns declarations
    are stripped before being sent to the deposit DB"""
    extrinsic_metadata = {
        "id": "hal-02960679",
        "author": {"name": "HAL", "email": "hal@ccsd.cnrs.fr"},
        "client": "hal",
        "codemeta:url": "https://hal.archives-ouvertes.fr/hal-02960679",
        "codemeta:name": "Compressive Spectral Clustering Toolbox",
        "codemeta:author": [
            {"codemeta:name": "Nicolas Tremblay", "codemeta:affiliation": "PANAMA"},
            {"codemeta:name": "Gilles Puy", "codemeta:affiliation": "PANAMA"},
            {"codemeta:name": "R{\\'e}mi Gribonval", "codemeta:affiliation": "PANAMA"},
            {"codemeta:name": "Pierre Vandergheynst"},
        ],
        # ...
    }

    original_artifacts = [
        {
            "url": "https://deposit.softwareheritage.org/1/private/1037/raw/",
            "length": 4546913,
            "filename": "archive.zip",
            "checksums": {
                "sha1": "01a0069c626a383de9a17ace40ecfd588e5c4f26",
                "sha256": "c780a6de91286c70ceecc69fe0c6d201d3fe944aa89e193f3a89ae85dc25c3b1",
            },
        }
    ]

    row = {
        "id": b"J\x9dc{\xa5\x07\xa2\xb93e%\x04(\xe6\xe3\xf0!\xf1\x94\xd0",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2016, 1, 29, 0, 0, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2020, 10, 8, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"hal: Deposit 1037 in collection hal",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "origin": {
                        "url": "https://hal.archives-ouvertes.fr/hal-02960679",
                        "type": "deposit",
                    },
                    "origin_metadata": {
                        "tool": {
                            "name": "swh-deposit",
                            "version": "0.2.0",
                            "configuration": {"sword_version": "2"},
                        },
                        "metadata": json.dumps(extrinsic_metadata),
                        "provider": {
                            "metadata": {},
                            "provider_url": "https://hal.archives-ouvertes.fr/",
                            "provider_name": "hal",
                            "provider_type": "deposit_client",
                        },
                    },
                },
                "when": "2020-10-09T13:38:25.888646+00:00",
                "provider": "https://deposit.softwareheritage.org/1/private/1037/meta/",
            },
            "original_artifact": original_artifacts,
        },
    }

    swhid = (
        "swh:1:dir:8bfdf74037ae1c51335995891c6226e0f85e46e2"
        ";origin=https://hal.archives-ouvertes.fr/hal-02960679"
        ";visit=swh:1:snp:bc4a2ddf84dd0cc13d74e1970a1471c2574ed6aa"
        ";anchor=swh:1:rev:4a9d637ba507a2b93365250428e6e3f021f194d0"
        ";path=/"
    )
    deposit_rows = [
        {
            "deposit.id": 1037,
            "deposit.external_id": "hal-02960679",
            "deposit.swhid_context": swhid,
            "deposit.status": "done",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2020,
                10,
                9,
                13,
                38,
                8,
                269611,
                tzinfo=datetime.timezone.utc,
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
        {
            "deposit.id": 1037,
            "deposit.external_id": "hal-02960679",
            "deposit.swhid_context": swhid,
            "deposit.status": "done",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2020,
                10,
                9,
                13,
                38,
                7,
                394544,
                tzinfo=datetime.timezone.utc,
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
    ]

    origin_url = "https://hal.archives-ouvertes.fr/hal-02960679"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 10, 9, 13, 38, 7, 394544, tzinfo=datetime.timezone.utc
                    ),
                    authority=HAL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:4a9d637ba507a2b93365250428e6e3f021f194d0"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 10, 9, 13, 38, 25, 888646, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:4a9d637ba507a2b93365250428e6e3f021f194d0"
                    ),
                ),
            ]
        ),
    ]


def test_deposit_3_and_wrong_external_id_in_metadata():
    extrinsic_metadata = {
        "title": "VTune Perf tool",
        "@xmlns": "http://www.w3.org/2005/Atom",
        "client": "swh",
        "codemeta:url": "https://software.intel.com/en-us/vtune",
        "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0",
        "codemeta:author": {
            "codemeta:name": "VTune developer",
            "codemeta:jobTitle": "Software Engineer",
        },
        "external_identifier": "vtune-perf-tool",
        "codemeta:dateCreated": "2019-05-14",
        "codemeta:description": "Modified version of Linux Perf tool which is used by Intel VTune Amplifier",
    }
    source_original_artifacts = [
        {
            "name": "archive.zip",
            "sha1": "07251dbb1d904d143fd7da9935701f17670d4d9b",
            "length": 4350528,
            "sha256": "1f7d111ac79e468002f3edf4b7b2487538d41f6bea362d49b2eb08a537efafb6",
            "sha1_git": "e2d894efcaad4ff36f09eda3b3c0096416b03429",
            "blake2s256": "e2c08b82efbc361fbb2d28aa8352668cd71217f165f63de16b61ed61ace7509d",
            "archive_type": "zip",
        }
    ]
    dest_original_artifacts = [
        {
            "length": 4350528,
            "archive_type": "zip",
            "filename": "archive.zip",
            "checksums": {
                "sha1": "07251dbb1d904d143fd7da9935701f17670d4d9b",
                "sha256": "1f7d111ac79e468002f3edf4b7b2487538d41f6bea362d49b2eb08a537efafb6",
                "sha1_git": "e2d894efcaad4ff36f09eda3b3c0096416b03429",
                "blake2s256": "e2c08b82efbc361fbb2d28aa8352668cd71217f165f63de16b61ed61ace7509d",
            },
        }
    ]

    row = {
        "id": b"\t5`S\xc4\x9a\xd0\xf9\xe6.Q\xc2\x9d>a|y\x11@\xdf",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2019, 5, 14, 0, 0, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2019, 5, 14, 0, 0, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"intel: Deposit 268 in collection intel",
        "metadata": {
            **extrinsic_metadata,
            "original_artifact": source_original_artifacts,
        },
    }

    swhid = (
        "swh:1:dir:527c8e4a67d391f2bf1bbc86dd94af5d5cfc8ef7"
        ";origin=https://software.intel.com/f80482de-90a8-4c32-bce4-6f6918d492ff"
        ";visit=swh:1:snp:49d60943d9c061da1aba6266a811412f9db8de2e"
        ";anchor=swh:1:rev:09356053c49ad0f9e62e51c29d3e617c791140df"
        ";path=/"
    )
    deposit_rows = [
        {
            "deposit.id": 268,
            "deposit.external_id": "f80482de-90a8-4c32-bce4-6f6918d492ff",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2019, 5, 14, 7, 49, 36, 775072, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://software.intel.com",
            "deposit_collection.name": "intel",
            "auth_user.username": "intel",
        },
        {
            "deposit.id": 268,
            "deposit.external_id": "f80482de-90a8-4c32-bce4-6f6918d492ff",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2019, 5, 14, 7, 49, 36, 477061, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://software.intel.com",
            "deposit_collection.name": "intel",
            "auth_user.username": "intel",
        },
        {
            "deposit.id": 268,
            "deposit.external_id": "f80482de-90a8-4c32-bce4-6f6918d492ff",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2019, 5, 14, 7, 28, 33, 210100, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://software.intel.com",
            "deposit_collection.name": "intel",
            "auth_user.username": "intel",
        },
        {
            "deposit.id": 268,
            "deposit.external_id": "f80482de-90a8-4c32-bce4-6f6918d492ff",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2019, 5, 14, 7, 28, 33, 41454, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://software.intel.com",
            "deposit_collection.name": "intel",
            "auth_user.username": "intel",
        },
    ]

    origin_url = "https://software.intel.com/f80482de-90a8-4c32-bce4-6f6918d492ff"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2019, 5, 14, 7, 49, 36, 775072, tzinfo=datetime.timezone.utc
                    ),
                    authority=INTEL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:09356053c49ad0f9e62e51c29d3e617c791140df"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2019, 5, 14, 7, 28, 33, 210100, tzinfo=datetime.timezone.utc
                    ),
                    authority=INTEL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:09356053c49ad0f9e62e51c29d3e617c791140df"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2019, 5, 14, 7, 49, 36, 775072, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(dest_original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:09356053c49ad0f9e62e51c29d3e617c791140df"
                    ),
                ),
            ]
        ),
    ]


def test_deposit_3_and_no_swhid():
    extrinsic_metadata = {
        "id": "hal-02337300",
        "@xmlns": "http://www.w3.org/2005/Atom",
        "author": {"name": "HAL", "email": "hal@ccsd.cnrs.fr"},
        "client": "hal",
        "codemeta:url": "https://hal.archives-ouvertes.fr/hal-02337300",
        "codemeta:name": "R package SMM, Simulation and Estimation of Multi-State Discrete-Time Semi-Markov and Markov Models",
        "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0",
        "codemeta:author": [
            # ...
        ],
        # ...
    }
    original_artifacts = [
        # ...
    ]

    row = {
        "id": b"\x91\xe5\xca\x8b'K\xf1\xa8cFd2\xd7Q\xf7A\xbc\x94\xba&",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2017, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2019, 11, 6, 14, 47, 30, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"hal: Deposit 342 in collection hal",
        "metadata": {
            **extrinsic_metadata,
            "original_artifact": original_artifacts,
        },
    }
    storage = Mock()

    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    assert storage.method_calls == []


def test_deposit_3_and_unknown_deposit():
    extrinsic_metadata = {
        "title": "Je suis GPL",
        "@xmlns": "http://www.w3.org/2005/Atom",
        "client": "swh",
        "codemeta:url": "https://forge.softwareheritage.org/source/jesuisgpl/",
        "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0",
        "codemeta:author": {
            "codemeta:name": "Stefano Zacchiroli",
            "codemeta:jobTitle": "Maintainer",
        },
        # ...
    }

    row = {
        "id": b"\x8e\x9c\xee\x14\xa6\xad9\xbc\xa44pw\xb8\x7f\xb5\xbb\xd8\x95;\xb1",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2018, 7, 23, 12, 25, 45, 907132, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2018, 7, 23, 12, 25, 45, 907132, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"swh: Deposit 159 in collection swh",
        "metadata": extrinsic_metadata,
    }

    origin_url = "https://software.intel.com/f80482de-90a8-4c32-bce4-6f6918d492ff"

    storage = Mock()

    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    assert storage.method_calls == []


def test_deposit_4_without_xmlns():
    extrinsic_metadata = {
        "{http://www.w3.org/2005/Atom}id": "hal-01243573",
        "{http://www.w3.org/2005/Atom}author": {
            "{http://www.w3.org/2005/Atom}name": "HAL",
            "{http://www.w3.org/2005/Atom}email": "hal@ccsd.cnrs.fr",
        },
        "{http://www.w3.org/2005/Atom}client": "hal",
        "{http://www.w3.org/2005/Atom}external_identifier": "hal-01243573",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}url": "https://hal-test.archives-ouvertes.fr/hal-01243573",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}name": "The assignment problem",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}author": {
            "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}name": "Morane Gruenpeter"
        },
        # ...
    }

    row = {
        "id": b"\x03\x98\x7f\x05n\xafE\x96\xcd \xd7\xb2\xee\x01\xc9\xb8L\xed\xdf\xa8",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2018, 1, 17, 12, 49, 30, 902891, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2018, 1, 17, 12, 49, 30, 902891, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b": Deposit 79 in collection hal",
        "metadata": extrinsic_metadata,
    }

    swhid = (
        "swh:1:dir:e04b2a7b8a8838da0693e9fd992a10d6fd211b50"
        ";origin=https://hal.archives-ouvertes.fr/hal-01243573"
        ";visit=swh:1:snp:c31851534c86676a040fb10f438728c90f1c9d55"
        ";anchor=swh:1:rev:43549ebbe70c9cdf0be1647e6319392eaa06f3a3"
        ";path=/"
    )
    deposit_rows = [
        {
            "deposit.id": 79,
            "deposit.external_id": "hal-01243573",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2018, 1, 17, 12, 49, 31, 208347, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
        {
            "deposit.id": 79,
            "deposit.external_id": "hal-01243573",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2018, 1, 17, 12, 49, 30, 645576, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
    ]

    origin_url = "https://hal.archives-ouvertes.fr/hal-01243573"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2018, 1, 17, 12, 49, 30, 645576, tzinfo=datetime.timezone.utc
                    ),
                    authority=HAL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json-with-expanded-namespaces",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:03987f056eaf4596cd20d7b2ee01c9b84ceddfa8"
                    ),
                ),
            ]
        ),
        # note: no original artifacts
    ]


def test_deposit_4_wrong_origin():
    extrinsic_metadata = {
        "{http://www.w3.org/2005/Atom}id": "hal-01588781",
        "{http://www.w3.org/2005/Atom}author": {
            "{http://www.w3.org/2005/Atom}name": "HAL",
            "{http://www.w3.org/2005/Atom}email": "hal@ccsd.cnrs.fr",
        },
        "{http://www.w3.org/2005/Atom}client": "hal",
        "{http://www.w3.org/2005/Atom}external_identifier": "hal-01588781",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}url": "https://inria.halpreprod.archives-ouvertes.fr/hal-01588781",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}name": "The assignment problem ",
        "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}author": {
            "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}name": "Morane Gruenpeter",
            "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}affiliation": "Initiative pour la Recherche et l'Innovation sur le Logiciel Libre",
        },
        # ...
    }

    row = {
        "id": b"-{\xcec\x1f\xc7\x91\x08\x03\x11\xeb\x83\\GB\x8eXjn\xa4",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2018, 1, 10, 13, 14, 51, 77033, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2018, 1, 10, 13, 14, 51, 77033, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b": Deposit 75 in collection hal",
        "metadata": extrinsic_metadata,
    }

    swhid = (
        "swh:1:dir:d8971c651fe256942aa4499a3ccdbaa305d3bade"
        ";origin=https://inria.halpreprod.archives-ouvertes.fr/hal-01588781"
        ";visit=swh:1:snp:7c70cc8ea5b79e376605fd6e9b3b04d98861ffc0"
        ";anchor=swh:1:rev:2d7bce631fc791080311eb835c47428e586a6ea4"
        ";path=/"
    )
    deposit_rows = [
        {
            "deposit.id": 75,
            "deposit.external_id": "hal-01588781",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2018, 1, 10, 13, 14, 51, 523963, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
        {
            "deposit.id": 75,
            "deposit.external_id": "hal-01588781",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2018, 1, 10, 13, 14, 50, 555143, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
    ]

    origin_url = "https://inria.halpreprod.archives-ouvertes.fr/hal-01588781"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2018, 1, 10, 13, 14, 50, 555143, tzinfo=datetime.timezone.utc
                    ),
                    authority=HAL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json-with-expanded-namespaces",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:2d7bce631fc791080311eb835c47428e586a6ea4"
                    ),
                ),
            ]
        ),
        # note: no original artifacts
    ]


def test_deposit_missing_metadata_in_revision():
    extrinsic_metadata = {
        "id": "hal-01243573",
        "@xmlns": "http://www.w3.org/2005/Atom",
        "author": {"name": "HAL", "email": "hal@ccsd.cnrs.fr"},
        "client": "hal",
        "committer": "Administrateur Du Ccsd",
        "codemeta:url": "https://hal-test.archives-ouvertes.fr/hal-01243573",
        "codemeta:name": "The assignment problem",
        "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0",
        "codemeta:author": {"codemeta:name": "Morane Gruenpeter"},
        "codemeta:version": "1",
        "codemeta:identifier": {
            "#text": "10.5281/zenodo.438684",
            "@name": "doi",
        },
        "external_identifier": "hal-01243573",
        "codemeta:dateCreated": "2017-11-16T14:54:23+01:00",
    }
    source_original_artifacts = [
        {
            "name": "archive.zip",
            "sha1": "e8e46324970cd5af7f98c5a86f33f47fa4a41b4a",
            "length": 118650,
            "sha256": "fec81b63d666c43524f966bbd3263da5bee55051d2b48c1659cca5f56fd953e5",
            "sha1_git": "9da2bbd08bec590b36ede2ed43d74cd510b10a79",
            "blake2s256": "5d0973ba3644cc2bcfdb41ff1891744337d6aa9547a7e59fe466f684b027f295",
            "archive_type": "zip",
        }
    ]
    dest_original_artifacts = [
        {
            "length": 118650,
            "archive_type": "zip",
            "filename": "archive.zip",
            "checksums": {
                "sha1": "e8e46324970cd5af7f98c5a86f33f47fa4a41b4a",
                "sha256": "fec81b63d666c43524f966bbd3263da5bee55051d2b48c1659cca5f56fd953e5",
                "sha1_git": "9da2bbd08bec590b36ede2ed43d74cd510b10a79",
                "blake2s256": "5d0973ba3644cc2bcfdb41ff1891744337d6aa9547a7e59fe466f684b027f295",
            },
        }
    ]

    row = {
        "id": b"\x03@v\xf3\xf4\x1e\xe1 N\xb9\xf6@\x82\xcb\xe6\xe9P\xd7\xbb\x8a",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2019, 2, 25, 15, 49, 16, 594536, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2019, 2, 25, 15, 49, 16, 594536, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"hal: Deposit 229 in collection hal",
        "metadata": {"original_artifact": source_original_artifacts},
    }

    swhid = (
        "swh:1:dir:3d65b6f065118cb856272829b459f0dfa55549aa"
        ";origin=https://hal-test.archives-ouvertes.fr/hal-01243573"
        ";visit=swh:1:snp:322c54ff4023d3216a994bc9ff9ee524ed80ee1f"
        ";anchor=swh:1:rev:034076f3f41ee1204eb9f64082cbe6e950d7bb8a"
        ";path=/"
    )
    deposit_rows = [
        {
            "deposit.id": 229,
            "deposit.external_id": "hal-01243573",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": None,
            "deposit_request.date": datetime.datetime(
                2019, 2, 25, 15, 54, 30, 102072, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
        {
            "deposit.id": 229,
            "deposit.external_id": "hal-01243573",
            "deposit.swhid_context": swhid,
            "deposit.status": "success",
            "deposit_request.metadata": extrinsic_metadata,
            "deposit_request.date": datetime.datetime(
                2019, 2, 25, 15, 49, 12, 302745, tzinfo=datetime.timezone.utc
            ),
            "deposit_client.provider_url": "https://hal.archives-ouvertes.fr/",
            "deposit_collection.name": "hal",
            "auth_user.username": "hal",
        },
    ]

    origin_url = "https://hal.archives-ouvertes.fr/hal-01243573"
    # /!\ not https://hal-test.archives-ouvertes.fr/hal-01243573
    #     do not trust the metadata!

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = get_mock_deposit_cur(deposit_rows)
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    deposit_cur.execute.assert_called_once()
    deposit_cur.__iter__.assert_called_once()

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2019, 2, 25, 15, 49, 12, 302745, tzinfo=datetime.timezone.utc
                    ),
                    authority=HAL_AUTHORITY,
                    fetcher=FETCHER,
                    format="sword-v2-atom-codemeta-v2-in-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:034076f3f41ee1204eb9f64082cbe6e950d7bb8a"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2019, 2, 25, 15, 54, 30, 102072, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(dest_original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:034076f3f41ee1204eb9f64082cbe6e950d7bb8a"
                    ),
                ),
            ]
        ),
    ]
