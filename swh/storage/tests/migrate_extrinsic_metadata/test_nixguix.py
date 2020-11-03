# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# flake8: noqa
# because of long lines

import copy
import datetime
import json
from unittest.mock import Mock, call

from swh.model.identifiers import parse_swhid
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    Origin,
    RawExtrinsicMetadata,
)
from swh.storage.migrate_extrinsic_metadata import cran_package_from_url, handle_row

FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions", version="0.0.1",
)
SWH_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.REGISTRY,
    url="https://softwareheritage.org/",
    metadata={},
)
NIX_UNSTABLE_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.FORGE,
    url="https://nix-community.github.io/nixpkgs-swh/sources-unstable.json",
    metadata={},
)

DIRECTORY_ID = b"a" * 20
DIRECTORY_SWHID = parse_swhid("swh:1:dir:" + DIRECTORY_ID.hex())


def test_nixguix():
    extrinsic_metadata = {
        "url": "https://files.pythonhosted.org/packages/source/a/alerta/alerta-7.4.5.tar.gz",
        "integrity": "sha256-km8RAaG1ep+tYR8eHVr3UWk+/MNEqdsBr1Di/g02LYQ=",
    }
    original_artifacts = [
        {
            "length": 34903,
            "filename": "alerta-7.4.5.tar.gz",
            "checksums": {
                "sha1": "66db4398b664de272fd5aa6610caa776b5e64651",
                "sha256": "926f1101a1b57a9fad611f1e1d5af751693efcc344a9db01af50e2fe0d362d84",
            },
        }
    ]

    row = {
        "id": b"\x00\x01\xbaM\xd0S\x94\x85\x02\x11\xd7\xb3\x85M\x99\x13\xd2:\xe3y",
        "directory": DIRECTORY_ID,
        "date": None,
        "committer_date": None,
        "type": "tar",
        "message": b"",
        "metadata": {
            "extrinsic": {
                "raw": extrinsic_metadata,
                "when": "2020-06-03T11:25:05.259341+00:00",
                "provider": "https://nix-community.github.io/nixpkgs-swh/sources-unstable.json",
            },
            "original_artifact": original_artifacts,
        },
    }

    origin_url = "https://nix-community.github.io/nixpkgs-swh/sources-unstable.json"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = None
    handle_row(row, storage, deposit_cur, dry_run=False)

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.DIRECTORY,
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 6, 3, 11, 25, 5, 259341, tzinfo=datetime.timezone.utc
                    ),
                    authority=NIX_UNSTABLE_AUTHORITY,
                    fetcher=FETCHER,
                    format="nixguix-sources-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                    revision=parse_swhid(
                        "swh:1:rev:0001ba4dd05394850211d7b3854d9913d23ae379"
                    ),
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.DIRECTORY,
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020, 6, 3, 11, 25, 5, 259341, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=parse_swhid(
                        "swh:1:rev:0001ba4dd05394850211d7b3854d9913d23ae379"
                    ),
                ),
            ]
        ),
    ]
