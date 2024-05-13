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

from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    RawExtrinsicMetadata,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.storage.migrate_extrinsic_metadata import cran_package_from_url, handle_row

FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions",
    version="0.0.1",
)
SWH_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.REGISTRY,
    url="https://softwareheritage.org/",
    metadata={},
)

DIRECTORY_ID = b"a" * 20
DIRECTORY_SWHID = ExtendedSWHID(
    object_type=ExtendedObjectType.DIRECTORY, object_id=DIRECTORY_ID
)


def test_cran_package_from_url():
    files = [
        ("https://cran.r-project.org/src/contrib/shapeR_0.1-5.tar.gz", "shapeR"),
        ("https://cran.r-project.org/src/contrib/hot.deck_1.1.tar.gz", "hot.deck"),
    ]

    for filename, project in files:
        assert cran_package_from_url(filename) == project


def test_cran():
    source_original_artifacts = [
        {
            "length": 170623,
            "filename": "ExtremeRisks_0.0.3.tar.gz",
            "checksums": {
                "sha1": "f2f19fc0f24b66b5ea9413366c632f3c229f7f3f",
                "sha256": "6f232556313019809dde3554149a1399bb1901a366b4965af49dc007d01945c9",
            },
        }
    ]
    dest_original_artifacts = [
        {
            "length": 170623,
            "filename": "ExtremeRisks_0.0.3.tar.gz",
            "checksums": {
                "sha1": "f2f19fc0f24b66b5ea9413366c632f3c229f7f3f",
                "sha256": "6f232556313019809dde3554149a1399bb1901a366b4965af49dc007d01945c9",
            },
            "url": "https://cran.r-project.org/src/contrib/ExtremeRisks_0.0.3.tar.gz",
        }
    ]

    row = {
        "id": b"\x00\x03a\xaa3\x84,\xbd\xea_\xa6\xe7}\xb6\x96\xb97\xeb\xd2i",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2020,
            5,
            5,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        ),
        "committer_date": datetime.datetime(
            2020,
            5,
            5,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        ),
        "type": "tar",
        "message": b"0.0.3",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "url": "https://cran.r-project.org/src/contrib/ExtremeRisks_0.0.3.tar.gz",
                    "version": "0.0.3",
                },
                "when": "2020-05-07T15:27:38.652281+00:00",
                "provider": "https://cran.r-project.org/package=ExtremeRisks",
            },
            "intrinsic": {
                "raw": {
                    "URL": "mypage.unibocconi.it/simonepadoan/",
                    "Date": "2020-05-05",
                    "Title": "Extreme Risk Measures",
                    "Author": "Simone Padoan [cre, aut],\n  Gilles Stupfler [aut]",
                    # ...
                    "Date/Publication": "2020-05-07 10:20:02 UTC",
                },
                "tool": "DESCRIPTION",
            },
            "original_artifact": source_original_artifacts,
        },
    }

    origin_url = "https://cran.r-project.org/package=ExtremeRisks"

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
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020,
                        5,
                        7,
                        15,
                        27,
                        38,
                        652281,
                        tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(dest_original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:000361aa33842cbdea5fa6e77db696b937ebd269"
                    ),
                ),
            ]
        ),
    ]


def test_cran_without_revision_date():
    """Tests a CRAN revision with a date in the metadata but not as revision date"""
    source_original_artifacts = [
        {
            "length": 8018,
            "filename": "gofgamma_1.0.tar.gz",
            "checksums": {
                "sha1": "58f2993140f9e9e1a136554f0af0174a252f2c7b",
                "sha256": "55408f004642b5043bb01de831a7e7a0b9f24a30cb0151e70c2d37abdc508d03",
            },
        }
    ]
    dest_original_artifacts = [
        {
            "length": 8018,
            "filename": "gofgamma_1.0.tar.gz",
            "checksums": {
                "sha1": "58f2993140f9e9e1a136554f0af0174a252f2c7b",
                "sha256": "55408f004642b5043bb01de831a7e7a0b9f24a30cb0151e70c2d37abdc508d03",
            },
            "url": "https://cran.r-project.org/src/contrib/gofgamma_1.0.tar.gz",
        }
    ]

    row = {
        "id": b'\x00\x00\xd4\xef^\x16a"\xae\xe6\x86*\xd3\x8a\x18\xceS\x86\xcc>',
        "directory": DIRECTORY_ID,
        "date": None,
        "committer_date": None,
        "type": "tar",
        "message": b"1.0",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "url": "https://cran.r-project.org/src/contrib/gofgamma_1.0.tar.gz",
                    "version": "1.0",
                },
                "when": "2020-04-30T11:01:57.832481+00:00",
                "provider": "https://cran.r-project.org/package=gofgamma",
            },
            "intrinsic": {
                "raw": {
                    "Type": "Package",
                    "Title": "Goodness-of-Fit Tests for the Gamma Distribution",
                    "Author": "Lucas Butsch [aut],\n  Bruno Ebner [aut, cre],\n  Steffen Betsch [aut]",
                    # ...
                },
                "tool": "DESCRIPTION",
            },
            "original_artifact": source_original_artifacts,
        },
    }

    origin_url = "https://cran.r-project.org/package=gofgamma"

    storage = Mock()

    def origin_get(urls):
        assert urls == [origin_url]
        return [Origin(url=origin_url)]

    storage.origin_get.side_effect = origin_get
    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    assert storage.method_calls == [
        call.origin_get([origin_url]),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020,
                        4,
                        30,
                        11,
                        1,
                        57,
                        832481,
                        tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(dest_original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:0000d4ef5e166122aee6862ad38a18ce5386cc3e"
                    ),
                ),
            ]
        ),
    ]


def test_cran_with_new_original_artifacts_format():
    original_artifacts = [
        {
            "url": "https://cran.r-project.org/src/contrib/r2mlm_0.1.0.tar.gz",
            "length": 346563,
            "filename": "r2mlm_0.1.0.tar.gz",
            "checksums": {
                "sha1": "25c06b4af523c35a7813b58dd0db414e79848501",
                "sha256": "c887fe6c4f78c94b2279759052e12d639cf80225b444c1f67931c6aa6f0faf23",
            },
        }
    ]

    row = {
        "id": b'."7\x82\xeeK\xa1R\xe4\xc8\x86\xf7\x97\x97bA\xc3\x9a\x9a\xab',
        "directory": DIRECTORY_ID,
        "date": None,
        "committer_date": None,
        "type": "tar",
        "message": b"0.1.0",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "url": "https://cran.r-project.org/src/contrib/r2mlm_0.1.0.tar.gz"
                },
                "when": "2020-09-25T14:04:20.926667+00:00",
                "provider": "https://cran.r-project.org/package=r2mlm",
            },
            "intrinsic": {
                "raw": {
                    "URL": "https://github.com/mkshaw/r2mlm",
                    "Type": "Package",
                    "Title": "R-Squared Measures for Multilevel Models",
                    "Author": "Mairead Shaw [aut, cre],\n  Jason Rights [aut],\n  Sonya Sterba [aut],\n  Jessica Flake [aut]",
                    # ...
                },
                "tool": "DESCRIPTION",
            },
            "original_artifact": original_artifacts,
        },
    }

    origin_url = "https://cran.r-project.org/package=r2mlm"

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
                    target=DIRECTORY_SWHID,
                    discovery_date=datetime.datetime(
                        2020,
                        9,
                        25,
                        14,
                        4,
                        20,
                        926667,
                        tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:2e223782ee4ba152e4c886f797976241c39a9aab"
                    ),
                ),
            ]
        ),
    ]
