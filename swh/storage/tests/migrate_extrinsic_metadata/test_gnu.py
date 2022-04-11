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


def test_gnu():
    original_artifacts = [
        {
            "length": 842501,
            "filename": "gperf-3.0.1.tar.gz",
            "checksums": {
                "sha1": "c4453ee492032b369006ee464f4dd4e2c0c0e650",
                "sha256": "5be283ef62e1bd26abdaaf88b416dbea4b14c360b09befcda2f055656dc43f87",
                "sha1_git": "bf1d5bb57d571101dd7b6acab2b78ae11bb861de",
                "blake2s256": "661f84afeb1e0b914defe2b249d424af1dfe380a96016b3282ae758c70e19a70",
            },
        }
    ]

    row = {
        "id": b"\x00\x1cqE\x8e@[%\xba\xcc\xc8\x0b\x99\xf6cM\xff\x9d+\x18",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2003, 6, 13, 0, 11, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2003, 6, 13, 0, 11, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"swh-loader-package: synthetic revision message",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "url": "https://ftp.gnu.org/gnu/gperf/gperf-3.0.1.tar.gz",
                    "time": "2003-06-13T00:11:00+00:00",
                    "length": 842501,
                    "version": "3.0.1",
                    "filename": "gperf-3.0.1.tar.gz",
                },
                "when": "2019-11-27T11:17:38.318997+00:00",
                "provider": "https://ftp.gnu.org/gnu/gperf/",
            },
            "intrinsic": {},
            "original_artifact": original_artifacts,
        },
    }

    origin_url = "https://ftp.gnu.org/gnu/gperf/"

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
                        2019, 11, 27, 11, 17, 38, 318997, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                    revision=CoreSWHID.from_string(
                        "swh:1:rev:001c71458e405b25baccc80b99f6634dff9d2b18"
                    ),
                ),
            ]
        ),
    ]
