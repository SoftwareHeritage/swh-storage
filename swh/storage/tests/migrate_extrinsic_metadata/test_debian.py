# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# flake8: noqa
# because of long lines

import copy
import datetime
import json
from unittest.mock import call, Mock

from swh.model.identifiers import parse_swhid
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    RawExtrinsicMetadata,
)

from swh.storage.migrate_extrinsic_metadata import handle_row


FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions", version="0.0.1",
)
SWH_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.REGISTRY,
    url="https://softwareheritage.org/",
    metadata={},
)


def test_debian_with_extrinsic():
    dest_original_artifacts = [
        {
            "length": 2936,
            "filename": "kalgebra_19.12.1-1.dsc",
            "checksums": {
                "sha1": "f869e9f1155b1ee6d28ae3b40060570152a358cd",
                "sha256": "75f77150aefdaa4bcf8bc5b1e9b8b90b5cb1651b76a068c5e58e5b83658d5d11",
            },
            "url": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1-1.dsc",
        },
        {
            "length": 1156408,
            "filename": "kalgebra_19.12.1.orig.tar.xz",
            "checksums": {
                "sha1": "e496032962212983a5359aebadfe13c4026fd45c",
                "sha256": "49d623186800eb8f6fbb91eb43fb14dff78e112624c9cda6b331d494d610b16a",
            },
            "url": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1.orig.tar.xz",
        },
        {
            "length": 10044,
            "filename": "kalgebra_19.12.1-1.debian.tar.xz",
            "checksums": {
                "sha1": "b518bfc2ac708b40577c595bd539faa8b84572db",
                "sha256": "1a30acd2699c3769da302f7a0c63a7d7b060f80925b38c8c43ce3bec92744d67",
            },
            "url": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1-1.debian.tar.xz",
        },
        {
            "length": 488,
            "filename": "kalgebra_19.12.1.orig.tar.xz.asc",
            "checksums": {
                "sha1": "ff53a5c21c1aef2b9caa38a02fa3488f43df4c20",
                "sha256": "a37e0b95bb1f16b19b0587bc5d3b99ba63a195d7f6335c4a359122ad96d682dd",
            },
            "url": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1.orig.tar.xz.asc",
        },
    ]

    source_original_artifacts = [
        {k: v for (k, v) in d.items() if k != "url"} for d in dest_original_artifacts
    ]

    row = {
        "id": b"\x00\x00\x03l1\x1e\xf3:(\x1b\x05h\x8fn\xad\xcf\xc0\x94:\xee",
        "date": datetime.datetime(
            2020, 1, 26, 22, 3, 24, tzinfo=datetime.timezone.utc,
        ),
        "date_offset": 60,
        "type": "dsc",
        "message": b"Synthetic revision for Debian source package kalgebra version 4:19.12.1-1",
        "metadata": {
            "extrinsic": {
                "raw": {
                    "id": 2718802,
                    "name": "kalgebra",
                    "files": {
                        "kalgebra_19.12.1-1.dsc": {
                            "uri": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1-1.dsc",
                            "name": "kalgebra_19.12.1-1.dsc",
                            "size": 2936,
                            "md5sum": "fd28f604d4cc31a0a305543230f1622a",
                            "sha256": "75f77150aefdaa4bcf8bc5b1e9b8b90b5cb1651b76a068c5e58e5b83658d5d11",
                        },
                        "kalgebra_19.12.1.orig.tar.xz": {
                            "uri": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1.orig.tar.xz",
                            "name": "kalgebra_19.12.1.orig.tar.xz",
                            "size": 1156408,
                            "md5sum": "34e09ed152da762d53101ea33634712b",
                            "sha256": "49d623186800eb8f6fbb91eb43fb14dff78e112624c9cda6b331d494d610b16a",
                        },
                        "kalgebra_19.12.1-1.debian.tar.xz": {
                            "uri": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1-1.debian.tar.xz",
                            "name": "kalgebra_19.12.1-1.debian.tar.xz",
                            "size": 10044,
                            "md5sum": "4f639f36143898d97d044f273f038e58",
                            "sha256": "1a30acd2699c3769da302f7a0c63a7d7b060f80925b38c8c43ce3bec92744d67",
                        },
                        "kalgebra_19.12.1.orig.tar.xz.asc": {
                            "uri": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1.orig.tar.xz.asc",
                            "name": "kalgebra_19.12.1.orig.tar.xz.asc",
                            "size": 488,
                            "md5sum": "3c29291e4e6f0c294de80feb8e9fce4c",
                            "sha256": "a37e0b95bb1f16b19b0587bc5d3b99ba63a195d7f6335c4a359122ad96d682dd",
                        },
                    },
                    "version": "4:19.12.1-1",
                    "revision_id": None,
                },
                "when": "2020-01-27T19:32:03.925498+00:00",
                "provider": "http://deb.debian.org/debian//pool/main/k/kalgebra/kalgebra_19.12.1-1.dsc",
            },
            "intrinsic": {
                "raw": {
                    "name": "kalgebra",
                    "version": "4:19.12.1-1",
                    # ...
                },
                "tool": "dsc",
            },
            "original_artifact": source_original_artifacts,
        },
    }

    storage = Mock()
    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    assert storage.method_calls == [
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:0000036c311ef33a281b05688f6eadcfc0943aee"
                    ),
                    discovery_date=datetime.datetime(
                        2020, 1, 26, 22, 3, 24, tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(dest_original_artifacts).encode(),
                ),
            ]
        )
    ]


def test_debian_without_extrinsic():
    source_original_artifacts = [
        {
            "name": "pymongo_1.10-1.dsc",
            "sha1": "81877c1ae4406c2519b9cc9c4557cf6b0775a241",
            "length": 99,
            "sha256": "40269a73f38ee4c2f9cc021f1d5d091cc59ca6e778c339684b7be030e29e282f",
            "sha1_git": "0ac7bdb8e4d10926c5d3e51baa2be7bb29a3966b",
        },
        {
            "name": "pymongo_1.10.orig.tar.gz",
            "sha1": "4f4c97641b86ac8f21396281bd1a7369236693c3",
            "length": 99,
            "sha256": "0b6bffb310782ffaeb3916c75790742ec5830c63a758fc711cd1f557eb5a4b5f",
            "sha1_git": "19ef0adda8868520d1ef9d4164b3ace4df1d62ad",
        },
        {
            "name": "pymongo_1.10-1.debian.tar.gz",
            "sha1": "fbf378296613c8d55e043aec98896b3e50a94971",
            "length": 99,
            "sha256": "3970cc70fe3ba6499a9c56ba4b4c6c3782f56433d0d17d72b7a0e2ceae31b513",
            "sha1_git": "2eea9904806050a8fda95edd5d4fa60d29c1fdec",
        },
    ]

    dest_original_artifacts = [
        {
            "length": 99,
            "filename": "pymongo_1.10-1.dsc",
            "checksums": {
                "sha1": "81877c1ae4406c2519b9cc9c4557cf6b0775a241",
                "sha256": "40269a73f38ee4c2f9cc021f1d5d091cc59ca6e778c339684b7be030e29e282f",
                "sha1_git": "0ac7bdb8e4d10926c5d3e51baa2be7bb29a3966b",
            },
        },
        {
            "length": 99,
            "filename": "pymongo_1.10.orig.tar.gz",
            "checksums": {
                "sha1": "4f4c97641b86ac8f21396281bd1a7369236693c3",
                "sha256": "0b6bffb310782ffaeb3916c75790742ec5830c63a758fc711cd1f557eb5a4b5f",
                "sha1_git": "19ef0adda8868520d1ef9d4164b3ace4df1d62ad",
            },
        },
        {
            "length": 99,
            "filename": "pymongo_1.10-1.debian.tar.gz",
            "checksums": {
                "sha1": "fbf378296613c8d55e043aec98896b3e50a94971",
                "sha256": "3970cc70fe3ba6499a9c56ba4b4c6c3782f56433d0d17d72b7a0e2ceae31b513",
                "sha1_git": "2eea9904806050a8fda95edd5d4fa60d29c1fdec",
            },
        },
    ]

    row = {
        "id": b"\x00\x00\x01\xc2\x8c\x8f\xca\x01\xb9\x04\xde\x92\xa2d\n\x86l\xe0<\xb7",
        "date": datetime.datetime(
            2011, 3, 31, 20, 17, 41, tzinfo=datetime.timezone.utc
        ),
        "date_offset": 0,
        "type": "dsc",
        "message": b"Synthetic revision for Debian source package pymongo version 1.10-1",
        "metadata": {
            "package_info": {
                "name": "pymongo",
                "version": "1.10-1",
                "changelog": {
                    # ...
                },
                "maintainers": [
                    {"name": "Federico Ceratto", "email": "federico.ceratto@gmail.com"},
                    {"name": "Janos Guljas", "email": "janos@resenje.org"},
                ],
                "pgp_signature": {
                    "date": "2011-03-31T21:02:44+00:00",
                    "keyid": "2BABC6254E66E7B8450AC3E1E6AA90171392B174",
                    "person": {"name": "David Paleino", "email": "d.paleino@gmail.com"},
                },
                "lister_metadata": {"id": 244296, "lister": "snapshot.debian.org"},
            },
            "original_artifact": source_original_artifacts,
        },
    }

    storage = Mock()
    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    assert storage.method_calls == [
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:000001c28c8fca01b904de92a2640a866ce03cb7"
                    ),
                    discovery_date=datetime.datetime(
                        2011, 3, 31, 20, 17, 41, tzinfo=datetime.timezone.utc
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(dest_original_artifacts).encode(),
                ),
            ]
        )
    ]