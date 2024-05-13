# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import namedtuple
import datetime
from typing import List

from attr import evolve

from swh.model.hashutil import DEFAULT_ALGORITHMS
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    RawExtrinsicMetadata,
)
from swh.model.swhids import ExtendedObjectType, ExtendedSWHID
from swh.storage.cassandra import converters
from swh.storage.cassandra.model import RawExtrinsicMetadataRow

# Test purposes
field_names: List[str] = list(DEFAULT_ALGORITHMS)
Row = namedtuple("Row", field_names)  # type: ignore


def test_row_to_content_hashes():
    for row in [
        Row(
            sha1=b"4\x972t\xcc\xefj\xb4\xdf\xaa\xf8e\x99y/\xa9\xc3\xfeF\x89",
            sha1_git=b"\xd8\x1c\xc0q\x0e\xb6\xcf\x9e\xfd[\x92\n\x84S\xe1\xe0qW\xb6\xcd",  # noqa
            sha256=b"g6P\xf96\xcb;\n/\x93\xce\t\xd8\x1b\xe1\x07H\xb1\xb2\x03\xc1\x9e\x81v\xb4\xee\xfc\x19d\xa0\xcf:",  # noqa
            blake2s256=b"\xd5\xfe\x199We'\xe4,\xfdv\xa9EZ$2\xfe\x7fVf\x95dW}\xd9<B\x80\xe7mf\x1d",  # noqa
        ),
        Row(
            sha1=b"4\x972t\xcc\xefj\xb4\xdf\xaa\xf8e\x99y/\xa9\xc3\xfeF\x89",  # noqa
            sha1_git=b"\xd8\x1c\xc0q\x0e\xb6\xcf\x9e\xfd[\x92\n\x84S\xe1\xe0qW\xb6\xcd",  # noqa
            sha256=b"h6P\xf96\xcb;\n/\x93\xce\t\xd8\x1b\xe1\x07H\xb1\xb2\x03\xc1\x9e\x81v\xb4\xee\xfc\x19d\xa0\xcf:",  # noqa
            blake2s256=b"\xd5\xfe\x199We'\xe4,\xfdv\xa9EZ$2\xfe\x7fVf\x95dW}\xd9<B\x80\xe7mf\x1d",  # noqa
        ),
    ]:
        actual_hashes = converters.row_to_content_hashes(row)

        assert len(actual_hashes) == len(DEFAULT_ALGORITHMS)
        for algo in DEFAULT_ALGORITHMS:
            assert actual_hashes[algo] == getattr(row, algo)


def test_row_to_extrinsic_metadata_conversion():
    """Conversion of row to raw extrinsic metadata should be ok"""
    swhid_template = "swh:1:{type}:43d1b8ba928846e2d2ca2e3b1a5edfb2ae5265f0"
    swhid_target_template = "swh:1:{type}:f25a28146e5089828fe80bf9147184e068b6e09f"

    authority_type = MetadataAuthorityType.REGISTRY
    authority_url = "https://softwareheritage.org/"
    fetcher_name = "swh.loader.package.golang.loader.GolangLoader"
    fetcher_version = "5.17.0"
    origin_url = "https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/mgn"
    raw_type = "something"

    def init_raw_row_dict(raw_extrinsic_metadata_d):
        row_raw_d = raw_extrinsic_metadata_d.copy()
        row_raw_d["type"] = raw_type
        row_raw_d.pop("authority", None)
        row_raw_d.pop("fetcher", None)

        row_raw_d["authority_type"] = authority_type.value
        row_raw_d["authority_url"] = authority_url
        row_raw_d["fetcher_name"] = fetcher_name
        row_raw_d["fetcher_version"] = fetcher_version

        # Empty values for "empty" conversion
        for key, empty_value in [
            ("snapshot", ""),
            ("release", ""),
            ("revision", ""),
            ("directory", ""),
            ("visit", 0),
            ("path", b""),
        ]:
            row_raw_d[key] = empty_value

        return row_raw_d

    def init_raw_dict(raw_extrinsic_metadata_d):
        raw_metadata_d = raw_extrinsic_metadata_d.copy()

        for key in ["snapshot", "release", "revision", "directory", "visit", "path"]:
            raw_metadata_d[key] = None

        return raw_metadata_d

    def swhid(obj_type):
        return swhid_template.format(type=ExtendedObjectType[obj_type.upper()].value)

    for key in ["snapshot", "release", "revision", "directory"]:
        if key == "directory":
            target_swhid = swhid_target_template.format(
                type=ExtendedObjectType.CONTENT.value
            )
        else:
            target_swhid = swhid_target_template.format(
                type=ExtendedObjectType.DIRECTORY.value
            )

        # template raw extrinsic metadata we are gonna adapt to ensure conversion is ok
        raw_extrinsic_metadata_d = RawExtrinsicMetadata(
            target=ExtendedSWHID.from_string(target_swhid),
            discovery_date=datetime.datetime(
                2024, 3, 18, 22, 59, 19, tzinfo=datetime.timezone.utc
            ),
            authority=MetadataAuthority(
                type=authority_type, url=authority_url, metadata=None
            ),
            fetcher=MetadataFetcher(
                name=fetcher_name, version=fetcher_version, metadata=None
            ),
            format="original-artifacts-json",
            metadata=b'[{"length": 97190}]',
            origin=origin_url,
        ).to_dict()

        swhid_str = swhid(key)
        raw_metadata_d = init_raw_dict(raw_extrinsic_metadata_d)
        raw_metadata_d[key] = swhid_str
        expected_raw_metadata = RawExtrinsicMetadata.from_dict(raw_metadata_d)

        row_raw_d = init_raw_row_dict(raw_metadata_d)
        row_raw_d[key] = swhid_str
        row_raw = RawExtrinsicMetadataRow.from_dict(row_raw_d)

        actual_raw = converters.row_to_raw_extrinsic_metadata(row_raw)

        # As we crafted an extrinsic metadata for test conversion purpose, we do not
        # care for the id object comparison. We just ensure the conversion is ok.
        assert actual_raw == evolve(expected_raw_metadata, id=actual_raw.id)
