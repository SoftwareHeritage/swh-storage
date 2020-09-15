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
    Origin,
    RawExtrinsicMetadata,
)

from swh.storage.migrate_extrinsic_metadata import (
    handle_row,
    npm_package_from_source_url,
)


FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions", version="0.0.1",
)
NPM_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.FORGE, url="https://npmjs.com/", metadata={},
)
SWH_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.REGISTRY,
    url="https://softwareheritage.org/",
    metadata={},
)


def test_npm_package_from_source_url():
    package_urls = [
        (
            "@l3ilkojr/jdinsults",
            "https://registry.npmjs.org/@l3ilkojr/jdinsults/-/jdinsults-3.0.0.tgz",
        ),
        ("simplemaps", "https://registry.npmjs.org/simplemaps/-/simplemaps-0.0.6.tgz"),
        (
            "@piximi/components",
            "https://registry.npmjs.org/@piximi/components/-/components-0.1.11.tgz",
        ),
        (
            "@chappa'ai/get-next-rc",
            "https://registry.npmjs.org/@chappa%27ai/get-next-rc/-/get-next-rc-1.0.0.tgz",
        ),
    ]

    for (package_name, source_url) in package_urls:
        assert npm_package_from_source_url(source_url) == package_name


def test_npm_1():
    """Tests loading a revision generated by a new NPM loader that
    has a provider."""

    extrinsic_metadata = {
        "_id": "@l3ilkojr/jdinsults@3.0.0",
        "dist": {
            "shasum": "b7f0d66090e0285f4e95d082d39bcb0c1b8f4ec8",
            "tarball": "https://registry.npmjs.org/@l3ilkojr/jdinsults/-/jdinsults-3.0.0.tgz",
            "fileCount": 4,
            "integrity": "sha512-qpv8Zg51g0l51VjODEooMUGSGanGUuQpzX5msfR7ZzbgTsgPbpDNyTIsQ0wQzI9RzCCUjS84Ii2VhMISEQcEUA==",
            "unpackedSize": 1583,
            "npm-signature": "-----BEGIN PGP SIGNATURE-----\r\nVersion: OpenPGP.js v3.0.4\r\nComment: https://openpgpjs.org\r\n\r\nwsFcBAEBCAAQBQJeUMS5CRA9TVsSAnZWagAAXpgP/0YgNOWN0U/Fz2RGeQhR\nVIKPvfGqZ2UfFxxUXWIc4QHvwyLCNUedCctpVdqnqmGJ9m/hj3K2zbRPD7Tm\n3nPl0HfzE7v3T8TDZfGhzW3c9mWxig+syr+sjo0EKyAgZVJ0mxbjOl4KHt+U\nQEwl/4falBsyYtK/pkCXWmmuC606QmPn/c6ZRD1Fw4vJjT9i5qi1KaBkIf6M\nnFmpOFxTcwxGGltOk3s3TKDtr8CIeWmdm3VkgsP2ErkPKAOcu12AT4/5tkg0\nDU+m1XmJb67rskb4Ncjvic/VutnPkEfNrk1IRXrmjDZBQbHtCJ7hd5ETmb9S\nE5WmMV8cpaGiW7AZvGTmkn5WETwQQU7po914zYiMg9+ozdwc7yC8cpGj/UoF\niKxsc1uxdfwWk/p3dShegEYM7sveloIXYsPaxbd84WRIfnwkWFZV82op96E3\neX+FRkhMfsHlK8OjZsBPXkppaB48jnZdm3GOOzT9YgyphV33j3J9GnNcDMDe\nriyCLV1BNSKDHElCDrvl1cBGg+C5qn/cTYjQdfEPPY2Hl2MgW9s4UV2s+YSx\n0BBd2A3j80wncP+Y7HFeC4Pv0SM0Pdq6xJaf3ELhj6j0rVZeTW1O3E/PFLXK\nnn/DZcsFXgIzjY+eBIMQgAhqyeJve8LeQNnGt3iNW10E2nZMpfc+dn0ESiwV\n2Gw4\r\n=8uqZ\r\n-----END PGP SIGNATURE-----\r\n",
        },
        "name": "@l3ilkojr/jdinsults",
        "version": "3.0.0",
        "_npmUser": {"name": "l3ilkojr", "email": "l3ilkojr@example.com"},
        "_npmVersion": "6.13.6",
        "description": "Generates insults",
        "directories": {},
        "maintainers": [{"name": "l3ilkojr", "email": "l3ilkojr@example.com"}],
        "_nodeVersion": "10.14.0",
        "_hasShrinkwrap": False,
        "_npmOperationalInternal": {
            "tmp": "tmp/jdinsults_3.0.0_1582351545285_0.2614827716102821",
            "host": "s3://npm-registry-packages",
        },
    }

    original_artifacts = [
        {
            "length": 1033,
            "filename": "jdinsults-3.0.0.tgz",
            "checksums": {
                "sha1": "b7f0d66090e0285f4e95d082d39bcb0c1b8f4ec8",
                "sha256": "42f22795ac883b02fded0b2bf3d8a77f6507d40bc67f28eea6b1b73eb59c515f",
            },
        }
    ]

    row = {
        "id": b"\x00\x00\x02\xa4\x9b\xba\x17\xca\x8c\xf3\x7f_=\x16\xaa\xac\xf9S`\xfc",
        "date": datetime.datetime(2020, 2, 22, 6, 5, 45, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2020, 2, 22, 6, 5, 45, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"3.0.0",
        "metadata": {
            "extrinsic": {
                "raw": extrinsic_metadata,
                "when": "2020-02-27T01:35:47.965375+00:00",
                "provider": "https://replicate.npmjs.com/%40l3ilkojr%2Fjdinsults/",
            },
            "intrinsic": {
                "raw": {"name": "@l3ilkojr/jdinsults", "version": "3.0.0"},
                "tool": "package.json",
            },
            "original_artifact": original_artifacts,
        },
    }

    origin_url = "https://www.npmjs.com/package/@l3ilkojr/jdinsults"

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
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:000002a49bba17ca8cf37f5f3d16aaacf95360fc"
                    ),
                    discovery_date=datetime.datetime(
                        2020, 2, 27, 1, 35, 47, 965375, tzinfo=datetime.timezone.utc,
                    ),
                    authority=NPM_AUTHORITY,
                    fetcher=FETCHER,
                    format="replicate-npm-package-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:000002a49bba17ca8cf37f5f3d16aaacf95360fc"
                    ),
                    discovery_date=datetime.datetime(
                        2020, 2, 27, 1, 35, 47, 965375, tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                ),
            ]
        ),
    ]


def test_npm_2_unscoped():
    """Tests loading a revision generated by an old NPM loader that doesn't
    have a provider; and the package name is unscoped (ie. doesn't contain a
    slash)."""

    extrinsic_metadata = {
        "bugs": {"url": "https://github.com/niwasawa/simplemaps/issues"},
        "name": "simplemaps",
        "author": "Naoki Iwasawa",
        "license": "MIT",
        # ...
    }

    package_source = {
        "url": "https://registry.npmjs.org/simplemaps/-/simplemaps-0.0.6.tgz",
        "date": "2016-12-23T07:21:29.733Z",
        "name": "simplemaps",
        "sha1": "e2b8222930196def764527f5c61048c5b28fe3c4",
        "sha256": "3ce94927bab5feafea5695d1fa4c2b8131413e53e249b32f9ac2ccff4d865a0b",
        "version": "0.0.6",
        "filename": "simplemaps-0.0.6.tgz",
        "blake2s256": "6769b4009f8162be2e745604b153443d4907a85781d31a724217a3e2d42a7462",
    }

    original_artifacts = [
        {
            "filename": "simplemaps-0.0.6.tgz",
            "checksums": {
                "sha1": "e2b8222930196def764527f5c61048c5b28fe3c4",
                "sha256": "3ce94927bab5feafea5695d1fa4c2b8131413e53e249b32f9ac2ccff4d865a0b",
                "blake2s256": "6769b4009f8162be2e745604b153443d4907a85781d31a724217a3e2d42a7462",
            },
            "url": "https://registry.npmjs.org/simplemaps/-/simplemaps-0.0.6.tgz",
        }
    ]

    row = {
        "id": b"\x00\x00\x04\xae\xed\t\xee\x08\x9cx\x12d\xc0M%d\xfdX\xfe\xb5",
        "date": datetime.datetime(
            2016, 12, 23, 7, 21, 29, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2016, 12, 23, 7, 21, 29, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"0.0.6",
        "metadata": {"package": extrinsic_metadata, "package_source": package_source,},
    }

    origin_url = "https://www.npmjs.com/package/simplemaps"

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
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:000004aeed09ee089c781264c04d2564fd58feb5"
                    ),
                    discovery_date=datetime.datetime(
                        2016, 12, 23, 7, 21, 29, tzinfo=datetime.timezone.utc,
                    ),
                    authority=NPM_AUTHORITY,
                    fetcher=FETCHER,
                    format="replicate-npm-package-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:000004aeed09ee089c781264c04d2564fd58feb5"
                    ),
                    discovery_date=datetime.datetime(
                        2016, 12, 23, 7, 21, 29, tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                ),
            ]
        ),
    ]


def test_npm_2_scoped():
    """Tests loading a revision generated by an old NPM loader that doesn't
    have a provider; and the package name is scoped (ie. in the format
    @org/name)."""

    extrinsic_metadata = {
        "bugs": {"url": "https://github.com/piximi/components/issues"},
        "name": "@piximi/components",
        # ...
    }

    package_source = {
        "url": "https://registry.npmjs.org/@piximi/components/-/components-0.1.11.tgz",
        "date": "2019-06-07T19:56:04.753Z",
        "name": "@piximi/components",
        "sha1": "4ab74e563cb61bb5b2022601a5133a2dd19d19ec",
        "sha256": "69bb980bd6de3277b6bca86fd79c91f1c28db6910c8d03ecd05b32b78a35188f",
        "version": "0.1.11",
        "filename": "components-0.1.11.tgz",
        "blake2s256": "ce33181d5eff25b70ffdd6f1a18acd472a1707ede23cd2adc6af272dfc40dbfd",
    }

    original_artifacts = [
        {
            "filename": "components-0.1.11.tgz",
            "checksums": {
                "sha1": "4ab74e563cb61bb5b2022601a5133a2dd19d19ec",
                "sha256": "69bb980bd6de3277b6bca86fd79c91f1c28db6910c8d03ecd05b32b78a35188f",
                "blake2s256": "ce33181d5eff25b70ffdd6f1a18acd472a1707ede23cd2adc6af272dfc40dbfd",
            },
            "url": "https://registry.npmjs.org/@piximi/components/-/components-0.1.11.tgz",
        }
    ]

    row = {
        "id": b"\x00\x00 \x19\xc5wXt\xbc\xed\x00zR\x9b\xd3\xb7\x8b\xf6\x04W",
        "date": datetime.datetime(2019, 6, 7, 19, 56, 4, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2019, 6, 7, 19, 56, 4, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"0.1.11",
        "metadata": {"package": extrinsic_metadata, "package_source": package_source,},
    }

    origin_url = "https://www.npmjs.com/package/@piximi/components"

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
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:00002019c5775874bced007a529bd3b78bf60457"
                    ),
                    discovery_date=datetime.datetime(
                        2019, 6, 7, 19, 56, 4, tzinfo=datetime.timezone.utc,
                    ),
                    authority=NPM_AUTHORITY,
                    fetcher=FETCHER,
                    format="replicate-npm-package-json",
                    metadata=json.dumps(extrinsic_metadata).encode(),
                    origin=origin_url,
                ),
            ]
        ),
        call.raw_extrinsic_metadata_add(
            [
                RawExtrinsicMetadata(
                    type=MetadataTargetType.REVISION,
                    id=parse_swhid(
                        "swh:1:rev:00002019c5775874bced007a529bd3b78bf60457"
                    ),
                    discovery_date=datetime.datetime(
                        2019, 6, 7, 19, 56, 4, tzinfo=datetime.timezone.utc,
                    ),
                    authority=SWH_AUTHORITY,
                    fetcher=FETCHER,
                    format="original-artifacts-json",
                    metadata=json.dumps(original_artifacts).encode(),
                    origin=origin_url,
                ),
            ]
        ),
    ]
