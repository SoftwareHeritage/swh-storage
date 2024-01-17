#!/usr/bin/env python3

# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This is an executable script to migrate extrinsic revision metadata from
the revision table to the new extrinsic metadata storage.

This is designed to be as conservative as possible, following this principle:
for each revision the script reads (in "handle_row"), it will read some of the
fields, write them directly to the metadata storage, and remove them.
Then it checks all the remaining fields are in a hardcoded list of fields that
are known not to require migration.

This means that every field that isn't migrated was explicitly reviewed while
writing this script.

Additionally, this script contains many assertions to prevent false positives
in its heuristics.
"""

import datetime
import hashlib
import itertools
import json
import os
import re
import sys
import time
from typing import Any, Dict, Optional
from urllib.error import HTTPError
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

import iso8601
import psycopg2

from swh.core.db import BaseDb
from swh.model.hashutil import hash_to_hex
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    RawExtrinsicMetadata,
    Sha1Git,
)
from swh.model.swhids import (
    CoreSWHID,
    ExtendedObjectType,
    ExtendedSWHID,
    ObjectType,
    QualifiedSWHID,
)
from swh.storage import get_storage
from swh.storage.algos.origin import iter_origin_visit_statuses, iter_origin_visits
from swh.storage.algos.snapshot import snapshot_get_all_branches

# XML namespaces and fields for metadata coming from the deposit:

CODEMETA_NS = "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0"
ATOM_NS = "http://www.w3.org/2005/Atom"
ATOM_KEYS = ["id", "author", "external_identifier", "title"]

# columns of the revision table (of the storage DB)
REVISION_COLS = [
    "id",
    "directory",
    "date",
    "committer_date",
    "type",
    "message",
    "metadata",
]

# columns of the tables of the deposit DB
DEPOSIT_COLS = [
    "deposit.id",
    "deposit.external_id",
    "deposit.swhid_context",
    "deposit.status",
    "deposit_request.metadata",
    "deposit_request.date",
    "deposit_client.provider_url",
    "deposit_collection.name",
    "auth_user.username",
]

# Formats we write to the extrinsic metadata storage
OLD_DEPOSIT_FORMAT = (
    "sword-v2-atom-codemeta-v2-in-json-with-expanded-namespaces"  # before february 2018
)
NEW_DEPOSIT_FORMAT = "sword-v2-atom-codemeta-v2-in-json"  # after february 2018
GNU_FORMAT = "gnu-tree-json"
NIXGUIX_FORMAT = "nixguix-sources-json"
NPM_FORMAT = "replicate-npm-package-json"
ORIGINAL_ARTIFACT_FORMAT = "original-artifacts-json"
PYPI_FORMAT = "pypi-project-json"

# Information about this script, for traceability
FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions",
    version="0.0.1",
)

# Authorities that we got the metadata from
AUTHORITIES = {
    "npmjs": MetadataAuthority(
        type=MetadataAuthorityType.FORGE, url="https://npmjs.com/", metadata={}
    ),
    "pypi": MetadataAuthority(
        type=MetadataAuthorityType.FORGE, url="https://pypi.org/", metadata={}
    ),
    "gnu": MetadataAuthority(
        type=MetadataAuthorityType.FORGE, url="https://ftp.gnu.org/", metadata={}
    ),
    "swh": MetadataAuthority(
        type=MetadataAuthorityType.REGISTRY,
        url="https://softwareheritage.org/",
        metadata={},
    ),  # for original_artifact (which are checksums computed by SWH)
}

# Regular expression for the format of revision messages written by the
# deposit loader
deposit_revision_message_re = re.compile(
    b"(?P<client>[a-z-]*): "
    b"Deposit (?P<deposit_id>[0-9]+) in collection (?P<collection>[a-z-]+).*"
)


# not reliable, because PyPI allows arbitrary names
def pypi_project_from_filename(filename):
    original_filename = filename
    if filename.endswith(".egg"):
        return None
    elif filename == "mongomotor-0.13.0.n.tar.gz":
        return "mongomotor"
    elif re.match(r"datahaven-rev[0-9]+\.tar\.gz", filename):
        return "datahaven"
    elif re.match(r"Dtls-[0-9]\.[0-9]\.[0-9]\.sdist_with_openssl\..*", filename):
        return "Dtls"
    elif re.match(r"(gae)?pytz-20[0-9][0-9][a-z]\.(tar\.gz|zip)", filename):
        return filename.split("-", 1)[0]
    elif filename.startswith(
        (
            "powny-",
            "obedient.powny-",
        )
    ):
        return filename.split("-")[0]
    elif filename.startswith("devpi-theme-16-"):
        return "devpi-theme-16"
    elif re.match("[^-]+-[0-9]+.tar.gz", filename):
        return filename.split("-")[0]
    elif filename == "ohai-1!0.tar.gz":
        return "ohai"
    elif filename == "collective.topicitemsevent-0.1dvl.tar.gz":
        return "collective.topicitemsevent"
    elif filename.startswith(
        ("SpiNNStorageHandlers-1!", "sPyNNakerExternalDevicesPlugin-1!")
    ):
        return filename.split("-")[0]
    elif filename.startswith("limnoria-201"):
        return "limnoria"
    elif filename.startswith("pytz-20"):
        return "pytz"
    elif filename.startswith("youtube_dl_server-alpha."):
        return "youtube_dl_server"
    elif filename == "json-extensions-b76bc7d.tar.gz":
        return "json-extensions"
    elif filename == "LitReview-0.6989ev.tar.gz":
        # typo of "dev"
        return "LitReview"
    elif filename.startswith("django_options-r"):
        return "django_options"
    elif filename == "Greater than, equal, or less Library-0.1.tar.gz":
        return "Greater-than-equal-or-less-Library"
    elif filename.startswith("upstart--main-"):
        return "upstart"
    elif filename == "duckduckpy0.1.tar.gz":
        return "duckduckpy"
    elif filename == "QUI for MPlayer snapshot_9-14-2011.zip":
        return "QUI-for-MPlayer"
    elif filename == "Eddy's Memory Game-1.0.zip":
        return "Eddy-s-Memory-Game"
    elif filename == "jekyll2nikola-0-0-1.tar.gz":
        return "jekyll2nikola"
    elif filename.startswith("ore.workflowed"):
        return "ore.workflowed"
    elif re.match("instancemanager-[0-9]*", filename):
        return "instancemanager"
    elif filename == "OrzMC_W&L-1.0.0.tar.gz":
        return "OrzMC-W-L"
    elif filename == "use0mk.tar.gz":
        return "use0mk"
    elif filename == "play-0-develop-1-gd67cd85.tar.gz":
        return "play"
    elif filename.startswith("mosaic-nist-"):
        return "mosaic-nist"
    elif filename.startswith("pypops-"):
        return "pypops"
    elif filename.startswith("pdfcomparator-"):
        return "pdfcomparator"
    elif filename.startswith("LabJackPython-"):
        return "LabJackPython"
    elif filename == "MD2K: Cerebral Cortex-3.0.0.tar.gz":
        return "cerebralcortex-kernel"
    elif filename.startswith("LyMaker-0 (copy)"):
        return "LyMaker"
    elif filename.startswith("python-tplink-smarthome-"):
        return "python-tplink-smarthome"
    elif filename.startswith("jtt=tm-utils-"):
        return "jtt-tm-utils"
    elif filename == "atproject0.1.tar.gz":
        return "atproject"
    elif filename == "labm8.tar.gz":
        return "labm8"
    elif filename == "Bugs Everywhere (BEurtle fork)-1.5.0.1.-2012-07-16-.zip":
        return "Bugs-Everywhere-BEurtle-fork"

    filename = filename.replace(" ", "-")

    match = re.match(
        r"^(?P<project_name>[a-z_.-]+)"  # project name
        r"\.(tar\.gz|tar\.bz2|tgz|zip)$",  # extension
        filename,
        re.I,
    )
    if match:
        return match.group("project_name")

    # First try with a rather strict format, but that allows accidentally
    # matching the version as part of the package name
    match = re.match(
        r"^(?P<project_name>[a-z0-9_.]+?([-_][a-z][a-z0-9.]+?)*?)"  # project name
        r"-v?"
        r"([0-9]+!)?"  # epoch
        r"[0-9_.]+([a-z]+[0-9]+)?"  # "main" version
        r"([.-]?(alpha|beta|dev|post|pre|rc)(\.?[0-9]+)?)*"  # development status
        r"([.-]?20[012][0-9]{5,9})?"  # date
        r"([.-]g?[0-9a-f]+)?"  # git commit
        r"([-+]py(thon)?(3k|[23](\.?[0-9]{1,2})?))?"  # python version
        r"\.(tar\.gz|tar\.bz2|tgz|zip)$",  # extension
        filename,
        re.I,
    )
    if match:
        return match.group("project_name")

    # If that doesn't work, give up on trying to parse version suffixes,
    # and just find the first version-like occurrence in the file name

    match = re.match(
        r"^(?P<project_name>[a-z0-9_.-]+?)"  # project name
        r"[-_.]v?"
        r"([0-9]+!)?"  # epoch
        r"("  # "main" version
        r"[0-9_]+\.[0-9_.]+([a-z]+[0-9]+)?"  # classic version number
        r"|20[012][0-9]{5,9}"  # date as integer
        r"|20[012][0-9]-[01][0-9]-[0-3][0-9]"  # date as ISO 8601
        r")"  # end of "main" version
        r"[a-z]?(dev|pre)?"  # direct version suffix
        r"([._-].*)?"  # extra suffixes
        r"\.(tar\.gz|tar\.bz2|tgz|zip)$",  # extension
        filename,
        re.I,
    )
    if match:
        return match.group("project_name")

    # If that still doesn't work, give one last chance if there's only one
    # dash or underscore in the name

    match = re.match(
        r"^(?P<project_name>[^_-]+)"  # project name
        r"[_-][^_-]+"  # version
        r"\.(tar\.gz|tar\.bz2|tgz|zip)$",  # extension
        filename,
    )
    assert match, original_filename
    return match.group("project_name")


def pypi_origin_from_project_name(project_name: str) -> str:
    return f"https://pypi.org/project/{project_name}/"


def pypi_origin_from_filename(storage, rev_id: bytes, filename: str) -> Optional[str]:
    project_name = pypi_project_from_filename(filename)
    origin = pypi_origin_from_project_name(project_name)
    # But unfortunately, the filename is user-provided, and doesn't
    # necessarily match the package name on pypi. Therefore, we need
    # to check it.
    if _check_revision_in_origin(storage, origin, rev_id):
        return origin

    # if the origin we guessed does not exist, query the PyPI API with the
    # project name we guessed. If only the capitalisation and dash/underscores
    # are wrong (by far the most common case), PyPI kindly corrects them.
    try:
        resp = urlopen(f"https://pypi.org/pypi/{project_name}/json/")
    except HTTPError as e:
        assert e.code == 404
        # nope; PyPI couldn't correct the wrong project name
        return None
    assert resp.code == 200, resp.code
    project_name = json.load(resp)["info"]["name"]
    origin = pypi_origin_from_project_name(project_name)

    if _check_revision_in_origin(storage, origin, rev_id):
        return origin
    else:
        # The origin exists, but the revision does not belong in it.
        # This happens sometimes, as the filename we guessed the origin
        # from is user-provided.
        return None


def cran_package_from_url(filename):
    match = re.match(
        r"^https://cran\.r-project\.org/src/contrib/"
        r"(?P<package_name>[a-zA-Z0-9.]+)_[0-9.-]+(\.tar\.gz)?$",
        filename,
    )
    assert match, filename
    return match.group("package_name")


def npm_package_from_source_url(package_source_url):
    match = re.match(
        "^https://registry.npmjs.org/(?P<package_name>.*)/-/[^/]+.tgz$",
        package_source_url,
    )
    assert match, package_source_url
    return unquote(match.group("package_name"))


def remove_atom_codemeta_metadata_with_xmlns(metadata):
    """Removes all known Atom and Codemeta metadata fields from the dict,
    assuming this is a dict generated by xmltodict without expanding namespaces.
    """
    keys_to_remove = ATOM_KEYS + ["@xmlns", "@xmlns:codemeta"]
    for key in list(metadata):
        if key.startswith("codemeta:") or key in keys_to_remove:
            del metadata[key]


def remove_atom_codemeta_metadata_without_xmlns(metadata):
    """Removes all known Atom and Codemeta metadata fields from the dict,
    assuming this is a dict generated by xmltodict with expanded namespaces.
    """
    for key in list(metadata):
        if key.startswith(("{%s}" % ATOM_NS, "{%s}" % CODEMETA_NS)):
            del metadata[key]


def _check_revision_in_origin(storage, origin, revision_id):
    seen_snapshots = set()  # no need to visit them again
    seen_revisions = set()

    for visit in iter_origin_visits(storage, origin):
        for status in iter_origin_visit_statuses(storage, origin, visit.visit):
            if status.snapshot is None:
                continue
            if status.snapshot in seen_snapshots:
                continue
            seen_snapshots.add(status.snapshot)
            snapshot = snapshot_get_all_branches(storage, status.snapshot)
            for branch_name, branch in snapshot.branches.items():
                if branch is None:
                    continue

                # If it's the revision passed as argument, then it is indeed in the
                # origin
                if branch.target == revision_id:
                    return True

                # Else, let's make sure the branch doesn't have any other revision

                # Get the revision at the top of the branch.
                if branch.target in seen_revisions:
                    continue
                seen_revisions.add(branch.target)
                revision = storage.revision_get([branch.target])[0]

                if revision is None:
                    # https://forge.softwareheritage.org/T997
                    continue

                # Check it doesn't have parents (else we would have to
                # recurse)
                assert revision.parents == (), "revision with parents"

    return False


def debian_origins_from_row(row, storage):
    """Guesses a Debian origin from a row. May return an empty list if it
    cannot reliably guess it, but all results are guaranteed to be correct."""
    filenames = [entry["filename"] for entry in row["metadata"]["original_artifact"]]
    package_names = {filename.split("_")[0] for filename in filenames}
    assert len(package_names) == 1, package_names
    (package_name,) = package_names

    candidate_origins = [
        f"deb://Debian/packages/{package_name}",
        f"deb://Debian-Security/packages/{package_name}",
        f"http://snapshot.debian.org/package/{package_name}/",
    ]

    return [
        origin
        for origin in candidate_origins
        if _check_revision_in_origin(storage, origin, row["id"])
    ]


# Cache of origins that are known to exist
_origins = set()


def assert_origin_exists(storage, origin):
    assert check_origin_exists(storage, origin), origin


def check_origin_exists(storage, origin):
    return (
        (
            hashlib.sha1(origin.encode()).digest() in _origins  # very fast
            or storage.origin_get([origin])[0] is not None  # slow, but up to date
        ),
        origin,
    )


def load_metadata(
    storage,
    revision_id,
    directory_id,
    discovery_date: datetime.datetime,
    metadata: Dict[str, Any],
    format: str,
    authority: MetadataAuthority,
    origin: Optional[str],
    dry_run: bool,
):
    """Does the actual loading to swh-storage."""
    directory_swhid = ExtendedSWHID(
        object_type=ExtendedObjectType.DIRECTORY, object_id=directory_id
    )
    revision_swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=revision_id)
    obj = RawExtrinsicMetadata(
        target=directory_swhid,
        discovery_date=discovery_date,
        authority=authority,
        fetcher=FETCHER,
        format=format,
        metadata=json.dumps(metadata).encode(),
        origin=origin,
        revision=revision_swhid,
    )
    if not dry_run:
        storage.raw_extrinsic_metadata_add([obj])


def handle_deposit_row(
    row,
    discovery_date: Optional[datetime.datetime],
    origin,
    storage,
    deposit_cur,
    dry_run: bool,
):
    """Loads metadata from the deposit database (which is more reliable as the
    metadata on the revision object, as some versions of the deposit loader were
    a bit lossy; and they used very different format for the field in the
    revision table).
    """
    parsed_message = deposit_revision_message_re.match(row["message"])
    assert parsed_message is not None, row["message"]

    deposit_id = int(parsed_message.group("deposit_id"))
    collection = parsed_message.group("collection").decode()
    client_name = parsed_message.group("client").decode()

    deposit_cur.execute(
        f"SELECT {', '.join(DEPOSIT_COLS)} FROM deposit "
        f"INNER JOIN deposit_collection "
        f" ON (deposit.collection_id=deposit_collection.id) "
        f"INNER JOIN deposit_client ON (deposit.client_id=deposit_client.user_ptr_id) "
        f"INNER JOIN auth_user ON (deposit.client_id=auth_user.id) "
        f"INNER JOIN deposit_request ON (deposit.id=deposit_request.deposit_id) "
        f"WHERE deposit.id = %s",
        (deposit_id,),
    )

    provider_urls = set()
    swhids = set()
    metadata_entries = []
    dates = set()
    external_identifiers = set()
    for deposit_request_row in deposit_cur:
        deposit_request = dict(zip(DEPOSIT_COLS, deposit_request_row))

        # Sanity checks to make sure we selected the right deposit
        assert deposit_request["deposit.id"] == deposit_id
        assert deposit_request["deposit_collection.name"] == collection, deposit_request
        if client_name != "":
            # Sometimes it's missing from the commit message
            assert deposit_request["auth_user.username"] == client_name

        # Date of the deposit request (either the initial request, of subsequent ones)
        date = deposit_request["deposit_request.date"]
        dates.add(date)

        if deposit_request["deposit.external_id"] == "hal-02355563":
            # Failed deposit
            swhids.add(
                "swh:1:rev:9293f230baca9814490d4fff7ac53d487a20edb6"
                ";origin=https://hal.archives-ouvertes.fr/hal-02355563"
            )
        else:
            assert deposit_request["deposit.swhid_context"], deposit_request
            swhids.add(deposit_request["deposit.swhid_context"])
        external_identifiers.add(deposit_request["deposit.external_id"])

        # Client of the deposit
        provider_urls.add(deposit_request["deposit_client.provider_url"])

        metadata = deposit_request["deposit_request.metadata"]
        if metadata is not None:
            json.dumps(metadata).encode()  # check it's valid
            if "@xmlns" in metadata:
                assert metadata["@xmlns"] == ATOM_NS
                assert metadata["@xmlns:codemeta"] in (CODEMETA_NS, [CODEMETA_NS])
                format = NEW_DEPOSIT_FORMAT
            elif "{http://www.w3.org/2005/Atom}id" in metadata:
                assert (
                    "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}author" in metadata
                    or "{http://www.w3.org/2005/Atom}author" in metadata
                )
                format = OLD_DEPOSIT_FORMAT
            else:
                # new format introduced in
                # https://forge.softwareheritage.org/D4065
                # it's the same as the first case, but with the @xmlns
                # declarations stripped
                # Most of them should have the "id", but some revisions,
                # like 4d3890004fade1f4ec3bf7004a4af0c490605128, are missing
                # this field
                assert "id" in metadata or "title" in metadata
                assert "codemeta:author" in metadata
                format = NEW_DEPOSIT_FORMAT
            metadata_entries.append((date, format, metadata))

    if discovery_date is None:
        discovery_date = max(dates)

    # Sanity checks to make sure deposit requests are consistent with each other
    assert len(metadata_entries) >= 1, deposit_id
    assert len(provider_urls) == 1, f"expected 1 provider url, got {provider_urls}"
    (provider_url,) = provider_urls
    assert len(swhids) == 1
    (swhid,) = swhids
    assert (
        len(external_identifiers) == 1
    ), f"expected 1 external identifier, got {external_identifiers}"
    (external_identifier,) = external_identifiers

    # computed the origin from the external_identifier if we don't have one
    if origin is None:
        origin = f"{provider_url.strip('/')}/{external_identifier}"

        # explicit list of mistakes that happened in the past, but shouldn't
        # happen again:
        if origin == "https://hal.archives-ouvertes.fr/hal-01588781":
            # deposit id 75
            origin = "https://inria.halpreprod.archives-ouvertes.fr/hal-01588781"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01588782":
            # deposit id 76
            origin = "https://inria.halpreprod.archives-ouvertes.fr/hal-01588782"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01592430":
            # deposit id 143
            origin = "https://hal-preprod.archives-ouvertes.fr/hal-01592430"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01588927":
            origin = "https://inria.halpreprod.archives-ouvertes.fr/hal-01588927"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01593875":
            # deposit id 175
            origin = "https://hal-preprod.archives-ouvertes.fr/hal-01593875"
        elif deposit_id == 160:
            assert origin == "https://www.softwareheritage.org/je-suis-gpl", origin
            origin = "https://forge.softwareheritage.org/source/jesuisgpl/"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01588942":
            # deposit id 90
            origin = "https://inria.halpreprod.archives-ouvertes.fr/hal-01588942"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01592499":
            # deposit id 162
            origin = "https://hal-preprod.archives-ouvertes.fr/hal-01592499"
        elif origin == "https://hal.archives-ouvertes.fr/hal-01588935":
            # deposit id 89
            origin = "https://hal-preprod.archives-ouvertes.fr/hal-01588935"

        assert_origin_exists(storage, origin)

    # check the origin we computed matches the one in the deposit db
    swhid_origin = QualifiedSWHID.from_string(swhid).origin
    if origin is not None:
        # explicit list of mistakes that happened in the past, but shouldn't
        # happen again:
        exceptions = [
            (
                # deposit id 229
                "https://hal.archives-ouvertes.fr/hal-01243573",
                "https://hal-test.archives-ouvertes.fr/hal-01243573",
            ),
            (
                # deposit id 199
                "https://hal.archives-ouvertes.fr/hal-01243065",
                "https://hal-test.archives-ouvertes.fr/hal-01243065",
            ),
            (
                # deposit id 164
                "https://hal.archives-ouvertes.fr/hal-01593855",
                "https://hal-preprod.archives-ouvertes.fr/hal-01593855",
            ),
        ]
        if (origin, swhid_origin) not in exceptions:
            assert origin == swhid_origin, (
                f"the origin we guessed from the deposit db or revision ({origin}) "
                f"doesn't match the one in the deposit db's SWHID ({swhid})"
            )

    authority = MetadataAuthority(
        type=MetadataAuthorityType.DEPOSIT_CLIENT,
        url=provider_url,
        metadata={},
    )

    for date, format, metadata in metadata_entries:
        load_metadata(
            storage,
            row["id"],
            row["directory"],
            date,
            metadata,
            format,
            authority=authority,
            origin=origin,
            dry_run=dry_run,
        )

    return (origin, discovery_date)


def handle_row(row: Dict[str, Any], storage, deposit_cur, dry_run: bool):
    type_ = row["type"]

    # default date in case we can't find a better one
    discovery_date = row["date"] or row["committer_date"]

    metadata = row["metadata"]

    if metadata is None:
        return

    if type_ == "dsc":
        origin = None  # it will be defined later, using debian_origins_from_row

        # TODO: the debian loader writes the changelog date as the revision's
        # author date and committer date. Instead, we should use the visit's date

        if "extrinsic" in metadata:
            extrinsic_files = metadata["extrinsic"]["raw"]["files"]
            for artifact_entry in metadata["original_artifact"]:
                extrinsic_file = extrinsic_files[artifact_entry["filename"]]
                for key in ("sha256",):
                    assert artifact_entry["checksums"][key] == extrinsic_file[key]
                    artifact_entry["url"] = extrinsic_file["uri"]
            del metadata["extrinsic"]

    elif type_ == "tar":
        provider = metadata.get("extrinsic", {}).get("provider")
        if provider is not None:
            # This is the format all the package loaders currently write, and
            # it is the easiest, thanks to the 'provider' and 'when' fields,
            # which have all the information we need to tell them easily
            # and generate accurate metadata

            discovery_date = iso8601.parse_date(metadata["extrinsic"]["when"])

            # New versions of the loaders write the provider; use it.
            if provider.startswith("https://replicate.npmjs.com/"):
                # npm loader format 1

                parsed_url = urlparse(provider)
                assert re.match("^/[^/]+/?$", parsed_url.path), parsed_url
                package_name = unquote(parsed_url.path.strip("/"))
                origin = "https://www.npmjs.com/package/" + package_name
                assert_origin_exists(storage, origin)

                load_metadata(
                    storage,
                    row["id"],
                    row["directory"],
                    discovery_date,
                    metadata["extrinsic"]["raw"],
                    NPM_FORMAT,
                    authority=AUTHORITIES["npmjs"],
                    origin=origin,
                    dry_run=dry_run,
                )
                del metadata["extrinsic"]

            elif provider.startswith("https://pypi.org/"):
                # pypi loader format 1

                match = re.match(
                    "https://pypi.org/pypi/(?P<project_name>.*)/json", provider
                )
                assert match, f"unexpected provider URL format: {provider}"
                project_name = match.group("project_name")
                origin = f"https://pypi.org/project/{project_name}/"
                assert_origin_exists(storage, origin)

                load_metadata(
                    storage,
                    row["id"],
                    row["directory"],
                    discovery_date,
                    metadata["extrinsic"]["raw"],
                    PYPI_FORMAT,
                    authority=AUTHORITIES["pypi"],
                    origin=origin,
                    dry_run=dry_run,
                )
                del metadata["extrinsic"]

            elif provider.startswith("https://cran.r-project.org/"):
                # cran loader

                provider = metadata["extrinsic"]["provider"]
                if provider.startswith("https://cran.r-project.org/package="):
                    origin = metadata["extrinsic"]["provider"]
                else:
                    package_name = cran_package_from_url(provider)
                    origin = f"https://cran.r-project.org/package={package_name}"
                assert origin is not None

                # Ideally we should assert the origin exists, but we can't:
                # https://forge.softwareheritage.org/T2536
                if (
                    hashlib.sha1(origin.encode()).digest() not in _origins
                    and storage.origin_get([origin])[0] is None
                ):
                    return

                raw_extrinsic_metadata = metadata["extrinsic"]["raw"]

                # this is actually intrinsic, ignore it
                if "version" in raw_extrinsic_metadata:
                    del raw_extrinsic_metadata["version"]

                # Copy the URL to the original_artifacts metadata
                assert len(metadata["original_artifact"]) == 1
                if "url" in metadata["original_artifact"][0]:
                    assert (
                        metadata["original_artifact"][0]["url"]
                        == raw_extrinsic_metadata["url"]
                    ), row
                else:
                    metadata["original_artifact"][0]["url"] = raw_extrinsic_metadata[
                        "url"
                    ]
                del raw_extrinsic_metadata["url"]

                assert (
                    raw_extrinsic_metadata == {}
                ), f"Unexpected metadata keys: {list(raw_extrinsic_metadata)}"

                del metadata["extrinsic"]

            elif (
                provider.startswith("https://nix-community.github.io/nixpkgs-swh/")
                or provider == "https://guix.gnu.org/sources.json"
            ):
                # nixguix loader
                origin = provider
                assert_origin_exists(storage, origin)

                authority = MetadataAuthority(
                    type=MetadataAuthorityType.FORGE,
                    url=provider,
                    metadata={},
                )
                assert row["date"] is None  # the nixguix loader does not write dates

                load_metadata(
                    storage,
                    row["id"],
                    row["directory"],
                    discovery_date,
                    metadata["extrinsic"]["raw"],
                    NIXGUIX_FORMAT,
                    authority=authority,
                    origin=origin,
                    dry_run=dry_run,
                )
                del metadata["extrinsic"]

            elif provider.startswith("https://ftp.gnu.org/"):
                # archive loader format 1

                origin = provider
                assert_origin_exists(storage, origin)

                assert len(metadata["original_artifact"]) == 1
                metadata["original_artifact"][0]["url"] = metadata["extrinsic"]["raw"][
                    "url"
                ]

                # Remove duplicate keys of original_artifacts
                for key in ("url", "time", "length", "version", "filename"):
                    del metadata["extrinsic"]["raw"][key]

                assert metadata["extrinsic"]["raw"] == {}
                del metadata["extrinsic"]

            elif provider.startswith("https://deposit.softwareheritage.org/"):
                origin = metadata["extrinsic"]["raw"]["origin"]["url"]
                assert_origin_exists(storage, origin)

                if "@xmlns" in metadata:
                    assert metadata["@xmlns"] == ATOM_NS
                    assert metadata["@xmlns:codemeta"] in (CODEMETA_NS, [CODEMETA_NS])
                    assert "intrinsic" not in metadata
                    assert "extra_headers" not in metadata

                    # deposit loader format 1
                    # in this case, the metadata seems to be both directly in metadata
                    # and in metadata["extrinsic"]["raw"]["metadata"]

                    (origin, discovery_date) = handle_deposit_row(
                        row, discovery_date, origin, storage, deposit_cur, dry_run
                    )

                    remove_atom_codemeta_metadata_with_xmlns(metadata)
                    if "client" in metadata:
                        del metadata["client"]
                    del metadata["extrinsic"]
                else:
                    # deposit loader format 2
                    actual_metadata = metadata["extrinsic"]["raw"]["origin_metadata"][
                        "metadata"
                    ]
                    if isinstance(actual_metadata, str):
                        # new format introduced in
                        # https://forge.softwareheritage.org/D4105
                        actual_metadata = json.loads(actual_metadata)
                    if "@xmlns" in actual_metadata:
                        assert actual_metadata["@xmlns"] == ATOM_NS
                        assert actual_metadata["@xmlns:codemeta"] in (
                            CODEMETA_NS,
                            [CODEMETA_NS],
                        )
                    elif "{http://www.w3.org/2005/Atom}id" in actual_metadata:
                        assert (
                            "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}author"
                            in actual_metadata
                        )
                    else:
                        # new format introduced in
                        # https://forge.softwareheritage.org/D4065
                        # it's the same as the first case, but with the @xmlns
                        # declarations stripped
                        # Most of them should have the "id", but some revisions,
                        # like 4d3890004fade1f4ec3bf7004a4af0c490605128, are missing
                        # this field
                        assert (
                            "id" in actual_metadata
                            or "title" in actual_metadata
                            or "atom:title" in actual_metadata
                        )
                        assert "codemeta:author" in actual_metadata

                    (origin, discovery_date) = handle_deposit_row(
                        row, discovery_date, origin, storage, deposit_cur, dry_run
                    )

                    del metadata["extrinsic"]
            else:
                assert False, f"unknown provider {provider}"

        # Older versions don't write the provider; use heuristics instead.
        elif (
            metadata.get("package_source", {})
            .get("url", "")
            .startswith("https://registry.npmjs.org/")
        ):
            # npm loader format 2

            package_source_url = metadata["package_source"]["url"]
            package_name = npm_package_from_source_url(package_source_url)
            origin = "https://www.npmjs.com/package/" + package_name
            assert_origin_exists(storage, origin)

            load_metadata(
                storage,
                row["id"],
                row["directory"],
                discovery_date,
                metadata["package"],
                NPM_FORMAT,
                authority=AUTHORITIES["npmjs"],
                origin=origin,
                dry_run=dry_run,
            )
            del metadata["package"]

            assert "original_artifact" not in metadata

            # rebuild an "original_artifact"-like metadata dict from what we
            # can salvage of "package_source"
            package_source_metadata = metadata["package_source"]
            keep_keys = {"blake2s256", "filename", "sha1", "sha256", "url"}
            discard_keys = {
                "date",  # is equal to the revision date
                "name",  # was loaded above
                "version",  # same
            }
            assert (
                set(package_source_metadata) == keep_keys | discard_keys
            ), package_source_metadata

            # will be loaded below
            metadata["original_artifact"] = [
                {
                    "filename": package_source_metadata["filename"],
                    "checksums": {
                        "sha1": package_source_metadata["sha1"],
                        "sha256": package_source_metadata["sha256"],
                        "blake2s256": package_source_metadata["blake2s256"],
                    },
                    "url": package_source_metadata["url"],
                }
            ]
            del metadata["package_source"]

        elif "@xmlns" in metadata:
            assert metadata["@xmlns:codemeta"] in (CODEMETA_NS, [CODEMETA_NS])
            assert "intrinsic" not in metadata
            assert "extra_headers" not in metadata

            # deposit loader format 3

            if row["message"] == b"swh: Deposit 159 in collection swh":
                # There is no deposit 159 in the deposit DB, for some reason
                assert (
                    hash_to_hex(row["id"]) == "8e9cee14a6ad39bca4347077b87fb5bbd8953bb1"
                )
                return
            elif row["message"] == b"hal: Deposit 342 in collection hal":
                # They have status 'failed' and no swhid
                return

            origin = None  # TODO
            discovery_date = None  # TODO

            (origin, discovery_date) = handle_deposit_row(
                row, discovery_date, origin, storage, deposit_cur, dry_run
            )
            remove_atom_codemeta_metadata_with_xmlns(metadata)
            if "client" in metadata:
                del metadata["client"]  # found in the deposit db
            if "committer" in metadata:
                del metadata["committer"]  # found on the revision object

        elif "{http://www.w3.org/2005/Atom}id" in metadata:
            assert (
                "{https://doi.org/10.5063/SCHEMA/CODEMETA-2.0}author" in metadata
                or "{http://www.w3.org/2005/Atom}author" in metadata
            )
            assert "intrinsic" not in metadata
            assert "extra_headers" not in metadata

            # deposit loader format 4

            origin = None
            discovery_date = None  # TODO

            (origin, discovery_date) = handle_deposit_row(
                row, discovery_date, origin, storage, deposit_cur, dry_run
            )
            remove_atom_codemeta_metadata_without_xmlns(metadata)

        elif hash_to_hex(row["id"]) == "a86747d201ab8f8657d145df4376676d5e47cf9f":
            # deposit 91, is missing "{http://www.w3.org/2005/Atom}id" for some
            # reason, and has an invalid oririn
            return

        elif (
            isinstance(metadata.get("original_artifact"), dict)
            and metadata["original_artifact"]["url"].startswith(
                "https://files.pythonhosted.org/"
            )
        ) or (
            isinstance(metadata.get("original_artifact"), list)
            and len(metadata.get("original_artifact")) == 1
            and metadata["original_artifact"][0]
            .get("url", "")
            .startswith("https://files.pythonhosted.org/")
        ):
            if isinstance(metadata.get("original_artifact"), dict):
                metadata["original_artifact"] = [metadata["original_artifact"]]

            assert len(metadata["original_artifact"]) == 1

            version = metadata.get("project", {}).get("version")
            filename = metadata["original_artifact"][0]["filename"]
            if version:
                origin = pypi_origin_from_project_name(filename.split("-" + version)[0])
                if not _check_revision_in_origin(storage, origin, row["id"]):
                    origin = None
            else:
                origin = None
            if origin is None:
                origin = pypi_origin_from_filename(storage, row["id"], filename)

            if "project" in metadata:
                # pypi loader format 2
                load_metadata(
                    storage,
                    row["id"],
                    row["directory"],
                    discovery_date,
                    metadata["project"],
                    PYPI_FORMAT,
                    authority=AUTHORITIES["pypi"],
                    origin=origin,
                    dry_run=dry_run,
                )
                del metadata["project"]
            else:
                assert set(metadata) == {"original_artifact"}, set(metadata)
                # pypi loader format 3
                pass  # nothing to do, there's no metadata

        elif row["message"] == b"synthetic revision message":
            assert isinstance(metadata["original_artifact"], list), metadata
            assert not any("url" in d for d in metadata["original_artifact"])

            # archive loader format 2

            origin = None

        elif deposit_revision_message_re.match(row["message"]):
            # deposit without metadata in the revision

            assert set(metadata) == {"original_artifact"}, metadata

            origin = None  # TODO
            discovery_date = None

            (origin, discovery_date) = handle_deposit_row(
                row, discovery_date, origin, storage, deposit_cur, dry_run
            )
        else:
            assert False, f"Unable to detect type of metadata for row: {row}"

    # Ignore common intrinsic metadata keys
    for key in ("intrinsic", "extra_headers"):
        if key in metadata:
            del metadata[key]

    # Ignore loader-specific intrinsic metadata keys
    if type_ == "hg":
        del metadata["node"]
    elif type_ == "dsc":
        if "package_info" in metadata:
            del metadata["package_info"]

    if "original_artifact" in metadata:
        for original_artifact in metadata["original_artifact"]:
            # Rename keys to the expected format of original-artifacts-json.
            rename_keys = [
                ("name", "filename"),  # eg. from old Debian loader
                ("size", "length"),  # eg. from old PyPI loader
            ]
            for old_name, new_name in rename_keys:
                if old_name in original_artifact:
                    assert new_name not in original_artifact
                    original_artifact[new_name] = original_artifact.pop(old_name)

            # Move the checksums to their own subdict, which is the expected format
            # of original-artifacts-json.
            if "sha1" in original_artifact:
                assert "checksums" not in original_artifact
                original_artifact["checksums"] = {}
                for key in ("sha1", "sha256", "sha1_git", "blake2s256"):
                    if key in original_artifact:
                        original_artifact["checksums"][key] = original_artifact.pop(key)

            if "date" in original_artifact:
                # The information comes from the package repository rather than SWH,
                # so it shouldn't be in the 'original-artifacts' metadata
                # (which has SWH as authority).
                # Moreover, it's not a very useful information, so let's just drop it.
                del original_artifact["date"]

            allowed_keys = {
                "checksums",
                "filename",
                "length",
                "url",
                "archive_type",
            }
            assert set(original_artifact) <= allowed_keys, set(original_artifact)

        if type_ == "dsc":
            assert origin is None
            origins = debian_origins_from_row(row, storage)
            if not origins:
                print(f"Missing Debian origin for revision: {hash_to_hex(row['id'])}")
        else:
            origins = [origin]

        for origin in origins:
            load_metadata(
                storage,
                row["id"],
                row["directory"],
                discovery_date,
                metadata["original_artifact"],
                ORIGINAL_ARTIFACT_FORMAT,
                authority=AUTHORITIES["swh"],
                origin=origin,
                dry_run=dry_run,
            )
        del metadata["original_artifact"]

    assert metadata == {}, (
        f"remaining metadata keys for {row['id'].hex()} (type: {row['type']}): "
        f"{metadata}"
    )


def create_fetchers(db):
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata_fetcher (name, version, metadata)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (FETCHER.name, FETCHER.version, FETCHER.metadata),
        )


def iter_revision_rows(storage_dbconn: str, first_id: Sha1Git):
    after_id = first_id
    failures = 0
    while True:
        try:
            storage_db = BaseDb.connect(storage_dbconn)
            with storage_db.cursor() as cur:
                while True:
                    cur.execute(
                        f"SELECT {', '.join(REVISION_COLS)} FROM revision "
                        f"WHERE id >= %s AND metadata IS NOT NULL AND type != 'git'"
                        f"ORDER BY id LIMIT 1000",
                        (after_id,),
                    )
                    new_rows = 0
                    for row in cur:
                        new_rows += 1
                        row_d = dict(zip(REVISION_COLS, row))
                        yield row_d
                    after_id = row_d["id"]
                    if new_rows == 0:
                        return
        except psycopg2.OperationalError as e:
            print(e)
            # most likely a temporary error, try again
            if failures >= 60:
                raise
            else:
                time.sleep(60)
                failures += 1


def main(storage_dbconn, storage_url, deposit_dbconn, first_id, limit, dry_run):
    storage_db = BaseDb.connect(storage_dbconn)
    deposit_db = BaseDb.connect(deposit_dbconn)
    storage = get_storage(
        "pipeline",
        steps=[
            {"cls": "retry"},
            {
                "cls": "postgresql",
                "db": storage_dbconn,
                "objstorage": {"cls": "memory", "args": {}},
            },
        ],
    )

    if not dry_run:
        create_fetchers(storage_db)
        # Not creating authorities, as the loaders are presumably already running
        # and created them already.
        # This also helps make sure this script doesn't accidentally create
        # authorities that differ from what the loaders use.

    total_rows = 0
    with deposit_db.cursor() as deposit_cur:
        rows = iter_revision_rows(storage_dbconn, first_id)
        if limit is not None:
            rows = itertools.islice(rows, limit)
        for row in rows:
            handle_row(row, storage, deposit_cur, dry_run)

            total_rows += 1

            if total_rows % 1000 == 0:
                percents = (
                    int.from_bytes(row["id"][0:4], byteorder="big") * 100 / (1 << 32)
                )
                print(
                    f"Processed {total_rows/1000000.:.2f}M rows "
                    f"(~{percents:.1f}%, last revision: {row['id'].hex()})"
                )


if __name__ == "__main__":
    if len(sys.argv) == 4:
        (_, storage_dbconn, storage_url, deposit_dbconn) = sys.argv
        first_id = "00" * 20
    elif len(sys.argv) == 5:
        (_, storage_dbconn, storage_url, deposit_dbconn, first_id) = sys.argv
        limit = None
    elif len(sys.argv) == 6:
        (_, storage_dbconn, storage_url, deposit_dbconn, first_id, limit_str) = sys.argv
        limit = int(limit_str)
    else:
        print(
            f"Syntax: {sys.argv[0]} <storage_dbconn> <storage_url> "
            f"<deposit_dbconn> [<first id> [limit]]"
        )
        exit(1)

    if os.path.isfile("./origins.txt"):
        # You can generate this file with:
        # psql service=swh-replica \
        #   -c "\copy (select digest(url, 'sha1') from origin) to stdout" \
        #   | pv -l > origins.txt
        print("Loading origins...")
        with open("./origins.txt") as fd:
            for line in fd:
                digest = line.strip()[3:]
                _origins.add(bytes.fromhex(digest))
        print("Done loading origins.")

    main(
        storage_dbconn,
        storage_url,
        deposit_dbconn,
        bytes.fromhex(first_id),
        limit,
        True,
    )
