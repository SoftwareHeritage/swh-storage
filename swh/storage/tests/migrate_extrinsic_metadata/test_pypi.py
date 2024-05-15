# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# flake8: noqa
# because of long lines

import copy
import datetime
import json
import urllib.error

import attr

from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    RawExtrinsicMetadata,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID
from swh.storage import get_storage
from swh.storage.interface import PagedResult
from swh.storage.migrate_extrinsic_metadata import (
    handle_row,
    pypi_origin_from_filename,
    pypi_project_from_filename,
)

FETCHER = MetadataFetcher(
    name="migrate-extrinsic-metadata-from-revisions",
    version="0.0.1",
)
PYPI_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.FORGE,
    url="https://pypi.org/",
)
SWH_AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.REGISTRY,
    url="https://softwareheritage.org/",
)

DIRECTORY_ID = b"a" * 20
DIRECTORY_SWHID = ExtendedSWHID(
    object_type=ExtendedObjectType.DIRECTORY, object_id=DIRECTORY_ID
)


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


def test_pypi_project_from_filename():
    files = [
        ("django-agent-trust-0.1.8.tar.gz", "django-agent-trust"),
        ("python_test-1.0.1.zip", "python_test"),
        ("py-evm-0.2.0a9.tar.gz", "py-evm"),
        ("collective.texttospeech-1.0rc1.tar.gz", "collective.texttospeech"),
        ("flatland-fork-0.4.post1.dev40550160.zip", "flatland-fork"),
        ("fake-factory-0.5.6-proper.tar.gz", "fake-factory"),
        ("ariane_procos-0.1.2-b05.tar.gz", "ariane_procos"),
        ("Yelpy-0.2.2dev.tar.gz", "Yelpy"),
        ("geventhttpclient_c-1.0a-t1.tar.gz", "geventhttpclient_c"),
        ("codeforlife-portal-1.0.0.post.dev618.tar.gz", "codeforlife-portal"),
        ("ChecklistDSL-0.0.1.alpha.1.tar.gz", "ChecklistDSL"),
        ("transifex-1.1.0beta.tar.gz", "transifex"),
        ("thespian-2.5.10.tar.bz2", "thespian"),
        ("janis pipelines-0.5.3.tar.gz", "janis-pipelines"),
        ("pants-1.0.0-beta.2.tar.gz", "pants"),
        ("uforge_python_sdk-3.8.4-RC15.tar.gz", "uforge_python_sdk"),
        ("virtuoso-0.11.0.48.b5865c2b46fb.tar.gz", "virtuoso"),
        ("cloud_ftp-v1.0.0.tar.gz", "cloud_ftp"),
        ("frozenordereddict-1.0.0.tgz", "frozenordereddict"),
        ("pywebsite-0.1.2pre.tar.gz", "pywebsite"),
        ("Flask Unchained-0.2.0.tar.gz", "Flask-Unchained"),
        ("mongomotor-0.13.0.n.tar.gz", "mongomotor"),
        ("datahaven-rev8784.tar.gz", "datahaven"),
        ("geopandas-0.1.0.dev-120d5ee.tar.gz", "geopandas"),
        ("aimmo-v0.1.1-alpha.post.dev61.tar.gz", "aimmo"),
        ("django-migrations-plus-0.1.0.dev5.gdd1abd3.tar.gz", "django-migrations-plus"),
        ("function_shield.tar.gz", "function_shield"),
        ("Dtls-0.1.0.sdist_with_openssl.mingw-win32.tar.gz", "Dtls"),
        ("pytz-2005m.tar.gz", "pytz"),
        ("python-librsync-0.1-3.tar.gz", "python-librsync"),
        ("powny-1.4.0-alpha-20141205-1452-f5a2b03.tar.gz", "powny"),
        ("stp-3pc-batch-0.1.11.tar.gz", "stp-3pc-batch"),
        ("obedient.powny-3.0.0-alpha-20141027-2102-9e53ebd.tar.gz", "obedient.powny"),
        ("mojimoji-0.0.9_2.tar.gz", "mojimoji"),
        ("devpi-theme-16-2.0.0.tar.gz", "devpi-theme-16"),
        ("Orange3-WONDER-1-1.0.7.tar.gz", "Orange3-WONDER-1"),
        ("obj-34.tar.gz", "obj"),
        ("pytorch-ignite-nightly-20190825.tar.gz", "pytorch-ignite-nightly"),
        ("tlds-2019081900.tar.gz", "tlds"),
        ("dominator-12.1.2-alpha-20141027-1446-ad46e0f.tar.gz", "dominator"),
        ("waferslim-1.0.0-py3.1.zip", "waferslim"),
        ("Beaver-21.tar.gz", "Beaver"),
        ("aimmo-0.post.dev460.tar.gz", "aimmo"),
        ("ohai-1!0.tar.gz", "ohai"),
        ("nevolution-risk-139.tar.gz", "nevolution-risk"),
        ("collective.topicitemsevent-0.1dvl.tar.gz", "collective.topicitemsevent"),
        ("lesscpy-0.9g.tar.gz", "lesscpy"),
        ("SpiNNStorageHandlers-1!4.0.0a1.tar.gz", "SpiNNStorageHandlers"),
        ("limnoria-2013-03-27T16:32:26+0100.tar.gz", "limnoria"),
        (
            "sPyNNakerExternalDevicesPlugin-1!4.0.0a2.tar.gz",
            "sPyNNakerExternalDevicesPlugin",
        ),
        ("django-bootstrap-italia_0.1.tar.gz", "django-bootstrap-italia"),
        ("sPyNNaker8-1!4.0.0a1.tar.gz", "sPyNNaker8"),
        ("betahaus.openmember-0.1adev-r1651.tar.gz", "betahaus.openmember"),
        ("mailer.0.8.0.zip", "mailer"),
        ("pytz-2005k.tar.bz2", "pytz"),
        ("aha.plugin.microne-0.62bdev.tar.gz", "aha.plugin.microne"),
        ("youtube_dl_server-alpha.3.tar.gz", "youtube_dl_server"),
        ("json-extensions-b76bc7d.tar.gz", "json-extensions"),
        ("LitReview-0.6989ev.tar.gz", "LitReview"),
        ("django_options-r5.tar.gz", "django_options"),
        ("ddlib-2013-11-07.tar.gz", "ddlib"),
        ("python-morfeusz-0.3000+py3k.tar.gz", "python-morfeusz"),
        ("gaepytz-2011h.zip", "gaepytz"),
        ("ftldat-r3.tar.gz", "ftldat"),
        ("tigretoolbox-0.0.0-py2.7-linux-x86_64.egg", None),
        (
            "Greater than, equal, or less Library-0.1.tar.gz",
            "Greater-than-equal-or-less-Library",
        ),
        ("upstart--main-.-VLazy.object.at.0x104ba8b50-.tar.gz", "upstart"),
        ("duckduckpy0.1.tar.gz", "duckduckpy"),
        ("QUI for MPlayer snapshot_9-14-2011.zip", "QUI-for-MPlayer"),
        ("Eddy's Memory Game-1.0.zip", "Eddy-s-Memory-Game"),
        ("jekyll2nikola-0-0-1.tar.gz", "jekyll2nikola"),
        ("ore.workflowed-0-6-2.tar.gz", "ore.workflowed"),
        ("instancemanager-1.0rc-r34317.tar.gz", "instancemanager"),
        ("OrzMC_W&L-1.0.0.tar.gz", "OrzMC-W-L"),
        ("use0mk.tar.gz", "use0mk"),
        ("play-0-develop-1-gd67cd85.tar.gz", "play"),
        ("mosaic-nist-2.0b1+f98ae80.tar.gz", "mosaic-nist"),
        ("pypops-201408-r3.tar.gz", "pypops"),
    ]

    for filename, project in files:
        assert pypi_project_from_filename(filename) == project


def test_pypi_origin_from_project_name(mocker):
    origin_url = "https://pypi.org/project/ProjectName/"

    storage = get_storage("memory")

    revision_id = b"41" * 10
    snapshot_id = b"42" * 10
    storage.origin_add([Origin(url=origin_url)])
    storage.origin_visit_add(
        [OriginVisit(origin=origin_url, visit=1, date=now(), type="pypi")]
    )
    storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin_url,
                visit=1,
                date=now(),
                status="partial",
                snapshot=snapshot_id,
            )
        ]
    )
    storage.snapshot_add(
        [
            Snapshot(
                id=snapshot_id,
                branches={
                    b"foo": SnapshotBranch(
                        target_type=SnapshotTargetType.REVISION,
                        target=revision_id,
                    )
                },
            )
        ]
    )

    class response:
        code = 200

        def read(self):
            return b'{"info": {"name": "ProjectName"}}'

    mock_urlopen = mocker.patch(
        "swh.storage.migrate_extrinsic_metadata.urlopen",
        return_value=response(),
    )

    assert (
        pypi_origin_from_filename(storage, revision_id, "ProjectName-1.0.0.tar.gz")
        == origin_url
    )
    mock_urlopen.assert_not_called()
    assert (
        pypi_origin_from_filename(storage, revision_id, "projectname-1.0.0.tar.gz")
        == origin_url
    )
    mock_urlopen.assert_called_once_with("https://pypi.org/pypi/projectname/json/")


def test_pypi_1():
    """Tests loading a revision generated by a new PyPI loader that
    has a provider."""

    extrinsic_metadata = {
        "url": "https://files.pythonhosted.org/packages/70/89/a498245baf1bf3dde73d3da00b4b067a8aa7c7378ad83472078803ea3e43/m3-ui-2.2.73.tar.gz",
        "size": 3933168,
        "digests": {
            "md5": "a374ac3f655e97df5db5335e2142d344",
            "sha256": "1bc2756f7d0d2e15cf5880ca697682ff35e8b58116bf73eb9c78b3db358c5b7d",
        },
        "has_sig": False,
        "filename": "m3-ui-2.2.73.tar.gz",
        "downloads": -1,
        "md5_digest": "a374ac3f655e97df5db5335e2142d344",
        "packagetype": "sdist",
        "upload_time": "2019-11-11T06:21:20",
        "comment_text": "",
        "python_version": "source",
        "requires_python": None,
        "upload_time_iso_8601": "2019-11-11T06:21:20.073082Z",
    }

    original_artifacts = [
        {
            "length": 3933168,
            "filename": "m3-ui-2.2.73.tar.gz",
            "checksums": {
                "sha1": "9f4ec7ce64b7fea4b122e85d47ea31146c367b03",
                "sha256": "1bc2756f7d0d2e15cf5880ca697682ff35e8b58116bf73eb9c78b3db358c5b7d",
            },
        }
    ]

    row = {
        "id": b"\x00\x00\x07a{S\xe7\xb1E\x8fi]\xd0}\xe4\xceU\xaf\x15\x17",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2019,
            11,
            11,
            6,
            21,
            20,
            tzinfo=datetime.timezone.utc,
        ),
        "committer_date": datetime.datetime(
            2019,
            11,
            11,
            6,
            21,
            20,
            tzinfo=datetime.timezone.utc,
        ),
        "type": "tar",
        "message": b"2.2.73",
        "metadata": {
            "extrinsic": {
                "raw": extrinsic_metadata,
                "when": "2020-01-23T18:43:09.109407+00:00",
                "provider": "https://pypi.org/pypi/m3-ui/json",
            },
            "intrinsic": {
                "raw": {
                    "name": "m3-ui",
                    "summary": "======",
                    "version": "2.2.73",
                    # ...
                    "metadata_version": "1.1",
                },
                "tool": "PKG-INFO",
            },
            "original_artifact": original_artifacts,
        },
    }

    origin_url = "https://pypi.org/project/m3-ui/"

    storage = get_storage("memory")
    storage.origin_add([Origin(url=origin_url)])
    storage.metadata_authority_add(
        [
            attr.evolve(PYPI_AUTHORITY, metadata={}),
            attr.evolve(SWH_AUTHORITY, metadata={}),
        ]
    )
    storage.metadata_fetcher_add([FETCHER])

    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    revision_swhid = CoreSWHID.from_string(
        "swh:1:rev:000007617b53e7b1458f695dd07de4ce55af1517"
    )
    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=PYPI_AUTHORITY,
    ) == PagedResult(
        results=[
            RawExtrinsicMetadata(
                target=DIRECTORY_SWHID,
                discovery_date=datetime.datetime(
                    2020,
                    1,
                    23,
                    18,
                    43,
                    9,
                    109407,
                    tzinfo=datetime.timezone.utc,
                ),
                authority=PYPI_AUTHORITY,
                fetcher=FETCHER,
                format="pypi-project-json",
                metadata=json.dumps(extrinsic_metadata).encode(),
                origin=origin_url,
                revision=revision_swhid,
            ),
        ],
        next_page_token=None,
    )
    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=SWH_AUTHORITY,
    ) == PagedResult(
        results=[
            RawExtrinsicMetadata(
                target=DIRECTORY_SWHID,
                discovery_date=datetime.datetime(
                    2020,
                    1,
                    23,
                    18,
                    43,
                    9,
                    109407,
                    tzinfo=datetime.timezone.utc,
                ),
                authority=SWH_AUTHORITY,
                fetcher=FETCHER,
                format="original-artifacts-json",
                metadata=json.dumps(original_artifacts).encode(),
                origin=origin_url,
                revision=revision_swhid,
            ),
        ],
        next_page_token=None,
    )


def test_pypi_2(mocker):
    """Tests loading a revision generated by an old PyPI loader that
    does not have a provider, but has 'project' metadata."""

    mocker.patch(
        "swh.storage.migrate_extrinsic_metadata.urlopen",
        side_effect=urllib.error.HTTPError(None, 404, "Not Found", None, None),
    )

    extrinsic_metadata = {
        "name": "jupyterhub-simx",
        "author": "Jupyter Development Team",
        "license": "BSD",
        "summary": "JupyterHub: A multi-user server for Jupyter notebooks",
        "version": "1.0.5",
        # ...
    }

    source_original_artifacts = [
        {
            "url": "https://files.pythonhosted.org/packages/72/28/a8098763d78e2c4607cb67602c0d726a97ac38d4c1f531aac28f49de2e1a/jupyterhub-simx-1.0.5.tar.gz",
            "date": "2019-01-23T22:10:55",
            "sha1": "ede3eadd5a06e70912e3ba7cfccef789c4ad3168",
            "size": 2346538,
            "sha256": "0399d7f5f0d90c525d369f0507ad0e8ef8729c1c7fa63aadfc46a27514d14a46",
            "filename": "jupyterhub-simx-1.0.5.tar.gz",
            "sha1_git": "734301124712182eb30fc90e97cc18cef5432f02",
            "blake2s256": "bb4aa82ffb5891a05dcf6d4dce3ad56fd2c18e9abdba9d20972910649d869322",
            "archive_type": "tar",
        }
    ]

    dest_original_artifacts = [
        {
            "url": "https://files.pythonhosted.org/packages/72/28/a8098763d78e2c4607cb67602c0d726a97ac38d4c1f531aac28f49de2e1a/jupyterhub-simx-1.0.5.tar.gz",
            "filename": "jupyterhub-simx-1.0.5.tar.gz",
            "archive_type": "tar",
            "length": 2346538,
            "checksums": {
                "sha1": "ede3eadd5a06e70912e3ba7cfccef789c4ad3168",
                "sha256": "0399d7f5f0d90c525d369f0507ad0e8ef8729c1c7fa63aadfc46a27514d14a46",
                "sha1_git": "734301124712182eb30fc90e97cc18cef5432f02",
                "blake2s256": "bb4aa82ffb5891a05dcf6d4dce3ad56fd2c18e9abdba9d20972910649d869322",
            },
        }
    ]

    row = {
        "id": b"\x00\x00\x04\xd68,J\xd4\xc0Q\x92fbl6U\x1f\x0eQ\xca",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(
            2019, 1, 23, 22, 10, 55, tzinfo=datetime.timezone.utc
        ),
        "committer_date": datetime.datetime(
            2019, 1, 23, 22, 10, 55, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"1.0.5",
        "metadata": {
            "project": extrinsic_metadata,
            "original_artifact": source_original_artifacts,
        },
    }

    origin_url = "https://pypi.org/project/jupyterhub-simx/"

    storage = get_storage("memory")

    storage.origin_add([Origin(url=origin_url)])
    storage.metadata_authority_add(
        [
            attr.evolve(PYPI_AUTHORITY, metadata={}),
            attr.evolve(SWH_AUTHORITY, metadata={}),
        ]
    )
    storage.metadata_fetcher_add([FETCHER])
    deposit_cur = None

    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    revision_swhid = CoreSWHID.from_string(
        "swh:1:rev:000004d6382c4ad4c0519266626c36551f0e51ca"
    )
    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=PYPI_AUTHORITY,
    ) == PagedResult(
        results=[
            RawExtrinsicMetadata(
                target=DIRECTORY_SWHID,
                discovery_date=datetime.datetime(
                    2019,
                    1,
                    23,
                    22,
                    10,
                    55,
                    tzinfo=datetime.timezone.utc,
                ),
                authority=PYPI_AUTHORITY,
                fetcher=FETCHER,
                format="pypi-project-json",
                metadata=json.dumps(extrinsic_metadata).encode(),
                origin=None,
                revision=revision_swhid,
            ),
        ],
        next_page_token=None,
    )
    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=SWH_AUTHORITY,
    ) == PagedResult(
        results=[
            RawExtrinsicMetadata(
                target=DIRECTORY_SWHID,
                discovery_date=datetime.datetime(
                    2019,
                    1,
                    23,
                    22,
                    10,
                    55,
                    tzinfo=datetime.timezone.utc,
                ),
                authority=SWH_AUTHORITY,
                fetcher=FETCHER,
                format="original-artifacts-json",
                metadata=json.dumps(dest_original_artifacts).encode(),
                origin=None,
                revision=revision_swhid,
            ),
        ],
        next_page_token=None,
    )


def test_pypi_3(mocker):
    """Tests loading a revision generated by a very old PyPI loader that
    does not have a provider or has 'project' metadata."""

    mocker.patch(
        "swh.storage.migrate_extrinsic_metadata.urlopen",
        side_effect=urllib.error.HTTPError(None, 404, "Not Found", None, None),
    )

    source_original_artifact = {
        "url": "https://files.pythonhosted.org/packages/34/4f/30087f22eaae8ad7077a28ce157342745a2977e264b8a8e4e7f804a8aa5e/PyPDFLite-0.1.32.tar.gz",
        "date": "2014-05-07T22:03:00",
        "sha1": "3289269f75b4111dd00eaea53e00330db9a1db12",
        "size": 46644,
        "sha256": "911497d655cf7ef6530c5b57773dad7da97e21cf4d608ad9ad1e38bd7bec7824",
        "filename": "PyPDFLite-0.1.32.tar.gz",
        "sha1_git": "1e5c38014731242cfa8594839bcba8a0c4e158c5",
        "blake2s256": "45792e57873f56d385c694e36c98a580cbba60d5ea91eb6fd0a2d1c71c1fb385",
        "archive_type": "tar",
    }

    dest_original_artifacts = [
        {
            "url": "https://files.pythonhosted.org/packages/34/4f/30087f22eaae8ad7077a28ce157342745a2977e264b8a8e4e7f804a8aa5e/PyPDFLite-0.1.32.tar.gz",
            "filename": "PyPDFLite-0.1.32.tar.gz",
            "archive_type": "tar",
            "length": 46644,
            "checksums": {
                "sha1": "3289269f75b4111dd00eaea53e00330db9a1db12",
                "sha256": "911497d655cf7ef6530c5b57773dad7da97e21cf4d608ad9ad1e38bd7bec7824",
                "sha1_git": "1e5c38014731242cfa8594839bcba8a0c4e158c5",
                "blake2s256": "45792e57873f56d385c694e36c98a580cbba60d5ea91eb6fd0a2d1c71c1fb385",
            },
        }
    ]

    row = {
        "id": b"N\xa9\x91|\xdfS\xcd\x13SJ\x04.N\xb3x{\x86\xc84\xd2",
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2014, 5, 7, 22, 3, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2014, 5, 7, 22, 3, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"0.1.32",
        "metadata": {"original_artifact": source_original_artifact},
    }

    origin_url = "https://pypi.org/project/PyPDFLite/"

    storage = get_storage("memory")

    storage.origin_add([Origin(url=origin_url)])
    storage.metadata_authority_add(
        [
            attr.evolve(PYPI_AUTHORITY, metadata={}),
            attr.evolve(SWH_AUTHORITY, metadata={}),
        ]
    )
    storage.metadata_fetcher_add([FETCHER])
    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    revision_swhid = CoreSWHID.from_string(
        "swh:1:rev:4ea9917cdf53cd13534a042e4eb3787b86c834d2"
    )

    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=PYPI_AUTHORITY,
    ) == PagedResult(
        results=[],
        next_page_token=None,
    )
    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=SWH_AUTHORITY,
    ) == PagedResult(
        results=[
            RawExtrinsicMetadata(
                target=DIRECTORY_SWHID,
                discovery_date=datetime.datetime(
                    2014,
                    5,
                    7,
                    22,
                    3,
                    tzinfo=datetime.timezone.utc,
                ),
                authority=SWH_AUTHORITY,
                fetcher=FETCHER,
                format="original-artifacts-json",
                metadata=json.dumps(dest_original_artifacts).encode(),
                origin=None,
                revision=revision_swhid,
            ),
        ],
        next_page_token=None,
    )


def test_pypi_good_origin():
    """Tests loading a revision whose origin we can find"""

    source_original_artifact = {
        "url": "https://files.pythonhosted.org/packages/34/4f/30087f22eaae8ad7077a28ce157342745a2977e264b8a8e4e7f804a8aa5e/PyPDFLite-0.1.32.tar.gz",
        "date": "2014-05-07T22:03:00",
        "sha1": "3289269f75b4111dd00eaea53e00330db9a1db12",
        "size": 46644,
        "sha256": "911497d655cf7ef6530c5b57773dad7da97e21cf4d608ad9ad1e38bd7bec7824",
        "filename": "PyPDFLite-0.1.32.tar.gz",
        "sha1_git": "1e5c38014731242cfa8594839bcba8a0c4e158c5",
        "blake2s256": "45792e57873f56d385c694e36c98a580cbba60d5ea91eb6fd0a2d1c71c1fb385",
        "archive_type": "tar",
    }

    dest_original_artifacts = [
        {
            "url": "https://files.pythonhosted.org/packages/34/4f/30087f22eaae8ad7077a28ce157342745a2977e264b8a8e4e7f804a8aa5e/PyPDFLite-0.1.32.tar.gz",
            "filename": "PyPDFLite-0.1.32.tar.gz",
            "archive_type": "tar",
            "length": 46644,
            "checksums": {
                "sha1": "3289269f75b4111dd00eaea53e00330db9a1db12",
                "sha256": "911497d655cf7ef6530c5b57773dad7da97e21cf4d608ad9ad1e38bd7bec7824",
                "sha1_git": "1e5c38014731242cfa8594839bcba8a0c4e158c5",
                "blake2s256": "45792e57873f56d385c694e36c98a580cbba60d5ea91eb6fd0a2d1c71c1fb385",
            },
        }
    ]

    revision_id = b"N\xa9\x91|\xdfS\xcd\x13SJ\x04.N\xb3x{\x86\xc84\xd2"
    row = {
        "id": revision_id,
        "directory": DIRECTORY_ID,
        "date": datetime.datetime(2014, 5, 7, 22, 3, tzinfo=datetime.timezone.utc),
        "committer_date": datetime.datetime(
            2014, 5, 7, 22, 3, tzinfo=datetime.timezone.utc
        ),
        "type": "tar",
        "message": b"0.1.32",
        "metadata": {"original_artifact": source_original_artifact},
    }

    origin_url = "https://pypi.org/project/PyPDFLite/"

    storage = get_storage("memory")

    snapshot_id = b"42" * 10
    storage.origin_add([Origin(url=origin_url)])
    storage.origin_visit_add(
        [OriginVisit(origin=origin_url, visit=1, date=now(), type="pypi")]
    )
    storage.origin_visit_status_add(
        [
            OriginVisitStatus(
                origin=origin_url,
                visit=1,
                date=now(),
                status="partial",
                snapshot=snapshot_id,
            )
        ]
    )
    storage.snapshot_add(
        [
            Snapshot(
                id=snapshot_id,
                branches={
                    b"foo": SnapshotBranch(
                        target_type=SnapshotTargetType.REVISION,
                        target=revision_id,
                    )
                },
            )
        ]
    )
    storage.metadata_authority_add(
        [
            attr.evolve(PYPI_AUTHORITY, metadata={}),
            attr.evolve(SWH_AUTHORITY, metadata={}),
        ]
    )
    storage.metadata_fetcher_add([FETCHER])
    deposit_cur = None
    handle_row(copy.deepcopy(row), storage, deposit_cur, dry_run=False)

    revision_swhid = CoreSWHID.from_string(
        "swh:1:rev:4ea9917cdf53cd13534a042e4eb3787b86c834d2"
    )

    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=PYPI_AUTHORITY,
    ) == PagedResult(
        results=[],
        next_page_token=None,
    )
    assert storage.raw_extrinsic_metadata_get(
        DIRECTORY_SWHID,
        authority=SWH_AUTHORITY,
    ) == PagedResult(
        results=[
            RawExtrinsicMetadata(
                target=DIRECTORY_SWHID,
                discovery_date=datetime.datetime(
                    2014,
                    5,
                    7,
                    22,
                    3,
                    tzinfo=datetime.timezone.utc,
                ),
                authority=SWH_AUTHORITY,
                fetcher=FETCHER,
                format="original-artifacts-json",
                metadata=json.dumps(dest_original_artifacts).encode(),
                origin=origin_url,
                revision=revision_swhid,
            ),
        ],
        next_page_token=None,
    )
