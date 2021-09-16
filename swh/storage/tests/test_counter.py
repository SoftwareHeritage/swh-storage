# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import attr
import pytest

from swh.storage import get_storage


@pytest.fixture
def swh_storage():
    storage_config = {
        "cls": "pipeline",
        "steps": [
            {"cls": "counter", "counters": {"cls": "memory"}},
            {"cls": "memory"},
        ],
    }

    return get_storage(**storage_config)


def test_counting_proxy_storage_content(swh_storage, sample_data):
    assert swh_storage.counters.counters["content"] == set()

    swh_storage.content_add([sample_data.content])

    assert swh_storage.counters.counters["content"] == {sample_data.content.sha1}

    swh_storage.content_add([sample_data.content2, sample_data.content3])

    assert swh_storage.counters.counters["content"] == {
        sample_data.content.sha1,
        sample_data.content2.sha1,
        sample_data.content3.sha1,
    }

    assert [
        attr.evolve(cnt, ctime=None)
        for cnt in swh_storage.content_find({"sha256": sample_data.content2.sha256})
    ] == [attr.evolve(sample_data.content2, data=None)]


def test_counting_proxy_storage_revision(swh_storage, sample_data):
    assert swh_storage.counters.counters["revision"] == set()

    swh_storage.revision_add([sample_data.revision])

    assert swh_storage.counters.counters["revision"] == {sample_data.revision.id}

    swh_storage.revision_add([sample_data.revision2, sample_data.revision3])

    assert swh_storage.counters.counters["revision"] == {
        sample_data.revision.id,
        sample_data.revision2.id,
        sample_data.revision3.id,
    }

    assert swh_storage.revision_get([sample_data.revision2.id]) == [
        sample_data.revision2
    ]
