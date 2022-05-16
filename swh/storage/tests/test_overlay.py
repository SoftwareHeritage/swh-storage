# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.storage.tests.test_in_memory import (
    TestInMemoryStorageGeneratedData as _TestInMemoryStorageGeneratedData,
)
from swh.storage.tests.test_in_memory import TestInMemoryStorage as _TestInMemoryStorage


@pytest.fixture(params=[1, 2])
def swh_storage_backend_config(request):
    yield {
        "cls": "overlay",
        "storages": [
            {
                "cls": "memory",
                "journal_writer": {
                    "cls": "memory",
                },
            }
            for _ in range(request.param)
        ],
    }


class TestOverlayProxy(_TestInMemoryStorage):
    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_types(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_content_get_partition(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_content_get_partition_full(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_content_get_partition_empty(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_content_get_partition_limit_none(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_content_get_partition_pagination_generate(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_revision_log(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_revision_log_with_limit(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_revision_log_unknown_revision(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_revision_shortlog(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_revision_shortlog_with_limit(self):
        pass

    @pytest.mark.skip("TODO: rewrite this test without hardcoded page_token")
    def test_origin_visit_get_with_statuses(self):
        pass

    @pytest.mark.skip("Not supported by the overlay proxy")
    def test_content_add_objstorage_first(self):
        pass


class TestOverlayProxyGeneratedData(_TestInMemoryStorageGeneratedData):
    pass
