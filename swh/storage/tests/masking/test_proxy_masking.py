# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools

import pytest

from swh.storage.exc import MaskedObjectException
from swh.storage.proxies.masking import MaskingProxyStorage
from swh.storage.proxies.masking.db import MaskedState
from swh.storage.tests.storage_data import StorageData
from swh.storage.tests.test_in_memory import TestInMemoryStorage as _TestStorage

MASKED_SWHIDS = {
    StorageData.content.swhid().to_extended(),
    StorageData.directory.swhid().to_extended(),
    StorageData.revision.swhid().to_extended(),
    StorageData.release.swhid().to_extended(),
    StorageData.snapshot.swhid().to_extended(),
    StorageData.origin.swhid(),
}


@pytest.fixture
def swh_storage(masking_db_postgresql, masking_admin, swh_storage_backend):
    # Create a request
    request = masking_admin.create_request(slug="foo", reason="bar")

    masking_admin.set_object_state(
        request_id=request.id,
        new_state=MaskedState.DECISION_PENDING,
        swhids=list(MASKED_SWHIDS),
    )

    return MaskingProxyStorage(
        db=masking_db_postgresql.info.dsn, storage=swh_storage_backend
    )


class TestStorage(_TestStorage):
    @pytest.mark.xfail(reason="typing.Protocol instance check is annoying")
    def test_types(self, *args, **kwargs):
        super().test_types(*args, **kwargs)

    @pytest.mark.xfail(
        reason="This test only adds one origin with a status, and it's masked"
    )
    def test_origin_visit_status_get_random(self, *args, **kwargs):
        super().test_origin_visit_status_get_random(*args, **kwargs)


EXPECTED_MASKED_EXCEPTIONS = {
    "test_content_add",
    "test_content_add__legacy",
    "test_content_add_duplicate",
    "test_content_add_from_lazy_content",
    "test_content_add_metadata",
    "test_content_add_twice",
    "test_content_find_ctime",
    "test_content_find_with_duplicate_blake2s256",
    "test_content_find_with_duplicate_input",
    "test_content_find_with_duplicate_sha256",
    "test_content_find_with_present_content",
    "test_content_get",
    "test_content_get_data_full_dict",
    "test_content_get_data_missing",
    "test_content_get_data_single_hash_dict",
    "test_content_get_data_two_hash_dict",
    "test_content_get_missing",
    "test_content_metadata_add",
    "test_content_metadata_add_duplicate",
    "test_content_metadata_get",
    "test_content_metadata_get_after",
    "test_content_metadata_get_authorities",
    "test_content_metadata_get_by_ids",
    "test_content_metadata_get_paginate",
    "test_content_metadata_get_paginate_same_date",
    "test_directory_add",
    "test_directory_add_with_raw_manifest",
    "test_directory_get_id_partition",
    "test_directory_ls_non_recursive",
    "test_directory_ls_recursive",
    "test_extid_add_extid_multicity",
    "test_extid_add_git",
    "test_extid_add_target_multicity",
    "test_extid_add_twice",
    "test_extid_version_behavior",
    "test_origin_add",
    "test_origin_get",
    "test_origin_get_by_sha1",
    "test_origin_metadata_add",
    "test_origin_metadata_add_duplicate",
    "test_origin_metadata_get",
    "test_origin_metadata_get_after",
    "test_origin_metadata_get_paginate",
    "test_origin_metadata_get_paginate_same_date",
    "test_origin_search_multiple_visit_types",
    "test_origin_search_no_regexp",
    "test_origin_search_regexp_fullstring",
    "test_origin_search_regexp_substring",
    "test_origin_search_single_result",
    "test_origin_search_with_visit_types",
    "test_origin_snapshot_get_all",
    "test_origin_visit_find_by_date",
    "test_origin_visit_find_by_date_and_type",
    "test_origin_visit_find_by_date_latest_visit",
    "test_origin_visit_get_all",
    "test_origin_visit_get_by",
    "test_origin_visit_get_latest",
    "test_origin_visit_get_latest__not_last",
    "test_origin_visit_get_latest__same_date",
    "test_origin_visit_get_latest_filter_type",
    "test_origin_visit_get_latest_order",
    "test_origin_visit_get_with_statuses",
    "test_origin_visit_status_get_all",
    "test_origin_visit_status_get_latest",
    "test_release_get",
    "test_release_get_order",
    "test_release_get_partition",
    "test_revision_add_fractional_timezone",
    "test_revision_get",
    "test_revision_get_no_parents",
    "test_revision_get_order",
    "test_revision_get_partition",
    "test_revision_log",
    "test_revision_shortlog",
    "test_snapshot_add_get",
    "test_snapshot_add_many",
    "test_snapshot_add_many_incremental",
    "test_snapshot_branch_get_by_name_missing_branch",
    "test_snapshot_get_id_partition",
}

for method_name in dir(TestStorage):
    if not method_name.startswith("test_"):
        continue

    method = getattr(TestStorage, method_name)

    @functools.wraps(method)
    def wrapped_test(*args, method=method, **kwargs):
        try:
            method(*args, **kwargs)
        except MaskedObjectException as exc:
            assert (
                method.__name__ in EXPECTED_MASKED_EXCEPTIONS
            ), f"{method.__name__} shouldn't raise a MaskedObjectException"

            assert exc.masked, "The exception was raised with no masked objects"

            assert (
                set(exc.masked.keys()) <= MASKED_SWHIDS
            ), "Found an unexpectedly masked object"
        else:
            assert (
                method.__name__ not in EXPECTED_MASKED_EXCEPTIONS
            ), f"{method.__name__} should raise a MaskedObjectException"

    setattr(TestStorage, method_name, wrapped_test)
