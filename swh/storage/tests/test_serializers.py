# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage.interface import ListOrder
from swh.model import model

from swh.storage.api.serializers import (
    _encode_enum,
    _decode_model_enum,
    _decode_storage_enum,
)


def test_model_enum_serialization(sample_data):
    result_enum = model.MetadataAuthorityType.DEPOSIT_CLIENT
    actual_serialized_enum = _encode_enum(result_enum)

    expected_serialized_enum = {
        "value": result_enum.value,
        "__type__": type(result_enum).__name__,
    }
    assert actual_serialized_enum == expected_serialized_enum

    decoded_paged_result = _decode_model_enum(actual_serialized_enum)
    assert decoded_paged_result == result_enum


def test_storage_enum_serialization(sample_data):
    result_enum = ListOrder.ASC
    actual_serialized_enum = _encode_enum(result_enum)

    expected_serialized_enum = {
        "value": result_enum.value,
        "__type__": type(result_enum).__name__,
    }
    assert actual_serialized_enum == expected_serialized_enum

    decoded_paged_result = _decode_storage_enum(actual_serialized_enum)
    assert decoded_paged_result == result_enum
