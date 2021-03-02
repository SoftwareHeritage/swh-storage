# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model objects."""

from typing import Callable, Dict, List, Tuple

from swh.model.identifiers import (
    SWHID,
    CoreSWHID,
    ExtendedSWHID,
    QualifiedSWHID,
    parse_swhid,
)
import swh.model.model as model
from swh.storage import interface


def _encode_model_object(obj):
    d = obj.to_dict()
    d["__type__"] = type(obj).__name__
    return d


def _encode_enum(obj):
    return {
        "value": obj.value,
        "__type__": type(obj).__name__,
    }


def _decode_model_enum(d):
    return getattr(model, d.pop("__type__"))(d["value"])


def _decode_storage_enum(d):
    return getattr(interface, d.pop("__type__"))(d["value"])


ENCODERS: List[Tuple[type, str, Callable]] = [
    (model.BaseModel, "model", _encode_model_object),
    (SWHID, "swhid", str),
    (CoreSWHID, "core_swhid", str),
    (ExtendedSWHID, "extended_swhid", str),
    (QualifiedSWHID, "qualified_swhid", str),
    (model.MetadataAuthorityType, "model_enum", _encode_enum),
    (interface.ListOrder, "storage_enum", _encode_enum),
]


DECODERS: Dict[str, Callable] = {
    "swhid": parse_swhid,
    "core_swhid": CoreSWHID.from_string,
    "extended_swhid": ExtendedSWHID.from_string,
    "qualified_swhid": QualifiedSWHID.from_string,
    "model": lambda d: getattr(model, d.pop("__type__")).from_dict(d),
    "model_enum": _decode_model_enum,
    "storage_enum": _decode_storage_enum,
}
