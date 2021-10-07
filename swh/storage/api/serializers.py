# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model objects."""

from typing import Callable, Dict, List, Tuple

from swh.model import model, swhids
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


def _decode_swhids_enum(d):
    return getattr(swhids, d.pop("__type__"))(d["value"])


def _decode_storage_enum(d):
    return getattr(interface, d.pop("__type__"))(d["value"])


ENCODERS: List[Tuple[type, str, Callable]] = [
    (model.BaseModel, "model", _encode_model_object),
    (swhids.CoreSWHID, "core_swhid", str),
    (swhids.ExtendedSWHID, "extended_swhid", str),
    (swhids.QualifiedSWHID, "qualified_swhid", str),
    (swhids.ObjectType, "identifiers_enum", _encode_enum),
    (model.MetadataAuthorityType, "model_enum", _encode_enum),
    (interface.ListOrder, "storage_enum", _encode_enum),
]


DECODERS: Dict[str, Callable] = {
    "core_swhid": swhids.CoreSWHID.from_string,
    "extended_swhid": swhids.ExtendedSWHID.from_string,
    "qualified_swhid": swhids.QualifiedSWHID.from_string,
    "model": lambda d: getattr(model, d.pop("__type__")).from_dict(d),
    "identifiers_enum": _decode_swhids_enum,
    "model_enum": _decode_model_enum,
    "storage_enum": _decode_storage_enum,
}
