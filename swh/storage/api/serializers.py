# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model objects."""

from typing import Callable, Dict, List, Tuple

from swh.model.identifiers import SWHID, parse_swhid
import swh.model.model as model


def _encode_model_object(obj):
    d = obj.to_dict()
    d["__type__"] = type(obj).__name__
    return d


def _encode_model_enum(obj):
    return {
        "value": obj.value,
        "__type__": type(obj).__name__,
    }


ENCODERS: List[Tuple[type, str, Callable]] = [
    (model.BaseModel, "model", _encode_model_object),
    (SWHID, "swhid", str),
    (model.MetadataTargetType, "model_enum", _encode_model_enum),
    (model.MetadataAuthorityType, "model_enum", _encode_model_enum),
]


DECODERS: Dict[str, Callable] = {
    "swhid": parse_swhid,
    "model": lambda d: getattr(model, d.pop("__type__")).from_dict(d),
    "model_enum": lambda d: getattr(model, d.pop("__type__"))(d["value"]),
}
