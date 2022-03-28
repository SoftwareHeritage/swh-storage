# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model objects."""

from typing import Any, Callable, Dict, List, Tuple

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


def _encode_origin_visit_with_statuses(
    ovws: interface.OriginVisitWithStatuses,
) -> Dict[str, Any]:
    return {
        "visit": ovws.visit.to_dict(),
        "statuses": [status.to_dict() for status in ovws.statuses],
    }


def _decode_origin_visit_with_statuses(
    ovws: Dict[str, Any],
) -> interface.OriginVisitWithStatuses:
    return interface.OriginVisitWithStatuses(
        visit=model.OriginVisit(**ovws["visit"]),
        statuses=[model.OriginVisitStatus(**status) for status in ovws["statuses"]],
    )


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
    # TODO: serialize this as "swhids_enum" when all peers support it in their DECODERS:
    (swhids.ObjectType, "identifiers_enum", _encode_enum),
    (model.MetadataAuthorityType, "model_enum", _encode_enum),
    (interface.ListOrder, "storage_enum", _encode_enum),
    (
        interface.OriginVisitWithStatuses,
        "origin_visit_with_statuses",
        _encode_origin_visit_with_statuses,
    ),
]


DECODERS: Dict[str, Callable] = {
    "core_swhid": swhids.CoreSWHID.from_string,
    "extended_swhid": swhids.ExtendedSWHID.from_string,
    "qualified_swhid": swhids.QualifiedSWHID.from_string,
    "model": lambda d: getattr(model, d.pop("__type__")).from_dict(d),
    "identifiers_enum": _decode_swhids_enum,
    "swhids_enum": _decode_swhids_enum,
    "model_enum": _decode_model_enum,
    "storage_enum": _decode_storage_enum,
    "origin_visit_with_statuses": _decode_origin_visit_with_statuses,
}
