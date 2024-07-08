# Copyright (C) 2020-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model objects."""

from typing import Any, Callable, Dict, List, Tuple
import uuid

from swh.model import model, swhids
from swh.storage import interface
from swh.storage.proxies.blocking.db import BlockingState, BlockingStatus
from swh.storage.proxies.masking.db import MaskedState, MaskedStatus


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


def _encode_object_reference(object_reference: interface.ObjectReference) -> List[str]:
    return [str(object_reference.source), str(object_reference.target)]


def _encode_snapshot_branch_by_name_response(
    branch_by_name_response: interface.SnapshotBranchByNameResponse,
) -> Dict[str, Any]:
    target = branch_by_name_response.target
    return {
        "branch_found": branch_by_name_response.branch_found,
        "target": target.to_dict() if target else None,
        "aliases_followed": branch_by_name_response.aliases_followed,
    }


def _encode_masked_status(masked_status: MaskedStatus):
    return {
        "state": masked_status.state.name,
        "request": str(masked_status.request),
    }


def _decode_masked_status(d: Dict[str, Any]):
    return MaskedStatus(state=MaskedState[d["state"]], request=uuid.UUID(d["request"]))


def _encode_blocking_status(blocking_status: BlockingStatus):
    return {
        "state": blocking_status.state.name,
        "request": str(blocking_status.request),
    }


def _decode_blocking_status(d: Dict[str, Any]):
    return BlockingStatus(
        state=BlockingState[d["state"]], request=uuid.UUID(d["request"])
    )


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


def _decode_object_reference(d):
    return interface.ObjectReference(
        source=swhids.ExtendedSWHID.from_string(d[0]),
        target=swhids.ExtendedSWHID.from_string(d[1]),
    )


def _decode_snapshot_branch_by_name_response(d):
    target = model.SnapshotBranch.from_dict(d["target"]) if d["target"] else None
    return interface.SnapshotBranchByNameResponse(
        branch_found=d["branch_found"],
        target=target,
        aliases_followed=d["aliases_followed"],
    )


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
    (
        interface.ObjectReference,
        "object_reference",
        _encode_object_reference,
    ),
    (
        interface.SnapshotBranchByNameResponse,
        "branch_by_name_response",
        _encode_snapshot_branch_by_name_response,
    ),
    (MaskedStatus, "masked_status", _encode_masked_status),
    (BlockingStatus, "blocking_status", _encode_blocking_status),
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
    "object_reference": _decode_object_reference,
    "branch_by_name_response": _decode_snapshot_branch_by_name_response,
    "masked_status": _decode_masked_status,
    "blocking_status": _decode_blocking_status,
}
