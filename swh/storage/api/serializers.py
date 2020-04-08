# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Decoder and encoders for swh-model objects."""

from typing import Callable, Dict, List, Tuple

import swh.model.model as model


def _encode_model_object(obj):
    d = obj.to_dict()
    d["__type__"] = type(obj).__name__
    return d


ENCODERS: List[Tuple[type, str, Callable]] = [
    (model.BaseModel, "model", _encode_model_object),
]


DECODERS: Dict[str, Callable] = {
    "model": lambda d: getattr(model, d.pop("__type__")).from_dict(d)
}
