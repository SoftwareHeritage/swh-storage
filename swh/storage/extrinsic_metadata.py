# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, cast, Dict

from swh.model.identifiers import PersistentId, parse_persistent_identifier

from .exc import StorageArgumentException

CONTEXT_KEYS: Dict[str, Dict[str, type]] = {}
CONTEXT_KEYS["origin"] = {}
CONTEXT_KEYS["snapshot"] = {"origin": str, "visit": int}
CONTEXT_KEYS["release"] = {**CONTEXT_KEYS["snapshot"], "snapshot": PersistentId}
CONTEXT_KEYS["revision"] = {**CONTEXT_KEYS["release"], "release": PersistentId}
CONTEXT_KEYS["directory"] = {
    **CONTEXT_KEYS["revision"],
    "revision": PersistentId,
    "path": bytes,
}
CONTEXT_KEYS["content"] = {**CONTEXT_KEYS["directory"], "directory": PersistentId}


def check_extrinsic_metadata_context(object_type: str, context: Dict[str, Any]):
    key_types = CONTEXT_KEYS[object_type]

    extra_keys = set(context) - set(key_types)
    if extra_keys:
        raise StorageArgumentException(f"Unknown context keys: {', '.join(extra_keys)}")

    for (key, value) in context.items():
        expected_type = key_types[key]
        expected_type_str = str(expected_type)  # for display

        # If an SWHID is expected and a string is given, parse it
        if expected_type is PersistentId and isinstance(value, str):
            value = parse_persistent_identifier(value)
            expected_type_str = "PersistentId or str"

        # Check the type of the context value
        if not isinstance(value, expected_type):
            raise StorageArgumentException(
                f"Context key {key} must have type {expected_type_str}, "
                f"but is {value!r}"
            )

        # If it is an SWHID, check it is also a core SWHID.
        if expected_type is PersistentId:
            value = cast(PersistentId, value)
            if value.metadata != {}:
                raise StorageArgumentException(
                    f"Context key {key} must be a core SWHID, "
                    f"but it has qualifiers {', '.join(value.metadata)}."
                )
