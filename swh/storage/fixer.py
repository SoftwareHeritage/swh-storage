# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Any, Callable, Dict, List

from swh.model.model import Origin

logger = logging.getLogger(__name__)


def _fix_content(content: Dict[str, Any]) -> Dict[str, Any]:
    """Filters-out invalid 'perms' key that leaked from swh.model.from_disk
    to the journal.

    >>> _fix_content({'perms': 0o100644, 'sha1_git': b'foo'})
    {'sha1_git': b'foo'}

    >>> _fix_content({'sha1_git': b'bar'})
    {'sha1_git': b'bar'}

    """
    content = content.copy()
    content.pop("perms", None)
    return content


def _fix_raw_extrinsic_metadata(obj_dict: Dict) -> Dict:
    """Fix legacy RawExtrinsicMetadata with type which is no longer part of the model.

    >>> _fix_raw_extrinsic_metadata({
    ...     'type': 'directory',
    ...     'target': 'swh:1:dir:460a586d1c95d120811eaadb398d534e019b5243',
    ... })
    {'target': 'swh:1:dir:460a586d1c95d120811eaadb398d534e019b5243'}
    >>> _fix_raw_extrinsic_metadata({
    ...     'type': 'origin',
    ...     'target': 'https://inria.halpreprod.archives-ouvertes.fr/hal-01667309',
    ... })
    {'target': 'swh:1:ori:155291d5b9ada4570672510509f93fcfd9809882'}

    """
    o = obj_dict.copy()
    if o.pop("type", None) == "origin":
        o["target"] = str(Origin(o["target"]).swhid())
    return o


object_fixers: Dict[str, Callable[[Dict], Dict]] = {
    "content": _fix_content,
    "raw_extrinsic_metadata": _fix_raw_extrinsic_metadata,
}


def fix_objects(object_type: str, objects: List[Dict]) -> List[Dict]:
    """
    Fix legacy objects from the journal to bring them up to date with the
    latest storage schema.
    """
    if object_type in object_fixers:
        fixer = object_fixers[object_type]
        objects = [fixer(v) for v in objects]
    return objects
