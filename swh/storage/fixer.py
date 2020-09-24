# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import logging
from typing import Any, Dict, List, Optional

from swh.model.identifiers import normalize_timestamp

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


def _fix_revision_pypi_empty_string(rev):
    """PyPI loader failed to encode empty strings as bytes, see:
    swh:1:rev:8f0095ee0664867055d03de9bcc8f95b91d8a2b9
    or https://forge.softwareheritage.org/D1772
    """
    rev = {
        **rev,
        "author": rev["author"].copy(),
        "committer": rev["committer"].copy(),
    }
    if rev["author"].get("email") == "":
        rev["author"]["email"] = b""
    if rev["author"].get("name") == "":
        rev["author"]["name"] = b""
    if rev["committer"].get("email") == "":
        rev["committer"]["email"] = b""
    if rev["committer"].get("name") == "":
        rev["committer"]["name"] = b""
    return rev


def _fix_revision_transplant_source(rev):
    if rev.get("metadata") and rev["metadata"].get("extra_headers"):
        rev = copy.deepcopy(rev)
        rev["metadata"]["extra_headers"] = [
            [key, value.encode("ascii")]
            if key == "transplant_source" and isinstance(value, str)
            else [key, value]
            for (key, value) in rev["metadata"]["extra_headers"]
        ]
    return rev


def _check_date(date):
    """Returns whether the date can be represented in backends with sane
    limits on timestamps and timezones (resp. signed 64-bits and
    signed 16 bits), and that microseconds is valid (ie. between 0 and 10^6).
    """
    if date is None:
        return True
    date = normalize_timestamp(date)
    return (
        (-(2 ** 63) <= date["timestamp"]["seconds"] < 2 ** 63)
        and (0 <= date["timestamp"]["microseconds"] < 10 ** 6)
        and (-(2 ** 15) <= date["offset"] < 2 ** 15)
    )


def _check_revision_date(rev):
    """Exclude revisions with invalid dates.
    See https://forge.softwareheritage.org/T1339"""
    return _check_date(rev["date"]) and _check_date(rev["committer_date"])


def _fix_revision(revision: Dict[str, Any]) -> Optional[Dict]:
    """Fix various legacy revision issues.

    Fix author/committer person:

    >>> from pprint import pprint
    >>> date = {
    ...     'timestamp': {
    ...         'seconds': 1565096932,
    ...         'microseconds': 0,
    ...     },
    ...     'offset': 0,
    ... }
    >>> rev0 = _fix_revision({
    ...     'id': b'rev-id',
    ...     'author': {'fullname': b'', 'name': '', 'email': ''},
    ...     'committer': {'fullname': b'', 'name': '', 'email': ''},
    ...     'date': date,
    ...     'committer_date': date,
    ...     'type': 'git',
    ...     'message': '',
    ...     'directory': b'dir-id',
    ...     'synthetic': False,
    ... })
    >>> rev0['author']
    {'fullname': b'', 'name': b'', 'email': b''}
    >>> rev0['committer']
    {'fullname': b'', 'name': b'', 'email': b''}

    Fix type of 'transplant_source' extra headers:

    >>> rev1 = _fix_revision({
    ...     'id': b'rev-id',
    ...     'author': {'fullname': b'', 'name': '', 'email': ''},
    ...     'committer': {'fullname': b'', 'name': '', 'email': ''},
    ...     'date': date,
    ...     'committer_date': date,
    ...     'metadata': {
    ...         'extra_headers': [
    ...             ['time_offset_seconds', b'-3600'],
    ...             ['transplant_source', '29c154a012a70f49df983625090434587622b39e']
    ...     ]},
    ...     'type': 'git',
    ...     'message': '',
    ...     'directory': b'dir-id',
    ...     'synthetic': False,
    ... })
    >>> pprint(rev1['metadata']['extra_headers'])
    [['time_offset_seconds', b'-3600'],
     ['transplant_source', b'29c154a012a70f49df983625090434587622b39e']]

    Revision with invalid date are filtered:

    >>> from copy import deepcopy
    >>> invalid_date1 = deepcopy(date)
    >>> invalid_date1['timestamp']['microseconds'] = 1000000000  # > 10^6
    >>> rev = _fix_revision({
    ...     'author': {'fullname': b'', 'name': '', 'email': ''},
    ...     'committer': {'fullname': b'', 'name': '', 'email': ''},
    ...     'date': invalid_date1,
    ...     'committer_date': date,
    ... })
    >>> rev is None
    True

    >>> invalid_date2 = deepcopy(date)
    >>> invalid_date2['timestamp']['seconds'] = 2**70  # > 10^63
    >>> rev = _fix_revision({
    ...     'author': {'fullname': b'', 'name': '', 'email': ''},
    ...     'committer': {'fullname': b'', 'name': '', 'email': ''},
    ...     'date': invalid_date2,
    ...     'committer_date': date,
    ... })
    >>> rev is None
    True

    >>> invalid_date3 = deepcopy(date)
    >>> invalid_date3['offset'] = 2**20  # > 10^15
    >>> rev = _fix_revision({
    ...     'author': {'fullname': b'', 'name': '', 'email': ''},
    ...     'committer': {'fullname': b'', 'name': '', 'email': ''},
    ...     'date': date,
    ...     'committer_date': invalid_date3,
    ... })
    >>> rev is None
    True

    """  # noqa
    rev = _fix_revision_pypi_empty_string(revision)
    rev = _fix_revision_transplant_source(rev)
    if not _check_revision_date(rev):
        logger.warning(
            "Invalid revision date detected: %(revision)s", {"revision": rev}
        )
        return None
    return rev


def _fix_origin(origin: Dict) -> Dict:
    """Fix legacy origin with type which is no longer part of the model.

    >>> from pprint import pprint
    >>> pprint(_fix_origin({
    ...     'url': 'http://foo',
    ... }))
    {'url': 'http://foo'}
    >>> pprint(_fix_origin({
    ...     'url': 'http://bar',
    ...     'type': 'foo',
    ... }))
    {'url': 'http://bar'}

    """
    o = origin.copy()
    o.pop("type", None)
    return o


def _fix_origin_visit(visit: Dict) -> Dict:
    """Fix various legacy origin visit issues.

    `visit['origin']` is a dict instead of an URL:

    >>> from datetime import datetime, timezone
    >>> from pprint import pprint
    >>> date = datetime(2020, 2, 27, 14, 39, 19, tzinfo=timezone.utc)
    >>> pprint(_fix_origin_visit({
    ...     'origin': {'url': 'http://foo'},
    ...     'date': date,
    ...     'type': 'git',
    ...     'status': 'ongoing',
    ...     'snapshot': None,
    ... }))
    {'date': datetime.datetime(2020, 2, 27, 14, 39, 19, tzinfo=datetime.timezone.utc),
     'origin': 'http://foo',
     'type': 'git'}

    `visit['type']` is missing , but `origin['visit']['type']` exists:

    >>> pprint(_fix_origin_visit(
    ...     {'origin': {'type': 'hg', 'url': 'http://foo'},
    ...     'date': date,
    ...     'status': 'ongoing',
    ...     'snapshot': None,
    ... }))
    {'date': datetime.datetime(2020, 2, 27, 14, 39, 19, tzinfo=datetime.timezone.utc),
     'origin': 'http://foo',
     'type': 'hg'}

    >>> pprint(_fix_origin_visit(
    ...     {'origin': {'type': 'hg', 'url': 'http://foo'},
    ...     'date': '2020-02-27 14:39:19+00:00',
    ...     'status': 'ongoing',
    ...     'snapshot': None,
    ... }))
    {'date': datetime.datetime(2020, 2, 27, 14, 39, 19, tzinfo=datetime.timezone.utc),
     'origin': 'http://foo',
     'type': 'hg'}

    Old visit format (origin_visit with no type) raises:

    >>> _fix_origin_visit({
    ...     'origin': {'url': 'http://foo'},
    ...     'date': date,
    ...     'status': 'ongoing',
    ...     'snapshot': None
    ... })
    Traceback (most recent call last):
    ...
    ValueError: Old origin visit format detected...

    >>> _fix_origin_visit({
    ...     'origin': 'http://foo',
    ...     'date': date,
    ...     'status': 'ongoing',
    ...     'snapshot': None
    ... })
    Traceback (most recent call last):
    ...
    ValueError: Old origin visit format detected...

    """  # noqa
    visit = visit.copy()
    if "type" not in visit:
        if isinstance(visit["origin"], dict) and "type" in visit["origin"]:
            # Very old version of the schema: visits did not have a type,
            # but their 'origin' field was a dict with a 'type' key.
            visit["type"] = visit["origin"]["type"]
        else:
            # Very old schema version: 'type' is missing, stop early

            # We expect the journal's origin_visit topic to no longer reference
            # such visits. If it does, the replayer must crash so we can fix
            # the journal's topic.
            raise ValueError(f"Old origin visit format detected: {visit}")
    if isinstance(visit["origin"], dict):
        # Old version of the schema: visit['origin'] was a dict.
        visit["origin"] = visit["origin"]["url"]
    date = visit["date"]
    if isinstance(date, str):
        visit["date"] = datetime.datetime.fromisoformat(date)
    # Those are no longer part of the model
    for key in ["status", "snapshot", "metadata"]:
        visit.pop(key, None)
    return visit


def fix_objects(object_type: str, objects: List[Dict]) -> List[Dict]:
    """
    Fix legacy objects from the journal to bring them up to date with the
    latest storage schema.
    """
    if object_type == "content":
        return [_fix_content(v) for v in objects]
    elif object_type == "revision":
        revisions = [_fix_revision(v) for v in objects]
        return [rev for rev in revisions if rev is not None]
    elif object_type == "origin":
        return [_fix_origin(v) for v in objects]
    elif object_type == "origin_visit":
        return [_fix_origin_visit(v) for v in objects]
    else:
        return objects
