# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model.hashutil import MultiHash


def origin_url_to_sha1(origin_url: str) -> bytes:
    """Convert an origin URL to a sha1. Encodes URL to utf-8."""
    return MultiHash.from_data(origin_url.encode("utf-8"), {"sha1"}).digest()["sha1"]
