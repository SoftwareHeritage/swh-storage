# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import hashlib

TOKEN_BEGIN = -(2**63)
"""Minimum value returned by the CQL function token()"""
TOKEN_END = 2**63 - 1
"""Maximum value returned by the CQL function token()"""


def hash_url(url: str) -> bytes:
    return hashlib.sha1(url.encode("utf8")).digest()
