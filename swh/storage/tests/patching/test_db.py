# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage.proxies.patching.db import PatchingAdmin, PatchingQuery


def test_set_display_name(patching_admin: PatchingAdmin, patching_query: PatchingQuery):
    assert patching_query.display_name([b"author1@example.com"]) == {}
    assert patching_query.display_name([b"author2@example.com"]) == {}

    patching_admin.set_display_name(
        b"author1@example.com", b"author2 <author2@example.com>"
    )

    assert patching_query.display_name([b"author1@example.com"]) == {
        b"author1@example.com": b"author2 <author2@example.com>"
    }
    assert patching_query.display_name([b"author2@example.com"]) == {}
