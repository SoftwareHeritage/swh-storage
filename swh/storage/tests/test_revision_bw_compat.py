# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import attr

from swh.core.utils import decode_with_escape
from swh.storage import get_storage
from swh.storage.tests.test_postgresql import db_transaction


def headers_to_db(git_headers):
    return [[key, decode_with_escape(value)] for key, value in git_headers]


def test_revision_extra_header_in_metadata(swh_storage_backend_config, sample_data):
    storage = get_storage(**swh_storage_backend_config)
    rev = sample_data.revision

    md_w_extra = dict(
        rev.metadata.items(),
        extra_headers=headers_to_db(
            [
                ["gpgsig", b"test123"],
                ["mergetag", b"foo\\bar"],
                ["mergetag", b"\x22\xaf\x89\x80\x01\x00"],
            ]
        ),
    )

    bw_rev = attr.evolve(rev, extra_headers=())
    object.__setattr__(bw_rev, "metadata", md_w_extra)
    assert bw_rev.extra_headers == ()

    assert storage.revision_add([bw_rev]) == {"revision:add": 1}

    # check data in the db are old format
    with db_transaction(storage) as (_, cur):
        cur.execute("SELECT metadata, extra_headers FROM revision")
        metadata, extra_headers = cur.fetchone()
    assert extra_headers == []
    assert metadata == bw_rev.metadata

    # check the Revision build from revision_get is the original, "new style", Revision
    assert storage.revision_get([rev.id]) == [rev]
