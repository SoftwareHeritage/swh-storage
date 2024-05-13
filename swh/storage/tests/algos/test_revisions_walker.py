# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from copy import deepcopy

from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.storage.algos.revisions_walker import get_revisions_walker

# For those tests, we will walk the following revisions history
# with different orderings:
#
# *   commit b364f53155044e5308a0f73abb3b5f01995a5b7d
# |\  Merge: 836d498 b94886c
# | | Author:     Adam <adam.janicki@roche.com>
# | | AuthorDate: Fri Oct 4 12:50:49 2013 +0200
# | | Commit:     Adam <adam.janicki@roche.com>
# | | CommitDate: Fri Oct 4 12:50:49 2013 +0200
# | |
# | |     Merge branch 'release/1.0'
# | |
# | * commit b94886c500c46e32dc3d7ebae8a5409accd592e5
# | | Author:     Adam <adam.janicki@roche.com>
# | | AuthorDate: Fri Oct 4 12:50:38 2013 +0200
# | | Commit:     Adam <adam.janicki@roche.com>
# | | CommitDate: Fri Oct 4 12:50:38 2013 +0200
# | |
# | |     updating poms for 1.0 release
# | |
# | * commit 0cb6b4611d65bee0f57821dac7f611e2f8a02433
# | | Author:     Adam <adam.janicki@roche.com>
# | | AuthorDate: Fri Oct 4 12:50:38 2013 +0200
# | | Commit:     Adam <adam.janicki@roche.com>
# | | CommitDate: Fri Oct 4 12:50:38 2013 +0200
# | |
# | |     updating poms for 1.0 release
# | |
# | * commit 2b0240c6d682bad51532eec15b8a7ed6b75c8d31
# | | Author:     Adam Janicki <janickia>
# | | AuthorDate: Fri Oct 4 12:50:22 2013 +0200
# | | Commit:     Adam Janicki <janickia>
# | | CommitDate: Fri Oct 4 12:50:22 2013 +0200
# | |
# | |     For 1.0 release. Allow untracked.
# | |
# | * commit b401c50863475db4440c85c10ac0b6423b61554d
# | | Author:     Adam <adam.janicki@roche.com>
# | | AuthorDate: Fri Oct 4 12:48:12 2013 +0200
# | | Commit:     Adam <adam.janicki@roche.com>
# | | CommitDate: Fri Oct 4 12:48:12 2013 +0200
# | |
# | |     updating poms for 1.0 release
# | |
# | * commit 9c5051397e5c2e0c258bb639c3dd34406584ca10
# |/  Author:     Adam Janicki <janickia>
# |   AuthorDate: Fri Oct 4 12:47:48 2013 +0200
# |   Commit:     Adam Janicki <janickia>
# |   CommitDate: Fri Oct 4 12:47:48 2013 +0200
# |
# |       For 1.0 release.
# |
# * commit 836d498396fb9b5d45c896885f84d8d60a5651dc
# | Author:     Adam Janicki <janickia>
# | AuthorDate: Fri Oct 4 12:08:16 2013 +0200
# | Commit:     Adam Janicki <janickia>
# | CommitDate: Fri Oct 4 12:08:16 2013 +0200
# |
# |     Add ignores
# |
# * commit ee96c2a2d397b79070d2b6fe3051290963748358
# | Author:     Adam <adam.janicki@roche.com>
# | AuthorDate: Fri Oct 4 10:48:16 2013 +0100
# | Commit:     Adam <adam.janicki@roche.com>
# | CommitDate: Fri Oct 4 10:48:16 2013 +0100
# |
# |     Reset author
# |
# * commit 8f89dda8e072383cf50d42532ae8f52ad89f8fdf
#   Author:     Adam <adam.janicki@roche.com>
#   AuthorDate: Fri Oct 4 02:20:19 2013 -0700
#   Commit:     Adam <adam.janicki@roche.com>
#   CommitDate: Fri Oct 4 02:20:19 2013 -0700
#
#       Initial commit

# raw dump of the above history in swh format
_revisions_list = [
    {
        "author": {
            "email": b"adam.janicki@roche.com",  # noqa
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883849},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883849},
        },
        "directory": b"\xefX\xe7\xa6\\\xda\xdf\xfdH\xdbH\xfbq\x96@{\x98?9\xfe",
        "id": b"\xb3d\xf51U\x04NS\x08\xa0\xf7:\xbb;_\x01\x99Z[}",
        "message": b"Merge branch 'release/1.0'",
        "metadata": None,
        "parents": [
            b"\x83mI\x83\x96\xfb\x9b]E\xc8\x96\x88_\x84\xd8\xd6\nVQ\xdc",
            b"\xb9H\x86\xc5\x00\xc4n2\xdc=~\xba\xe8\xa5@\x9a\xcc\xd5\x92\xe5",
        ],  # noqa
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883838},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883838},
        },
        "directory": b"\xefX\xe7\xa6\\\xda\xdf\xfdH\xdbH\xfbq\x96@{\x98?9\xfe",
        "id": b"\xb9H\x86\xc5\x00\xc4n2\xdc=~\xba\xe8\xa5@\x9a\xcc\xd5\x92\xe5",
        "message": b"updating poms for 1.0 release",
        "metadata": None,
        "parents": [
            b"\x0c\xb6\xb4a\x1de\xbe\xe0\xf5x!\xda\xc7\xf6\x11\xe2\xf8\xa0$3"
        ],  # noqa
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883838},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883838},
        },
        "directory": b"\xefX\xe7\xa6\\\xda\xdf\xfdH\xdbH\xfbq\x96@{\x98?9\xfe",
        "id": b"\x0c\xb6\xb4a\x1de\xbe\xe0\xf5x!\xda\xc7\xf6\x11\xe2\xf8\xa0$3",
        "message": b"updating poms for 1.0 release",
        "metadata": None,
        "parents": [b"+\x02@\xc6\xd6\x82\xba\xd5\x152\xee\xc1[\x8a~\xd6\xb7\\\x8d1"],
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"janickia",
            "fullname": b"Adam Janicki <janickia>",
            "id": 8040906,
            "name": b"Adam Janicki",
        },
        "committer": {
            "email": b"janickia",
            "fullname": b"Adam Janicki <janickia>",
            "id": 8040906,
            "name": b"Adam Janicki",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883822},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883822},
        },
        "directory": b"\xefX\xe7\xa6\\\xda\xdf\xfdH\xdbH\xfbq\x96@{\x98?9\xfe",
        "id": b"+\x02@\xc6\xd6\x82\xba\xd5\x152\xee\xc1[\x8a~\xd6\xb7\\\x8d1",
        "message": b"For 1.0 release. Allow untracked.\n",
        "metadata": None,
        "parents": [b"\xb4\x01\xc5\x08cG]\xb4D\x0c\x85\xc1\n\xc0\xb6B;aUM"],
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883692},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883692},
        },
        "directory": b"d@\xe7\x143w\xcb\xf7\xad\xae\x91\xd5\xec\xd8\x95\x82"
        b"\x02\xa6=\x1b",
        "id": b"\xb4\x01\xc5\x08cG]\xb4D\x0c\x85\xc1\n\xc0\xb6B;aUM",
        "message": b"updating poms for 1.0 release",
        "metadata": None,
        "parents": [b"\x9cPQ9~\\.\x0c%\x8b\xb69\xc3\xdd4@e\x84\xca\x10"],
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"janickia",
            "fullname": b"Adam Janicki <janickia>",
            "id": 8040906,
            "name": b"Adam Janicki",
        },
        "committer": {
            "email": b"janickia",
            "fullname": b"Adam Janicki <janickia>",
            "id": 8040906,
            "name": b"Adam Janicki",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883668},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380883668},
        },
        "directory": b"\n\x857\x94r\xbe\xcc\x04=\xe9}\xe5\xfd\xdf?nR\xe6\xa7\x9e",
        "id": b"\x9cPQ9~\\.\x0c%\x8b\xb69\xc3\xdd4@e\x84\xca\x10",
        "message": b"For 1.0 release.\n",
        "metadata": None,
        "parents": [b"\x83mI\x83\x96\xfb\x9b]E\xc8\x96\x88_\x84\xd8\xd6\nVQ\xdc"],
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"janickia",
            "fullname": b"Adam Janicki <janickia>",
            "id": 8040906,
            "name": b"Adam Janicki",
        },
        "committer": {
            "email": b"janickia",
            "fullname": b"Adam Janicki <janickia>",
            "id": 8040906,
            "name": b"Adam Janicki",
        },
        "committer_date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380881296},
        },
        "date": {
            "offset_bytes": b"+0200",
            "timestamp": {"microseconds": 0, "seconds": 1380881296},
        },
        "directory": b".\xf9\xa5\xcb\xb0\xd3\xdc\x9b{\xb8\x81\x03l\xe2P\x16c\x0b|\xe6",  # noqa
        "id": b"\x83mI\x83\x96\xfb\x9b]E\xc8\x96\x88_\x84\xd8\xd6\nVQ\xdc",
        "message": b"Add ignores\n",
        "metadata": None,
        "parents": [b"\xee\x96\xc2\xa2\xd3\x97\xb7\x90p\xd2\xb6\xfe0Q)\tct\x83X"],
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer_date": {
            "offset_bytes": b"+0100",
            "timestamp": {"microseconds": 0, "seconds": 1380880096},
        },
        "date": {
            "offset_bytes": b"+0100",
            "timestamp": {"microseconds": 0, "seconds": 1380880096},
        },
        "directory": b"\xc7r\xc4\x9f\xc0$\xd4\xab\xff\xcb]\xf6<\xcb\x8b~\xec\xc4\xd1)",  # noqa
        "id": b"\xee\x96\xc2\xa2\xd3\x97\xb7\x90p\xd2\xb6\xfe0Q)\tct\x83X",
        "message": b"Reset author\n",
        "metadata": None,
        "parents": [b"\x8f\x89\xdd\xa8\xe0r8<\xf5\rBS*\xe8\xf5*\xd8\x9f\x8f\xdf"],
        "synthetic": False,
        "type": "git",
    },
    {
        "author": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer": {
            "email": b"adam.janicki@roche.com",
            "fullname": b"Adam <adam.janicki@roche.com>",
            "id": 8040905,
            "name": b"Adam",
        },
        "committer_date": {
            "offset_bytes": b"-0700",
            "timestamp": {"microseconds": 0, "seconds": 1380878419},
        },
        "date": {
            "offset_bytes": b"-0700",
            "timestamp": {"microseconds": 0, "seconds": 1380878419},
        },
        "directory": b"WS\xbaX\xd6x{q\x8f\x020i\xc5\x95\xa01\xf7y\xb2\x80",
        "id": b"\x8f\x89\xdd\xa8\xe0r8<\xf5\rBS*\xe8\xf5*\xd8\x9f\x8f\xdf",
        "message": b"Initial commit\n",
        "metadata": None,
        "parents": [],
        "synthetic": False,
        "type": "git",
    },
]


_rev_start = "b364f53155044e5308a0f73abb3b5f01995a5b7d"

_rev_missing = "836d498396fb9b5d45c896885f84d8d60a5651dc"


def check_revisions_ordering(
    mocker,
    rev_walker_type,
    expected_result,
    truncated_history,
    revisions_list=_revisions_list,
):
    storage = mocker.patch("swh.storage.postgresql.storage.Storage")

    if not truncated_history:
        storage.revision_log.return_value = revisions_list
    else:
        revs_lists_truncated = [
            None if hash_to_hex(rev["id"]) == _rev_missing else rev
            for rev in revisions_list
        ]

        storage.revision_log.return_value = revs_lists_truncated

    revs_walker = get_revisions_walker(
        rev_walker_type, storage, hash_to_bytes(_rev_start)
    )

    assert list(map(hash_to_bytes, expected_result)) == [
        rev["id"] for rev in revs_walker
    ]

    assert revs_walker.is_history_truncated() == truncated_history

    if truncated_history:
        missing_revs = revs_walker.missing_revisions()
        assert missing_revs == {hash_to_bytes(_rev_missing)}
    else:
        assert revs_walker.missing_revisions() == set()


def test_revisions_walker_committer_date(mocker):
    # revisions should be returned in reverse chronological order
    # of their committer date
    expected_result = [
        "b364f53155044e5308a0f73abb3b5f01995a5b7d",
        "b94886c500c46e32dc3d7ebae8a5409accd592e5",
        "0cb6b4611d65bee0f57821dac7f611e2f8a02433",
        "2b0240c6d682bad51532eec15b8a7ed6b75c8d31",
        "b401c50863475db4440c85c10ac0b6423b61554d",
        "9c5051397e5c2e0c258bb639c3dd34406584ca10",
        "836d498396fb9b5d45c896885f84d8d60a5651dc",
        "ee96c2a2d397b79070d2b6fe3051290963748358",
        "8f89dda8e072383cf50d42532ae8f52ad89f8fdf",
    ]

    check_revisions_ordering(
        mocker, "committer_date", expected_result, truncated_history=False
    )


def test_revisions_walker_dfs(mocker):
    # revisions should be returned in the same order they are
    # visited when performing a depth-first search in pre order
    # on the revisions DAG
    expected_result = [
        "b364f53155044e5308a0f73abb3b5f01995a5b7d",
        "836d498396fb9b5d45c896885f84d8d60a5651dc",
        "ee96c2a2d397b79070d2b6fe3051290963748358",
        "8f89dda8e072383cf50d42532ae8f52ad89f8fdf",
        "b94886c500c46e32dc3d7ebae8a5409accd592e5",
        "0cb6b4611d65bee0f57821dac7f611e2f8a02433",
        "2b0240c6d682bad51532eec15b8a7ed6b75c8d31",
        "b401c50863475db4440c85c10ac0b6423b61554d",
        "9c5051397e5c2e0c258bb639c3dd34406584ca10",
    ]

    check_revisions_ordering(mocker, "dfs", expected_result, truncated_history=False)


def test_revisions_walker_dfs_post(mocker):
    # revisions should be returned in the same order they are
    # visited when performing a depth-first search in post order
    # on the revisions DAG
    expected_result = [
        "b364f53155044e5308a0f73abb3b5f01995a5b7d",
        "b94886c500c46e32dc3d7ebae8a5409accd592e5",
        "0cb6b4611d65bee0f57821dac7f611e2f8a02433",
        "2b0240c6d682bad51532eec15b8a7ed6b75c8d31",
        "b401c50863475db4440c85c10ac0b6423b61554d",
        "9c5051397e5c2e0c258bb639c3dd34406584ca10",
        "836d498396fb9b5d45c896885f84d8d60a5651dc",
        "ee96c2a2d397b79070d2b6fe3051290963748358",
        "8f89dda8e072383cf50d42532ae8f52ad89f8fdf",
    ]

    check_revisions_ordering(
        mocker, "dfs_post", expected_result, truncated_history=False
    )


def test_revisions_walker_bfs(mocker):
    # revisions should be returned in the same order they are
    # visited when performing a breadth-first search on the
    # revisions DAG
    expected_result = [
        "b364f53155044e5308a0f73abb3b5f01995a5b7d",
        "836d498396fb9b5d45c896885f84d8d60a5651dc",
        "b94886c500c46e32dc3d7ebae8a5409accd592e5",
        "ee96c2a2d397b79070d2b6fe3051290963748358",
        "0cb6b4611d65bee0f57821dac7f611e2f8a02433",
        "8f89dda8e072383cf50d42532ae8f52ad89f8fdf",
        "2b0240c6d682bad51532eec15b8a7ed6b75c8d31",
        "b401c50863475db4440c85c10ac0b6423b61554d",
        "9c5051397e5c2e0c258bb639c3dd34406584ca10",
    ]

    check_revisions_ordering(mocker, "bfs", expected_result, truncated_history=False)


def test_revisions_walker_truncated_history(mocker):
    expected_result = [
        "b364f53155044e5308a0f73abb3b5f01995a5b7d",
        "b94886c500c46e32dc3d7ebae8a5409accd592e5",
        "0cb6b4611d65bee0f57821dac7f611e2f8a02433",
        "2b0240c6d682bad51532eec15b8a7ed6b75c8d31",
        "b401c50863475db4440c85c10ac0b6423b61554d",
        "9c5051397e5c2e0c258bb639c3dd34406584ca10",
    ]

    for revs_walker_type in ("committer_date", "bfs", "dfs", "dfs_post"):
        check_revisions_ordering(
            mocker, revs_walker_type, expected_result, truncated_history=True
        )


def test_revisions_walker_no_committer_date(mocker):
    expected_result = [
        "b364f53155044e5308a0f73abb3b5f01995a5b7d",
        "b94886c500c46e32dc3d7ebae8a5409accd592e5",
        "0cb6b4611d65bee0f57821dac7f611e2f8a02433",
        "2b0240c6d682bad51532eec15b8a7ed6b75c8d31",
        "b401c50863475db4440c85c10ac0b6423b61554d",
        "9c5051397e5c2e0c258bb639c3dd34406584ca10",
        "836d498396fb9b5d45c896885f84d8d60a5651dc",
        "ee96c2a2d397b79070d2b6fe3051290963748358",
        "8f89dda8e072383cf50d42532ae8f52ad89f8fdf",
    ]

    revisions_list = deepcopy(_revisions_list)
    for revision in revisions_list:
        revision["committer_date"] = None

    check_revisions_ordering(
        mocker,
        "committer_date",
        expected_result,
        truncated_history=False,
        revisions_list=revisions_list,
    )
