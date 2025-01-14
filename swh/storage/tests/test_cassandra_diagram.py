# Copyright (C) 2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage.cassandra.diagram import dot_diagram


def test_cassandra_dot_diagram():
    assert dot_diagram()
