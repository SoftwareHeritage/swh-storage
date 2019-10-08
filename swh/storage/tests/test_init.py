# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from unittest.mock import patch

from swh.storage import get_storage

from swh.storage.api.client import RemoteStorage
from swh.storage.storage import Storage as DbStorage
from swh.storage.in_memory import Storage as MemoryStorage


@patch('swh.storage.storage.psycopg2.pool')
def test_get_storage(mock_pool):
    """Instantiating an existing storage should be ok

    """
    mock_pool.ThreadedConnectionPool.return_value = None
    for cls, real_class, dummy_args in [
            ('remote', RemoteStorage, {'url': 'url'}),
            ('memory', MemoryStorage, {}),
            ('local', DbStorage, {
                'db': 'postgresql://db', 'objstorage': {
                    'cls': 'memory', 'args': {},
                },
            }),
    ]:
        actual_storage = get_storage(cls, args=dummy_args)
        assert actual_storage is not None
        assert isinstance(actual_storage, real_class)


def test_get_storage_failure():
    """Instantiating an unknown storage should raise

    """
    with pytest.raises(ValueError, match='Unknown storage class `unknown`'):
        get_storage('unknown', args=[])
