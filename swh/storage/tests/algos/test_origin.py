# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

from swh.storage import get_storage
from swh.storage.algos.origin import iter_origins


def assert_list_eq(left, right, msg=None):
    assert list(left) == list(right), msg


storage_config = {
    'cls': 'validate',
    'storage': {
        'cls': 'memory',
    }
}


def test_iter_origins():
    storage = get_storage(**storage_config)
    origins = storage.origin_add([
        {'url': 'bar'},
        {'url': 'qux'},
        {'url': 'quuz'},
    ])
    assert_list_eq(iter_origins(storage), origins)
    assert_list_eq(iter_origins(storage, batch_size=1), origins)
    assert_list_eq(iter_origins(storage, batch_size=2), origins)

    for i in range(1, 5):
        assert_list_eq(
            iter_origins(storage, origin_from=i+1),
            origins[i:],
            i)

        assert_list_eq(
            iter_origins(storage, origin_from=i+1, batch_size=1),
            origins[i:],
            i)

        assert_list_eq(
            iter_origins(storage, origin_from=i+1, batch_size=2),
            origins[i:],
            i)

        for j in range(i, 5):
            assert_list_eq(
                iter_origins(
                    storage, origin_from=i+1, origin_to=j+1),
                origins[i:j],
                (i, j))

            assert_list_eq(
                iter_origins(
                    storage, origin_from=i+1, origin_to=j+1, batch_size=1),
                origins[i:j],
                (i, j))

            assert_list_eq(
                iter_origins(
                    storage, origin_from=i+1, origin_to=j+1, batch_size=2),
                origins[i:j],
                (i, j))


@patch('swh.storage.in_memory.InMemoryStorage.origin_get_range')
def test_iter_origins_batch_size(mock_origin_get_range):
    storage = get_storage(**storage_config)
    mock_origin_get_range.return_value = []

    list(iter_origins(storage))
    mock_origin_get_range.assert_called_with(
        origin_from=1, origin_count=10000)

    list(iter_origins(storage, batch_size=42))
    mock_origin_get_range.assert_called_with(
        origin_from=1, origin_count=42)
