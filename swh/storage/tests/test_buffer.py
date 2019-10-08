# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage.buffer import BufferingProxyStorage


def test_buffering_proxy_storage_content_threshold_not_hit(sample_data):
    contents = sample_data['content']
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'content': 10,
        }
    )
    s = storage.content_add([contents[0], contents[1]])
    assert s == {}

    # contents have not been written to storage
    missing_contents = storage.content_missing(
        [contents[0], contents[1]])
    assert set(missing_contents) == set(
        [contents[0]['sha1'], contents[1]['sha1']])

    s = storage.flush()
    assert s == {
        'content:add': 1 + 1,
        'content:add:bytes': contents[0]['length'] + contents[1]['length'],
        'skipped_content:add': 0
    }

    missing_contents = storage.content_missing(
        [contents[0], contents[1]])
    assert list(missing_contents) == []


def test_buffering_proxy_storage_content_threshold_nb_hit(sample_data):
    contents = sample_data['content']
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'content': 1,
        }
    )

    s = storage.content_add([contents[0]])
    assert s == {
        'content:add': 1,
        'content:add:bytes': contents[0]['length'],
        'skipped_content:add': 0
    }

    missing_contents = storage.content_missing([contents[0]])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_content_threshold_bytes_hit(sample_data):
    contents = sample_data['content']
    content_bytes_min_batch_size = 20
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'content': 10,
            'content_bytes': content_bytes_min_batch_size,
        }
    )

    assert contents[0]['length'] > content_bytes_min_batch_size

    s = storage.content_add([contents[0]])
    assert s == {
        'content:add': 1,
        'content:add:bytes': contents[0]['length'],
        'skipped_content:add': 0
    }

    missing_contents = storage.content_missing([contents[0]])
    assert list(missing_contents) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_directory_threshold_not_hit(sample_data):
    directories = sample_data['directory']
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'directory': 10,
        }
    )
    s = storage.directory_add([directories[0]])
    assert s == {}

    directory_id = directories[0]['id']
    missing_directories = storage.directory_missing(
        [directory_id])
    assert list(missing_directories) == [directory_id]

    s = storage.flush()
    assert s == {
        'directory:add': 1,
    }

    missing_directories = storage.directory_missing(
        [directory_id])
    assert list(missing_directories) == []


def test_buffering_proxy_storage_directory_threshold_hit(sample_data):
    directories = sample_data['directory']
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'directory': 1,
        }
    )
    s = storage.directory_add([directories[0]])
    assert s == {
        'directory:add': 1,
    }

    missing_directories = storage.directory_missing(
        [directories[0]['id']])
    assert list(missing_directories) == []

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_revision_threshold_not_hit(sample_data):
    revisions = sample_data['revision']
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'revision': 10,
        }
    )
    s = storage.revision_add([revisions[0]])
    assert s == {}

    revision_id = revisions[0]['id']
    missing_revisions = storage.revision_missing(
        [revision_id])
    assert list(missing_revisions) == [revision_id]

    s = storage.flush()
    assert s == {
        'revision:add': 1,
    }

    missing_revisions = storage.revision_missing(
        [revision_id])
    assert list(missing_revisions) == []


def test_buffering_proxy_storage_revision_threshold_hit(sample_data):
    revisions = sample_data['revision']
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        min_batch_size={
            'revision': 1,
        }
    )
    s = storage.revision_add([revisions[0]])
    assert s == {
        'revision:add': 1,
    }

    missing_revisions = storage.revision_missing(
        [revisions[0]['id']])
    assert list(missing_revisions) == []

    s = storage.flush()
    assert s == {}
