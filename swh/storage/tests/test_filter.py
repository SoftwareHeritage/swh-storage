# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.storage.filter import FilteringProxyStorage


def test_filtering_proxy_storage_content(sample_data):
    sample_content = sample_data['content'][0]
    storage = FilteringProxyStorage(storage={'cls': 'memory'})

    content = next(storage.content_get([sample_content['sha1']]))
    assert not content

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
        'content:add:bytes': sample_content['length'],
        'skipped_content:add': 0
    }

    content = next(storage.content_get([sample_content['sha1']]))
    assert content is not None

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 0,
        'content:add:bytes': 0,
        'skipped_content:add': 0
    }


def test_filtering_proxy_storage_revision(sample_data):
    sample_revision = sample_data['revision'][0]
    storage = FilteringProxyStorage(storage={'cls': 'memory'})

    revision = next(storage.revision_get([sample_revision['id']]))
    assert not revision

    s = storage.revision_add([sample_revision])
    assert s == {
        'revision:add': 1,
    }

    revision = next(storage.revision_get([sample_revision['id']]))
    assert revision is not None

    s = storage.revision_add([sample_revision])
    assert s == {
        'revision:add': 0,
    }


def test_filtering_proxy_storage_directory(sample_data):
    sample_directory = sample_data['directory'][0]
    storage = FilteringProxyStorage(storage={'cls': 'memory'})

    directory = next(storage.directory_missing([sample_directory['id']]))
    assert directory

    s = storage.directory_add([sample_directory])
    assert s == {
        'directory:add': 1,
    }

    directory = list(storage.directory_missing([sample_directory['id']]))
    assert not directory

    s = storage.directory_add([sample_directory])
    assert s == {
        'directory:add': 0,
    }
