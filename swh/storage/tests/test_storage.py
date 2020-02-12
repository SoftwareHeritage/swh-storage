# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from contextlib import contextmanager
import datetime
import inspect
import itertools
import math
import queue
import random
import threading

from collections import defaultdict
from datetime import timedelta
from unittest.mock import Mock

import psycopg2
import pytest

from hypothesis import given, strategies, settings, HealthCheck

from typing import ClassVar, Optional

from swh.model import from_disk, identifiers
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Release, Revision
from swh.model.hypothesis_strategies import objects
from swh.storage import HashCollision, get_storage
from swh.storage.converters import origin_url_to_sha1 as sha1
from swh.storage.exc import StorageArgumentException
from swh.storage.interface import StorageInterface

from .storage_data import data


@contextmanager
def db_transaction(storage):
    with storage.db() as db:
        with db.transaction() as cur:
            yield db, cur


def normalize_entity(entity):
    entity = copy.deepcopy(entity)
    for key in ('date', 'committer_date'):
        if key in entity:
            entity[key] = identifiers.normalize_timestamp(entity[key])
    return entity


def transform_entries(dir_, *, prefix=b''):
    for ent in dir_['entries']:
        yield {
            'dir_id': dir_['id'],
            'type': ent['type'],
            'target': ent['target'],
            'name': prefix + ent['name'],
            'perms': ent['perms'],
            'status': None,
            'sha1': None,
            'sha1_git': None,
            'sha256': None,
            'length': None,
        }


def cmpdir(directory):
    return (directory['type'], directory['dir_id'])


def short_revision(revision):
    return [revision['id'], revision['parents']]


def assert_contents_ok(expected_contents, actual_contents,
                       keys_to_check={'sha1', 'data'}):
    """Assert that a given list of contents matches on a given set of keys.

    """
    for k in keys_to_check:
        expected_list = set([c.get(k) for c in expected_contents])
        actual_list = set([c.get(k) for c in actual_contents])
        assert actual_list == expected_list, k


class TestStorage:
    """Main class for Storage testing.

    This class is used as-is to test local storage (see TestLocalStorage
    below) and remote storage (see TestRemoteStorage in
    test_remote_storage.py.

    We need to have the two classes inherit from this base class
    separately to avoid nosetests running the tests from the base
    class twice.
    """
    maxDiff = None  # type: ClassVar[Optional[int]]

    def test_types(self, swh_storage_backend_config):
        """Checks all methods of StorageInterface are implemented by this
        backend, and that they have the same signature."""
        # Create an instance of the protocol (which cannot be instantiated
        # directly, so this creates a subclass, then instantiates it)
        interface = type('_', (StorageInterface,), {})()
        storage = get_storage(**swh_storage_backend_config)

        assert 'content_add' in dir(interface)

        missing_methods = []

        for meth_name in dir(interface):
            if meth_name.startswith('_'):
                continue
            interface_meth = getattr(interface, meth_name)
            try:
                concrete_meth = getattr(storage, meth_name)
            except AttributeError:
                if not getattr(interface_meth, 'deprecated_endpoint', False):
                    # The backend is missing a (non-deprecated) endpoint
                    missing_methods.append(meth_name)
                continue

            expected_signature = inspect.signature(interface_meth)
            actual_signature = inspect.signature(concrete_meth)

            assert expected_signature == actual_signature, meth_name

        assert missing_methods == []

    def test_check_config(self, swh_storage):
        assert swh_storage.check_config(check_write=True)
        assert swh_storage.check_config(check_write=False)

    def test_content_add(self, swh_storage):
        cont = data.cont

        insertion_start_time = datetime.datetime.now(tz=datetime.timezone.utc)
        actual_result = swh_storage.content_add([cont])
        insertion_end_time = datetime.datetime.now(tz=datetime.timezone.utc)

        assert actual_result == {
            'content:add': 1,
            'content:add:bytes': cont['length'],
        }

        assert list(swh_storage.content_get([cont['sha1']])) == \
            [{'sha1': cont['sha1'], 'data': cont['data']}]

        expected_cont = data.cont
        del expected_cont['data']
        journal_objects = list(swh_storage.journal_writer.journal.objects)
        for (obj_type, obj) in journal_objects:
            assert insertion_start_time <= obj['ctime']
            assert obj['ctime'] <= insertion_end_time
            del obj['ctime']
        assert journal_objects == [('content', expected_cont)]

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['content'] == 1

    def test_content_add_from_generator(self, swh_storage):
        def _cnt_gen():
            yield data.cont

        actual_result = swh_storage.content_add(_cnt_gen())

        assert actual_result == {
            'content:add': 1,
            'content:add:bytes': data.cont['length'],
        }

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['content'] == 1

    def test_content_add_validation(self, swh_storage):
        cont = data.cont

        with pytest.raises(StorageArgumentException, match='status'):
            swh_storage.content_add([{**cont, 'status': 'absent'}])

        with pytest.raises(StorageArgumentException, match='status'):
            swh_storage.content_add([{**cont, 'status': 'foobar'}])

        with pytest.raises(StorageArgumentException, match="(?i)length"):
            swh_storage.content_add([{**cont, 'length': -2}])

        with pytest.raises(StorageArgumentException, match="reason"):
            swh_storage.content_add([{**cont, 'reason': 'foobar'}])

    def test_skipped_content_add_validation(self, swh_storage):
        cont = data.cont.copy()
        del cont['data']

        with pytest.raises(StorageArgumentException, match='status'):
            swh_storage.skipped_content_add([{**cont, 'status': 'visible'}])

        with pytest.raises(StorageArgumentException, match='reason') as cm:
            swh_storage.skipped_content_add([{**cont, 'status': 'absent'}])

        if type(cm.value) == psycopg2.IntegrityError:
            assert cm.exception.pgcode == \
                psycopg2.errorcodes.NOT_NULL_VIOLATION

    def test_content_get_missing(self, swh_storage):
        cont = data.cont

        swh_storage.content_add([cont])

        # Query a single missing content
        results = list(swh_storage.content_get(
            [data.cont2['sha1']]))
        assert results == [None]

        # Check content_get does not abort after finding a missing content
        results = list(swh_storage.content_get(
            [data.cont['sha1'], data.cont2['sha1']]))
        assert results == [{'sha1': cont['sha1'], 'data': cont['data']}, None]

        # Check content_get does not discard found countent when it finds
        # a missing content.
        results = list(swh_storage.content_get(
            [data.cont2['sha1'], data.cont['sha1']]))
        assert results == [None, {'sha1': cont['sha1'], 'data': cont['data']}]

    def test_content_add_different_input(self, swh_storage):
        cont = data.cont
        cont2 = data.cont2

        actual_result = swh_storage.content_add([cont, cont2])
        assert actual_result == {
            'content:add': 2,
            'content:add:bytes': cont['length'] + cont2['length'],
            }

    def test_content_add_twice(self, swh_storage):
        actual_result = swh_storage.content_add([data.cont])
        assert actual_result == {
            'content:add': 1,
            'content:add:bytes': data.cont['length'],
            }
        assert len(swh_storage.journal_writer.journal.objects) == 1

        actual_result = swh_storage.content_add([data.cont, data.cont2])
        assert actual_result == {
            'content:add': 1,
            'content:add:bytes': data.cont2['length'],
            }
        assert 2 <= len(swh_storage.journal_writer.journal.objects) <= 3

        assert len(swh_storage.content_find(data.cont)) == 1
        assert len(swh_storage.content_find(data.cont2)) == 1

    def test_content_add_collision(self, swh_storage):
        cont1 = data.cont

        # create (corrupted) content with same sha1{,_git} but != sha256
        cont1b = cont1.copy()
        sha256_array = bytearray(cont1b['sha256'])
        sha256_array[0] += 1
        cont1b['sha256'] = bytes(sha256_array)

        with pytest.raises(HashCollision) as cm:
            swh_storage.content_add([cont1, cont1b])

        assert cm.value.args[0] in ['sha1', 'sha1_git', 'blake2s256']

    def test_content_update(self, swh_storage):
        if hasattr(swh_storage, 'storage'):
            swh_storage.journal_writer.journal = None  # TODO, not supported

        cont = copy.deepcopy(data.cont)

        swh_storage.content_add([cont])
        # alter the sha1_git for example
        cont['sha1_git'] = hash_to_bytes(
            '3a60a5275d0333bf13468e8b3dcab90f4046e654')

        swh_storage.content_update([cont], keys=['sha1_git'])

        results = swh_storage.content_get_metadata([cont['sha1']])
        del cont['data']
        assert results == {cont['sha1']: [cont]}

    def test_content_add_metadata(self, swh_storage):
        cont = data.cont
        del cont['data']
        cont['ctime'] = datetime.datetime.now()

        actual_result = swh_storage.content_add_metadata([cont])
        assert actual_result == {
            'content:add': 1,
            }

        expected_cont = cont.copy()
        del expected_cont['ctime']
        assert swh_storage.content_get_metadata([cont['sha1']]) == {
            cont['sha1']: [expected_cont]
        }

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('content', cont)]

    def test_content_add_metadata_different_input(self, swh_storage):
        cont = data.cont
        del cont['data']
        cont['ctime'] = datetime.datetime.now()
        cont2 = data.cont2
        del cont2['data']
        cont2['ctime'] = datetime.datetime.now()

        actual_result = swh_storage.content_add_metadata([cont, cont2])
        assert actual_result == {
            'content:add': 2,
            }

    def test_content_add_metadata_collision(self, swh_storage):
        cont1 = data.cont
        del cont1['data']
        cont1['ctime'] = datetime.datetime.now()

        # create (corrupted) content with same sha1{,_git} but != sha256
        cont1b = cont1.copy()
        sha256_array = bytearray(cont1b['sha256'])
        sha256_array[0] += 1
        cont1b['sha256'] = bytes(sha256_array)

        with pytest.raises(HashCollision) as cm:
            swh_storage.content_add_metadata([cont1, cont1b])

        assert cm.value.args[0] in ['sha1', 'sha1_git', 'blake2s256']

    def test_skipped_content_add(self, swh_storage):
        cont = data.skipped_cont
        cont2 = data.skipped_cont2
        cont2['blake2s256'] = None

        missing = list(swh_storage.skipped_content_missing([cont, cont2]))

        assert len(missing) == 2

        actual_result = swh_storage.skipped_content_add([cont, cont, cont2])

        assert 2 <= actual_result.pop('skipped_content:add') <= 3
        assert actual_result == {}

        missing = list(swh_storage.skipped_content_missing([cont, cont2]))

        assert missing == []

    @pytest.mark.property_based
    @settings(deadline=None)  # this test is very slow
    @given(strategies.sets(
        elements=strategies.sampled_from(
            ['sha256', 'sha1_git', 'blake2s256']),
        min_size=0))
    def test_content_missing(self, swh_storage, algos):
        algos |= {'sha1'}
        cont2 = data.cont2
        missing_cont = data.missing_cont
        swh_storage.content_add([cont2])
        test_contents = [cont2]
        missing_per_hash = defaultdict(list)
        for i in range(256):
            test_content = missing_cont.copy()
            for hash in algos:
                test_content[hash] = bytes([i]) + test_content[hash][1:]
                missing_per_hash[hash].append(test_content[hash])
            test_contents.append(test_content)

        assert set(swh_storage.content_missing(test_contents)) == \
            set(missing_per_hash['sha1'])

        for hash in algos:
            assert set(swh_storage.content_missing(
                test_contents, key_hash=hash)) == set(missing_per_hash[hash])

    @pytest.mark.property_based
    @given(strategies.sets(
        elements=strategies.sampled_from(
            ['sha256', 'sha1_git', 'blake2s256']),
        min_size=0))
    def test_content_missing_unknown_algo(self, swh_storage, algos):
        algos |= {'sha1'}
        cont2 = data.cont2
        missing_cont = data.missing_cont
        swh_storage.content_add([cont2])
        test_contents = [cont2]
        missing_per_hash = defaultdict(list)
        for i in range(16):
            test_content = missing_cont.copy()
            for hash in algos:
                test_content[hash] = bytes([i]) + test_content[hash][1:]
                missing_per_hash[hash].append(test_content[hash])
            test_content['nonexisting_algo'] = b'\x00'
            test_contents.append(test_content)

        assert set(
            swh_storage.content_missing(test_contents)) == set(
            missing_per_hash['sha1'])

        for hash in algos:
            assert set(swh_storage.content_missing(
                test_contents, key_hash=hash)) == set(
                missing_per_hash[hash])

    def test_content_missing_per_sha1(self, swh_storage):
        # given
        cont2 = data.cont2
        missing_cont = data.missing_cont
        swh_storage.content_add([cont2])
        # when
        gen = swh_storage.content_missing_per_sha1([cont2['sha1'],
                                                    missing_cont['sha1']])
        # then
        assert list(gen) == [missing_cont['sha1']]

    def test_content_missing_per_sha1_git(self, swh_storage):
        cont = data.cont
        cont2 = data.cont2
        missing_cont = data.missing_cont

        swh_storage.content_add([cont, cont2])

        contents = [cont['sha1_git'], cont2['sha1_git'],
                    missing_cont['sha1_git']]

        missing_contents = swh_storage.content_missing_per_sha1_git(contents)
        assert list(missing_contents) == [missing_cont['sha1_git']]

    def test_content_get_partition(self, swh_storage, swh_contents):
        """content_get_partition paginates results if limit exceeded"""
        expected_contents = [c for c in swh_contents
                             if c['status'] != 'absent']

        actual_contents = []
        for i in range(16):
            actual_result = swh_storage.content_get_partition(i, 16)
            assert actual_result['next_page_token'] is None
            actual_contents.extend(actual_result['contents'])

        assert_contents_ok(
            expected_contents, actual_contents, ['sha1'])

    def test_content_get_partition_full(self, swh_storage, swh_contents):
        """content_get_partition for a single partition returns all available
        contents"""
        expected_contents = [c for c in swh_contents
                             if c['status'] != 'absent']

        actual_result = swh_storage.content_get_partition(0, 1)
        assert actual_result['next_page_token'] is None

        actual_contents = actual_result['contents']
        assert_contents_ok(
            expected_contents, actual_contents, ['sha1'])

    def test_content_get_partition_empty(self, swh_storage, swh_contents):
        """content_get_partition when at least one of the partitions is
        empty"""
        expected_contents = {cont['sha1'] for cont in swh_contents
                             if cont['status'] != 'absent'}
        # nb_partitions = smallest power of 2 such that at least one of
        # the partitions is empty
        nb_partitions = 1 << math.floor(math.log2(len(swh_contents)) + 1)

        seen_sha1s = []

        for i in range(nb_partitions):
            actual_result = swh_storage.content_get_partition(
                i, nb_partitions, limit=len(swh_contents)+1)

            for cont in actual_result['contents']:
                seen_sha1s.append(cont['sha1'])

            # Limit is higher than the max number of results
            assert actual_result['next_page_token'] is None

        assert set(seen_sha1s) == expected_contents

    def test_content_get_partition_limit_none(self, swh_storage):
        """content_get_partition call with wrong limit input should fail"""
        with pytest.raises(StorageArgumentException) as e:
            swh_storage.content_get_partition(1, 16, limit=None)

        assert e.value.args == ('limit should not be None',)

    def test_generate_content_get_partition_pagination(
            self, swh_storage, swh_contents):
        """content_get_partition returns contents within range provided"""
        expected_contents = [c for c in swh_contents
                             if c['status'] != 'absent']

        # retrieve contents
        actual_contents = []
        for i in range(4):
            page_token = None
            while True:
                actual_result = swh_storage.content_get_partition(
                    i, 4, limit=3, page_token=page_token)
                actual_contents.extend(actual_result['contents'])
                page_token = actual_result['next_page_token']

                if page_token is None:
                    break

        assert_contents_ok(
            expected_contents, actual_contents, ['sha1'])

    def test_content_get_metadata(self, swh_storage):
        cont1 = data.cont
        cont2 = data.cont2

        swh_storage.content_add([cont1, cont2])

        actual_md = swh_storage.content_get_metadata(
            [cont1['sha1'], cont2['sha1']])

        # we only retrieve the metadata
        cont1.pop('data')
        cont2.pop('data')

        assert actual_md[cont1['sha1']] == [cont1]
        assert actual_md[cont2['sha1']] == [cont2]
        assert len(actual_md.keys()) == 2

    def test_content_get_metadata_missing_sha1(self, swh_storage):
        cont1 = data.cont
        cont2 = data.cont2
        missing_cont = data.missing_cont

        swh_storage.content_add([cont1, cont2])

        actual_contents = swh_storage.content_get_metadata(
            [missing_cont['sha1']])

        assert actual_contents == {missing_cont['sha1']: []}

    def test_content_get_random(self, swh_storage):
        swh_storage.content_add([data.cont, data.cont2, data.cont3])

        assert swh_storage.content_get_random() in {
            data.cont['sha1_git'], data.cont2['sha1_git'],
            data.cont3['sha1_git']}

    def test_directory_add(self, swh_storage):
        init_missing = list(swh_storage.directory_missing([data.dir['id']]))
        assert [data.dir['id']] == init_missing

        actual_result = swh_storage.directory_add([data.dir])
        assert actual_result == {'directory:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) == \
            [('directory', data.dir)]

        actual_data = list(swh_storage.directory_ls(data.dir['id']))
        expected_data = list(transform_entries(data.dir))

        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

        after_missing = list(swh_storage.directory_missing([data.dir['id']]))
        assert after_missing == []

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['directory'] == 1

    def test_directory_add_from_generator(self, swh_storage):
        def _dir_gen():
            yield data.dir

        actual_result = swh_storage.directory_add(directories=_dir_gen())
        assert actual_result == {'directory:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) == \
            [('directory', data.dir)]

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['directory'] == 1

    def test_directory_add_validation(self, swh_storage):
        dir_ = copy.deepcopy(data.dir)
        dir_['entries'][0]['type'] = 'foobar'

        with pytest.raises(StorageArgumentException, match='type.*foobar'):
            swh_storage.directory_add([dir_])

        dir_ = copy.deepcopy(data.dir)
        del dir_['entries'][0]['target']

        with pytest.raises(StorageArgumentException, match='target') as cm:
            swh_storage.directory_add([dir_])

        if type(cm.value) == psycopg2.IntegrityError:
            assert cm.value.pgcode == psycopg2.errorcodes.NOT_NULL_VIOLATION

    def test_directory_add_twice(self, swh_storage):
        actual_result = swh_storage.directory_add([data.dir])
        assert actual_result == {'directory:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('directory', data.dir)]

        actual_result = swh_storage.directory_add([data.dir])
        assert actual_result == {'directory:add': 0}

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('directory', data.dir)]

    def test_directory_get_recursive(self, swh_storage):
        init_missing = list(swh_storage.directory_missing([data.dir['id']]))
        assert init_missing == [data.dir['id']]

        actual_result = swh_storage.directory_add(
            [data.dir, data.dir2, data.dir3])
        assert actual_result == {'directory:add': 3}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('directory', data.dir),
            ('directory', data.dir2),
            ('directory', data.dir3)]

        # List directory containing a file and an unknown subdirectory
        actual_data = list(swh_storage.directory_ls(
            data.dir['id'], recursive=True))
        expected_data = list(transform_entries(data.dir))
        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

        # List directory containing a file and an unknown subdirectory
        actual_data = list(swh_storage.directory_ls(
            data.dir2['id'], recursive=True))
        expected_data = list(transform_entries(data.dir2))
        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

        # List directory containing a known subdirectory, entries should
        # be both those of the directory and of the subdir
        actual_data = list(swh_storage.directory_ls(
            data.dir3['id'], recursive=True))
        expected_data = list(itertools.chain(
            transform_entries(data.dir3),
            transform_entries(data.dir, prefix=b'subdir/')))
        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

    def test_directory_get_non_recursive(self, swh_storage):
        init_missing = list(swh_storage.directory_missing([data.dir['id']]))
        assert init_missing == [data.dir['id']]

        actual_result = swh_storage.directory_add(
            [data.dir, data.dir2, data.dir3])
        assert actual_result == {'directory:add': 3}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('directory', data.dir),
            ('directory', data.dir2),
            ('directory', data.dir3)]

        # List directory containing a file and an unknown subdirectory
        actual_data = list(swh_storage.directory_ls(data.dir['id']))
        expected_data = list(transform_entries(data.dir))
        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

        # List directory contaiining a single file
        actual_data = list(swh_storage.directory_ls(data.dir2['id']))
        expected_data = list(transform_entries(data.dir2))
        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

        # List directory containing a known subdirectory, entries should
        # only be those of the parent directory, not of the subdir
        actual_data = list(swh_storage.directory_ls(data.dir3['id']))
        expected_data = list(transform_entries(data.dir3))
        assert sorted(expected_data, key=cmpdir) \
            == sorted(actual_data, key=cmpdir)

    def test_directory_entry_get_by_path(self, swh_storage):
        # given
        init_missing = list(swh_storage.directory_missing([data.dir3['id']]))
        assert [data.dir3['id']] == init_missing

        actual_result = swh_storage.directory_add([data.dir3, data.dir4])
        assert actual_result == {'directory:add': 2}

        expected_entries = [
            {
                'dir_id': data.dir3['id'],
                'name': b'foo',
                'type': 'file',
                'target': data.cont['sha1_git'],
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'status': None,
                'perms': from_disk.DentryPerms.content,
                'length': None,
            },
            {
                'dir_id': data.dir3['id'],
                'name': b'subdir',
                'type': 'dir',
                'target': data.dir['id'],
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'status': None,
                'perms': from_disk.DentryPerms.directory,
                'length': None,
            },
            {
                'dir_id': data.dir3['id'],
                'name': b'hello',
                'type': 'file',
                'target': b'12345678901234567890',
                'sha1': None,
                'sha1_git': None,
                'sha256': None,
                'status': None,
                'perms': from_disk.DentryPerms.content,
                'length': None,
            },
        ]

        # when (all must be found here)
        for entry, expected_entry in zip(
                data.dir3['entries'], expected_entries):
            actual_entry = swh_storage.directory_entry_get_by_path(
                data.dir3['id'],
                [entry['name']])
            assert actual_entry == expected_entry

        # same, but deeper
        for entry, expected_entry in zip(
                data.dir3['entries'], expected_entries):
            actual_entry = swh_storage.directory_entry_get_by_path(
                data.dir4['id'],
                [b'subdir1', entry['name']])
            expected_entry = expected_entry.copy()
            expected_entry['name'] = b'subdir1/' + expected_entry['name']
            assert actual_entry == expected_entry

        # when (nothing should be found here since data.dir is not persisted.)
        for entry in data.dir['entries']:
            actual_entry = swh_storage.directory_entry_get_by_path(
                data.dir['id'],
                [entry['name']])
            assert actual_entry is None

    def test_directory_get_random(self, swh_storage):
        swh_storage.directory_add([data.dir, data.dir2, data.dir3])

        assert swh_storage.directory_get_random() in \
            {data.dir['id'], data.dir2['id'], data.dir3['id']}

    def test_revision_add(self, swh_storage):
        init_missing = swh_storage.revision_missing([data.revision['id']])
        assert list(init_missing) == [data.revision['id']]

        actual_result = swh_storage.revision_add([data.revision])
        assert actual_result == {'revision:add': 1}

        end_missing = swh_storage.revision_missing([data.revision['id']])
        assert list(end_missing) == []

        normalized_revision = Revision.from_dict(data.revision).to_dict()

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('revision', normalized_revision)]

        # already there so nothing added
        actual_result = swh_storage.revision_add([data.revision])
        assert actual_result == {'revision:add': 0}

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['revision'] == 1

    def test_revision_add_from_generator(self, swh_storage):
        def _rev_gen():
            yield data.revision

        actual_result = swh_storage.revision_add(_rev_gen())
        assert actual_result == {'revision:add': 1}

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['revision'] == 1

    def test_revision_add_validation(self, swh_storage):
        rev = copy.deepcopy(data.revision)
        rev['date']['offset'] = 2**16

        with pytest.raises(StorageArgumentException, match='offset') as cm:
            swh_storage.revision_add([rev])

        if type(cm.value) == psycopg2.DataError:
            assert cm.value.pgcode \
                == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE

        rev = copy.deepcopy(data.revision)
        rev['committer_date']['offset'] = 2**16

        with pytest.raises(StorageArgumentException, match='offset') as cm:
            swh_storage.revision_add([rev])

        if type(cm.value) == psycopg2.DataError:
            assert cm.value.pgcode \
                == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE

        rev = copy.deepcopy(data.revision)
        rev['type'] = 'foobar'

        with pytest.raises(StorageArgumentException, match='(?i)type') as cm:
            swh_storage.revision_add([rev])

        if type(cm.value) == psycopg2.DataError:
            assert cm.value.pgcode == \
                psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION

    def test_revision_add_twice(self, swh_storage):
        actual_result = swh_storage.revision_add([data.revision])
        assert actual_result == {'revision:add': 1}

        normalized_revision = Revision.from_dict(data.revision).to_dict()
        normalized_revision2 = Revision.from_dict(data.revision2).to_dict()

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('revision', normalized_revision)]

        actual_result = swh_storage.revision_add(
            [data.revision, data.revision2])
        assert actual_result == {'revision:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('revision', normalized_revision),
                ('revision', normalized_revision2)]

    def test_revision_add_name_clash(self, swh_storage):
        revision1 = data.revision
        revision2 = data.revision2

        revision1['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe',
            'email': b'john.doe@example.com'
        }
        revision2['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe ',
            'email': b'john.doe@example.com '
        }
        actual_result = swh_storage.revision_add([revision1, revision2])
        assert actual_result == {'revision:add': 2}

    def test_revision_log(self, swh_storage):
        # given
        # data.revision4 -is-child-of-> data.revision3
        swh_storage.revision_add([data.revision3,
                                  data.revision4])

        # when
        actual_results = list(swh_storage.revision_log(
            [data.revision4['id']]))

        # hack: ids generated
        for actual_result in actual_results:
            if 'id' in actual_result['author']:
                del actual_result['author']['id']
            if 'id' in actual_result['committer']:
                del actual_result['committer']['id']

        assert len(actual_results) == 2  # rev4 -child-> rev3
        assert actual_results[0] == normalize_entity(data.revision4)
        assert actual_results[1] == normalize_entity(data.revision3)

        normalized_revision3 = Revision.from_dict(data.revision3).to_dict()
        normalized_revision4 = Revision.from_dict(data.revision4).to_dict()

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('revision', normalized_revision3),
            ('revision', normalized_revision4)]

    def test_revision_log_with_limit(self, swh_storage):
        # given
        # data.revision4 -is-child-of-> data.revision3
        swh_storage.revision_add([data.revision3,
                                  data.revision4])
        actual_results = list(swh_storage.revision_log(
            [data.revision4['id']], 1))

        # hack: ids generated
        for actual_result in actual_results:
            if 'id' in actual_result['author']:
                del actual_result['author']['id']
            if 'id' in actual_result['committer']:
                del actual_result['committer']['id']

        assert len(actual_results) == 1
        assert actual_results[0] == data.revision4

    def test_revision_log_unknown_revision(self, swh_storage):
        rev_log = list(swh_storage.revision_log([data.revision['id']]))
        assert rev_log == []

    def test_revision_shortlog(self, swh_storage):
        # given
        # data.revision4 -is-child-of-> data.revision3
        swh_storage.revision_add([data.revision3,
                                  data.revision4])

        # when
        actual_results = list(swh_storage.revision_shortlog(
            [data.revision4['id']]))

        assert len(actual_results) == 2  # rev4 -child-> rev3
        assert list(actual_results[0]) == short_revision(data.revision4)
        assert list(actual_results[1]) == short_revision(data.revision3)

    def test_revision_shortlog_with_limit(self, swh_storage):
        # given
        # data.revision4 -is-child-of-> data.revision3
        swh_storage.revision_add([data.revision3,
                                  data.revision4])
        actual_results = list(swh_storage.revision_shortlog(
            [data.revision4['id']], 1))

        assert len(actual_results) == 1
        assert list(actual_results[0]) == short_revision(data.revision4)

    def test_revision_get(self, swh_storage):
        swh_storage.revision_add([data.revision])

        actual_revisions = list(swh_storage.revision_get(
            [data.revision['id'], data.revision2['id']]))

        # when
        if 'id' in actual_revisions[0]['author']:
            del actual_revisions[0]['author']['id']  # hack: ids are generated
        if 'id' in actual_revisions[0]['committer']:
            del actual_revisions[0]['committer']['id']

        assert len(actual_revisions) == 2
        assert actual_revisions[0] == normalize_entity(data.revision)
        assert actual_revisions[1] is None

    def test_revision_get_no_parents(self, swh_storage):
        swh_storage.revision_add([data.revision3])

        get = list(swh_storage.revision_get([data.revision3['id']]))

        assert len(get) == 1
        assert get[0]['parents'] == []  # no parents on this one

    def test_revision_get_random(self, swh_storage):
        swh_storage.revision_add(
            [data.revision, data.revision2, data.revision3])

        assert swh_storage.revision_get_random() in \
            {data.revision['id'], data.revision2['id'], data.revision3['id']}

    def test_release_add(self, swh_storage):
        normalized_release = Release.from_dict(data.release).to_dict()
        normalized_release2 = Release.from_dict(data.release2).to_dict()

        init_missing = swh_storage.release_missing([data.release['id'],
                                                    data.release2['id']])
        assert [data.release['id'], data.release2['id']] == list(init_missing)

        actual_result = swh_storage.release_add([data.release, data.release2])
        assert actual_result == {'release:add': 2}

        end_missing = swh_storage.release_missing([data.release['id'],
                                                   data.release2['id']])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('release', normalized_release),
            ('release', normalized_release2)]

        # already present so nothing added
        actual_result = swh_storage.release_add([data.release, data.release2])
        assert actual_result == {'release:add': 0}

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['release'] == 2

    def test_release_add_from_generator(self, swh_storage):
        def _rel_gen():
            yield data.release
            yield data.release2

        normalized_release = Release.from_dict(data.release).to_dict()
        normalized_release2 = Release.from_dict(data.release2).to_dict()

        actual_result = swh_storage.release_add(_rel_gen())
        assert actual_result == {'release:add': 2}

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('release', normalized_release),
            ('release', normalized_release2)]

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['release'] == 2

    def test_release_add_no_author_date(self, swh_storage):
        release = data.release

        release['author'] = None
        release['date'] = None

        actual_result = swh_storage.release_add([release])
        assert actual_result == {'release:add': 1}

        end_missing = swh_storage.release_missing([data.release['id']])
        assert list(end_missing) == []

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('release', release)]

    def test_release_add_validation(self, swh_storage):
        rel = copy.deepcopy(data.release)
        rel['date']['offset'] = 2**16

        with pytest.raises(StorageArgumentException, match='offset') as cm:
            swh_storage.release_add([rel])

        if type(cm.value) == psycopg2.DataError:
            assert cm.value.pgcode \
                == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE

        rel = copy.deepcopy(data.release)
        rel['author'] = None

        with pytest.raises(StorageArgumentException, match='date') as cm:
            swh_storage.release_add([rel])

        if type(cm.value) == psycopg2.IntegrityError:
            assert cm.value.pgcode == psycopg2.errorcodes.CHECK_VIOLATION

    def test_release_add_twice(self, swh_storage):
        actual_result = swh_storage.release_add([data.release])
        assert actual_result == {'release:add': 1}

        normalized_release = Release.from_dict(data.release).to_dict()
        normalized_release2 = Release.from_dict(data.release2).to_dict()

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('release', normalized_release)]

        actual_result = swh_storage.release_add([data.release, data.release2])
        assert actual_result == {'release:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('release', normalized_release),
                ('release', normalized_release2)]

    def test_release_add_name_clash(self, swh_storage):
        release1 = data.release.copy()
        release2 = data.release2.copy()

        release1['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe',
            'email': b'john.doe@example.com'
        }
        release2['author'] = {
            'fullname': b'John Doe <john.doe@example.com>',
            'name': b'John Doe ',
            'email': b'john.doe@example.com '
        }
        actual_result = swh_storage.release_add([release1, release2])
        assert actual_result == {'release:add': 2}

    def test_release_get(self, swh_storage):
        # given
        swh_storage.release_add([data.release, data.release2])

        # when
        actual_releases = list(swh_storage.release_get([data.release['id'],
                                                        data.release2['id']]))

        # then
        for actual_release in actual_releases:
            if 'id' in actual_release['author']:
                del actual_release['author']['id']  # hack: ids are generated

        assert [
            normalize_entity(data.release), normalize_entity(data.release2)] \
            == [actual_releases[0], actual_releases[1]]

        unknown_releases = \
            list(swh_storage.release_get([data.release3['id']]))

        assert unknown_releases[0] is None

    def test_release_get_random(self, swh_storage):
        swh_storage.release_add([data.release, data.release2, data.release3])

        assert swh_storage.release_get_random() in \
            {data.release['id'], data.release2['id'], data.release3['id']}

    def test_origin_add_one(self, swh_storage):
        origin0 = swh_storage.origin_get(data.origin)
        assert origin0 is None

        id = swh_storage.origin_add_one(data.origin)

        actual_origin = swh_storage.origin_get({'url': data.origin['url']})
        assert actual_origin['url'] == data.origin['url']

        id2 = swh_storage.origin_add_one(data.origin)

        assert id == id2

    def test_origin_add(self, swh_storage):
        origin0 = swh_storage.origin_get([data.origin])[0]
        assert origin0 is None

        origin1, origin2 = swh_storage.origin_add([data.origin, data.origin2])

        actual_origin = swh_storage.origin_get([{
            'url': data.origin['url'],
        }])[0]
        assert actual_origin['url'] == origin1['url']

        actual_origin2 = swh_storage.origin_get([{
            'url': data.origin2['url'],
        }])[0]
        assert actual_origin2['url'] == origin2['url']

        if 'id' in actual_origin:
            del actual_origin['id']
            del actual_origin2['id']

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('origin', actual_origin),
                ('origin', actual_origin2)]

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['origin'] == 2

    def test_origin_add_from_generator(self, swh_storage):
        def _ori_gen():
            yield data.origin
            yield data.origin2

        origin1, origin2 = swh_storage.origin_add(_ori_gen())

        actual_origin = swh_storage.origin_get([{
            'url': data.origin['url'],
        }])[0]
        assert actual_origin['url'] == origin1['url']

        actual_origin2 = swh_storage.origin_get([{
            'url': data.origin2['url'],
        }])[0]
        assert actual_origin2['url'] == origin2['url']

        if 'id' in actual_origin:
            del actual_origin['id']
            del actual_origin2['id']

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('origin', actual_origin),
                ('origin', actual_origin2)]

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['origin'] == 2

    def test_origin_add_twice(self, swh_storage):
        add1 = swh_storage.origin_add([data.origin, data.origin2])
        assert list(swh_storage.journal_writer.journal.objects) \
            == [('origin', data.origin),
                ('origin', data.origin2)]

        add2 = swh_storage.origin_add([data.origin, data.origin2])
        assert list(swh_storage.journal_writer.journal.objects) \
            == [('origin', data.origin),
                ('origin', data.origin2)]

        assert add1 == add2

    def test_origin_add_validation(self, swh_storage):
        with pytest.raises(StorageArgumentException, match='url'):
            swh_storage.origin_add([{'type': 'git'}])

    def test_origin_get_legacy(self, swh_storage):
        assert swh_storage.origin_get(data.origin) is None
        swh_storage.origin_add_one(data.origin)

        actual_origin0 = swh_storage.origin_get(
            {'url': data.origin['url']})
        assert actual_origin0['url'] == data.origin['url']

    def test_origin_get(self, swh_storage):
        assert swh_storage.origin_get(data.origin) is None
        swh_storage.origin_add_one(data.origin)

        actual_origin0 = swh_storage.origin_get(
            [{'url': data.origin['url']}])
        assert len(actual_origin0) == 1
        assert actual_origin0[0]['url'] == data.origin['url']

    def _generate_random_visits(self, nb_visits=100, start=0, end=7):
        """Generate random visits within the last 2 months (to avoid
        computations)

        """
        visits = []
        today = datetime.datetime.now(tz=datetime.timezone.utc)
        for weeks in range(nb_visits, 0, -1):
            hours = random.randint(0, 24)
            minutes = random.randint(0, 60)
            seconds = random.randint(0, 60)
            days = random.randint(0, 28)
            weeks = random.randint(start, end)
            date_visit = today - timedelta(
                weeks=weeks, hours=hours, minutes=minutes,
                seconds=seconds, days=days)
            visits.append(date_visit)
        return visits

    def test_origin_visit_get_random(self, swh_storage):
        swh_storage.origin_add(data.origins)
        # Add some random visits within the selection range
        visits = self._generate_random_visits()
        visit_type = 'git'

        # Add visits to those origins
        for origin in data.origins:
            for date_visit in visits:
                visit = swh_storage.origin_visit_add(
                    origin['url'], date=date_visit, type=visit_type)
                swh_storage.origin_visit_update(
                    origin['url'], visit_id=visit['visit'], status='full')

        swh_storage.refresh_stat_counters()

        stats = swh_storage.stat_counters()
        assert stats['origin'] == len(data.origins)
        assert stats['origin_visit'] == len(data.origins) * len(visits)

        random_origin_visit = swh_storage.origin_visit_get_random(visit_type)
        assert random_origin_visit
        assert random_origin_visit['origin'] is not None
        original_urls = [o['url'] for o in data.origins]
        assert random_origin_visit['origin'] in original_urls

    def test_origin_visit_get_random_nothing_found(self, swh_storage):
        swh_storage.origin_add(data.origins)
        visit_type = 'hg'
        # Add some visits outside of the random generation selection so nothing
        # will be found by the random selection
        visits = self._generate_random_visits(nb_visits=3, start=13, end=24)
        for origin in data.origins:
            for date_visit in visits:
                visit = swh_storage.origin_visit_add(
                    origin['url'], date=date_visit, type=visit_type)
                swh_storage.origin_visit_update(
                    origin['url'], visit_id=visit['visit'], status='full')

        random_origin_visit = swh_storage.origin_visit_get_random(visit_type)
        assert random_origin_visit is None

    def test_origin_get_by_sha1(self, swh_storage):
        assert swh_storage.origin_get(data.origin) is None
        swh_storage.origin_add_one(data.origin)

        origins = list(swh_storage.origin_get_by_sha1([
            sha1(data.origin['url'])
        ]))
        assert len(origins) == 1
        assert origins[0]['url'] == data.origin['url']

    def test_origin_get_by_sha1_not_found(self, swh_storage):
        assert swh_storage.origin_get(data.origin) is None
        origins = list(swh_storage.origin_get_by_sha1([
            sha1(data.origin['url'])
        ]))
        assert len(origins) == 1
        assert origins[0] is None

    def test_origin_search_single_result(self, swh_storage):
        found_origins = list(swh_storage.origin_search(data.origin['url']))
        assert len(found_origins) == 0

        found_origins = list(swh_storage.origin_search(data.origin['url'],
                                                       regexp=True))
        assert len(found_origins) == 0

        swh_storage.origin_add_one(data.origin)
        origin_data = {
            'url': data.origin['url']}
        found_origins = list(swh_storage.origin_search(data.origin['url']))
        assert len(found_origins) == 1
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        assert found_origins[0] == origin_data

        found_origins = list(swh_storage.origin_search(
            '.' + data.origin['url'][1:-1] + '.', regexp=True))
        assert len(found_origins) == 1
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        assert found_origins[0] == origin_data

        swh_storage.origin_add_one(data.origin2)
        origin2_data = {'url': data.origin2['url']}
        found_origins = list(swh_storage.origin_search(data.origin2['url']))
        assert len(found_origins) == 1
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        assert found_origins[0] == origin2_data

        found_origins = list(swh_storage.origin_search(
            '.' + data.origin2['url'][1:-1] + '.', regexp=True))
        assert len(found_origins) == 1
        if 'id' in found_origins[0]:
            del found_origins[0]['id']
        assert found_origins[0] == origin2_data

    def test_origin_search_no_regexp(self, swh_storage):
        swh_storage.origin_add_one(data.origin)
        swh_storage.origin_add_one(data.origin2)

        origin = swh_storage.origin_get({'url': data.origin['url']})
        origin2 = swh_storage.origin_get({'url': data.origin2['url']})

        # no pagination
        found_origins = list(swh_storage.origin_search('/'))
        assert len(found_origins) == 2

        # offset=0
        found_origins0 = list(swh_storage.origin_search('/', offset=0, limit=1)) # noqa
        assert len(found_origins0) == 1
        assert found_origins0[0] in [origin, origin2]

        # offset=1
        found_origins1 = list(swh_storage.origin_search('/', offset=1, limit=1)) # noqa
        assert len(found_origins1) == 1
        assert found_origins1[0] in [origin, origin2]

        # check both origins were returned
        assert found_origins0 != found_origins1

    def test_origin_search_regexp_substring(self, swh_storage):
        swh_storage.origin_add_one(data.origin)
        swh_storage.origin_add_one(data.origin2)

        origin = swh_storage.origin_get({'url': data.origin['url']})
        origin2 = swh_storage.origin_get({'url': data.origin2['url']})

        # no pagination
        found_origins = list(swh_storage.origin_search('/', regexp=True))
        assert len(found_origins) == 2

        # offset=0
        found_origins0 = list(swh_storage.origin_search('/', offset=0, limit=1, regexp=True)) # noqa
        assert len(found_origins0) == 1
        assert found_origins0[0] in [origin, origin2]

        # offset=1
        found_origins1 = list(swh_storage.origin_search('/', offset=1, limit=1, regexp=True)) # noqa
        assert len(found_origins1) == 1
        assert found_origins1[0] in [origin, origin2]

        # check both origins were returned
        assert found_origins0 != found_origins1

    def test_origin_search_regexp_fullstring(self, swh_storage):
        swh_storage.origin_add_one(data.origin)
        swh_storage.origin_add_one(data.origin2)

        origin = swh_storage.origin_get({'url': data.origin['url']})
        origin2 = swh_storage.origin_get({'url': data.origin2['url']})

        # no pagination
        found_origins = list(swh_storage.origin_search('.*/.*', regexp=True))
        assert len(found_origins) == 2

        # offset=0
        found_origins0 = list(swh_storage.origin_search('.*/.*', offset=0, limit=1, regexp=True)) # noqa
        assert len(found_origins0) == 1
        assert found_origins0[0] in [origin, origin2]

        # offset=1
        found_origins1 = list(swh_storage.origin_search('.*/.*', offset=1, limit=1, regexp=True)) # noqa
        assert len(found_origins1) == 1
        assert found_origins1[0] in [origin, origin2]

        # check both origins were returned
        assert found_origins0 != found_origins1

    def test_origin_visit_add(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin2)

        origin_url = data.origin2['url']
        date_visit = datetime.datetime.now(datetime.timezone.utc)

        # Round to milliseconds before insertion, so equality doesn't fail
        # after a round-trip through a DB (eg. Cassandra)
        date_visit = date_visit.replace(
            microsecond=round(date_visit.microsecond, -3))

        # when
        origin_visit1 = swh_storage.origin_visit_add(
            origin_url,
            type=data.type_visit1,
            date=date_visit)

        actual_origin_visits = list(swh_storage.origin_visit_get(
            origin_url))
        assert {
                'origin': origin_url,
                'date': date_visit,
                'visit': origin_visit1['visit'],
                'type': data.type_visit1,
                'status': 'ongoing',
                'metadata': None,
                'snapshot': None,
            } in actual_origin_visits

        origin_visit = {
            'origin': origin_url,
            'date': date_visit,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        objects = list(swh_storage.journal_writer.journal.objects)
        assert ('origin', data.origin2) in objects
        assert ('origin_visit', origin_visit) in objects

    def test_origin_visit_get__unknown_origin(self, swh_storage):
        assert [] == list(swh_storage.origin_visit_get('foo'))

    def test_origin_visit_add_default_type(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin2)
        origin_url = data.origin2['url']

        # when
        date_visit = datetime.datetime.now(datetime.timezone.utc)
        date_visit2 = date_visit + datetime.timedelta(minutes=1)

        # Round to milliseconds before insertion, so equality doesn't fail
        # after a round-trip through a DB (eg. Cassandra)
        date_visit = date_visit.replace(
            microsecond=round(date_visit.microsecond, -3))
        date_visit2 = date_visit2.replace(
            microsecond=round(date_visit2.microsecond, -3))

        origin_visit1 = swh_storage.origin_visit_add(
            origin_url,
            date=date_visit,
            type=data.type_visit1,
        )
        origin_visit2 = swh_storage.origin_visit_add(
            origin_url,
            date=date_visit2,
            type=data.type_visit2,
        )

        # then
        assert origin_visit1['origin'] == origin_url
        assert origin_visit1['visit'] is not None

        actual_origin_visits = list(swh_storage.origin_visit_get(
            origin_url))
        expected_visits = [
                {
                    'origin': origin_url,
                    'date': date_visit,
                    'visit': origin_visit1['visit'],
                    'type': data.type_visit1,
                    'status': 'ongoing',
                    'metadata': None,
                    'snapshot': None,
                },
                {
                    'origin': origin_url,
                    'date': date_visit2,
                    'visit': origin_visit2['visit'],
                    'type': data.type_visit2,
                    'status': 'ongoing',
                    'metadata': None,
                    'snapshot': None,
                },
            ]
        for visit in expected_visits:
            assert visit in actual_origin_visits

        objects = list(swh_storage.journal_writer.journal.objects)
        assert ('origin', data.origin2) in objects

        for visit in expected_visits:
            assert ('origin_visit', visit) in objects

    def test_origin_visit_add_validation(self, swh_storage):
        origin_url = swh_storage.origin_add_one(data.origin2)

        with pytest.raises(StorageArgumentException) as cm:
            swh_storage.origin_visit_add(origin_url, date=[b'foo'], type='git')

        if type(cm.value) == psycopg2.ProgrammingError:
            assert cm.value.pgcode \
                == psycopg2.errorcodes.UNDEFINED_FUNCTION

    def test_origin_visit_update(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin)
        origin_url = data.origin['url']

        date_visit = datetime.datetime.now(datetime.timezone.utc)
        date_visit2 = date_visit + datetime.timedelta(minutes=1)

        # Round to milliseconds before insertion, so equality doesn't fail
        # after a round-trip through a DB (eg. Cassandra)
        date_visit = date_visit.replace(
            microsecond=round(date_visit.microsecond, -3))
        date_visit2 = date_visit2.replace(
            microsecond=round(date_visit2.microsecond, -3))

        origin_visit1 = swh_storage.origin_visit_add(
            origin_url,
            date=date_visit,
            type=data.type_visit1,
        )

        origin_visit2 = swh_storage.origin_visit_add(
            origin_url,
            date=date_visit2,
            type=data.type_visit2
        )

        swh_storage.origin_add_one(data.origin2)
        origin_url2 = data.origin2['url']
        origin_visit3 = swh_storage.origin_visit_add(
            origin_url2,
            date=date_visit2,
            type=data.type_visit3
        )

        # when
        visit1_metadata = {
            'contents': 42,
            'directories': 22,
        }
        swh_storage.origin_visit_update(
            origin_url,
            origin_visit1['visit'], status='full',
            metadata=visit1_metadata)
        swh_storage.origin_visit_update(
            origin_url2,
            origin_visit3['visit'], status='partial')

        # then
        actual_origin_visits = list(swh_storage.origin_visit_get(
            origin_url))
        expected_visits = [{
            'origin': origin_url,
            'date': date_visit,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'full',
            'metadata': visit1_metadata,
            'snapshot': None,
        }, {
            'origin': origin_url,
            'date': date_visit2,
            'visit': origin_visit2['visit'],
            'type': data.type_visit2,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }]
        for visit in expected_visits:
            assert visit in actual_origin_visits

        actual_origin_visits_bis = list(swh_storage.origin_visit_get(
            origin_url,
            limit=1))
        assert actual_origin_visits_bis == [
            {
                'origin': origin_url,
                'date': date_visit,
                'visit': origin_visit1['visit'],
                'type': data.type_visit1,
                'status': 'full',
                'metadata': visit1_metadata,
                'snapshot': None,
            }]

        actual_origin_visits_ter = list(swh_storage.origin_visit_get(
            origin_url,
            last_visit=origin_visit1['visit']))
        assert actual_origin_visits_ter == [
            {
                'origin': origin_url,
                'date': date_visit2,
                'visit': origin_visit2['visit'],
                'type': data.type_visit2,
                'status': 'ongoing',
                'metadata': None,
                'snapshot': None,
            }]

        actual_origin_visits2 = list(swh_storage.origin_visit_get(
            origin_url2))
        assert actual_origin_visits2 == [
            {
                'origin': origin_url2,
                'date': date_visit2,
                'visit': origin_visit3['visit'],
                'type': data.type_visit3,
                'status': 'partial',
                'metadata': None,
                'snapshot': None,
            }]

        data1 = {
            'origin': origin_url,
            'date': date_visit,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': origin_url,
            'date': date_visit2,
            'visit': origin_visit2['visit'],
            'type': data.type_visit2,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data3 = {
            'origin': origin_url2,
            'date': date_visit2,
            'visit': origin_visit3['visit'],
            'type': data.type_visit3,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data4 = {
            'origin': origin_url,
            'date': date_visit,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'metadata': visit1_metadata,
            'status': 'full',
            'snapshot': None,
        }
        data5 = {
            'origin': origin_url2,
            'date': date_visit2,
            'visit': origin_visit3['visit'],
            'type': data.type_visit3,
            'status': 'partial',
            'metadata': None,
            'snapshot': None,
        }
        objects = list(swh_storage.journal_writer.journal.objects)
        assert ('origin', data.origin) in objects
        assert ('origin', data.origin2) in objects
        assert ('origin_visit', data1) in objects
        assert ('origin_visit', data2) in objects
        assert ('origin_visit', data3) in objects
        assert ('origin_visit', data4) in objects
        assert ('origin_visit', data5) in objects

    def test_origin_visit_update_validation(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        visit = swh_storage.origin_visit_add(
            origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )

        with pytest.raises(StorageArgumentException, match='status') as cm:
            swh_storage.origin_visit_update(
                origin_url, visit['visit'], status='foobar')

        if type(cm.value) == psycopg2.DataError:
            assert cm.value.pgcode == \
                psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION

    def test_origin_visit_find_by_date(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin)

        swh_storage.origin_visit_add(
            data.origin['url'],
            date=data.date_visit2,
            type=data.type_visit1,
        )

        origin_visit2 = swh_storage.origin_visit_add(
            data.origin['url'],
            date=data.date_visit3,
            type=data.type_visit2,
        )

        origin_visit3 = swh_storage.origin_visit_add(
            data.origin['url'],
            date=data.date_visit2,
            type=data.type_visit3,
        )

        # Simple case
        visit = swh_storage.origin_visit_find_by_date(
            data.origin['url'], data.date_visit3)
        assert visit['visit'] == origin_visit2['visit']

        # There are two visits at the same date, the latest must be returned
        visit = swh_storage.origin_visit_find_by_date(
            data.origin['url'], data.date_visit2)
        assert visit['visit'] == origin_visit3['visit']

    def test_origin_visit_find_by_date__unknown_origin(self, swh_storage):
        swh_storage.origin_visit_find_by_date('foo', data.date_visit2)

    def test_origin_visit_update_missing_snapshot(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin)
        origin_url = data.origin['url']

        origin_visit = swh_storage.origin_visit_add(
            origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )

        # when
        swh_storage.origin_visit_update(
            origin_url,
            origin_visit['visit'],
            snapshot=data.snapshot['id'])

        # then
        actual_origin_visit = swh_storage.origin_visit_get_by(
            origin_url,
            origin_visit['visit'])
        assert actual_origin_visit['snapshot'] == data.snapshot['id']

        # when
        swh_storage.snapshot_add([data.snapshot])
        assert actual_origin_visit['snapshot'] == data.snapshot['id']

    def test_origin_visit_get_by(self, swh_storage):
        swh_storage.origin_add_one(data.origin)
        swh_storage.origin_add_one(data.origin2)

        origin_url = data.origin['url']
        origin2_url = data.origin2['url']

        origin_visit1 = swh_storage.origin_visit_add(
            origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )

        swh_storage.snapshot_add([data.snapshot])
        swh_storage.origin_visit_update(
            origin_url,
            origin_visit1['visit'],
            snapshot=data.snapshot['id'])

        # Add some other {origin, visit} entries
        swh_storage.origin_visit_add(
            origin_url,
            date=data.date_visit3,
            type=data.type_visit3,
        )
        swh_storage.origin_visit_add(
            origin2_url,
            date=data.date_visit3,
            type=data.type_visit3,
        )

        # when
        visit1_metadata = {
            'contents': 42,
            'directories': 22,
        }

        swh_storage.origin_visit_update(
            origin_url,
            origin_visit1['visit'], status='full',
            metadata=visit1_metadata)

        expected_origin_visit = origin_visit1.copy()
        expected_origin_visit.update({
            'origin': origin_url,
            'visit': origin_visit1['visit'],
            'date': data.date_visit2,
            'type': data.type_visit2,
            'metadata': visit1_metadata,
            'status': 'full',
            'snapshot': data.snapshot['id'],
        })

        # when
        actual_origin_visit1 = swh_storage.origin_visit_get_by(
            origin_url,
            origin_visit1['visit'])

        # then
        assert actual_origin_visit1 == expected_origin_visit

    def test_origin_visit_get_by__unknown_origin(self, swh_storage):
        assert swh_storage.origin_visit_get_by('foo', 10) is None

    def test_origin_visit_upsert_new(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin2)
        origin_url = data.origin2['url']

        # when
        swh_storage.origin_visit_upsert([
            {
                 'origin': origin_url,
                 'date': data.date_visit2,
                 'visit': 123,
                 'type': data.type_visit2,
                 'status': 'full',
                 'metadata': None,
                 'snapshot': None,
             },
            {
                 'origin': origin_url,
                 'date': '2018-01-01 23:00:00+00',
                 'visit': 1234,
                 'type': data.type_visit2,
                 'status': 'full',
                 'metadata': None,
                 'snapshot': None,
             },
        ])

        # then
        actual_origin_visits = list(swh_storage.origin_visit_get(
            origin_url))
        assert actual_origin_visits == [
            {
                'origin': origin_url,
                'date': data.date_visit2,
                'visit': 123,
                'type': data.type_visit2,
                'status': 'full',
                'metadata': None,
                'snapshot': None,
            },
            {
                'origin': origin_url,
                'date': data.date_visit3,
                'visit': 1234,
                'type': data.type_visit2,
                'status': 'full',
                'metadata': None,
                'snapshot': None,
            },
        ]

        data1 = {
            'origin': origin_url,
            'date': data.date_visit2,
            'visit': 123,
            'type': data.type_visit2,
            'status': 'full',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': origin_url,
            'date': data.date_visit3,
            'visit': 1234,
            'type': data.type_visit2,
            'status': 'full',
            'metadata': None,
            'snapshot': None,
        }
        assert list(swh_storage.journal_writer.journal.objects) == [
            ('origin', data.origin2),
            ('origin_visit', data1),
            ('origin_visit', data2)]

    def test_origin_visit_upsert_existing(self, swh_storage):
        # given
        swh_storage.origin_add_one(data.origin2)
        origin_url = data.origin2['url']

        # when
        origin_visit1 = swh_storage.origin_visit_add(
            origin_url,
            date=data.date_visit2,
            type=data.type_visit1,
        )
        swh_storage.origin_visit_upsert([{
             'origin': origin_url,
             'date': data.date_visit2,
             'visit': origin_visit1['visit'],
             'type': data.type_visit1,
             'status': 'full',
             'metadata': None,
             'snapshot': None,
         }])

        # then
        assert origin_visit1['origin'] == origin_url
        assert origin_visit1['visit'] is not None

        actual_origin_visits = list(swh_storage.origin_visit_get(
            origin_url))
        assert actual_origin_visits == [
            {
                'origin': origin_url,
                'date': data.date_visit2,
                'visit': origin_visit1['visit'],
                'type': data.type_visit1,
                'status': 'full',
                'metadata': None,
                'snapshot': None,
            }]

        data1 = {
            'origin': origin_url,
            'date': data.date_visit2,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': origin_url,
            'date': data.date_visit2,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'full',
            'metadata': None,
            'snapshot': None,
        }
        assert list(swh_storage.journal_writer.journal.objects) == [
            ('origin', data.origin2),
            ('origin_visit', data1),
            ('origin_visit', data2)]

    def test_origin_visit_get_by_no_result(self, swh_storage):
        swh_storage.origin_add([data.origin])
        actual_origin_visit = swh_storage.origin_visit_get_by(
            data.origin['url'], 999)
        assert actual_origin_visit is None

    def test_origin_visit_get_latest(self, swh_storage):
        swh_storage.origin_add_one(data.origin)
        origin_url = data.origin['url']
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit1_id = origin_visit1['visit']
        origin_visit2 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )
        visit2_id = origin_visit2['visit']

        # Add a visit with the same date as the previous one
        origin_visit3 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )
        visit3_id = origin_visit3['visit']

        origin_visit1 = swh_storage.origin_visit_get_by(origin_url, visit1_id)
        origin_visit2 = swh_storage.origin_visit_get_by(origin_url, visit2_id)
        origin_visit3 = swh_storage.origin_visit_get_by(origin_url, visit3_id)

        # Two visits, both with no snapshot
        assert origin_visit3 == swh_storage.origin_visit_get_latest(origin_url)
        assert swh_storage.origin_visit_get_latest(
            origin_url, require_snapshot=True) is None

        # Add snapshot to visit1; require_snapshot=True makes it return
        # visit1 and require_snapshot=False still returns visit2
        swh_storage.snapshot_add([data.complete_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit1_id,
            snapshot=data.complete_snapshot['id'])
        assert {**origin_visit1, 'snapshot': data.complete_snapshot['id']} \
            == swh_storage.origin_visit_get_latest(
                origin_url, require_snapshot=True)

        assert origin_visit3 == swh_storage.origin_visit_get_latest(origin_url)

        # Status filter: all three visits are status=ongoing, so no visit
        # returned
        assert swh_storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full']) is None

        # Mark the first visit as completed and check status filter again
        swh_storage.origin_visit_update(
            origin_url,
            visit1_id, status='full')
        assert {
            **origin_visit1,
            'snapshot': data.complete_snapshot['id'],
            'status': 'full'} == swh_storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full'])

        assert origin_visit3 == swh_storage.origin_visit_get_latest(origin_url)

        # Add snapshot to visit2 and check that the new snapshot is returned
        swh_storage.snapshot_add([data.empty_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit2_id,
            snapshot=data.empty_snapshot['id'])
        assert {**origin_visit2, 'snapshot': data.empty_snapshot['id']} == \
            swh_storage.origin_visit_get_latest(
                origin_url, require_snapshot=True)

        assert origin_visit3 == swh_storage.origin_visit_get_latest(origin_url)

        # Check that the status filter is still working
        assert {
            **origin_visit1,
            'snapshot': data.complete_snapshot['id'],
            'status': 'full'} == swh_storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full'])

        # Add snapshot to visit3 (same date as visit2)
        swh_storage.snapshot_add([data.complete_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit3_id, snapshot=data.complete_snapshot['id'])
        assert {
            **origin_visit1,
            'snapshot': data.complete_snapshot['id'],
            'status': 'full'} == swh_storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full'])
        assert {
            **origin_visit1,
            'snapshot': data.complete_snapshot['id'],
            'status': 'full'} == swh_storage.origin_visit_get_latest(
                origin_url, allowed_statuses=['full'], require_snapshot=True)
        assert {
            **origin_visit3,
            'snapshot': data.complete_snapshot['id']
            } == swh_storage.origin_visit_get_latest(origin_url)

        assert {
            **origin_visit3,
            'snapshot': data.complete_snapshot['id']
            } == swh_storage.origin_visit_get_latest(
                origin_url, require_snapshot=True)

    def test_person_fullname_unicity(self, swh_storage):
        # given (person injection through revisions for example)
        revision = data.revision

        # create a revision with same committer fullname but wo name and email
        revision2 = copy.deepcopy(data.revision2)
        revision2['committer'] = dict(revision['committer'])
        revision2['committer']['email'] = None
        revision2['committer']['name'] = None

        swh_storage.revision_add([revision])
        swh_storage.revision_add([revision2])

        # when getting added revisions
        revisions = list(
            swh_storage.revision_get([revision['id'], revision2['id']]))

        # then
        # check committers are the same
        assert revisions[0]['committer'] == revisions[1]['committer']

    def test_snapshot_add_get_empty(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit_id = origin_visit1['visit']

        actual_result = swh_storage.snapshot_add([data.empty_snapshot])
        assert actual_result == {'snapshot:add': 1}

        swh_storage.origin_visit_update(
            origin_url, visit_id, snapshot=data.empty_snapshot['id'])

        by_id = swh_storage.snapshot_get(data.empty_snapshot['id'])
        assert by_id == {**data.empty_snapshot, 'next_branch': None}

        by_ov = swh_storage.snapshot_get_by_origin_visit(origin_url, visit_id)
        assert by_ov == {**data.empty_snapshot, 'next_branch': None}

        data1 = {
            'origin': origin_url,
            'date': data.date_visit1,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': origin_url,
            'date': data.date_visit1,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': data.empty_snapshot['id'],
        }
        assert list(swh_storage.journal_writer.journal.objects) == \
            [('origin', data.origin),
             ('origin_visit', data1),
             ('snapshot', data.empty_snapshot),
             ('origin_visit', data2)]

    def test_snapshot_add_get_complete(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit_id = origin_visit1['visit']

        actual_result = swh_storage.snapshot_add([data.complete_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit_id, snapshot=data.complete_snapshot['id'])
        assert actual_result == {'snapshot:add': 1}

        by_id = swh_storage.snapshot_get(data.complete_snapshot['id'])
        assert by_id == {**data.complete_snapshot, 'next_branch': None}

        by_ov = swh_storage.snapshot_get_by_origin_visit(origin_url, visit_id)
        assert by_ov == {**data.complete_snapshot, 'next_branch': None}

    def test_snapshot_add_many(self, swh_storage):
        actual_result = swh_storage.snapshot_add(
            [data.snapshot, data.complete_snapshot])
        assert actual_result == {'snapshot:add': 2}

        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get(data.complete_snapshot['id'])

        assert {**data.snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get(data.snapshot['id'])

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['snapshot'] == 2

    def test_snapshot_add_many_from_generator(self, swh_storage):
        def _snp_gen():
            yield data.snapshot
            yield data.complete_snapshot

        actual_result = swh_storage.snapshot_add(_snp_gen())
        assert actual_result == {'snapshot:add': 2}

        swh_storage.refresh_stat_counters()
        assert swh_storage.stat_counters()['snapshot'] == 2

    def test_snapshot_add_many_incremental(self, swh_storage):
        actual_result = swh_storage.snapshot_add([data.complete_snapshot])
        assert actual_result == {'snapshot:add': 1}

        actual_result2 = swh_storage.snapshot_add(
            [data.snapshot, data.complete_snapshot])
        assert actual_result2 == {'snapshot:add': 1}

        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get(data.complete_snapshot['id'])

        assert {**data.snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get(data.snapshot['id'])

    def test_snapshot_add_twice(self, swh_storage):
        actual_result = swh_storage.snapshot_add([data.empty_snapshot])
        assert actual_result == {'snapshot:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('snapshot', data.empty_snapshot)]

        actual_result = swh_storage.snapshot_add([data.snapshot])
        assert actual_result == {'snapshot:add': 1}

        assert list(swh_storage.journal_writer.journal.objects) \
            == [('snapshot', data.empty_snapshot),
                ('snapshot', data.snapshot)]

    def test_snapshot_add_validation(self, swh_storage):
        snap = copy.deepcopy(data.snapshot)
        snap['branches'][b'foo'] = {'target_type': 'revision'}

        with pytest.raises(StorageArgumentException, match='target'):
            swh_storage.snapshot_add([snap])

        snap = copy.deepcopy(data.snapshot)
        snap['branches'][b'foo'] = {'target': b'\x42'*20}

        with pytest.raises(StorageArgumentException, match='target_type'):
            swh_storage.snapshot_add([snap])

    def test_snapshot_add_count_branches(self, swh_storage):
        actual_result = swh_storage.snapshot_add([data.complete_snapshot])
        assert actual_result == {'snapshot:add': 1}

        snp_id = data.complete_snapshot['id']
        snp_size = swh_storage.snapshot_count_branches(snp_id)

        expected_snp_size = {
            'alias': 1,
            'content': 1,
            'directory': 2,
            'release': 1,
            'revision': 1,
            'snapshot': 1,
            None: 1
        }
        assert snp_size == expected_snp_size

    def test_snapshot_add_get_paginated(self, swh_storage):
        swh_storage.snapshot_add([data.complete_snapshot])

        snp_id = data.complete_snapshot['id']
        branches = data.complete_snapshot['branches']
        branch_names = list(sorted(branches))

        # Test branch_from
        snapshot = swh_storage.snapshot_get_branches(
            snp_id, branches_from=b'release')

        rel_idx = branch_names.index(b'release')
        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in branch_names[rel_idx:]
            },
            'next_branch': None,
        }

        assert snapshot == expected_snapshot

        # Test branches_count
        snapshot = swh_storage.snapshot_get_branches(
            snp_id, branches_count=1)

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                branch_names[0]: branches[branch_names[0]],
            },
            'next_branch': b'content',
        }
        assert snapshot == expected_snapshot

        # test branch_from + branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, branches_from=b'directory', branches_count=3)

        dir_idx = branch_names.index(b'directory')
        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in branch_names[dir_idx:dir_idx + 3]
            },
            'next_branch': branch_names[dir_idx + 3],
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get_filtered(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit_id = origin_visit1['visit']

        swh_storage.snapshot_add([data.complete_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit_id, snapshot=data.complete_snapshot['id'])

        snp_id = data.complete_snapshot['id']
        branches = data.complete_snapshot['branches']

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=['release', 'revision'])

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: tgt
                for name, tgt in branches.items()
                if tgt and tgt['target_type'] in ['release', 'revision']
            },
            'next_branch': None,
        }

        assert snapshot == expected_snapshot

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=['alias'])

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: tgt
                for name, tgt in branches.items()
                if tgt and tgt['target_type'] == 'alias'
            },
            'next_branch': None,
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get_filtered_and_paginated(self, swh_storage):
        swh_storage.snapshot_add([data.complete_snapshot])

        snp_id = data.complete_snapshot['id']
        branches = data.complete_snapshot['branches']
        branch_names = list(sorted(branches))

        # Test branch_from

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_from=b'directory2')

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in (b'directory2', b'release')
            },
            'next_branch': None,
        }

        assert snapshot == expected_snapshot

        # Test branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_count=1)

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                b'directory': branches[b'directory']
            },
            'next_branch': b'directory2',
        }
        assert snapshot == expected_snapshot

        # Test branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_count=2)

        expected_snapshot = {
            'id': snp_id,
            'branches': {
                name: branches[name]
                for name in (b'directory', b'directory2')
            },
            'next_branch': b'release',
        }
        assert snapshot == expected_snapshot

        # test branch_from + branches_count

        snapshot = swh_storage.snapshot_get_branches(
            snp_id, target_types=['directory', 'release'],
            branches_from=b'directory2', branches_count=1)

        dir_idx = branch_names.index(b'directory2')
        expected_snapshot = {
            'id': snp_id,
            'branches': {
                branch_names[dir_idx]: branches[branch_names[dir_idx]],
            },
            'next_branch': b'release',
        }

        assert snapshot == expected_snapshot

    def test_snapshot_add_get(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit_id = origin_visit1['visit']

        swh_storage.snapshot_add([data.snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit_id, snapshot=data.snapshot['id'])

        by_id = swh_storage.snapshot_get(data.snapshot['id'])
        assert by_id == {**data.snapshot, 'next_branch': None}

        by_ov = swh_storage.snapshot_get_by_origin_visit(origin_url, visit_id)
        assert by_ov == {**data.snapshot, 'next_branch': None}

        origin_visit_info = swh_storage.origin_visit_get_by(
            origin_url, visit_id)
        assert origin_visit_info['snapshot'] == data.snapshot['id']

    def test_snapshot_add_nonexistent_visit(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        visit_id = 54164461156

        swh_storage.journal_writer.journal.objects[:] = []

        swh_storage.snapshot_add([data.snapshot])

        with pytest.raises(StorageArgumentException):
            swh_storage.origin_visit_update(
                origin_url, visit_id, snapshot=data.snapshot['id'])

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('snapshot', data.snapshot)]

    def test_snapshot_add_twice__by_origin_visit(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit1_id = origin_visit1['visit']
        swh_storage.snapshot_add([data.snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit1_id, snapshot=data.snapshot['id'])

        by_ov1 = swh_storage.snapshot_get_by_origin_visit(
            origin_url, visit1_id)
        assert by_ov1 == {**data.snapshot, 'next_branch': None}

        origin_visit2 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )
        visit2_id = origin_visit2['visit']

        swh_storage.snapshot_add([data.snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit2_id, snapshot=data.snapshot['id'])

        by_ov2 = swh_storage.snapshot_get_by_origin_visit(
            origin_url, visit2_id)
        assert by_ov2 == {**data.snapshot, 'next_branch': None}

        data1 = {
            'origin': origin_url,
            'date': data.date_visit1,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data2 = {
            'origin': origin_url,
            'date': data.date_visit1,
            'visit': origin_visit1['visit'],
            'type': data.type_visit1,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': data.snapshot['id'],
        }
        data3 = {
            'origin': origin_url,
            'date': data.date_visit2,
            'visit': origin_visit2['visit'],
            'type': data.type_visit2,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': None,
        }
        data4 = {
            'origin': origin_url,
            'date': data.date_visit2,
            'visit': origin_visit2['visit'],
            'type': data.type_visit2,
            'status': 'ongoing',
            'metadata': None,
            'snapshot': data.snapshot['id'],
        }
        assert list(swh_storage.journal_writer.journal.objects) \
            == [('origin', data.origin),
                ('origin_visit', data1),
                ('snapshot', data.snapshot),
                ('origin_visit', data2),
                ('origin_visit', data3),
                ('origin_visit', data4)]

    def test_snapshot_get_latest(self, swh_storage):
        origin_url = data.origin['url']
        swh_storage.origin_add_one(data.origin)
        origin_url = data.origin['url']
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit1_id = origin_visit1['visit']
        origin_visit2 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )
        visit2_id = origin_visit2['visit']

        # Add a visit with the same date as the previous one
        origin_visit3 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit2,
            type=data.type_visit3,
        )
        visit3_id = origin_visit3['visit']

        # Two visits, both with no snapshot: latest snapshot is None
        assert swh_storage.snapshot_get_latest(origin_url) is None

        # Add snapshot to visit1, latest snapshot = visit 1 snapshot
        swh_storage.snapshot_add([data.complete_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit1_id, snapshot=data.complete_snapshot['id'])
        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(origin_url)

        # Status filter: all three visits are status=ongoing, so no snapshot
        # returned
        assert swh_storage.snapshot_get_latest(
                origin_url,
                allowed_statuses=['full']) is None

        # Mark the first visit as completed and check status filter again
        swh_storage.origin_visit_update(origin_url, visit1_id, status='full')
        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(
                origin_url,
                allowed_statuses=['full'])

        # Add snapshot to visit2 and check that the new snapshot is returned
        swh_storage.snapshot_add([data.empty_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit2_id, snapshot=data.empty_snapshot['id'])
        assert {**data.empty_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(origin_url)

        # Check that the status filter is still working
        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(
                origin_url,
                allowed_statuses=['full'])

        # Add snapshot to visit3 (same date as visit2) and check that
        # the new snapshot is returned
        swh_storage.snapshot_add([data.complete_snapshot])
        swh_storage.origin_visit_update(
            origin_url, visit3_id, snapshot=data.complete_snapshot['id'])
        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(origin_url)

    def test_snapshot_get_latest__missing_snapshot(self, swh_storage):
        # Origin does not exist
        origin_url = data.origin['url']
        assert swh_storage.snapshot_get_latest(origin_url) is None

        swh_storage.origin_add_one(data.origin)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit1,
            type=data.type_visit1,
        )
        visit1_id = origin_visit1['visit']
        origin_visit2 = swh_storage.origin_visit_add(
            origin=origin_url,
            date=data.date_visit2,
            type=data.type_visit2,
        )
        visit2_id = origin_visit2['visit']

        # Two visits, both with no snapshot: latest snapshot is None
        assert swh_storage.snapshot_get_latest(origin_url) is None

        # Add unknown snapshot to visit1, check that the inconsistency is
        # detected
        swh_storage.origin_visit_update(
            origin_url,
            visit1_id, snapshot=data.complete_snapshot['id'])
        with pytest.raises(Exception):
            # XXX: should the exception be more specific than this?
            swh_storage.snapshot_get_latest(origin_url)

        # Status filter: both visits are status=ongoing, so no snapshot
        # returned
        assert swh_storage.snapshot_get_latest(
            origin_url,
            allowed_statuses=['full']) is None

        # Mark the first visit as completed and check status filter again
        swh_storage.origin_visit_update(
            origin_url,
            visit1_id, status='full')
        with pytest.raises(Exception):
            # XXX: should the exception be more specific than this?
            swh_storage.snapshot_get_latest(
                origin_url,
                allowed_statuses=['full']),

        # Actually add the snapshot and check status filter again
        swh_storage.snapshot_add([data.complete_snapshot])
        assert {**data.complete_snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(origin_url)

        # Add unknown snapshot to visit2 and check that the inconsistency
        # is detected
        swh_storage.origin_visit_update(
            origin_url,
            visit2_id, snapshot=data.snapshot['id'])
        with pytest.raises(Exception):
            # XXX: should the exception be more specific than this?
            swh_storage.snapshot_get_latest(
                origin_url)

        # Actually add that snapshot and check that the new one is returned
        swh_storage.snapshot_add([data.snapshot])
        assert{**data.snapshot, 'next_branch': None} \
            == swh_storage.snapshot_get_latest(origin_url)

    def test_snapshot_get_random(self, swh_storage):
        swh_storage.snapshot_add(
            [data.snapshot, data.empty_snapshot, data.complete_snapshot])

        assert swh_storage.snapshot_get_random() in {
            data.snapshot['id'], data.empty_snapshot['id'],
            data.complete_snapshot['id']}

    def test_snapshot_missing(self, swh_storage):
        snap = data.snapshot
        missing_snap = data.empty_snapshot
        snapshots = [snap['id'], missing_snap['id']]
        swh_storage.snapshot_add([snap])

        missing_snapshots = swh_storage.snapshot_missing(snapshots)

        assert list(missing_snapshots) == [missing_snap['id']]

    def test_stat_counters(self, swh_storage):
        expected_keys = ['content', 'directory',
                         'origin', 'revision']

        # Initially, all counters are 0

        swh_storage.refresh_stat_counters()
        counters = swh_storage.stat_counters()
        assert set(expected_keys) <= set(counters)
        for key in expected_keys:
            assert counters[key] == 0

        # Add a content. Only the content counter should increase.

        swh_storage.content_add([data.cont])

        swh_storage.refresh_stat_counters()
        counters = swh_storage.stat_counters()

        assert set(expected_keys) <= set(counters)
        for key in expected_keys:
            if key != 'content':
                assert counters[key] == 0
        assert counters['content'] == 1

        # Add other objects. Check their counter increased as well.

        swh_storage.origin_add_one(data.origin2)
        origin_visit1 = swh_storage.origin_visit_add(
            origin=data.origin2['url'],
            date=data.date_visit2,
            type=data.type_visit2,
        )
        swh_storage.snapshot_add([data.snapshot])
        swh_storage.origin_visit_update(
            data.origin2['url'], origin_visit1['visit'],
            snapshot=data.snapshot['id'])
        swh_storage.directory_add([data.dir])
        swh_storage.revision_add([data.revision])
        swh_storage.release_add([data.release])

        swh_storage.refresh_stat_counters()
        counters = swh_storage.stat_counters()
        assert counters['content'] == 1
        assert counters['directory'] == 1
        assert counters['snapshot'] == 1
        assert counters['origin'] == 1
        assert counters['origin_visit'] == 1
        assert counters['revision'] == 1
        assert counters['release'] == 1
        assert counters['snapshot'] == 1
        if 'person' in counters:
            assert counters['person'] == 3

    def test_content_find_ctime(self, swh_storage):
        cont = data.cont.copy()
        del cont['data']
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        cont['ctime'] = now
        swh_storage.content_add_metadata([cont])

        actually_present = swh_storage.content_find({'sha1': cont['sha1']})

        # check ctime up to one second
        dt = actually_present[0]['ctime'] - now
        assert abs(dt.total_seconds()) <= 1
        del actually_present[0]['ctime']

        assert actually_present[0] == {
            'sha1': cont['sha1'],
            'sha256': cont['sha256'],
            'sha1_git': cont['sha1_git'],
            'blake2s256': cont['blake2s256'],
            'length': cont['length'],
            'status': 'visible'
        }

    def test_content_find_with_present_content(self, swh_storage):
        # 1. with something to find
        cont = data.cont
        swh_storage.content_add([cont, data.cont2])

        actually_present = swh_storage.content_find(
            {'sha1': cont['sha1']}
            )
        assert 1 == len(actually_present)
        actually_present[0].pop('ctime')

        assert actually_present[0] == {
                'sha1': cont['sha1'],
                'sha256': cont['sha256'],
                'sha1_git': cont['sha1_git'],
                'blake2s256': cont['blake2s256'],
                'length': cont['length'],
                'status': 'visible'
            }

        # 2. with something to find
        actually_present = swh_storage.content_find(
            {'sha1_git': cont['sha1_git']})
        assert 1 == len(actually_present)

        actually_present[0].pop('ctime')
        assert actually_present[0] == {
                'sha1': cont['sha1'],
                'sha256': cont['sha256'],
                'sha1_git': cont['sha1_git'],
                'blake2s256': cont['blake2s256'],
                'length': cont['length'],
                'status': 'visible'
            }

        # 3. with something to find
        actually_present = swh_storage.content_find(
            {'sha256': cont['sha256']})
        assert 1 == len(actually_present)

        actually_present[0].pop('ctime')
        assert actually_present[0] == {
                'sha1': cont['sha1'],
                'sha256': cont['sha256'],
                'sha1_git': cont['sha1_git'],
                'blake2s256': cont['blake2s256'],
                'length': cont['length'],
                'status': 'visible'
            }

        # 4. with something to find
        actually_present = swh_storage.content_find({
            'sha1': cont['sha1'],
            'sha1_git': cont['sha1_git'],
            'sha256': cont['sha256'],
            'blake2s256': cont['blake2s256'],
        })
        assert 1 == len(actually_present)

        actually_present[0].pop('ctime')
        assert actually_present[0] == {
                'sha1': cont['sha1'],
                'sha256': cont['sha256'],
                'sha1_git': cont['sha1_git'],
                'blake2s256': cont['blake2s256'],
                'length': cont['length'],
                'status': 'visible'
            }

    def test_content_find_with_non_present_content(self, swh_storage):
        # 1. with something that does not exist
        missing_cont = data.missing_cont

        actually_present = swh_storage.content_find(
            {'sha1': missing_cont['sha1']})

        assert actually_present == []

        # 2. with something that does not exist
        actually_present = swh_storage.content_find(
            {'sha1_git': missing_cont['sha1_git']})

        assert actually_present == []

        # 3. with something that does not exist
        actually_present = swh_storage.content_find(
            {'sha256': missing_cont['sha256']})

        assert actually_present == []

    def test_content_find_with_duplicate_input(self, swh_storage):
        cont1 = data.cont
        duplicate_cont = cont1.copy()

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(duplicate_cont['sha1'])
        sha1_array[0] += 1
        duplicate_cont['sha1'] = bytes(sha1_array)
        sha1git_array = bytearray(duplicate_cont['sha1_git'])
        sha1git_array[0] += 1
        duplicate_cont['sha1_git'] = bytes(sha1git_array)
        # Inject the data
        swh_storage.content_add([cont1, duplicate_cont])
        finder = {'blake2s256': duplicate_cont['blake2s256'],
                  'sha256': duplicate_cont['sha256']}
        actual_result = list(swh_storage.content_find(finder))

        cont1.pop('data')
        duplicate_cont.pop('data')
        actual_result[0].pop('ctime')
        actual_result[1].pop('ctime')

        expected_result = [
           cont1, duplicate_cont
        ]
        for result in expected_result:
            assert result in actual_result

    def test_content_find_with_duplicate_sha256(self, swh_storage):
        cont1 = data.cont
        duplicate_cont = cont1.copy()

        # Create fake data with colliding sha256
        for hashalgo in ('sha1', 'sha1_git', 'blake2s256'):
            value = bytearray(duplicate_cont[hashalgo])
            value[0] += 1
            duplicate_cont[hashalgo] = bytes(value)
        swh_storage.content_add([cont1, duplicate_cont])

        finder = {
            'sha256': duplicate_cont['sha256']
            }
        actual_result = list(swh_storage.content_find(finder))
        assert len(actual_result) == 2

        cont1.pop('data')
        duplicate_cont.pop('data')
        actual_result[0].pop('ctime')
        actual_result[1].pop('ctime')
        expected_result = [
            cont1, duplicate_cont
        ]
        assert expected_result == sorted(actual_result,
                                         key=lambda x: x['sha1'])

        # Find with both sha256 and blake2s256
        finder = {
            'sha256': duplicate_cont['sha256'],
            'blake2s256': duplicate_cont['blake2s256']
        }
        actual_result = list(swh_storage.content_find(finder))
        assert len(actual_result) == 1
        actual_result[0].pop('ctime')

        expected_result = [duplicate_cont]
        assert actual_result[0] == duplicate_cont

    def test_content_find_with_duplicate_blake2s256(self, swh_storage):
        cont1 = data.cont
        duplicate_cont = cont1.copy()

        # Create fake data with colliding sha256 and blake2s256
        sha1_array = bytearray(duplicate_cont['sha1'])
        sha1_array[0] += 1
        duplicate_cont['sha1'] = bytes(sha1_array)
        sha1git_array = bytearray(duplicate_cont['sha1_git'])
        sha1git_array[0] += 1
        duplicate_cont['sha1_git'] = bytes(sha1git_array)
        sha256_array = bytearray(duplicate_cont['sha256'])
        sha256_array[0] += 1
        duplicate_cont['sha256'] = bytes(sha256_array)
        swh_storage.content_add([cont1, duplicate_cont])
        finder = {
            'blake2s256': duplicate_cont['blake2s256']
            }
        actual_result = list(swh_storage.content_find(finder))

        cont1.pop('data')
        duplicate_cont.pop('data')
        actual_result[0].pop('ctime')
        actual_result[1].pop('ctime')
        expected_result = [
            cont1, duplicate_cont
        ]
        for result in expected_result:
            assert result in actual_result

        # Find with both sha256 and blake2s256
        finder = {
            'sha256': duplicate_cont['sha256'],
            'blake2s256': duplicate_cont['blake2s256']
        }
        actual_result = list(swh_storage.content_find(finder))

        actual_result[0].pop('ctime')

        expected_result = [
            duplicate_cont
        ]
        assert expected_result == actual_result

    def test_content_find_bad_input(self, swh_storage):
        # 1. with bad input
        with pytest.raises(StorageArgumentException):
            swh_storage.content_find({})  # empty is bad

        # 2. with bad input
        with pytest.raises(StorageArgumentException):
            swh_storage.content_find(
                {'unknown-sha1': 'something'})  # not the right key

    def test_object_find_by_sha1_git(self, swh_storage):
        sha1_gits = [b'00000000000000000000']
        expected = {
            b'00000000000000000000': [],
        }

        swh_storage.content_add([data.cont])
        sha1_gits.append(data.cont['sha1_git'])
        expected[data.cont['sha1_git']] = [{
            'sha1_git': data.cont['sha1_git'],
            'type': 'content',
        }]

        swh_storage.directory_add([data.dir])
        sha1_gits.append(data.dir['id'])
        expected[data.dir['id']] = [{
            'sha1_git': data.dir['id'],
            'type': 'directory',
        }]

        swh_storage.revision_add([data.revision])
        sha1_gits.append(data.revision['id'])
        expected[data.revision['id']] = [{
            'sha1_git': data.revision['id'],
            'type': 'revision',
        }]

        swh_storage.release_add([data.release])
        sha1_gits.append(data.release['id'])
        expected[data.release['id']] = [{
            'sha1_git': data.release['id'],
            'type': 'release',
        }]

        ret = swh_storage.object_find_by_sha1_git(sha1_gits)

        assert expected == ret

    def test_tool_add(self, swh_storage):
        tool = {
            'name': 'some-unknown-tool',
            'version': 'some-version',
            'configuration': {"debian-package": "some-package"},
        }

        actual_tool = swh_storage.tool_get(tool)
        assert actual_tool is None  # does not exist

        # add it
        actual_tools = swh_storage.tool_add([tool])

        assert len(actual_tools) == 1
        actual_tool = actual_tools[0]
        assert actual_tool is not None  # now it exists
        new_id = actual_tool.pop('id')
        assert actual_tool == tool

        actual_tools2 = swh_storage.tool_add([tool])
        actual_tool2 = actual_tools2[0]
        assert actual_tool2 is not None  # now it exists
        new_id2 = actual_tool2.pop('id')

        assert new_id == new_id2
        assert actual_tool == actual_tool2

    def test_tool_add_multiple(self, swh_storage):
        tool = {
            'name': 'some-unknown-tool',
            'version': 'some-version',
            'configuration': {"debian-package": "some-package"},
        }

        actual_tools = list(swh_storage.tool_add([tool]))
        assert len(actual_tools) == 1

        new_tools = [tool, {
            'name': 'yet-another-tool',
            'version': 'version',
            'configuration': {},
        }]

        actual_tools = swh_storage.tool_add(new_tools)
        assert len(actual_tools) == 2

        # order not guaranteed, so we iterate over results to check
        for tool in actual_tools:
            _id = tool.pop('id')
            assert _id is not None
            assert tool in new_tools

    def test_tool_get_missing(self, swh_storage):
        tool = {
            'name': 'unknown-tool',
            'version': '3.1.0rc2-31-ga2cbb8c',
            'configuration': {"command_line": "nomossa <filepath>"},
        }

        actual_tool = swh_storage.tool_get(tool)

        assert actual_tool is None

    def test_tool_metadata_get_missing_context(self, swh_storage):
        tool = {
            'name': 'swh-metadata-translator',
            'version': '0.0.1',
            'configuration': {"context": "unknown-context"},
        }

        actual_tool = swh_storage.tool_get(tool)

        assert actual_tool is None

    def test_tool_metadata_get(self, swh_storage):
        tool = {
            'name': 'swh-metadata-translator',
            'version': '0.0.1',
            'configuration': {"type": "local", "context": "npm"},
        }
        expected_tool = swh_storage.tool_add([tool])[0]

        # when
        actual_tool = swh_storage.tool_get(tool)

        # then
        assert expected_tool == actual_tool

    def test_metadata_provider_get(self, swh_storage):
        # given
        no_provider = swh_storage.metadata_provider_get(6459456445615)
        assert no_provider is None
        # when
        provider_id = swh_storage.metadata_provider_add(
            data.provider['name'],
            data.provider['type'],
            data.provider['url'],
            data.provider['metadata'])

        actual_provider = swh_storage.metadata_provider_get(provider_id)
        expected_provider = {
            'provider_name': data.provider['name'],
            'provider_url': data.provider['url']
        }
        # then
        del actual_provider['id']
        assert actual_provider, expected_provider

    def test_metadata_provider_get_by(self, swh_storage):
        # given
        no_provider = swh_storage.metadata_provider_get_by({
            'provider_name': data.provider['name'],
            'provider_url': data.provider['url']
        })
        assert no_provider is None
        # when
        provider_id = swh_storage.metadata_provider_add(
            data.provider['name'],
            data.provider['type'],
            data.provider['url'],
            data.provider['metadata'])

        actual_provider = swh_storage.metadata_provider_get_by({
            'provider_name': data.provider['name'],
            'provider_url': data.provider['url']
        })
        # then
        assert provider_id, actual_provider['id']

    def test_origin_metadata_add(self, swh_storage):
        # given
        origin = data.origin
        swh_storage.origin_add([origin])[0]

        tools = swh_storage.tool_add([data.metadata_tool])
        tool = tools[0]

        swh_storage.metadata_provider_add(
                           data.provider['name'],
                           data.provider['type'],
                           data.provider['url'],
                           data.provider['metadata'])
        provider = swh_storage.metadata_provider_get_by({
                            'provider_name': data.provider['name'],
                            'provider_url': data.provider['url']
                      })

        # when adding for the same origin 2 metadatas
        n_om = len(list(swh_storage.origin_metadata_get_by(origin['url'])))
        swh_storage.origin_metadata_add(
                    origin['url'],
                    data.origin_metadata['discovery_date'],
                    provider['id'],
                    tool['id'],
                    data.origin_metadata['metadata'])
        swh_storage.origin_metadata_add(
                    origin['url'],
                    '2015-01-01 23:00:00+00',
                    provider['id'],
                    tool['id'],
                    data.origin_metadata2['metadata'])
        n_actual_om = len(list(
            swh_storage.origin_metadata_get_by(origin['url'])))
        # then
        assert n_actual_om == n_om + 2

    def test_origin_metadata_get(self, swh_storage):
        # given
        origin_url = data.origin['url']
        origin_url2 = data.origin2['url']
        swh_storage.origin_add([data.origin])
        swh_storage.origin_add([data.origin2])

        swh_storage.metadata_provider_add(data.provider['name'],
                                          data.provider['type'],
                                          data.provider['url'],
                                          data.provider['metadata'])
        provider = swh_storage.metadata_provider_get_by({
            'provider_name': data.provider['name'],
            'provider_url': data.provider['url']
        })
        tool = swh_storage.tool_add([data.metadata_tool])[0]
        # when adding for the same origin 2 metadatas
        swh_storage.origin_metadata_add(
                    origin_url,
                    data.origin_metadata['discovery_date'],
                    provider['id'],
                    tool['id'],
                    data.origin_metadata['metadata'])
        swh_storage.origin_metadata_add(
                    origin_url2,
                    data.origin_metadata2['discovery_date'],
                    provider['id'],
                    tool['id'],
                    data.origin_metadata2['metadata'])
        swh_storage.origin_metadata_add(
                    origin_url,
                    data.origin_metadata2['discovery_date'],
                    provider['id'],
                    tool['id'],
                    data.origin_metadata2['metadata'])
        all_metadatas = list(sorted(swh_storage.origin_metadata_get_by(
            origin_url), key=lambda x: x['discovery_date']))
        metadatas_for_origin2 = list(swh_storage.origin_metadata_get_by(
            origin_url2))
        expected_results = [{
            'origin_url': origin_url,
            'discovery_date': datetime.datetime(
                                2015, 1, 1, 23, 0,
                                tzinfo=datetime.timezone.utc),
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            },
            'provider_id': provider['id'],
            'provider_name': 'hal',
            'provider_type': 'deposit-client',
            'provider_url': 'http:///hal/inria',
            'tool_id': tool['id']
        }, {
            'origin_url': origin_url,
            'discovery_date': datetime.datetime(
                                2017, 1, 1, 23, 0,
                                tzinfo=datetime.timezone.utc),
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            },
            'provider_id': provider['id'],
            'provider_name': 'hal',
            'provider_type': 'deposit-client',
            'provider_url': 'http:///hal/inria',
            'tool_id': tool['id']
        }]

        # then
        assert len(all_metadatas) == 2
        assert len(metadatas_for_origin2) == 1
        assert all_metadatas == expected_results

    def test_metadata_provider_add(self, swh_storage):
        provider = {
            'provider_name': 'swMATH',
            'provider_type': 'registry',
            'provider_url': 'http://www.swmath.org/',
            'metadata': {
                'email': 'contact@swmath.org',
                'license': 'All rights reserved'
            }
        }
        provider['id'] = provider_id = swh_storage.metadata_provider_add(
            **provider)
        assert provider == swh_storage.metadata_provider_get_by(
            {'provider_name': 'swMATH',
             'provider_url': 'http://www.swmath.org/'})
        assert provider == swh_storage.metadata_provider_get(provider_id)

    def test_origin_metadata_get_by_provider_type(self, swh_storage):
        # given
        origin_url = data.origin['url']
        origin_url2 = data.origin2['url']
        swh_storage.origin_add([data.origin])
        swh_storage.origin_add([data.origin2])
        provider1_id = swh_storage.metadata_provider_add(
                           data.provider['name'],
                           data.provider['type'],
                           data.provider['url'],
                           data.provider['metadata'])
        provider1 = swh_storage.metadata_provider_get_by({
                            'provider_name': data.provider['name'],
                            'provider_url': data.provider['url']
                   })
        assert provider1 == swh_storage.metadata_provider_get(provider1_id)

        provider2_id = swh_storage.metadata_provider_add(
                            'swMATH',
                            'registry',
                            'http://www.swmath.org/',
                            {'email': 'contact@swmath.org',
                             'license': 'All rights reserved'})
        provider2 = swh_storage.metadata_provider_get_by({
                            'provider_name': 'swMATH',
                            'provider_url': 'http://www.swmath.org/'
                   })
        assert provider2 == swh_storage.metadata_provider_get(provider2_id)

        # using the only tool now inserted in the data.sql, but for this
        # provider should be a crawler tool (not yet implemented)
        tool = swh_storage.tool_add([data.metadata_tool])[0]

        # when adding for the same origin 2 metadatas
        swh_storage.origin_metadata_add(
            origin_url,
            data.origin_metadata['discovery_date'],
            provider1['id'],
            tool['id'],
            data.origin_metadata['metadata'])
        swh_storage.origin_metadata_add(
            origin_url2,
            data.origin_metadata2['discovery_date'],
            provider2['id'],
            tool['id'],
            data.origin_metadata2['metadata'])
        provider_type = 'registry'
        m_by_provider = list(swh_storage.origin_metadata_get_by(
            origin_url2,
            provider_type))
        for item in m_by_provider:
            if 'id' in item:
                del item['id']
        expected_results = [{
            'origin_url': origin_url2,
            'discovery_date': datetime.datetime(
                                2017, 1, 1, 23, 0,
                                tzinfo=datetime.timezone.utc),
            'metadata': {
                'name': 'test_origin_metadata',
                'version': '0.0.1'
            },
            'provider_id': provider2['id'],
            'provider_name': 'swMATH',
            'provider_type': provider_type,
            'provider_url': 'http://www.swmath.org/',
            'tool_id': tool['id']
        }]
        # then

        assert len(m_by_provider) == 1
        assert m_by_provider == expected_results


class TestStorageGeneratedData:
    def test_generate_content_get(self, swh_storage, swh_contents):
        contents_with_data = [c for c in swh_contents
                              if c['status'] != 'absent']
        # input the list of sha1s we want from storage
        get_sha1s = [c['sha1'] for c in contents_with_data]

        # retrieve contents
        actual_contents = list(swh_storage.content_get(get_sha1s))
        assert None not in actual_contents
        assert_contents_ok(contents_with_data, actual_contents)

    def test_generate_content_get_metadata(self, swh_storage, swh_contents):
        # input the list of sha1s we want from storage
        expected_contents = [c for c in swh_contents
                             if c['status'] != 'absent']
        get_sha1s = [c['sha1'] for c in expected_contents]

        # retrieve contents
        meta_contents = swh_storage.content_get_metadata(get_sha1s)

        assert len(list(meta_contents)) == len(get_sha1s)

        actual_contents = []
        for contents in meta_contents.values():
            actual_contents.extend(contents)

        keys_to_check = {'length', 'status',
                         'sha1', 'sha1_git', 'sha256', 'blake2s256'}

        assert_contents_ok(expected_contents, actual_contents,
                           keys_to_check=keys_to_check)

    def test_generate_content_get_range(self, swh_storage, swh_contents):
        """content_get_range returns complete range"""
        present_contents = [c for c in swh_contents
                            if c['status'] != 'absent']

        get_sha1s = sorted([c['sha1'] for c in swh_contents
                            if c['status'] != 'absent'])
        start = get_sha1s[2]
        end = get_sha1s[-2]
        actual_result = swh_storage.content_get_range(start, end)

        assert actual_result['next'] is None

        actual_contents = actual_result['contents']
        expected_contents = [c for c in present_contents
                             if start <= c['sha1'] <= end]
        if expected_contents:
            assert_contents_ok(
                expected_contents, actual_contents, ['sha1'])
        else:
            assert actual_contents == []

    def test_generate_content_get_range_full(self, swh_storage, swh_contents):
        """content_get_range for a full range returns all available contents"""
        present_contents = [c for c in swh_contents
                            if c['status'] != 'absent']

        start = b'0' * 40
        end = b'f' * 40
        actual_result = swh_storage.content_get_range(start, end)
        assert actual_result['next'] is None

        actual_contents = actual_result['contents']
        expected_contents = [c for c in present_contents
                             if start <= c['sha1'] <= end]
        if expected_contents:
            assert_contents_ok(
                expected_contents, actual_contents, ['sha1'])
        else:
            assert actual_contents == []

    def test_generate_content_get_range_empty(self, swh_storage, swh_contents):
        """content_get_range for an empty range returns nothing"""
        start = b'0' * 40
        end = b'f' * 40
        actual_result = swh_storage.content_get_range(end, start)
        assert actual_result['next'] is None
        assert len(actual_result['contents']) == 0

    def test_generate_content_get_range_limit_none(self, swh_storage):
        """content_get_range call with wrong limit input should fail"""
        with pytest.raises(StorageArgumentException) as e:
            swh_storage.content_get_range(start=None, end=None, limit=None)

        assert e.value.args == ('limit should not be None',)

    def test_generate_content_get_range_no_limit(
            self, swh_storage, swh_contents):
        """content_get_range returns contents within range provided"""
        # input the list of sha1s we want from storage
        get_sha1s = sorted([c['sha1'] for c in swh_contents
                            if c['status'] != 'absent'])
        start = get_sha1s[0]
        end = get_sha1s[-1]

        # retrieve contents
        actual_result = swh_storage.content_get_range(start, end)

        actual_contents = actual_result['contents']
        assert actual_result['next'] is None
        assert len(actual_contents) == len(get_sha1s)

        expected_contents = [c for c in swh_contents
                             if c['status'] != 'absent']
        assert_contents_ok(
            expected_contents, actual_contents, ['sha1'])

    def test_generate_content_get_range_limit(self, swh_storage, swh_contents):
        """content_get_range paginates results if limit exceeded"""
        contents_map = {c['sha1']: c for c in swh_contents}

        # input the list of sha1s we want from storage
        get_sha1s = sorted([c['sha1'] for c in swh_contents
                            if c['status'] != 'absent'])
        start = get_sha1s[0]
        end = get_sha1s[-1]

        # retrieve contents limited to n-1 results
        limited_results = len(get_sha1s) - 1
        actual_result = swh_storage.content_get_range(
            start, end, limit=limited_results)

        actual_contents = actual_result['contents']
        assert actual_result['next'] == get_sha1s[-1]
        assert len(actual_contents) == limited_results

        expected_contents = [contents_map[sha1] for sha1 in get_sha1s[:-1]]
        assert_contents_ok(
            expected_contents, actual_contents, ['sha1'])

        # retrieve next part
        actual_results2 = swh_storage.content_get_range(start=end, end=end)
        assert actual_results2['next'] is None
        actual_contents2 = actual_results2['contents']
        assert len(actual_contents2) == 1

        assert_contents_ok(
            [contents_map[get_sha1s[-1]]], actual_contents2, ['sha1'])

    def test_origin_get_range_from_zero(self, swh_storage, swh_origins):
        actual_origins = list(
            swh_storage.origin_get_range(origin_from=0,
                                         origin_count=0))
        assert len(actual_origins) == 0

        actual_origins = list(
            swh_storage.origin_get_range(origin_from=0,
                                         origin_count=1))
        assert len(actual_origins) == 1
        assert actual_origins[0]['id'] == 1
        assert actual_origins[0]['url'] == swh_origins[0]['url']

    @pytest.mark.parametrize('origin_from,origin_count', [
        (1, 1), (1, 10), (1, 20), (1, 101), (11, 0),
        (11, 10), (91, 11)])
    def test_origin_get_range(
            self, swh_storage, swh_origins, origin_from, origin_count):
        actual_origins = list(
            swh_storage.origin_get_range(origin_from=origin_from,
                                         origin_count=origin_count))

        origins_with_id = list(enumerate(swh_origins, start=1))
        expected_origins = [
            {
                'url': origin['url'],
                'id': origin_id,
            }
            for (origin_id, origin)
            in origins_with_id[origin_from-1:origin_from+origin_count-1]
        ]

        assert actual_origins == expected_origins

    @pytest.mark.parametrize('limit', [1, 7, 10, 100, 1000])
    def test_origin_list(self, swh_storage, swh_origins, limit):
        returned_origins = []

        page_token = None
        i = 0
        while True:
            result = swh_storage.origin_list(
                page_token=page_token, limit=limit)
            assert len(result['origins']) <= limit

            returned_origins.extend(
                origin['url'] for origin in result['origins'])

            i += 1
            page_token = result.get('next_page_token')

            if page_token is None:
                assert i*limit >= len(swh_origins)
                break
            else:
                assert len(result['origins']) == limit

        expected_origins = [origin['url'] for origin in swh_origins]
        assert sorted(returned_origins) == sorted(expected_origins)

    ORIGINS = [
        'https://github.com/user1/repo1',
        'https://github.com/user2/repo1',
        'https://github.com/user3/repo1',
        'https://gitlab.com/user1/repo1',
        'https://gitlab.com/user2/repo1',
        'https://forge.softwareheritage.org/source/repo1',
    ]

    def test_origin_count(self, swh_storage):
        swh_storage.origin_add([{'url': url} for url in self.ORIGINS])

        assert swh_storage.origin_count('github') == 3
        assert swh_storage.origin_count('gitlab') == 2
        assert swh_storage.origin_count('.*user.*', regexp=True) == 5
        assert swh_storage.origin_count('.*user.*', regexp=False) == 0
        assert swh_storage.origin_count('.*user1.*', regexp=True) == 2
        assert swh_storage.origin_count('.*user1.*', regexp=False) == 0

    def test_origin_count_with_visit_no_visits(self, swh_storage):
        swh_storage.origin_add([{'url': url} for url in self.ORIGINS])

        # none of them have visits, so with_visit=True => 0
        assert swh_storage.origin_count('github', with_visit=True) == 0
        assert swh_storage.origin_count('gitlab', with_visit=True) == 0
        assert swh_storage.origin_count('.*user.*', regexp=True,
                                        with_visit=True) == 0
        assert swh_storage.origin_count('.*user.*', regexp=False,
                                        with_visit=True) == 0
        assert swh_storage.origin_count('.*user1.*', regexp=True,
                                        with_visit=True) == 0
        assert swh_storage.origin_count('.*user1.*', regexp=False,
                                        with_visit=True) == 0

    def test_origin_count_with_visit_with_visits_no_snapshot(
            self, swh_storage):
        swh_storage.origin_add([{'url': url} for url in self.ORIGINS])
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        swh_storage.origin_visit_add(
            origin='https://github.com/user1/repo1', date=now, type='git')

        assert swh_storage.origin_count('github', with_visit=False) == 3
        # it has a visit, but no snapshot, so with_visit=True => 0
        assert swh_storage.origin_count('github', with_visit=True) == 0

        assert swh_storage.origin_count('gitlab', with_visit=False) == 2
        # these gitlab origins have no visit
        assert swh_storage.origin_count('gitlab', with_visit=True) == 0

        assert swh_storage.origin_count('github.*user1', regexp=True,
                                        with_visit=False) == 1
        assert swh_storage.origin_count('github.*user1', regexp=True,
                                        with_visit=True) == 0
        assert swh_storage.origin_count('github', regexp=True,
                                        with_visit=True) == 0

    def test_origin_count_with_visit_with_visits_and_snapshot(
            self, swh_storage):
        swh_storage.origin_add([{'url': url} for url in self.ORIGINS])
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        swh_storage.snapshot_add([data.snapshot])
        visit = swh_storage.origin_visit_add(
            origin='https://github.com/user1/repo1', date=now, type='git')
        swh_storage.origin_visit_update(
            origin='https://github.com/user1/repo1', visit_id=visit['visit'],
            snapshot=data.snapshot['id'])

        assert swh_storage.origin_count('github', with_visit=False) == 3
        # github/user1 has a visit and a snapshot, so with_visit=True => 1
        assert swh_storage.origin_count('github', with_visit=True) == 1

        assert swh_storage.origin_count('github.*user1', regexp=True,
                                        with_visit=False) == 1
        assert swh_storage.origin_count('github.*user1', regexp=True,
                                        with_visit=True) == 1
        assert swh_storage.origin_count('github', regexp=True,
                                        with_visit=True) == 1

    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(strategies.lists(objects(), max_size=2))
    def test_add_arbitrary(self, swh_storage, objects):
        for (obj_type, obj) in objects:
            obj = obj.to_dict()
            if obj_type == 'origin_visit':
                origin = obj.pop('origin')
                swh_storage.origin_add_one({'url': origin})
                if 'visit' in obj:
                    del obj['visit']
                swh_storage.origin_visit_add(
                    origin, obj['date'], obj['type'])
            else:
                if obj_type == 'content' and obj['status'] == 'absent':
                    obj_type = 'skipped_content'
                method = getattr(swh_storage, obj_type + '_add')
                try:
                    method([obj])
                except HashCollision:
                    pass


@pytest.mark.db
class TestLocalStorage:
    """Test the local storage"""
    # This test is only relevant on the local storage, with an actual
    # objstorage raising an exception
    def test_content_add_objstorage_exception(self, swh_storage):
        swh_storage.objstorage.content_add = Mock(
            side_effect=Exception('mocked broken objstorage')
        )

        with pytest.raises(Exception) as e:
            swh_storage.content_add([data.cont])

        assert e.value.args == ('mocked broken objstorage',)
        missing = list(swh_storage.content_missing([data.cont]))
        assert missing == [data.cont['sha1']]


@pytest.mark.db
class TestStorageRaceConditions:
    @pytest.mark.xfail
    def test_content_add_race(self, swh_storage):

        results = queue.Queue()

        def thread():
            try:
                with db_transaction(swh_storage) as (db, cur):
                    ret = swh_storage.content_add([data.cont], db=db,
                                                  cur=cur)
                results.put((threading.get_ident(), 'data', ret))
            except Exception as e:
                results.put((threading.get_ident(), 'exc', e))

        t1 = threading.Thread(target=thread)
        t2 = threading.Thread(target=thread)
        t1.start()
        # this avoids the race condition
        # import time
        # time.sleep(1)
        t2.start()
        t1.join()
        t2.join()

        r1 = results.get(block=False)
        r2 = results.get(block=False)

        with pytest.raises(queue.Empty):
            results.get(block=False)
        assert r1[0] != r2[0]
        assert r1[1] == 'data', 'Got exception %r in Thread%s' % (r1[2], r1[0])
        assert r2[1] == 'data', 'Got exception %r in Thread%s' % (r2[2], r2[0])


@pytest.mark.db
class TestPgStorage:
    """This class is dedicated for the rare case where the schema needs to
       be altered dynamically.

       Otherwise, the tests could be blocking when ran altogether.

    """

    def test_content_update_with_new_cols(self, swh_storage):
        swh_storage.journal_writer.journal = None  # TODO, not supported

        with db_transaction(swh_storage) as (_, cur):
            cur.execute("""alter table content
                           add column test text default null,
                           add column test2 text default null""")

        cont = copy.deepcopy(data.cont2)
        swh_storage.content_add([cont])
        cont['test'] = 'value-1'
        cont['test2'] = 'value-2'

        swh_storage.content_update([cont], keys=['test', 'test2'])
        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                '''SELECT sha1, sha1_git, sha256, length, status,
                   test, test2
                   FROM content WHERE sha1 = %s''',
                (cont['sha1'],))

            datum = cur.fetchone()

        assert datum == (cont['sha1'], cont['sha1_git'], cont['sha256'],
                         cont['length'], 'visible',
                         cont['test'], cont['test2'])

        with db_transaction(swh_storage) as (_, cur):
            cur.execute("""alter table content drop column test,
                                               drop column test2""")

    def test_content_add_db(self, swh_storage):
        cont = data.cont

        actual_result = swh_storage.content_add([cont])

        assert actual_result == {
            'content:add': 1,
            'content:add:bytes': cont['length'],
            }

        if hasattr(swh_storage, 'objstorage'):
            assert cont['sha1'] in swh_storage.objstorage.objstorage

        with db_transaction(swh_storage) as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, length, status'
                        ' FROM content WHERE sha1 = %s',
                        (cont['sha1'],))
            datum = cur.fetchone()

        assert datum == (cont['sha1'], cont['sha1_git'], cont['sha256'],
                         cont['length'], 'visible')

        expected_cont = cont.copy()
        del expected_cont['data']
        journal_objects = list(swh_storage.journal_writer.journal.objects)
        for (obj_type, obj) in journal_objects:
            del obj['ctime']
        assert journal_objects == [('content', expected_cont)]

    def test_content_add_metadata_db(self, swh_storage):
        cont = data.cont
        del cont['data']
        cont['ctime'] = datetime.datetime.now()

        actual_result = swh_storage.content_add_metadata([cont])

        assert actual_result == {
            'content:add': 1,
            }

        if hasattr(swh_storage, 'objstorage'):
            assert cont['sha1'] not in swh_storage.objstorage.objstorage
        with db_transaction(swh_storage) as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, length, status'
                        ' FROM content WHERE sha1 = %s',
                        (cont['sha1'],))
            datum = cur.fetchone()
        assert datum == (cont['sha1'], cont['sha1_git'], cont['sha256'],
                         cont['length'], 'visible')

        assert list(swh_storage.journal_writer.journal.objects) == [
            ('content', cont)]

    def test_skipped_content_add_db(self, swh_storage):
        cont = data.skipped_cont
        cont2 = data.skipped_cont2
        cont2['blake2s256'] = None

        actual_result = swh_storage.skipped_content_add([cont, cont, cont2])

        assert 2 <= actual_result.pop('skipped_content:add') <= 3
        assert actual_result == {}

        with db_transaction(swh_storage) as (_, cur):
            cur.execute('SELECT sha1, sha1_git, sha256, blake2s256, '
                        'length, status, reason '
                        'FROM skipped_content ORDER BY sha1_git')

            dbdata = cur.fetchall()

        assert len(dbdata) == 2
        assert dbdata[0] == (cont['sha1'], cont['sha1_git'], cont['sha256'],
                             cont['blake2s256'], cont['length'], 'absent',
                             'Content too long')

        assert dbdata[1] == (cont2['sha1'], cont2['sha1_git'], cont2['sha256'],
                             cont2['blake2s256'], cont2['length'], 'absent',
                             'Content too long')
