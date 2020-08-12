# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dataclasses

import pytest

from swh.storage.cassandra.model import BaseRow
from swh.storage.in_memory import SortedList, Table
from swh.storage.tests.test_storage import TestStorage as _TestStorage
from swh.storage.tests.test_storage import (
    TestStorageGeneratedData as _TestStorageGeneratedData,
)


# tests are executed using imported classes (TestStorage and
# TestStorageGeneratedData) using overloaded swh_storage fixture
# below


@pytest.fixture
def swh_storage_backend_config():
    yield {
        "cls": "memory",
        "journal_writer": {"cls": "memory",},
    }


parametrize = pytest.mark.parametrize(
    "items",
    [
        [1, 2, 3, 4, 5, 6, 10, 100],
        [10, 100, 6, 5, 4, 3, 2, 1],
        [10, 4, 5, 6, 1, 2, 3, 100],
    ],
)


@parametrize
def test_sorted_list_iter(items):
    list1 = SortedList()
    for item in items:
        list1.add(item)
    assert list(list1) == sorted(items)

    list2 = SortedList(items)
    assert list(list2) == sorted(items)


@parametrize
def test_sorted_list_iter__key(items):
    list1 = SortedList(key=lambda item: -item)
    for item in items:
        list1.add(item)
    assert list(list1) == list(reversed(sorted(items)))

    list2 = SortedList(items, key=lambda item: -item)
    assert list(list2) == list(reversed(sorted(items)))


@parametrize
def test_sorted_list_iter_from(items):
    list_ = SortedList(items)
    for split in items:
        expected = sorted(item for item in items if item >= split)
        assert list(list_.iter_from(split)) == expected, f"split: {split}"


@parametrize
def test_sorted_list_iter_from__key(items):
    list_ = SortedList(items, key=lambda item: -item)
    for split in items:
        expected = reversed(sorted(item for item in items if item <= split))
        assert list(list_.iter_from(-split)) == list(expected), f"split: {split}"


@parametrize
def test_sorted_list_iter_after(items):
    list_ = SortedList(items)
    for split in items:
        expected = sorted(item for item in items if item > split)
        assert list(list_.iter_after(split)) == expected, f"split: {split}"


@parametrize
def test_sorted_list_iter_after__key(items):
    list_ = SortedList(items, key=lambda item: -item)
    for split in items:
        expected = reversed(sorted(item for item in items if item < split))
        assert list(list_.iter_after(-split)) == list(expected), f"split: {split}"


@dataclasses.dataclass
class Row(BaseRow):
    PARTITION_KEY = ("col1", "col2")
    CLUSTERING_KEY = ("col3", "col4")

    col1: str
    col2: str
    col3: str
    col4: str
    col5: str
    col6: int


def test_table_keys():
    table = Table(Row)

    primary_key = ("foo", "bar", "baz", "qux")
    partition_key = ("foo", "bar")
    clustering_key = ("baz", "qux")

    row = Row(col1="foo", col2="bar", col3="baz", col4="qux", col5="quux", col6=4)
    assert table.partition_key(row) == partition_key
    assert table.clustering_key(row) == clustering_key
    assert table.primary_key(row) == primary_key

    assert table.primary_key_from_dict(row.to_dict()) == primary_key
    assert table.split_primary_key(primary_key) == (partition_key, clustering_key)


def test_table():
    table = Table(Row)

    row1 = Row(col1="foo", col2="bar", col3="baz", col4="qux", col5="quux", col6=4)
    row2 = Row(col1="foo", col2="bar", col3="baz", col4="qux2", col5="quux", col6=4)
    row3 = Row(col1="foo", col2="bar", col3="baz", col4="qux1", col5="quux", col6=4)
    row4 = Row(col1="foo", col2="bar2", col3="baz", col4="qux1", col5="quux", col6=4)
    partition_key = ("foo", "bar")
    partition_key4 = ("foo", "bar2")
    primary_key1 = ("foo", "bar", "baz", "qux")
    primary_key2 = ("foo", "bar", "baz", "qux2")
    primary_key3 = ("foo", "bar", "baz", "qux1")
    primary_key4 = ("foo", "bar2", "baz", "qux1")

    table.insert(row1)
    table.insert(row2)
    table.insert(row3)
    table.insert(row4)

    assert table.get_from_primary_key(primary_key1) == row1
    assert table.get_from_primary_key(primary_key2) == row2
    assert table.get_from_primary_key(primary_key3) == row3
    assert table.get_from_primary_key(primary_key4) == row4

    # order matters
    assert list(table.get_from_token(table.token(partition_key))) == [row1, row3, row2]

    # order matters
    assert list(table.get_from_partition_key(partition_key)) == [row1, row3, row2]

    assert list(table.get_from_partition_key(partition_key4)) == [row4]

    all_rows = list(table.iter_all())
    assert len(all_rows) == 4
    for row in (row1, row2, row3, row4):
        assert (table.primary_key(row), row) in all_rows


class TestInMemoryStorage(_TestStorage):
    @pytest.mark.skip(
        'The "person" table of the pgsql is a legacy thing, and not '
        "supported by the cassandra backend."
    )
    def test_person_fullname_unicity(self):
        pass

    @pytest.mark.skip("content_update is not yet implemented for Cassandra")
    def test_content_update(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count(self):
        pass


class TestInMemoryStorageGeneratedData(_TestStorageGeneratedData):
    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_no_visits(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_with_visits_and_snapshot(self):
        pass

    @pytest.mark.skip("Not supported by Cassandra")
    def test_origin_count_with_visit_with_visits_no_snapshot(self):
        pass
