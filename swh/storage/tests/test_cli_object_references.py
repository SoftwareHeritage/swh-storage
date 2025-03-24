# Copyright (C) 2020-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging

import pytest

from swh.storage import get_storage

from .test_cli import invoke

logger = logging.getLogger(__name__)


@pytest.fixture(params=["postgresql", "cassandra"])
def swh_storage_backend_config(
    request, swh_storage_postgresql_backend_config, swh_storage_cassandra_backend_config
):
    """An swh-storage object that gets injected into the CLI functions."""
    if request.param == "postgresql":
        return swh_storage_postgresql_backend_config
    else:
        return swh_storage_cassandra_backend_config


@pytest.mark.parametrize(
    ("start", "end", "expected_weeks", "unexpected_weeks"),
    (
        pytest.param(
            "2020-02-03",
            "2020-02-09",
            [("2020w06", "2020-02-03", "2020-02-10")],
            ["2020w05", "2020w07"],
            id="single-week",
        ),
        pytest.param(
            "2023-01-01",
            "2023-01-01",
            [("2022w52", "2022-12-26", "2023-01-02")],
            ["2022w51", "2023w01"],
            id="week-in-another-year",
        ),
        pytest.param(
            "2023-01-01",
            "2023-01-17",
            [
                ("2022w52", "2022-12-26", "2023-01-02"),
                ("2023w01", "2023-01-02", "2023-01-09"),
                ("2023w02", "2023-01-09", "2023-01-16"),
                ("2023w03", "2023-01-16", "2023-01-23"),
            ],
            ["2022w51", "2023w04"],
            id="multiple-weeks",
        ),
    ),
)
def test_create_object_reference_partitions(
    swh_storage_backend_config,
    swh_storage,
    start,
    end,
    expected_weeks,
    unexpected_weeks,
):
    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_backend_config,
            ],
        }
    }

    result = invoke(
        "create-object-reference-partitions",
        start,
        end,
        local_config=storage_config,
    )

    assert result.exit_code == 0, result.output

    partitions = swh_storage.object_references_list_partitions()

    for partition, (week, expected_start, expected_end) in zip(
        partitions, expected_weeks
    ):
        assert partition.table_name == f"object_references_{week}"
        assert partition.start == datetime.date.fromisoformat(expected_start)
        assert partition.end == datetime.date.fromisoformat(expected_end)

    table_names = {partition.table_name for partition in partitions}
    for week in unexpected_weeks:
        assert f"object_references_{week}" not in table_names


@pytest.fixture
def swh_storage_with_partitions(swh_storage_backend_config):
    # Setup partitions from 2022-12-26 00:00 to 2023-01-15 23:59
    swh_storage = get_storage(**swh_storage_backend_config)
    swh_storage.object_references_create_partition(2022, 52)
    for week in range(1, 3):
        swh_storage.object_references_create_partition(2023, week)
    return swh_storage


@pytest.mark.parametrize(
    ("before", "listed_in_output", "remaining"),
    (
        pytest.param(
            "2022-12-25",
            [],
            ["2022w52", "2023w01", "2023w02"],
            id="older-than-first-week",
        ),
        pytest.param(
            "2023-01-01",
            [],
            ["2022w52", "2023w01", "2023w02"],
            id="sunday-of-first-week",
        ),
        pytest.param(
            "2023-01-02",
            ["2022 week 52"],
            ["2023w01", "2023w02"],
            id="middle-of-second-week",
        ),
        pytest.param(
            "2023-01-12",
            ["2022 week 52", "2023 week 01"],
            ["2023w02"],
            id="middle-of-third-week",
        ),
    ),
)
def test_remove_old_object_reference_partitions(
    swh_storage_backend_config,
    swh_storage_with_partitions,
    before,
    listed_in_output,
    remaining,
):
    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_backend_config,
            ],
        }
    }

    result = invoke(
        "remove-old-object-reference-partitions",
        before,
        input="y\n",
        local_config=storage_config,
    )

    assert result.exit_code == 0, result.output

    assert all(s in result.output for s in listed_in_output)

    partitions = swh_storage_with_partitions.object_references_list_partitions()

    table_names = {partition.table_name for partition in partitions}

    # We get a partition created for the current week when the schema is initialized
    table_names.discard(f"object_references_{datetime.date.today().strftime('%Yw%W')}")
    assert {f"object_references_{week}" for week in remaining} == table_names


def test_remove_old_object_reference_partitions_postgresql_refuses_to_remove_all(
    swh_storage_backend_config, swh_storage_with_partitions
):
    if swh_storage_backend_config["cls"] == "cassandra":
        raise pytest.skip("Cassandra supports removing all partitions")
    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_backend_config,
            ],
        }
    }

    result = invoke(
        "remove-old-object-reference-partitions",
        # A partition for this week is created with the schema,
        # so we just ask to delete
        (datetime.date.today() + datetime.timedelta(weeks=1)).strftime("%Y-%m-%d"),
        local_config=storage_config,
    )

    assert result.exit_code != 0
    assert "Trying to remove all existing partitions" in result.output

    partitions = swh_storage_with_partitions.object_references_list_partitions()

    # The three partition created in the fixture
    assert len(partitions) == 3


def test_remove_old_object_reference_partitions_using_force_argument(
    swh_storage_backend_config, swh_storage_with_partitions
):
    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_backend_config,
            ],
        }
    }

    swh_storage_with_partitions.object_references_create_partition(
        *datetime.date.today().isocalendar()[0:2]
    )

    result = invoke(
        "remove-old-object-reference-partitions",
        "--force",
        "2023-01-31",
        local_config=storage_config,
    )

    assert result.exit_code == 0, result.output

    partitions = swh_storage_with_partitions.object_references_list_partitions()

    table_names = {partition.table_name for partition in partitions}

    assert not any(
        f"object_references_{week}" in table_names
        for week in ("2023w01", "2023w02", "2023w03")
    )
