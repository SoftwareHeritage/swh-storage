# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import logging
import os
import pathlib
import re
import tempfile
from unittest.mock import patch

from click.testing import CliRunner
from confluent_kafka import Producer
import pytest
import yaml

from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.model.model import Origin, Snapshot, SnapshotBranch, SnapshotTargetType
from swh.storage import get_storage
from swh.storage.cli import storage as cli
from swh.storage.replay import OBJECT_CONVERTERS

logger = logging.getLogger(__name__)


CLI_CONFIG = {
    "storage": {
        "cls": "memory",
    },
}


@pytest.fixture
def swh_storage():
    """An swh-storage object that gets injected into the CLI functions."""
    storage = get_storage(**CLI_CONFIG["storage"])
    with patch("swh.storage.get_storage") as get_storage_mock:
        get_storage_mock.return_value = storage
        yield storage


def invoke(*args, env=None, input=None, journal_config=None, local_config=None):
    config = local_config or copy.deepcopy(CLI_CONFIG)
    if journal_config:
        config["journal_client"] = journal_config.copy()
        config["journal_client"]["cls"] = "kafka"

    runner = CliRunner()
    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        ret = runner.invoke(
            cli,
            args,
            obj={"log_level": logging.DEBUG},
            env=env,
            input=input,
        )
        return ret


@pytest.mark.cassandra
def test_create_keyspace(
    swh_storage_cassandra_cluster,
    cassandra_auth_provider_config,
):
    (hosts, port) = swh_storage_cassandra_cluster
    keyspace = "test" + os.urandom(10).hex()

    storage_config = dict(
        cls="cassandra",
        hosts=hosts,
        port=port,
        keyspace=keyspace,
        journal_writer={"cls": "memory"},
        objstorage={"cls": "memory"},
        auth_provider=cassandra_auth_provider_config,
    )

    result = invoke("create-keyspace", local_config={"storage": storage_config})
    assert result.exit_code == 0, result.output
    assert result.output == "Done.\n"

    # Check we can write and read to it
    storage = get_storage(**storage_config)
    origin = Origin(url="http://example.org")
    storage.origin_add([origin])
    assert storage.origin_get([origin.url]) == [origin]


def test_replay(
    swh_storage,
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
):
    kafka_prefix += ".swh.journal.objects"

    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test-producer",
            "acks": "all",
        }
    )

    snapshot = Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target_type=SnapshotTargetType.REVISION,
                target=b"\x01" * 20,
            )
        },
    )
    snapshot_dict = snapshot.to_dict()

    producer.produce(
        topic=kafka_prefix + ".snapshot",
        key=key_to_kafka(snapshot.id),
        value=value_to_kafka(snapshot_dict),
    )
    producer.flush()

    logger.debug("Flushed producer")

    result = invoke(
        "replay",
        "--stop-after-objects",
        "1",
        journal_config={
            "brokers": [kafka_server],
            "group_id": kafka_consumer_group,
            "prefix": kafka_prefix,
        },
    )

    expected = r"Done.\n"
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    assert swh_storage.snapshot_get(snapshot.id) == {
        **snapshot_dict,
        "next_branch": None,
    }


def test_replay_with_exceptions(
    swh_storage,
    kafka_prefix: str,
    kafka_consumer_group: str,
    kafka_server: str,
    tmp_path: pathlib.Path,
):
    kafka_prefix += ".swh.journal.objects"

    producer = Producer(
        {
            "bootstrap.servers": kafka_server,
            "client.id": "test-producer",
            "acks": "all",
        }
    )

    snapshot = Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target_type=SnapshotTargetType.REVISION,
                target=b"\x01" * 20,
            )
        },
    )
    snapshot_dict = snapshot.to_dict()
    producer.produce(
        topic=kafka_prefix + ".snapshot",
        key=key_to_kafka(snapshot.id),
        value=value_to_kafka(snapshot_dict),
    )
    # add 2 invalid snapshots
    inv_id = b"\x00" * 20
    inv_snapshot = {**snapshot_dict, "id": inv_id}
    producer.produce(
        topic=kafka_prefix + ".snapshot",
        key=key_to_kafka(inv_id),
        value=value_to_kafka(inv_snapshot),
    )
    inv_id2 = b"\x02" * 20
    inv_snapshot2 = {**snapshot_dict, "id": inv_id2}
    producer.produce(
        topic=kafka_prefix + ".snapshot",
        key=key_to_kafka(inv_id2),
        value=value_to_kafka(inv_snapshot2),
    )

    producer.flush()

    logger.debug("Flushed producer")

    # make an exception for the the first invalid snp only
    excfile = tmp_path / "swhids.txt"
    excfile.write_text(
        f"swh:1:snp:0000000000000000000000000000000000000000,{snapshot.id.hex()}\n\n"
    )

    result = invoke(
        "replay",
        "--stop-after-objects",
        "3",
        "--known-mismatched-hashes",
        str(excfile.absolute()),
        journal_config={
            "brokers": [kafka_server],
            "group_id": kafka_consumer_group,
            "prefix": kafka_prefix,
            "privileged": True,  # required to activate validation
        },
    )

    expected = r"Done.\n"
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    assert swh_storage.snapshot_get(snapshot.id) == dict(
        **snapshot_dict,
        next_branch=None,
    )
    assert swh_storage.snapshot_get(inv_id) == dict(
        **inv_snapshot,
        next_branch=None,
    )
    assert swh_storage.snapshot_get(inv_id2) is None


def test_replay_type_list():
    result = invoke(
        "replay",
        "--help",
    )
    assert result.exit_code == 0, result.output
    types_in_help = re.findall("--type [[]([a-z_|]+)[]]", result.output)
    assert len(types_in_help) == 1
    types = types_in_help[0].split("|")

    assert sorted(types) == sorted(list(OBJECT_CONVERTERS.keys())), (
        "Make sure the list of accepted types in cli.py "
        "matches implementation in replay.py"
    )


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
def test_create_object_reference_partitions_postgresql(
    swh_storage_postgresql_backend_config, start, end, expected_weeks, unexpected_weeks
):
    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_postgresql_backend_config,
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

    swh_storage = get_storage(**swh_storage_postgresql_backend_config)

    with swh_storage.db() as db:
        partitions = db.object_references_list_partitions()

    for partition, (week, expected_start, expected_end) in zip(
        partitions, expected_weeks
    ):
        assert partition.table_name == f"object_references_{week}"
        assert partition.start == datetime.datetime.fromisoformat(expected_start)
        assert partition.end == datetime.datetime.fromisoformat(expected_end)

    table_names = {partition.table_name for partition in partitions}
    for week in unexpected_weeks:
        assert f"object_references_{week}" not in table_names


@pytest.fixture
def swh_storage_with_partitions(swh_storage_postgresql_backend_config):
    # Setup partitions from 2022-12-26 00:00 to 2023-01-15 23:59
    swh_storage = get_storage(**swh_storage_postgresql_backend_config)
    with swh_storage.db() as db:
        with db.transaction() as cur:
            db.object_references_create_partition(2022, 52, cur)
            for week in range(1, 3):
                db.object_references_create_partition(2023, week, cur)
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
def test_remove_old_object_reference_partitions_postgresql(
    swh_storage_postgresql_backend_config,
    swh_storage_with_partitions,
    before,
    listed_in_output,
    remaining,
):
    from datetime import datetime

    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_postgresql_backend_config,
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

    with swh_storage_with_partitions.db() as db:
        partitions = db.object_references_list_partitions()

    table_names = {partition.table_name for partition in partitions}

    # We get a partition created for the current week when the schema is initialized
    table_names.discard(f"object_references_{datetime.now().strftime('%Yw%W')}")
    assert {f"object_references_{week}" for week in remaining} == table_names


def test_remove_old_object_reference_partitions_postgresql_refuses_to_remove_all(
    swh_storage_postgresql_backend_config, swh_storage_with_partitions
):
    from datetime import datetime, timedelta

    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_postgresql_backend_config,
            ],
        }
    }

    result = invoke(
        "remove-old-object-reference-partitions",
        # A partition for this week is created with the schema,
        # so we just ask to delete
        (datetime.now() + timedelta(weeks=1)).strftime("%Y-%m-%d"),
        local_config=storage_config,
    )

    assert result.exit_code != 0
    assert "Trying to remove all existing partitions" in result.output

    with swh_storage_with_partitions.db() as db:
        partitions = db.object_references_list_partitions()

    # The three partition created in the fixture + the one created when the
    # schema was initialized.
    assert len(partitions) == 4


def test_remove_old_object_reference_partitions_postgresql_using_force_argument(
    swh_storage_postgresql_backend_config, swh_storage_with_partitions
):
    storage_config = {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "record_references"},
                swh_storage_postgresql_backend_config,
            ],
        }
    }

    result = invoke(
        "remove-old-object-reference-partitions",
        "--force",
        "2023-01-31",
        local_config=storage_config,
    )

    assert result.exit_code == 0, result.output

    with swh_storage_with_partitions.db() as db:
        partitions = db.object_references_list_partitions()

    table_names = {partition.table_name for partition in partitions}

    assert not any(
        f"object_references_{week}" in table_names
        for week in ("2023w01", "2023w02", "2023w03")
    )
