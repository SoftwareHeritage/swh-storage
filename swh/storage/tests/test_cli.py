# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import logging
import pathlib
import re
import tempfile
from unittest.mock import patch

from click.testing import CliRunner
from confluent_kafka import Producer
import pytest
import yaml

from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.model.model import Snapshot, SnapshotBranch, TargetType
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


@pytest.fixture
def monkeypatch_retry_sleep(monkeypatch):
    from swh.journal.replay import copy_object, obj_in_objstorage

    monkeypatch.setattr(copy_object.retry, "sleep", lambda x: None)
    monkeypatch.setattr(obj_in_objstorage.retry, "sleep", lambda x: None)


def invoke(*args, env=None, journal_config=None, local_config=None):
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
        )
        return ret


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
                target_type=TargetType.REVISION,
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
                target_type=TargetType.REVISION,
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

    from .test_postgresql import get_object_references_partition_bounds

    partitions = get_object_references_partition_bounds(swh_storage)

    for week, expected_start, expected_end in expected_weeks:
        assert (
            partitions[f"object_references_{week}"]
            == f"FOR VALUES FROM ('{expected_start}') TO ('{expected_end}')"
        )

    for week in unexpected_weeks:
        assert f"object_references_{week}" not in partitions
