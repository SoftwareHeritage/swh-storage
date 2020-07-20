# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import logging
import re
import tempfile
import yaml

from unittest.mock import patch

import pytest

from click.testing import CliRunner
from confluent_kafka import Producer

from swh.model.model import Snapshot, SnapshotBranch, TargetType
from swh.journal.serializers import key_to_kafka, value_to_kafka
from swh.storage import get_storage
from swh.storage.cli import storage as cli


logger = logging.getLogger(__name__)


CLI_CONFIG = {
    "storage": {"cls": "memory",},
}


@pytest.fixture
def swh_storage():
    """An swh-storage object that gets injected into the CLI functions."""
    storage = get_storage(**CLI_CONFIG["storage"])
    with patch("swh.storage.cli.get_storage") as get_storage_mock:
        get_storage_mock.return_value = storage
        yield storage


@pytest.fixture
def monkeypatch_retry_sleep(monkeypatch):
    from swh.journal.replay import copy_object, obj_in_objstorage

    monkeypatch.setattr(copy_object.retry, "sleep", lambda x: None)
    monkeypatch.setattr(obj_in_objstorage.retry, "sleep", lambda x: None)


def invoke(*args, env=None, journal_config=None):
    config = copy.deepcopy(CLI_CONFIG)
    if journal_config:
        config["journal_client"] = journal_config.copy()
        config["journal_client"]["cls"] = "kafka"

    runner = CliRunner()
    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        ret = runner.invoke(cli, args, obj={"log_level": logging.DEBUG}, env=env,)
        return ret


def test_replay(
    swh_storage, kafka_prefix: str, kafka_consumer_group: str, kafka_server: str,
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
                target_type=TargetType.REVISION, target=b"\x01" * 20,
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
