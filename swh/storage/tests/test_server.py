# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict

import pytest
import yaml

from swh.core.config import load_from_envvar
from swh.storage.api.server import (
    StorageServerApp,
    load_and_check_config,
    make_app_from_configfile,
)


def prepare_config_file(tmpdir, content, name="config.yml"):
    """Prepare configuration file in `$tmpdir/name` with content `content`.

    Args:
        tmpdir (LocalPath): root directory
        content (str/dict): Content of the file either as string or as a dict.
                            If a dict, converts the dict into a yaml string.
        name (str): configuration filename

    Returns
        path (str) of the configuration file prepared.

    """
    config_path = tmpdir / name
    if isinstance(content, dict):  # convert if needed
        content = yaml.dump(content)
    config_path.write_text(content, encoding="utf-8")
    # pytest on python3.5 does not support LocalPath manipulation, so
    # convert path to string
    return str(config_path)


@pytest.mark.parametrize("storage_class", [None, ""])
def test_load_and_check_config_no_configuration(storage_class):
    """Inexistent configuration files raises"""
    with pytest.raises(EnvironmentError, match="Configuration file must be defined"):
        load_and_check_config(storage_class)


def test_load_and_check_config_inexistent_file():
    config_path = "/some/inexistent/config.yml"
    expected_error = f"Configuration file {config_path} does not exist"
    with pytest.raises(FileNotFoundError, match=expected_error):
        load_and_check_config(config_path)


def test_load_and_check_config_wrong_configuration(tmpdir):
    """Wrong configuration raises"""
    config_path = prepare_config_file(tmpdir, "something: useless")
    with pytest.raises(KeyError, match="Missing 'storage' configuration"):
        load_and_check_config(config_path)


def test_load_and_check_config_local_config_fine(tmpdir):
    """'local' complete configuration is fine"""
    config = {
        "storage": {
            "cls": "postgresql",
            "db": "db",
            "objstorage": "something",
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path)
    assert cfg == config


@pytest.fixture
def swh_storage_server_config(
    swh_storage_backend_config: Dict[str, Any]
) -> Dict[str, Any]:
    return {"storage": swh_storage_backend_config}


@pytest.fixture
def swh_storage_config(monkeypatch, swh_storage_server_config, tmp_path):
    conf_path = os.path.join(str(tmp_path), "storage.yml")
    with open(conf_path, "w") as f:
        f.write(yaml.dump(swh_storage_server_config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conf_path)
    return conf_path


def test_server_make_app_from_config_file(swh_storage_config):
    app = make_app_from_configfile()
    expected_cfg = load_from_envvar()

    assert app is not None
    assert isinstance(app, StorageServerApp)
    assert app.config["storage"] == expected_cfg["storage"]

    app2 = make_app_from_configfile()
    assert app is app2
