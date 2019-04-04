# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import pytest
import yaml

from unittest.mock import patch

from swh.storage.api.server import (
    load_and_check_config, send_metric,
    OPERATIONS_METRIC, OPERATIONS_UNIT_METRIC
)


def prepare_config_file(tmpdir, content, name='config.yml'):
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
    config_path.write_text(content, encoding='utf-8')
    # pytest on python3.5 does not support LocalPath manipulation, so
    # convert path to string
    return str(config_path)


def test_load_and_check_config_no_configuration():
    """Inexistant configuration files raises"""
    with pytest.raises(EnvironmentError) as e:
        load_and_check_config(None)

    assert e.value.args[0] == 'Configuration file must be defined'

    config_path = '/some/inexistant/config.yml'
    with pytest.raises(FileNotFoundError) as e:
        load_and_check_config(config_path)

    assert e.value.args[0] == 'Configuration file %s does not exist' % (
        config_path, )


def test_load_and_check_config_wrong_configuration(tmpdir):
    """Wrong configuration raises"""
    config_path = prepare_config_file(tmpdir, 'something: useless')
    with pytest.raises(KeyError) as e:
        load_and_check_config(config_path)

    assert e.value.args[0] == 'Missing \'%storage\' configuration'


def test_load_and_check_config_remote_config_local_type_raise(tmpdir):
    """'local' configuration without 'local' storage raises"""
    config = {
        'storage': {
            'cls': 'remote',
            'args': {}
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    with pytest.raises(ValueError) as e:
        load_and_check_config(config_path, type='local')

    assert (
        e.value.args[0] ==
        "The storage backend can only be started with a 'local' configuration"
    )


def test_load_and_check_config_local_incomplete_configuration(tmpdir):
    """Incomplete 'local' configuration should raise"""
    config = {
        'storage': {
            'cls': 'local',
            'args': {
                'db': 'database',
                'objstorage': 'object_storage'
            }
        }
    }

    for key in ('db', 'objstorage'):
        c = copy.deepcopy(config)
        c['storage']['args'].pop(key)
        config_path = prepare_config_file(tmpdir, c)
        with pytest.raises(ValueError) as e:
            load_and_check_config(config_path)

        assert (
            e.value.args[0] ==
            "Invalid configuration; missing '%s' config entry" % key
        )


def test_load_and_check_config_local_config_fine(tmpdir):
    """'Remote configuration is fine"""
    config = {
        'storage': {
            'cls': 'local',
            'args': {
                'db': 'db',
                'objstorage': 'something',
            }
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path, type='local')
    assert cfg == config


def test_load_and_check_config_remote_config_fine(tmpdir):
    """'Remote configuration is fine"""
    config = {
        'storage': {
            'cls': 'remote',
            'args': {}
        }
    }
    config_path = prepare_config_file(tmpdir, config)
    cfg = load_and_check_config(config_path, type='any')

    assert cfg == config


def test_send_metric_unknown_unit():
    r = send_metric('content', count=10, method_name='content_add')
    assert r is False
    r = send_metric('sthg:add:bytes:extra', count=10, method_name='sthg_add')
    assert r is False


def test_send_metric_no_value():
    r = send_metric('content:add', count=0, method_name='content_add')
    assert r is False


@patch('swh.storage.api.server.statsd.increment')
def test_send_metric_no_unit(mock_statsd):
    r = send_metric('content:add', count=10, method_name='content_add')

    mock_statsd.assert_called_with(OPERATIONS_METRIC, 10, tags={
        'endpoint': 'content_add',
        'object_type': 'content',
        'operation': 'add',
    })

    assert r


@patch('swh.storage.api.server.statsd.increment')
def test_send_metric_unit(mock_statsd):
    unit_ = 'bytes'
    r = send_metric('c:add:%s' % unit_, count=100, method_name='c_add')

    expected_metric = OPERATIONS_UNIT_METRIC.format(unit=unit_)
    mock_statsd.assert_called_with(
        expected_metric, 100, tags={
            'endpoint': 'c_add',
            'object_type': 'c',
            'operation': 'add',
        })

    assert r
