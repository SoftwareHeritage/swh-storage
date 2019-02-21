# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import yaml

from swh.storage.api.server import load_and_check_config


def test_load_and_check_config_no_configuration():
    """Inexistant configuration files raises"""
    with pytest.raises(EnvironmentError) as e:
        load_and_check_config(None)

    assert e.value.args[0] == 'Configuration file must be defined'

    config_path = '/some/inexistant/config.yml'
    with pytest.raises(EnvironmentError) as e:
        load_and_check_config(config_path)

    assert e.value.args[0] == 'Configuration file %s does not exist' % (
        config_path, )


def test_load_and_check_config_wrong_configuration(tmpdir):
    """Wrong configuration raises"""
    config_path = tmpdir / 'config.yml'
    config_path.write_text('something: useless', encoding='utf-8')

    with pytest.raises(EnvironmentError) as e:
        load_and_check_config(config_path)

    assert e.value.args[0] == 'Missing \'%storage\' configuration'


def test_load_and_check_config_remote_config_local_type_raise(tmpdir):
    """'local' configuration without 'local' storage raises"""
    config_path = tmpdir / 'config.yml'
    config = {
        'storage': {
            'cls': 'remote',
            'args': {}
        }
    }

    config_path.write_text(yaml.dump(config), encoding='utf-8')
    with pytest.raises(EnvironmentError) as e:
        load_and_check_config(config_path, type='local')

    assert (
        e.value.args[0] ==
        "The storage backend can only be started with a 'local' configuration"
    )


def test_load_and_check_config_local_incomplete_configuration(tmpdir):
    """Incomplete 'local' configuration should raise"""
    config_path = tmpdir / 'config.yml'

    config = {
        'storage': {
            'cls': 'local',
            'args': {
                'db': 'database',
                'objstorage': 'object_storage'
            }
        }
    }

    import copy
    for key in ('db', 'objstorage'):
        c = copy.deepcopy(config)
        c['storage']['args'].pop(key)
        config_path.write_text(yaml.dump(c), encoding='utf-8')
        with pytest.raises(EnvironmentError) as e:
            load_and_check_config(config_path)

        assert (
            e.value.args[0] ==
            "Invalid configuration; missing '%s' config entry" % key
        )


def test_load_and_check_config_local_config_fine(tmpdir):
    """'Remote configuration is fine"""
    config_path = tmpdir / 'config.yml'

    config = {
        'storage': {
            'cls': 'local',
            'args': {
                'db': 'db',
                'objstorage': 'something',
            }
        }
    }

    config_path.write_text(yaml.dump(config), encoding='utf-8')
    cfg = load_and_check_config(config_path, type='local')
    assert cfg == config


def test_load_and_check_config_remote_config_fine(tmpdir):
    """'Remote configuration is fine"""
    config_path = tmpdir / 'config.yml'

    config = {
        'storage': {
            'cls': 'remote',
            'args': {}
        }
    }

    config_path.write_text(yaml.dump(config), encoding='utf-8')
    cfg = load_and_check_config(config_path, type='any')

    assert cfg == config
