# Copyright (C) 2019-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import multiprocessing.util

from hypothesis import settings
import pytest

try:
    import pytest_cov
    import pytest_cov.embed
except ImportError:
    pytest_cov = None

from typing import Iterable

from swh.model.model import BaseContent, Origin
from swh.model.tests.generate_testdata import gen_contents, gen_origins
from swh.storage.interface import StorageInterface

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)
# Load the fast profile by default to overcome default hypothesis values
# (max_examples=100, deadline=200) that are unsuitable for our tests.
# This can still be overloaded via the --hypothesis-profile option.
settings.load_profile("fast")

if pytest_cov is not None and int(pytest_cov.__version__.split(".")[0]) < 4:
    # pytest_cov + multiprocessing can cause a segmentation fault when starting
    # the child process <https://forge.softwareheritage.org/P706>; so we're
    # removing pytest-coverage's hook that runs when a child process starts.
    # This means code run in child processes won't be counted in the coverage
    # report, but this is not an issue because the only code that runs only in
    # child processes is the RPC server.
    registry = multiprocessing.util._afterfork_registry  # type: ignore[attr-defined]
    for key, value in registry.items():
        if value is pytest_cov.embed.multiprocessing_start:
            del registry[key]
            break
    else:
        assert False, "missing pytest_cov.embed.multiprocessing_start?"


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql_backend_config):
    """Storage should test with its journal writer collaborator on"""
    yield {
        **swh_storage_postgresql_backend_config,
        "journal_writer": {
            "cls": "memory",
        },
    }


@pytest.fixture
def swh_contents(swh_storage: StorageInterface) -> Iterable[BaseContent]:
    contents = [BaseContent.from_dict(c) for c in gen_contents(n=20)]
    swh_storage.content_add([c for c in contents if c.status != "absent"])
    swh_storage.skipped_content_add([c for c in contents if c.status == "absent"])
    return contents


@pytest.fixture
def swh_origins(swh_storage: StorageInterface) -> Iterable[Origin]:
    origins = [Origin.from_dict(o) for o in gen_origins(n=100)]
    swh_storage.origin_add(origins)
    return origins
