# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import multiprocessing.util

from hypothesis import settings

try:
    import pytest_cov.embed
except ImportError:
    pytest_cov = None

from swh.model.tests.generate_testdata import gen_contents, gen_origins
from swh.model.model import (
    Content,
    Directory,
    Origin,
    OriginVisit,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)


OBJECT_FACTORY = {
    "content": Content.from_dict,
    "directory": Directory.from_dict,
    "origin": Origin.from_dict,
    "origin_visit": OriginVisit.from_dict,
    "release": Release.from_dict,
    "revision": Revision.from_dict,
    "skipped_content": SkippedContent.from_dict,
    "snapshot": Snapshot.from_dict,
}


# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)


if pytest_cov is not None:
    # pytest_cov + multiprocessing can cause a segmentation fault when starting
    # the child process <https://forge.softwareheritage.org/P706>; so we're
    # removing pytest-coverage's hook that runs when a child process starts.
    # This means code run in child processes won't be counted in the coverage
    # report, but this is not an issue because the only code that runs only in
    # child processes is the RPC server.
    for (key, value) in multiprocessing.util._afterfork_registry.items():
        if value is pytest_cov.embed.multiprocessing_start:
            del multiprocessing.util._afterfork_registry[key]
            break
    else:
        assert False, "missing pytest_cov.embed.multiprocessing_start?"


@pytest.fixture
def swh_contents(swh_storage):
    contents = gen_contents(n=20)
    swh_storage.content_add([c for c in contents if c["status"] != "absent"])
    swh_storage.skipped_content_add([c for c in contents if c["status"] == "absent"])
    return contents


@pytest.fixture
def swh_origins(swh_storage):
    origins = gen_origins(n=100)
    swh_storage.origin_add(origins)
    return origins
