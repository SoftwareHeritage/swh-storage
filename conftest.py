# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from hypothesis import settings
from typing import Dict


# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)


@pytest.fixture
def sample_data() -> Dict:
    """Pre-defined sample storage object data to manipulate

    Returns:
        Dict of data (keys: content, directory, revision, person)

    """
    sample_content = {
        'blake2s256': b'\xbf?\x05\xed\xc1U\xd2\xc5\x168Xm\x93\xde}f(HO@\xd0\xacn\x04\x1e\x9a\xb9\xfa\xbf\xcc\x08\xc7',  # noqa
        'sha1': b'g\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'sha1_git': b'\xf2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
        'sha256': b"\x87\x022\xedZN\x84\xe8za\xf8'(oA\xc9k\xb1\x80c\x80\xe7J\x06\xea\xd2\xd5\xbeB\x19\xb8\xce",  # noqa
        'length': 48,
        'data': b'temp file for testing content storage conversion',
        'status': 'visible',
    }

    sample_content2 = {
        'blake2s256': b'\xbf?\x05\xed\xc1U\xd2\xc5\x168Xm\x93\xde}f(HO@\xd0\xacn\x04\x1e\x9a\xb9\xfa\xbf\xcc\x08\xc7',  # noqa
        'sha1': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'sha1_git': b'\xc2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
        'sha256': b"\x77\x022\xedZN\x84\xe8za\xf8'(oA\xc9k\xb1\x80c\x80\xe7J\x06\xea\xd2\xd5\xbeB\x19\xb8\xce",  # noqa
        'length': 50,
        'data': b'temp file for testing content storage conversion 2',
        'status': 'visible',
    }

    sample_directory = {
        'id': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'entries': []
    }

    sample_person = {
        'name': b'John Doe',
        'email': b'john.doe@institute.org',
        'fullname': b'John Doe <john.doe@institute.org>'
    }

    sample_revision = {
        'id': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
        'message': b'something',
        'author': sample_person,
        'committer': sample_person,
        'date': 1567591673,
        'committer_date': 1567591673,
        'type': 'tar',
        'directory': b'\xc2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
        'synthetic': False,
        'metadata': {},
        'parents': [],
    }

    return {
        'content': [sample_content, sample_content2],
        'person': [sample_person],
        'directory': [sample_directory],
        'revision': [sample_revision],
    }
