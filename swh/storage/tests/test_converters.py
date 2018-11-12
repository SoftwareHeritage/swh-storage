# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.storage import converters


class TestConverters(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    def test_db_to_author(self):
        # when
        actual_author = converters.db_to_author(
            1, b'fullname', b'name', b'email')

        # then
        self.assertEqual(actual_author, {
            'id': 1,
            'fullname': b'fullname',
            'name': b'name',
            'email': b'email',
        })

    def test_db_to_revision(self):
        # when
        actual_revision = converters.db_to_revision({
            'id': 'revision-id',
            'date': None,
            'date_offset': None,
            'date_neg_utc_offset': None,
            'committer_date': None,
            'committer_date_offset': None,
            'committer_date_neg_utc_offset': None,
            'type': 'rev',
            'directory': b'dir-sha1',
            'message': b'commit message',
            'author_id': 'auth-id',
            'author_fullname': b'auth-fullname',
            'author_name': b'auth-name',
            'author_email': b'auth-email',
            'committer_id': 'comm-id',
            'committer_fullname': b'comm-fullname',
            'committer_name': b'comm-name',
            'committer_email': b'comm-email',
            'metadata': {},
            'synthetic': False,
            'parents': [123, 456]
        })

        # then
        self.assertEqual(actual_revision, {
            'id': 'revision-id',
            'author': {
                'id': 'auth-id',
                'fullname': b'auth-fullname',
                'name': b'auth-name',
                'email': b'auth-email',
            },
            'date': None,
            'committer': {
                'id': 'comm-id',
                'fullname': b'comm-fullname',
                'name': b'comm-name',
                'email': b'comm-email',
            },
            'committer_date': None,
            'type': 'rev',
            'directory': b'dir-sha1',
            'message': b'commit message',
            'metadata': {},
            'synthetic': False,
            'parents': [123, 456],
        })

    def test_db_to_release(self):
        # when
        actual_release = converters.db_to_release({
            'id': b'release-id',
            'target': b'revision-id',
            'target_type': 'revision',
            'date': None,
            'date_offset': None,
            'date_neg_utc_offset': None,
            'name': b'release-name',
            'comment': b'release comment',
            'synthetic': True,
            'author_id': 'auth-id',
            'author_fullname': b'auth-fullname',
            'author_name': b'auth-name',
            'author_email': b'auth-email',
        })

        # then
        self.assertEqual(actual_release, {
            'author': {
                'id': 'auth-id',
                'fullname': b'auth-fullname',
                'name': b'auth-name',
                'email': b'auth-email',
            },
            'date': None,
            'id': b'release-id',
            'name': b'release-name',
            'message': b'release comment',
            'synthetic': True,
            'target': b'revision-id',
            'target_type': 'revision'
        })

    def test_db_to_git_headers(self):
        raw_data = [
            ['gpgsig', b'garbage\x89a\x43b\x14'],
            ['extra', [b'fo\\\\\\o', b'bar\\', b'inval\\\\\x99id']],
        ]

        db_data = converters.git_headers_to_db(raw_data)
        loop = converters.db_to_git_headers(db_data)
        self.assertEqual(raw_data, loop)
