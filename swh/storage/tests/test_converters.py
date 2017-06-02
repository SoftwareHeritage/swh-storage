# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.storage import converters


@attr('!db')
class TestConverters(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    @istest
    def db_to_author(self):
        # when
        actual_author = converters.db_to_author(
            1, b'fullname', b'name', b'email')

        # then
        self.assertEquals(actual_author, {
            'id': 1,
            'fullname': b'fullname',
            'name': b'name',
            'email': b'email',
        })

    @istest
    def db_to_revision(self):
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
        self.assertEquals(actual_revision, {
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

    @istest
    def db_to_release(self):
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
        self.assertEquals(actual_release, {
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

    @istest
    def db_to_git_headers(self):
        raw_data = [
            ['gpgsig', b'garbage\x89a\x43b\x14'],
            ['extra', [b'fo\\\\\\o', b'bar\\', b'inval\\\\\x99id']],
        ]

        db_data = converters.git_headers_to_db(raw_data)
        loop = converters.db_to_git_headers(db_data)
        self.assertEquals(raw_data, loop)

    @istest
    def ctags_to_db(self):
        input_ctag = {
            'id': b'some-id',
            'indexer_configuration_id': 100,
            'ctags': [
                {
                    'name': 'some-name',
                    'kind': 'some-kind',
                    'line': 10,
                    'lang': 'Yaml',
                }, {
                    'name': 'main',
                    'kind': 'function',
                    'line': 12,
                    'lang': 'Yaml',
                },
            ]
        }

        expected_ctags = [
            {
                'id': b'some-id',
                'name': 'some-name',
                'kind': 'some-kind',
                'line': 10,
                'lang': 'Yaml',
                'indexer_configuration_id': 100,
            }, {
                'id': b'some-id',
                'name': 'main',
                'kind': 'function',
                'line': 12,
                'lang': 'Yaml',
                'indexer_configuration_id': 100,
            }]

        # when
        actual_ctags = list(converters.ctags_to_db(input_ctag))

        # then
        self.assertEquals(actual_ctags, expected_ctags)

    @istest
    def db_to_ctags(self):
        input_ctags = {
            'id': b'some-id',
            'name': 'some-name',
            'kind': 'some-kind',
            'line': 10,
            'lang': 'Yaml',
            'tool_id': 200,
            'tool_name': 'some-toolname',
            'tool_version': 'some-toolversion',
            'tool_configuration': {}
        }
        expected_ctags = {
            'id': b'some-id',
            'name': 'some-name',
            'kind': 'some-kind',
            'line': 10,
            'lang': 'Yaml',
            'tool': {
                'id': 200,
                'name': 'some-toolname',
                'version': 'some-toolversion',
                'configuration': {},
            }
        }

        # when
        actual_ctags = converters.db_to_ctags(input_ctags)

        # then
        self.assertEquals(actual_ctags, expected_ctags)

    @istest
    def db_to_mimetype(self):
        input_mimetype = {
            'id': b'some-id',
            'tool_id': 10,
            'tool_name': 'some-toolname',
            'tool_version': 'some-toolversion',
            'tool_configuration': {},
            'encoding': b'ascii',
            'mimetype': b'text/plain',
        }

        expected_mimetype = {
            'id': b'some-id',
            'encoding': b'ascii',
            'mimetype': b'text/plain',
            'tool': {
                'id': 10,
                'name': 'some-toolname',
                'version': 'some-toolversion',
                'configuration': {},
            }
        }

        actual_mimetype = converters.db_to_mimetype(input_mimetype)

        self.assertEquals(actual_mimetype, expected_mimetype)

    @istest
    def db_to_language(self):
        input_language = {
            'id': b'some-id',
            'tool_id': 20,
            'tool_name': 'some-toolname',
            'tool_version': 'some-toolversion',
            'tool_configuration': {},
            'lang': b'css',
        }

        expected_language = {
            'id': b'some-id',
            'lang': b'css',
            'tool': {
                'id': 20,
                'name': 'some-toolname',
                'version': 'some-toolversion',
                'configuration': {},
            }
        }

        actual_language = converters.db_to_language(input_language)

        self.assertEquals(actual_language, expected_language)

    @istest
    def db_to_fossology_license(self):
        input_license = {
            'id': b'some-id',
            'tool_id': 20,
            'tool_name': 'nomossa',
            'tool_version': '5.22',
            'tool_configuration': {},
            'licenses': ['GPL2.0'],
        }

        expected_license = {
            'id': b'some-id',
            'licenses': ['GPL2.0'],
            'tool': {
                'id': 20,
                'name': 'nomossa',
                'version': '5.22',
                'configuration': {},
            }
        }

        actual_license = converters.db_to_fossology_license(input_license)

        self.assertEquals(actual_license, expected_license)
