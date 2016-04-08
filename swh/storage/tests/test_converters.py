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
    def backslashescape_errors(self):
        raw_data_err = b'abcd\x80'
        with self.assertRaises(UnicodeDecodeError):
            raw_data_err.decode('utf-8', 'strict')

        self.assertEquals(
            raw_data_err.decode('utf-8', 'backslashescape'),
            'abcd\\x80',
        )

        raw_data_ok = b'abcd\xc3\xa9'
        self.assertEquals(
            raw_data_ok.decode('utf-8', 'backslashescape'),
            raw_data_ok.decode('utf-8', 'strict'),
        )

        unicode_data = 'abcdef\u00a3'
        self.assertEquals(
            unicode_data.encode('ascii', 'backslashescape'),
            b'abcdef\\xa3',
        )

    @istest
    def encode_with_unescape(self):
        valid_data = '\\x01020304\\x00'
        valid_data_encoded = b'\x01020304\x00'

        self.assertEquals(
            valid_data_encoded,
            converters.encode_with_unescape(valid_data)
        )

    @istest
    def encode_with_unescape_invalid_escape(self):
        invalid_data = 'test\\abcd'

        with self.assertRaises(ValueError) as exc:
            converters.encode_with_unescape(invalid_data)

        self.assertIn('invalid escape', exc.exception.args[0])
        self.assertIn('position 4', exc.exception.args[0])

    @istest
    def decode_with_escape(self):
        backslashes = b'foo\\bar\\\\baz'
        backslashes_escaped = 'foo\\\\bar\\\\\\\\baz'

        self.assertEquals(
            backslashes_escaped,
            converters.decode_with_escape(backslashes),
        )

        valid_utf8 = b'foo\xc3\xa2'
        valid_utf8_escaped = 'foo\u00e2'

        self.assertEquals(
            valid_utf8_escaped,
            converters.decode_with_escape(valid_utf8),
        )

        invalid_utf8 = b'foo\xa2'
        invalid_utf8_escaped = 'foo\\xa2'

        self.assertEquals(
            invalid_utf8_escaped,
            converters.decode_with_escape(invalid_utf8),
        )

        valid_utf8_nul = b'foo\xc3\xa2\x00'
        valid_utf8_nul_escaped = 'foo\u00e2\\x00'

        self.assertEquals(
            valid_utf8_nul_escaped,
            converters.decode_with_escape(valid_utf8_nul),
        )

    @istest
    def db_to_git_headers(self):
        raw_data = [
            ['gpgsig', b'garbage\x89a\x43b\x14'],
            ['extra', [b'fo\\\\\\o', b'bar\\', b'inval\\\\\x99id']],
        ]

        db_data = converters.git_headers_to_db(raw_data)
        loop = converters.db_to_git_headers(db_data)
        self.assertEquals(raw_data, loop)
