# Copyright (C) 2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import unittest

from swh.storage.listener import decode


class ListenerUtils(unittest.TestCase):
    def test_decode(self):
        inputs = [
            ('content', json.dumps({
                'sha1': '34973274ccef6ab4dfaaf86599792fa9c3fe4689',
            })),
            ('origin', json.dumps({
                'url': 'https://some/origin',
                'type': 'svn',
            })),
            ('origin_visit', json.dumps({
                'visit': 2,
                'origin': {
                    'url': 'https://some/origin',
                    'type': 'hg',
                }
            }))
        ]

        expected_inputs = [{
            'sha1': bytes.fromhex('34973274ccef6ab4dfaaf86599792fa9c3fe4689'),
        }, {
            'url': 'https://some/origin',
            'type': 'svn',
        }, {
            'visit': 2,
            'origin': {
                'url': 'https://some/origin',
                'type': 'hg'
            },
        }]

        for i, (object_type, obj) in enumerate(inputs):
            actual_value = decode(object_type, obj)
            self.assertEqual(actual_value, expected_inputs[i])
