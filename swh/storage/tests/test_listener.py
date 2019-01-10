# Copyright (C) 2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import os
import unittest
import unittest.mock

import pytest

from swh.core.tests.db_testing import SingleDbTestFixture
from swh.storage.tests.storage_testing import StorageTestFixture
from swh.storage.tests.test_storage import TestStorageData
import swh.storage.listener as listener
from swh.storage.db import Db
from . import SQL_DIR


@pytest.mark.db
class ListenerTest(StorageTestFixture, SingleDbTestFixture,
                   TestStorageData, unittest.TestCase):
    TEST_DB_NAME = 'softwareheritage-test-storage'
    TEST_DB_DUMP = os.path.join(SQL_DIR, '*.sql')

    def setUp(self):
        super().setUp()
        self.db = Db(self.conn)

    def tearDown(self):
        self.db.conn.close()
        super().tearDown()

    def test_notify(self):
        class MockProducer:
            def send(self, topic, value):
                sent.append((topic, value))

            def flush(self):
                pass

        listener.register_all_notifies(self.db)

        # Add an origin and an origin visit
        origin_id = self.storage.origin_add_one(self.origin)
        visit = self.storage.origin_visit_add(origin_id, date=self.date_visit1)
        visit_id = visit['visit']

        sent = []
        listener.run_once(self.db, MockProducer(), 'swh.tmp_journal.new', 10)
        self.assertEqual(sent, [
            ('swh.tmp_journal.new.origin',
             {'type': 'git', 'url': 'file:///dev/null'}),
            ('swh.tmp_journal.new.origin_visit',
             {'origin': 1, 'visit': 1}),
        ])

        # Update the status of the origin visit
        self.storage.origin_visit_update(origin_id, visit_id, status='full')

        sent = []
        listener.run_once(self.db, MockProducer(), 'swh.tmp_journal.new', 10)
        self.assertEqual(sent, [
            ('swh.tmp_journal.new.origin_visit',
             {'origin': 1, 'visit': 1}),
        ])


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
            actual_value = listener.decode(object_type, obj)
            self.assertEqual(actual_value, expected_inputs[i])
