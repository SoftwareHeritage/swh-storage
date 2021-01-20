# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import queue
import threading
from unittest.mock import Mock

import attr
import pytest

from swh.storage.postgresql.db import Db
from swh.storage.tests.storage_tests import TestStorage  # noqa
from swh.storage.tests.storage_tests import TestStorageGeneratedData  # noqa
from swh.storage.utils import now


@contextmanager
def db_transaction(storage):
    with storage.db() as db:
        with db.transaction() as cur:
            yield db, cur


@pytest.mark.db
class TestLocalStorage:
    """Test the local storage"""

    # This test is only relevant on the local storage, with an actual
    # objstorage raising an exception
    def test_content_add_objstorage_exception(self, swh_storage, sample_data):
        content = sample_data.content

        swh_storage.objstorage.content_add = Mock(
            side_effect=Exception("mocked broken objstorage")
        )

        with pytest.raises(Exception, match="mocked broken"):
            swh_storage.content_add([content])

        missing = list(swh_storage.content_missing([content.hashes()]))
        assert missing == [content.sha1]


@pytest.mark.db
class TestStorageRaceConditions:
    @pytest.mark.xfail
    def test_content_add_race(self, swh_storage, sample_data):
        content = attr.evolve(sample_data.content, ctime=now())

        results = queue.Queue()

        def thread():
            try:
                with db_transaction(swh_storage) as (db, cur):
                    ret = swh_storage._content_add_metadata(db, cur, [content])
                results.put((threading.get_ident(), "data", ret))
            except Exception as e:
                results.put((threading.get_ident(), "exc", e))

        t1 = threading.Thread(target=thread)
        t2 = threading.Thread(target=thread)
        t1.start()
        # this avoids the race condition
        # import time
        # time.sleep(1)
        t2.start()
        t1.join()
        t2.join()

        r1 = results.get(block=False)
        r2 = results.get(block=False)

        with pytest.raises(queue.Empty):
            results.get(block=False)
        assert r1[0] != r2[0]
        assert r1[1] == "data", "Got exception %r in Thread%s" % (r1[2], r1[0])
        assert r2[1] == "data", "Got exception %r in Thread%s" % (r2[2], r2[0])


@pytest.mark.db
class TestPgStorage:
    """This class is dedicated for the rare case where the schema needs to
       be altered dynamically.

       Otherwise, the tests could be blocking when ran altogether.

    """

    def test_content_update_with_new_cols(self, swh_storage, sample_data):
        content, content2 = sample_data.contents[:2]

        swh_storage.journal_writer.journal = None  # TODO, not supported

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                """alter table content
                           add column test text default null,
                           add column test2 text default null"""
            )

        swh_storage.content_add([content])

        cont = content.to_dict()
        cont["test"] = "value-1"
        cont["test2"] = "value-2"

        swh_storage.content_update([cont], keys=["test", "test2"])
        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                """SELECT sha1, sha1_git, sha256, length, status,
                   test, test2
                   FROM content WHERE sha1 = %s""",
                (cont["sha1"],),
            )

            datum = cur.fetchone()

        assert datum == (
            cont["sha1"],
            cont["sha1_git"],
            cont["sha256"],
            cont["length"],
            "visible",
            cont["test"],
            cont["test2"],
        )

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                """alter table content drop column test,
                                               drop column test2"""
            )

    def test_content_add_db(self, swh_storage, sample_data):
        content = sample_data.content

        actual_result = swh_storage.content_add([content])

        assert actual_result == {
            "content:add": 1,
            "content:add:bytes": content.length,
        }

        if hasattr(swh_storage, "objstorage"):
            assert content.sha1 in swh_storage.objstorage.objstorage

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "SELECT sha1, sha1_git, sha256, length, status"
                " FROM content WHERE sha1 = %s",
                (content.sha1,),
            )
            datum = cur.fetchone()

        assert datum == (
            content.sha1,
            content.sha1_git,
            content.sha256,
            content.length,
            "visible",
        )

        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        assert contents[0] == attr.evolve(content, data=None)

    def test_content_add_metadata_db(self, swh_storage, sample_data):
        content = attr.evolve(sample_data.content, data=None, ctime=now())

        actual_result = swh_storage.content_add_metadata([content])

        assert actual_result == {
            "content:add": 1,
        }

        if hasattr(swh_storage, "objstorage"):
            assert content.sha1 not in swh_storage.objstorage.objstorage
        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "SELECT sha1, sha1_git, sha256, length, status"
                " FROM content WHERE sha1 = %s",
                (content.sha1,),
            )
            datum = cur.fetchone()
        assert datum == (
            content.sha1,
            content.sha1_git,
            content.sha256,
            content.length,
            "visible",
        )

        contents = [
            obj
            for (obj_type, obj) in swh_storage.journal_writer.journal.objects
            if obj_type == "content"
        ]
        assert len(contents) == 1
        assert contents[0] == content

    def test_skipped_content_add_db(self, swh_storage, sample_data):
        content, cont2 = sample_data.skipped_contents[:2]
        content2 = attr.evolve(cont2, blake2s256=None)

        actual_result = swh_storage.skipped_content_add([content, content, content2])

        assert 2 <= actual_result.pop("skipped_content:add") <= 3
        assert actual_result == {}

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "SELECT sha1, sha1_git, sha256, blake2s256, "
                "length, status, reason "
                "FROM skipped_content ORDER BY sha1_git"
            )

            dbdata = cur.fetchall()

        assert len(dbdata) == 2
        assert dbdata[0] == (
            content.sha1,
            content.sha1_git,
            content.sha256,
            content.blake2s256,
            content.length,
            "absent",
            "Content too long",
        )

        assert dbdata[1] == (
            content2.sha1,
            content2.sha1_git,
            content2.sha256,
            content2.blake2s256,
            content2.length,
            "absent",
            "Content too long",
        )

    def test_clear_buffers(self, swh_storage):
        """Calling clear buffers on real storage does nothing

        """
        assert swh_storage.clear_buffers() is None

    def test_flush(self, swh_storage):
        """Calling clear buffers on real storage does nothing

        """
        assert swh_storage.flush() == {}

    def test_dbversion(self, swh_storage):
        with swh_storage.db() as db:
            assert db.check_dbversion()

    def test_dbversion_mismatch(self, swh_storage, monkeypatch):
        monkeypatch.setattr(Db, "current_version", -1)
        with swh_storage.db() as db:
            assert db.check_dbversion() is False

    def test_check_config(self, swh_storage):
        assert swh_storage.check_config(check_write=True)
        assert swh_storage.check_config(check_write=False)

    def test_check_config_dbversion(self, swh_storage, monkeypatch):
        monkeypatch.setattr(Db, "current_version", -1)
        assert swh_storage.check_config(check_write=True) is False
        assert swh_storage.check_config(check_write=False) is False
