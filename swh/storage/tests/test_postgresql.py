# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import datetime
import queue
import threading
import time
from unittest.mock import Mock

import attr
import pytest

from swh.model import from_disk
from swh.model.model import Directory, ExtID, Person, Snapshot
from swh.model.swhids import CoreSWHID, ObjectType
from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.storage.tests.storage_tests import TestStorage as _TestStorage
from swh.storage.tests.storage_tests import TestStorageGeneratedData  # noqa
from swh.storage.utils import now


@contextmanager
def db_transaction(storage):
    with storage.db() as db:
        with db.transaction() as cur:
            yield db, cur


@pytest.mark.db
def test_pgstorage_flavor(swh_storage):
    assert swh_storage.get_flavor() == "default"


class TestStorage(_TestStorage):
    @pytest.mark.skip(
        "Directory pagination is not implemented in the postgresql backend yet."
    )
    def test_directory_get_entries_pagination(self):
        pass


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

    def test_directory_add_race(self, swh_storage, sample_data):
        # generate a bunch of similar/identical Directory objects to add
        # concurrently
        directories = []
        entries = sample_data.directory7.to_dict()["entries"]
        for cnt in sample_data.contents:
            fentry = {
                "name": b"the_file",
                "type": "file",
                "target": cnt.sha1_git,
                "perms": from_disk.DentryPerms.content,
            }
            for entry in entries:
                directories.append(
                    Directory.from_dict({"entries": (entry,) + (fentry,)})
                )
            directories.append(Directory.from_dict({"entries": entries + (fentry,)}))
            directories.append(Directory.from_dict({"entries": (fentry,)}))
        directories *= 5

        # used to gather each thread execution result as triplets (thread_id,
        # result_type, payload), payload can be a normal result or an exception
        results = queue.Queue()

        def thread(dirs):
            try:
                with db_transaction(swh_storage) as (db, cur):
                    ret = swh_storage.directory_add(directories=dirs, db=db, cur=cur)
                    # give a chance for another thread to have some cpu
                    time.sleep(0.1)
                results.put((threading.get_ident(), "data", ret))
            except Exception as e:
                results.put((threading.get_ident(), "exc", e))

        ts = [threading.Thread(target=thread, args=([d],)) for d in directories]
        [t.start() for t in ts]
        [t.join() for t in ts]

        rs = [results.get(block=False) for _ in ts]

        with pytest.raises(queue.Empty):
            results.get(block=False)

        for tid, rtype, payload in rs:
            if rtype == "exc":
                # there might be UniqueViolation errors, but only on the
                # directory table, not the directory_entry_xxx ones
                assert (
                    payload.diag.constraint_name == "directory_pkey"
                ), "Got exception %r in Thread%s" % (payload, tid)

        for d in directories:
            entries = tuple(swh_storage.directory_get_entries(d.id).results)
            assert sorted(entries) == sorted(d.entries)


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
            assert content.hashes() in swh_storage.objstorage.objstorage

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
            assert content.hashes() not in swh_storage.objstorage.objstorage
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

    def test_revision_get_displayname_behavior(self, swh_storage, sample_data):
        """Check revision_get behavior when displayname is set"""
        revision, revision2 = sample_data.revisions[:2]

        # Make authors and committers known
        revision = attr.evolve(
            revision,
            author=Person.from_fullname(b"author1 <author1@example.com>"),
            committer=Person.from_fullname(b"committer1 <committer1@example.com>"),
        )
        revision = attr.evolve(revision, id=revision.compute_hash())
        revision2 = attr.evolve(
            revision2,
            author=Person.from_fullname(b"author2 <author2@example.com>"),
            committer=Person.from_fullname(b"committer2 <committer2@example.com>"),
        )
        revision2 = attr.evolve(revision2, id=revision2.compute_hash())

        add_result = swh_storage.revision_add([revision, revision2])
        assert add_result == {"revision:add": 2}

        # Before displayname change
        revisions = swh_storage.revision_get([revision.id, revision2.id])
        assert revisions == [revision, revision2]

        displayname = b"Display Name <displayname@example.com>"

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "UPDATE person set displayname = %s where fullname = %s",
                (displayname, revision.author.fullname),
            )

        revisions = swh_storage.revision_get([revision.id, revision2.id])
        assert revisions == [
            attr.evolve(revision, author=Person.from_fullname(displayname)),
            revision2,
        ]

        revisions = swh_storage.revision_get(
            [revision.id, revision2.id], ignore_displayname=True
        )
        assert revisions == [revision, revision2]

    def test_revision_log_displayname_behavior(self, swh_storage, sample_data):
        """Check revision_log behavior when displayname is set"""
        revision, revision2 = sample_data.revisions[:2]

        # Make authors, committers and parenthood relationship known
        # (revision2 -[parent]-> revision1)
        revision = attr.evolve(
            revision,
            author=Person.from_fullname(b"author1 <author1@example.com>"),
            committer=Person.from_fullname(b"committer1 <committer1@example.com>"),
        )
        revision = attr.evolve(revision, id=revision.compute_hash())
        revision2 = attr.evolve(
            revision2,
            parents=(revision.id,),
            author=Person.from_fullname(b"author2 <author2@example.com>"),
            committer=Person.from_fullname(b"committer2 <committer2@example.com>"),
        )
        revision2 = attr.evolve(revision2, id=revision2.compute_hash())

        add_result = swh_storage.revision_add([revision, revision2])
        assert add_result == {"revision:add": 2}

        # Before displayname change
        revisions = swh_storage.revision_log([revision2.id])
        assert list(revisions) == [revision2.to_dict(), revision.to_dict()]

        displayname = b"Display Name <displayname@example.com>"

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "UPDATE person set displayname = %s where fullname = %s",
                (displayname, revision.author.fullname),
            )

        revisions = swh_storage.revision_log([revision2.id])
        assert list(revisions) == [
            revision2.to_dict(),
            attr.evolve(revision, author=Person.from_fullname(displayname)).to_dict(),
        ]

        revisions = swh_storage.revision_log([revision2.id], ignore_displayname=True)
        assert list(revisions) == [revision2.to_dict(), revision.to_dict()]

    def test_release_get_displayname_behavior(self, swh_storage, sample_data):
        """Check release_get behavior when displayname is set"""
        release, release2 = sample_data.releases[:2]

        # Make authors known
        release = attr.evolve(
            release,
            author=Person.from_fullname(b"author1 <author1@example.com>"),
        )
        release = attr.evolve(release, id=release.compute_hash())
        release2 = attr.evolve(
            release2,
            author=Person.from_fullname(b"author2 <author2@example.com>"),
        )
        release2 = attr.evolve(release2, id=release2.compute_hash())

        add_result = swh_storage.release_add([release, release2])
        assert add_result == {"release:add": 2}

        # Before displayname change
        releases = swh_storage.release_get([release.id, release2.id])
        assert releases == [release, release2]

        displayname = b"Display Name <displayname@example.com>"

        with db_transaction(swh_storage) as (_, cur):
            cur.execute(
                "UPDATE person set displayname = %s where fullname = %s",
                (displayname, release.author.fullname),
            )

        releases = swh_storage.release_get([release.id, release2.id])
        assert releases == [
            attr.evolve(release, author=Person.from_fullname(displayname)),
            release2,
        ]

        releases = swh_storage.release_get(
            [release.id, release2.id], ignore_displayname=True
        )
        assert releases == [release, release2]

    def test_object_references_create_and_list_partition(self, swh_storage):
        with db_transaction(swh_storage) as (db, cur):
            db.object_references_create_partition(year=2020, week=6, cur=cur)
            partitions = db.object_references_list_partitions(cur=cur)

        # We get a partition for this week initialized on schema creation
        assert len(partitions) == 2
        assert partitions[0].table_name == "object_references_2020w06"
        assert partitions[0].year == 2020
        assert partitions[0].week == 6
        assert partitions[0].start == datetime.datetime.fromisoformat("2020-02-03")
        assert partitions[0].end == datetime.datetime.fromisoformat("2020-02-10")
        this_year, this_week = datetime.datetime.now().isocalendar()[0:2]
        assert (
            partitions[1].table_name
            == f"object_references_{this_year:04d}w{this_week:02d}"
        )
        assert partitions[1].year == this_year
        assert partitions[1].week == this_week

    def test_clear_buffers(self, swh_storage):
        """Calling clear buffers on real storage does nothing"""
        assert swh_storage.clear_buffers() is None

    def test_flush(self, swh_storage):
        """Calling clear buffers on real storage does nothing"""
        assert swh_storage.flush() == {}

    def test_check_config(self, swh_storage):
        assert swh_storage.check_config(check_write=True)
        assert swh_storage.check_config(check_write=False)

    def test_check_config_dbversion(self, swh_storage, monkeypatch):
        swh_storage.current_version = -1
        assert swh_storage.check_config(check_write=True) is False
        assert swh_storage.check_config(check_write=False) is False

    def test_object_delete(self, swh_storage, sample_data):
        affected_tables = [
            "origin",
            "origin_visit",
            "origin_visit_status",
            "snapshot",
            # `snapshot_branch` is left out, we leave stale data there by
            # design.
            "snapshot_branches",
            "release",
            "revision",
            "revision_history",
            "directory",
            # `directory_entry_*` are left out on purpose.
            # We leave stale data there by design.
            "skipped_content",
            "content",
            # `metadata_authority` and `metadata_fetcher` are left out
            # for the time being. Tracked as:
            # https://gitlab.softwareheritage.org/swh/devel/swh-alter/-/issues/21
            "raw_extrinsic_metadata",
        ]

        swh_storage.content_add(sample_data.contents)
        swh_storage.skipped_content_add(sample_data.skipped_contents)
        swh_storage.directory_add(sample_data.directories)
        swh_storage.revision_add(sample_data.git_revisions)
        swh_storage.release_add(sample_data.releases)
        swh_storage.snapshot_add(sample_data.snapshots)
        swh_storage.origin_add(sample_data.origins)
        swh_storage.origin_visit_add(sample_data.origin_visits)
        swh_storage.origin_visit_status_add(sample_data.origin_visit_statuses)
        swh_storage.metadata_authority_add(sample_data.authorities)
        swh_storage.metadata_fetcher_add(sample_data.fetchers)
        swh_storage.raw_extrinsic_metadata_add(sample_data.content_metadata)
        swh_storage.raw_extrinsic_metadata_add(sample_data.origin_metadata)
        swhids = (
            [content.swhid().to_extended() for content in sample_data.contents]
            + [
                skipped_content.swhid().to_extended()
                for skipped_content in sample_data.skipped_contents
            ]
            + [directory.swhid().to_extended() for directory in sample_data.directories]
            + [revision.swhid().to_extended() for revision in sample_data.revisions]
            + [release.swhid().to_extended() for release in sample_data.releases]
            + [snapshot.swhid().to_extended() for snapshot in sample_data.snapshots]
            + [origin.swhid() for origin in sample_data.origins]
            + [emd.swhid() for emd in sample_data.content_metadata]
            + [emd.swhid() for emd in sample_data.origin_metadata]
        )

        # Ensure we properly loaded our data
        with db_transaction(swh_storage) as (_, cur):
            for table in affected_tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                assert cur.fetchone()[0] >= 1, f"{table} is not populated"

        result = swh_storage.object_delete(swhids)
        assert result == {
            "content:delete": 3,
            "content:delete:bytes": 0,
            "skipped_content:delete": 2,
            "directory:delete": 7,
            "release:delete": 3,
            "revision:delete": 4,
            "snapshot:delete": 3,
            "origin:delete": 7,
            "origin_visit:delete": 3,
            "origin_visit_status:delete": 3,
            "cnt_metadata:delete": 3,
            "ori_metadata:delete": 3,
        }

        # Ensure we properly removed our data
        with db_transaction(swh_storage) as (_, cur):
            for table in affected_tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                assert cur.fetchone()[0] == 0, f"{table} is not empty"

    def test_extid_delete_for_target(self, swh_storage, sample_data):
        swh_storage.revision_add([sample_data.revision, sample_data.hg_revision])
        swh_storage.directory_add([sample_data.directory, sample_data.directory2])
        extid_for_same_target = ExtID(
            target=CoreSWHID(
                object_type=ObjectType.REVISION, object_id=sample_data.revision.id
            ),
            extid_type="drink_some",
            extid=bytes.fromhex("c0ffee"),
        )
        result = swh_storage.extid_add(sample_data.extids + (extid_for_same_target,))
        assert result == {"extid:add": 5}

        result = swh_storage.extid_delete_for_target(
            [sample_data.revision.swhid(), sample_data.directory2.swhid()]
        )
        assert result == {"extid:delete": 3}

        extids = swh_storage.extid_get_from_target(
            target_type=ObjectType.REVISION, ids=[sample_data.hg_revision.id]
        )
        assert extids == [sample_data.extid2]
        extids = swh_storage.extid_get_from_target(
            target_type=ObjectType.DIRECTORY, ids=[sample_data.directory.id]
        )
        assert extids == [sample_data.extid3]

        result = swh_storage.extid_delete_for_target(
            [sample_data.hg_revision.swhid(), sample_data.directory.swhid()]
        )
        assert result == {"extid:delete": 2}

    def test_delete_snapshot_common_branches(self, swh_storage):
        common_branches = {
            b"branch1": {"target": bytes(20), "target_type": "revision"},
            b"branch2": {"target": bytes(20), "target_type": "release"},
        }

        snapshot1 = Snapshot.from_dict(
            {
                "branches": {
                    **common_branches,
                }
            }
        )
        snapshot2 = Snapshot.from_dict(
            {
                "branches": {
                    **common_branches,
                    b"branch3": {"target": bytes(20), "target_type": "content"},
                }
            }
        )

        swh_storage.snapshot_add([snapshot1, snapshot2])

        assert snapshot1 == snapshot_get_all_branches(swh_storage, snapshot1.id)
        assert snapshot2 == snapshot_get_all_branches(swh_storage, snapshot2.id)

        result = swh_storage.object_delete([snapshot2.swhid().to_extended()])

        assert result == {
            "content:delete": 0,
            "content:delete:bytes": 0,
            "skipped_content:delete": 0,
            "directory:delete": 0,
            "release:delete": 0,
            "revision:delete": 0,
            "snapshot:delete": 1,
            "origin:delete": 0,
            "origin_visit:delete": 0,
            "origin_visit_status:delete": 0,
        }

        assert snapshot1 == snapshot_get_all_branches(swh_storage, snapshot1.id)
        assert snapshot_get_all_branches(swh_storage, snapshot2.id) is None
