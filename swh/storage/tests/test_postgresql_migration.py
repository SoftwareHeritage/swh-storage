# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import os
import subprocess

import attr
import pytest
from pytest_postgresql import factories

from swh.core.utils import numfile_sortkey as sortkey
from swh.storage import get_storage

from .storage_tests import transform_entries

DIR = os.path.dirname(__file__)


pg_storage_migration_proc = factories.postgresql_proc()
pg_storage_migration = factories.postgresql("pg_storage_migration_proc")


def psql_run_file(dsn, filename):
    subprocess.check_call(
        [
            "psql",
            "--quiet",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            dsn,
            "-f",
            filename,
        ]
    )


@pytest.fixture
def storage(pg_storage_migration):
    for filename in sorted(
        glob.glob(os.path.join(DIR, "data", "sql-v0.18.0", "*.sql"))
    ):
        psql_run_file(pg_storage_migration.dsn, filename)

    config = {
        "cls": "local",
        "db": pg_storage_migration.dsn,
        "objstorage": {"cls": "memory"},
        "check_config": False,  # it would error on the dbversion number
    }
    return get_storage(**config)


@pytest.mark.db
class TestPgStorageMigration:
    """Creates an old schema, inserts some data, runs migrations, and checks the
    data still exists."""

    def _migrate(self, db):
        current_version = db.dbversion()["version"]

        filenames = sorted(
            glob.glob(os.path.join(DIR, "../../../sql/upgrades/*.sql")), key=sortkey,
        )

        nb_migrations = 0

        for filename in filenames:
            (version_str, ext) = os.path.splitext(os.path.basename(filename))
            assert ext == ".sql"
            version = int(version_str)

            if version <= current_version:
                # this migration file is older than the current schema version
                assert nb_migrations == 0
                continue

            nb_migrations += 1
            psql_run_file(db.conn.dsn, filename)

        assert nb_migrations, "no migrations applied"

    @pytest.mark.parametrize("migrate_after_insert", (True, False))
    def test_content(self, storage, sample_data, migrate_after_insert):
        swh_storage = storage
        if migrate_after_insert:
            swh_storage.content_add(sample_data.contents)

        with swh_storage.db() as db:
            self._migrate(db)

        if not migrate_after_insert:
            swh_storage.content_add(sample_data.contents)

        for content in sample_data.contents:
            assert not list(swh_storage.content_missing([content.to_dict()]))
            assert swh_storage.content_get([content.sha1]) == [
                attr.evolve(content, data=None)
            ]

    @pytest.mark.parametrize("migrate_after_insert", (True, False))
    def test_skipped_content(self, storage, sample_data, migrate_after_insert):
        swh_storage = storage
        if migrate_after_insert:
            swh_storage.skipped_content_add(sample_data.skipped_contents)

        with swh_storage.db() as db:
            self._migrate(db)

        if not migrate_after_insert:
            swh_storage.skipped_content_add(sample_data.skipped_contents)

        for skipped_content in sample_data.skipped_contents:
            assert not list(
                swh_storage.skipped_content_missing([skipped_content.to_dict()])
            )

    @pytest.mark.parametrize("migrate_after_insert", (True, False))
    def test_directory(self, storage, sample_data, migrate_after_insert):
        swh_storage = storage
        if migrate_after_insert:
            swh_storage.directory_add(sample_data.directories)
            swh_storage.content_add(sample_data.contents)

        with swh_storage.db() as db:
            self._migrate(db)

        if not migrate_after_insert:
            swh_storage.directory_add(sample_data.directories)
            swh_storage.content_add(sample_data.contents)

        for directory in sample_data.directories:
            assert not list(swh_storage.directory_missing([directory.id]))

            actual_data = list(swh_storage.directory_ls(directory.id))
            expected_data = list(transform_entries(storage, directory))

            for data in actual_data:
                assert data in expected_data

    @pytest.mark.parametrize("migrate_after_insert", (True, False))
    def test_revision(self, storage, sample_data, migrate_after_insert):
        swh_storage = storage
        if migrate_after_insert:
            swh_storage.revision_add(sample_data.revisions)

        with swh_storage.db() as db:
            self._migrate(db)

        if not migrate_after_insert:
            swh_storage.revision_add(sample_data.revisions)

        for revision in sample_data.revisions:
            assert not list(swh_storage.revision_missing([revision.id]))
            assert swh_storage.revision_get([revision.id]) == [revision]

    @pytest.mark.parametrize("migrate_after_insert", (True, False))
    def test_release(self, storage, sample_data, migrate_after_insert):
        swh_storage = storage
        if migrate_after_insert:
            swh_storage.release_add(sample_data.releases)

        with swh_storage.db() as db:
            self._migrate(db)

        if not migrate_after_insert:
            swh_storage.release_add(sample_data.releases)

        for release in sample_data.releases:
            assert not list(swh_storage.release_missing([release.id]))
            assert swh_storage.release_get([release.id]) == [release]

    @pytest.mark.parametrize("migrate_after_insert", (True, False))
    def test_snapshot(self, storage, sample_data, migrate_after_insert):
        swh_storage = storage
        if migrate_after_insert:
            swh_storage.snapshot_add(sample_data.snapshots)

        with swh_storage.db() as db:
            self._migrate(db)

        if not migrate_after_insert:
            swh_storage.snapshot_add(sample_data.snapshots)

        for snapshot in sample_data.snapshots:
            assert not list(swh_storage.snapshot_missing([snapshot.id]))
            assert swh_storage.snapshot_get(snapshot.id) == {
                **snapshot.to_dict(),
                "next_branch": None,
            }
