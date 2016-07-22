-- SWH DB schema upgrade
-- from_version: 69
-- to_version: 70
-- description: Drop the archiver's related tables in main schema (move to its own database)

INSERT INTO dbversion(version, release, description)
VALUES(70, now(), 'Work In Progress');

DROP TABLE content_archive;

DROP TABLE archives;

DROP TYPE archive_status;

DROP DOMAIN archive_id;
