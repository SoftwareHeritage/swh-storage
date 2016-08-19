-- SWH DB schema upgrade
-- from_version: 1
-- to_version: 2
-- description: Add a 'corrupted' status into the archive_content status

INSERT INTO dbversion(version, release, description)
VALUES(2, now(), 'Work In Progress');

ALTER TYPE archive_status ADD VALUE 'corrupted';
