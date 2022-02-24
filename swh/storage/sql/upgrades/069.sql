-- SWH DB schema upgrade
-- from_version: 68
-- to_version: 69
-- description: add tables for the archiver.

insert into dbversion(version, release, description)
      values(69, now(), 'Work In Progress');

CREATE DOMAIN archive_id AS TEXT;

CREATE TABLE archives (
  id   archive_id PRIMARY KEY,
  url  TEXT
);

CREATE TYPE archive_status AS ENUM (
  'missing',
  'ongoing',
  'present'
);

CREATE TABLE content_archive (
  content_id  sha1 REFERENCES content(sha1),
  archive_id  archive_id REFERENCES archives(id),
  status      archive_status,
  mtime       timestamptz,
  PRIMARY KEY (content_id, archive_id)
);
