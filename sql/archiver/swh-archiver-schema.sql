-- In order to archive the content of the object storage, add
-- some tables to keep trace of what have already been archived.

create table dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

comment on table dbversion is 'Schema update tracking';

INSERT INTO dbversion(version, release, description)
VALUES(1, now(), 'Work In Progress');

CREATE TYPE archive_id AS ENUM (
  'uffizi',
  'banco'
);

CREATE TABLE archive (
  id   archive_id PRIMARY KEY,
  url  TEXT
);

comment on table archive is 'Possible archives';
comment on column archive.id is 'Short identifier for the archive';
comment on column archive.url is 'Url identifying the archiver api';

CREATE TYPE archive_status AS ENUM (
  'missing',
  'ongoing',
  'present'
);

comment on type archive_status is 'Status of a given archive';

-- a SHA1 checksum (not necessarily originating from Git)
CREATE DOMAIN sha1 AS bytea CHECK (LENGTH(VALUE) = 20);

CREATE TABLE content_archive (
  content_id sha1 unique,
  copies jsonb
);

comment on table content_archive is 'Referencing the status and whereabouts of a content';
comment on column content_archive.content_id is 'content identifier';
comment on column content_archive.copies is 'map archive_id -> { "status": archive_status, "mtime": epoch timestamp }'
