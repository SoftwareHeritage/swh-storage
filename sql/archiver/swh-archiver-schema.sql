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
VALUES(10, now(), 'Work In Progress');

CREATE TABLE archive (
  id bigserial PRIMARY KEY,
  name text not null
);

create unique index on archive(name);

comment on table archive is 'The archives in which contents are stored';
comment on column archive.id is 'Short identifier for archives';
comment on column archive.name is 'Name of the archive';

CREATE TYPE archive_status AS ENUM (
  'missing',
  'ongoing',
  'present',
  'corrupted'
);

comment on type archive_status is 'Status of a given copy of a content';

-- a SHA1 checksum (not necessarily originating from Git)
CREATE DOMAIN sha1 AS bytea CHECK (LENGTH(VALUE) = 20);

-- a bucket for which we count items
CREATE DOMAIN bucket AS bytea CHECK (LENGTH(VALUE) = 2);

create table content (
  id bigserial primary key,
  sha1 sha1 not null
);

comment on table content is 'All the contents being archived by Software Heritage';
comment on column content.id is 'Short id for the content being archived';
comment on column content.sha1 is 'SHA1 hash of the content being archived';

create unique index on content(sha1);

create table content_copies (
  content_id bigint not null, -- references content(id)
  archive_id bigint not null, -- references archive(id)
  mtime timestamptz,
  status archive_status not null,
  primary key (content_id, archive_id)
);

comment on table content_copies is 'Tracking of all content copies in the archives';
comment on column content_copies.mtime is 'Last update time of the copy';
comment on column content_copies.status is 'Status of the copy';
