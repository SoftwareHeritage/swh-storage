-- SWH DB schema upgrade
-- from_version: 86
-- to_version: 87
-- description: indexer: Add indexer's new content properties table

insert into dbversion(version, release, description)
      values(87, now(), 'Work In Progress');

-- Properties (mimetype, encoding, etc...)
create table content_mimetype (
id sha1 primary key references content(sha1) not null,
  mimetype bytea not null,
  encoding bytea not null
);

comment on table content_mimetype is 'Metadata associated to a raw content';
comment on column content_mimetype.mimetype is 'Raw content Mimetype';
comment on column content_mimetype.encoding is 'Raw content encoding';
