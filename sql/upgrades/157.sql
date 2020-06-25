-- SWH DB schema upgrade
-- from_version: 156
-- to_version: 157
-- description: Add extrinsic artifact metadata

-- latest schema version
insert into dbversion(version, release, description)
      values(157, now(), 'Work In Progress');

create domain swhid as text check (value ~ '^swh:[0-9]+:.*');

alter table origin_metadata
  rename to object_metadata;


-- Use the origin URL as identifier, instead of the origin id
alter table object_metadata
  add column type text;
comment on column object_metadata.type is 'the type of object (content/directory/revision/release/snapshot/origin) the metadata is on';

alter table object_metadata
  add column origin_url text;

update object_metadata
  set
    type = 'origin',
    origin_url = origin.url
  from origin
  where object_metadata.origin_id = origin.id;

alter table object_metadata
  alter column type set not null;
alter table object_metadata
  alter column origin_url set not null;

alter table object_metadata
  drop column id;
alter table object_metadata
  drop column origin_id;

alter table object_metadata
  rename column origin_url to id;
comment on column object_metadata.id is 'the SWHID or origin URL for which the metadata was found';

create unique index object_metadata_content_authority_date_fetcher
  on object_metadata(id, authority_id, discovery_date, fetcher_id);


-- Add context columns
alter table object_metadata
  add column origin text;
alter table object_metadata
  add column visit bigint;
alter table object_metadata
  add column snapshot swhid;
alter table object_metadata
  add column release swhid;
alter table object_metadata
  add column revision swhid;
alter table object_metadata
  add column path bytea;
alter table object_metadata
  add column directory swhid;
