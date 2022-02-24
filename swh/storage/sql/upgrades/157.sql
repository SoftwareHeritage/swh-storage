-- SWH DB schema upgrade
-- from_version: 156
-- to_version: 157
-- description: Add extrinsic artifact metadata

-- latest schema version
insert into dbversion(version, release, description)
      values(157, now(), 'Work In Progress');

create domain swhid as text check (value ~ '^swh:[0-9]+:.*');

-- Extrinsic metadata on a DAG objects and origins.
create table object_metadata
(
  type           text          not null,
  id             text          not null,

  -- metadata source
  authority_id   bigint        not null,
  fetcher_id     bigint        not null,
  discovery_date timestamptz   not null,

  -- metadata itself
  format         text          not null,
  metadata       bytea         not null,

  -- context
  origin         text,
  visit          bigint,
  snapshot       swhid,
  release        swhid,
  revision       swhid,
  path           bytea,
  directory      swhid
);

comment on table object_metadata is 'keeps all metadata found concerning an object';
comment on column object_metadata.type is 'the type of object (content/directory/revision/release/snapshot/origin) the metadata is on';
comment on column object_metadata.id is 'the SWHID or origin URL for which the metadata was found';
comment on column object_metadata.discovery_date is 'the date of retrieval';
comment on column object_metadata.authority_id is 'the metadata provider: github, openhub, deposit, etc.';
comment on column object_metadata.fetcher_id is 'the tool used for extracting metadata: loaders, crawlers, etc.';
comment on column object_metadata.format is 'name of the format of metadata, used by readers to interpret it.';
comment on column object_metadata.metadata is 'original metadata in opaque format';

-- migrate data from origin_metadata
insert into object_metadata(type, id, authority_id, fetcher_id, discovery_date, format, metadata)
	select 'origin', (select url from origin o where o.id = om.origin_id), authority_id, fetcher_id, discovery_date, format, metadata,
	from origin_metadata om;

create unique index object_metadata_content_authority_date_fetcher
  on object_metadata(id, authority_id, discovery_date, fetcher_id);

alter table object_metadata
  add constraint object_metadata_authority_fkey
  foreign key (authority_id) references metadata_authority(id) not valid;

alter table object_metadata
  validate constraint object_metadata_authority_fkey;

alter table object_metadata
  add constraint object_metadata_fetcher_fkey
  foreign key (fetcher_id) references metadata_fetcher(id) not valid;

alter table object_metadata
  validate constraint object_metadata_fetcher_fkey;
