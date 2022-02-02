-- SWH DB schema upgrade
-- from_version: 148
-- to_version: 149
-- description: Implement extrinsic origin-metadata specification

-- latest schema version
insert into dbversion(version, release, description)
      values(149, now(), 'Work In Progress');

-- metadata_fetcher

alter table tool
    rename to metadata_fetcher;
comment on table metadata_fetcher is 'Tools used to retrieve metadata';

alter table metadata_fetcher
    rename column configuration to metadata;

comment on column metadata_fetcher.id is 'Internal identifier of the fetcher';
comment on column metadata_fetcher.name is 'Fetcher name';
comment on column metadata_fetcher.version is 'Fetcher version';
comment on column metadata_fetcher.metadata is 'Extra information about the fetcher';

alter index tool_pkey
    rename to metadata_fetcher_pkey;
create unique index metadata_fetcher_name_version
    on metadata_fetcher(name, version);

drop index tool_tool_name_tool_version_tool_configuration_idx;
  -- was an index on (name, version, configuration)
  -- this is the name in production; in new setups it would be called tool_name_version_configuration_idx
  -- drop index tool_name_version_configuration_idx;

-- metadata_authority

alter table metadata_provider
    rename to metadata_authority;
comment on table metadata_authority is 'Metadata authority information';

drop index metadata_provider_provider_name_provider_url_idx;
  -- was an index on (provider_name, provider_url)

alter table metadata_authority
    drop column provider_name;
alter table metadata_authority
    rename column provider_type to type;
alter table metadata_authority
    rename column provider_url to url;

comment on column metadata_authority.id is 'Internal identifier of the authority';
comment on column metadata_authority.type is 'Type of authority (deposit/forge/registry)';
comment on column metadata_authority.url is 'Authority''s uri';
comment on column metadata_authority.metadata is 'Other metadata about authority';

alter index metadata_provider_pkey
    rename to metadata_authority_pkey;
alter index metadata_provider_type_url
    rename to metadata_authority_type_url;

-- origin_metadata

alter table origin_metadata
    rename column provider_id to authority_id;
alter table origin_metadata
    rename column tool_id to fetcher_id;
alter table origin_metadata
    add column format text default 'sword-v2-atom-codemeta-v2-in-json';
alter table origin_metadata
    rename column metadata to metadata_jsonb;
alter table origin_metadata
    add column metadata bytea;

-- migrates metadata_jsonb (a jsonb) to metadata (a bytea)
--update origin_metadata
--    set metadata=metadata_jsonb::text::bytea;
update origin_metadata
    set metadata=convert_to(metadata_jsonb::text, 'utf-8');

create index origin_metadata_origin_authority_date
    on origin_metadata(origin_id, authority_id, discovery_date);

drop index origin_metadata_origin_id_provider_id_tool_id_idx;
  -- was an index on (origin_id, provider_id, tool_id)

alter table origin_metadata
    drop column metadata_jsonb;

comment on column origin_metadata.authority_id is 'the metadata provider: github, openhub, deposit, etc.';
comment on column origin_metadata.fetcher_id is 'the tool used for extracting metadata: loaders, crawlers, etc.';
comment on column origin_metadata.format is 'name of the format of metadata, used by readers to interpret it.';
comment on column origin_metadata.metadata is 'original metadata in opaque format';


-- cleanup unused functions

drop function swh_mktemp_tool;
drop function swh_tool_add;

drop function swh_origin_metadata_get_by_origin(text);
drop function swh_origin_metadata_get_by_provider_type(text, text);

drop function swh_origin_metadata_get_by_origin(int);
drop function swh_origin_metadata_get_by_provider_type(int, text);

drop type origin_metadata_signature;
