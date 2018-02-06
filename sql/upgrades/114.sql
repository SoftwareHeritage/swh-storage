-- SWH DB schema upgrade
-- from_version: 113
-- to_version: 114
-- description: Delete indexer's data model and data

insert into dbversion(version, release, description)
values(114, now(), 'Work In Progress');

-- Tools

create table tool (
  id serial not null,
  name text not null,
  version text not null,
  configuration jsonb
);

comment on table tool is 'Tool information';
comment on column tool.id is 'Tool identifier';
comment on column tool.name is 'Tool name';
comment on column tool.version is 'Tool version';
comment on column tool.configuration is 'Tool configuration: command line, flags, etc...';

create unique index tool_pkey on tool(id);
alter table tool add primary key using index tool_pkey;

create unique index on tool(tool_name, tool_version, tool_configuration);

alter table origin_metadata add constraint origin_metadata_tool_key foreign key (tool_id) references tool(id) not valid;

create or replace function swh_mktemp_tool()
    returns void
    language sql
as $$
    create temporary table tmp_tool (
      like tool including defaults
    ) on commit drop;
    alter table tmp_tool drop column id;
$$;

create or replace function swh_tool_add()
    returns setof tool
    language plpgsql
as $$
begin
      insert into tool(name, version, configuration)
      select name, version, configuration from tmp_tool tmp
      on conflict(name, version, configuration) do nothing;

      return query
          select id, name, version, configuration
          from tmp_tool join tool
              using(name, version, configuration);

      return;
end
$$;

-- clean up

drop table content_fossology_license cascade;
drop table content_mimetype cascade;
drop table content_language cascade;
drop table content_ctags cascade;
drop table revision_metadata cascade;
drop table content_metadata cascade;
drop table fossology_license cascade;

drop type content_language_signature cascade;
drop type content_mimetype_signature cascade;
drop type content_ctags_signature cascade;
drop type content_fossology_license_signature cascade;
drop type content_metadata_signature cascade;
drop type revision_metadata_signature cascade;

drop type languages cascade;
drop type ctags_languages cascade;

alter table origin_metadata drop constraint origin_metadata_tool_fkey;

drop table indexer_configuration cascade;
