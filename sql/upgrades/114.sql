-- SWH DB schema upgrade
-- from_version: 113
-- to_version: 114
-- description: Delete indexer's data model and data

drop table fossology_license;
drop table content_mimetype;
drop table content_language;
drop table content_ctags;
drop table content_fossology_license;
drop table revision_metadata;
drop table content_metadata;
drop table indexer_configuration;

drop type ctags_languages;
drop type languages;

-- Tools

create table tool (
  id serial not null,
  tool_name text not null,
  tool_version text not null,
  tool_configuration jsonb
);

comment on table tool is 'Tool information';
comment on column tool.id is 'Tool identifier';
comment on column tool.tool_version is 'Tool name';
comment on column tool.tool_version is 'Tool version';
comment on column tool.tool_configuration is 'Tool configuration: command line, flags, etc...';

create unique index tool_pkey on tool(id);
alter table tool add primary key using index tool_pkey;

create unique index on tool(tool_name, tool_version, tool_configuration);
