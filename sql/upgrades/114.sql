-- SWH DB schema upgrade
-- from_version: 113
-- to_version: 114
-- description: Delete indexer's data model and data

insert into dbversion(version, release, description)
values(114, now(), 'Work In Progress');

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

alter table origin_metadata add constraint origin_metadata_tool_key foreign key (tool_id) references tool(id) not valid;

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
