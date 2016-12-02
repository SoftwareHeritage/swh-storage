-- SWH DB schema upgrade
-- from_version: 96
-- to_version: 97
-- description: Update indexer configuration

insert into dbversion(version, release, description)
      values(97, now(), 'Work In Progress');

------------------------
-- Update Schema + data
------------------------

update indexer_configuration
set tool_configuration='{"command_line": "nomossa <filepath>"}'
where tool_name='nomos' and tool_version='3.1.0rc2-31-ga2cbb8c';

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('universal-ctags', '~git7859817b', '{"command_line": "ctags --fields=+lnz --sort=no --links=no --output-format=json <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('file', '5.22', '{"command_line": "file --mime <filepath>"}');

-- ctags

alter table content_ctags
  add column indexer_configuration_id bigserial;

comment on column content_ctags.indexer_configuration_id is 'Tool used to compute the information';

update content_ctags
set indexer_configuration_id = (select id
                                from indexer_configuration
                                where tool_name='universal-ctags');

alter table content_ctags
  alter column indexer_configuration_id set not null;

alter table content_ctags
  add constraint content_ctags_indexer_configuration_id_idx
  foreign key (indexer_configuration_id) references indexer_configuration(id);

drop index content_ctags_id_md5_kind_line_lang_idx;
create unique index on content_ctags(id, md5(name), kind, line, lang, indexer_configuration_id);

-- language

alter table content_language
  add column indexer_configuration_id bigserial;

comment on column content_language.indexer_configuration_id is 'Tool used to compute the information';

update content_language
set indexer_configuration_id = (select id from indexer_configuration where tool_name='pygments');

alter table content_language
  alter column indexer_configuration_id set not null;

alter table content_language
  add constraint content_language_indexer_configuration_id_idx
  foreign key (indexer_configuration_id) references indexer_configuration(id);

alter table content_language
  drop constraint content_language_pkey;

alter table content_language
  add primary key(id, indexer_configuration_id);

-- mimetype

alter table content_mimetype
  add column indexer_configuration_id bigserial;

comment on column content_mimetype.indexer_configuration_id is 'Tool used to compute the information';

update content_mimetype
set indexer_configuration_id = (select id from indexer_configuration where tool_name='file');

alter table content_mimetype
  alter column indexer_configuration_id
  set not null;

alter table content_mimetype
  add constraint content_mimetype_indexer_configuration_id_idx
  foreign key (indexer_configuration_id) references indexer_configuration(id);

alter table content_mimetype
  drop constraint content_mimetype_pkey;

alter table content_mimetype
  add primary key(id, indexer_configuration_id);

-- fossology-license

comment on column content_fossology_license.indexer_configuration_id is 'Tool used to compute the information';

alter table content_fossology_license
  alter column indexer_configuration_id
  set not null;

alter table content_fossology_license
  add primary key using index content_fossology_license_id_license_id_indexer_configurati_idx;

