-- SWH DB schema upgrade
-- from_version: 96
-- to_version: 97
-- description: Update indexer configuration

insert into dbversion(version, release, description)
      values(97, now(), 'Work In Progress');

update indexer_configuration
set tool_configuration='{"command_line": "nomossa <filepath>"}'
where tool_name='nomos' and tool_version='3.1.0rc2-31-ga2cbb8c';

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('universal-ctags', '~git7859817b', '{"command_line": "ctags --fields=+lnz --sort=no --links=no --output-format=json <filepath>"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments"}');

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('file', '5.22', '{"command_line": "file --mime <filepath>"}');

alter table content_ctags add column indexer_configuration_id bigserial;

update content_ctags
set indexer_configuration_id = (select id from indexer_configuration where tool_name='universal-ctags');

alter table content_ctags
add constraint content_ctags_indexer_configuration_id_idx
foreign key (indexer_configuration_id) references indexer_configuration(id);

alter table content_language add column indexer_configuration_id bigserial;

update content_language
set indexer_configuration_id = (select id from indexer_configuration where tool_name='pygments');

alter table content_language
add constraint content_language_indexer_configuration_id_idx
foreign key (indexer_configuration_id) references indexer_configuration(id);

alter table content_mimetype add column indexer_configuration_id bigserial;

update content_mimetype
set indexer_configuration_id = (select id from indexer_configuration where tool_name='file');

alter table content_mimetype
add constraint content_mimetype_indexer_configuration_id_idx
foreign key (indexer_configuration_id) references indexer_configuration(id);
