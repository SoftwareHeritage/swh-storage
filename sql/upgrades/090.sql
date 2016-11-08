-- SWH DB schema upgrade
-- from_version: 89
-- to_version: 90
-- description: indexer: Add content_ctags

insert into dbversion(version, release, description)
      values(90, now(), 'Work In Progress');

comment on type content_status is 'Content visibility';
comment on type entity_type is 'Entity types';
comment on type revision_type is 'Possible revision types';
comment on type object_type is 'Data object types stored in data model';
comment on type languages is 'Languages recognized by language indexer';
comment on type ctags_languages is 'Languages recognized by ctags indexer';
