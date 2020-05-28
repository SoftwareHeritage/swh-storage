-- SWH DB schema upgrade
-- from_version: 149
-- to_version: 150
-- description: Add not null values with default values

-- latest schema version
insert into dbversion(version, release, description)
      values(150, now(), 'Work In Progress');

update metadata_authority
set metadata='{}'::jsonb where metadata is null;

alter table metadata_authority
  alter column metadata set not null;

alter table origin_metadata
  alter column format set not null;

alter table origin_metadata
  alter column format set default 'sword-v2-atom-codemeta-v2-in-json';

alter table origin_metadata
  alter column metadata set not null;
