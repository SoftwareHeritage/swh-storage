-- SWH DB schema upgrade
-- from_version: 188
-- to_version: 189
-- description: Add extended_object_type enum, use it in object_references

create type extended_object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'origin', 'raw_extrinsic_metadata');
comment on type extended_object_type is 'Data object types stored in data model, with an extended SWHID';

alter table object_references
  alter column source_type type extended_object_type using source_type::text::extended_object_type,
  alter column target_type type extended_object_type using target_type::text::extended_object_type;
