create type extended_object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'origin', 'raw_extrinsic_metadata');
comment on type extended_object_type is 'Data object types stored in data model';
