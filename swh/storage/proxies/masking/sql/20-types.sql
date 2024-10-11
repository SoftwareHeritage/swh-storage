create type extended_object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'origin', 'raw_extrinsic_metadata');
comment on type extended_object_type is 'Data object types stored in data model';

create type masked_state as enum ('visible', 'decision_pending', 'restricted');
comment on type masked_state is 'The degree to which an object is masked';
