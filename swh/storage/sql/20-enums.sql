---
--- Software Heritage Data Types
---

create type content_status as enum ('absent', 'visible', 'hidden');
comment on type content_status is 'Content visibility';

create type revision_type as enum ('git', 'tar', 'dsc', 'svn', 'hg', 'cvs', 'bzr');
comment on type revision_type is 'Possible revision types';

create type object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot');
comment on type object_type is 'Data object types stored in data model';

create type snapshot_target as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'alias');
comment on type snapshot_target is 'Types of targets for snapshot branches';

create type origin_visit_state as enum (
  'created',
  'ongoing',
  'full',
  'partial',
  'not_found',
  'failed'
);
comment on type origin_visit_state IS 'Possible origin visit status values';

create type extended_object_type as enum ('content', 'directory', 'revision', 'release', 'snapshot', 'origin', 'raw_extrinsic_metadata');
comment on type extended_object_type is 'Data object types stored in data model';
