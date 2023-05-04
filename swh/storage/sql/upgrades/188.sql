-- SWH DB schema upgrade
-- from_version: 187
-- to_version: 188
-- description: Add object_references table


create table object_references
(
  insertion_date date not null default now(),
  source_type object_type not null,
  source sha1_git not null,
  target_type object_type not null,
  target sha1_git not null
);

comment on table object_references is 'Recent edges from the object (source_type, source) to the object (target_type, target) inserted in the archive';
comment on column object_references.insertion_date is 'Date that the edge was inserted in the archive';
comment on column object_references.source_type is 'Object type for the source of the edge';
comment on column object_references.source is 'Object id for the source of the edge';
comment on column object_references.target_type is 'Object type for the target of the edge';
comment on column object_references.target is 'Object id for the target of the edge';

-- object_references

create unique index concurrently object_references_pkey on object_references(target_type, target, source_type, source, insertion_date);
alter table object_references add primary key using object_references_pkey;
