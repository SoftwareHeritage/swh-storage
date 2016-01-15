-- SWH DB schema upgrade
-- from_version: 39
-- to_version: 40
-- description: Open entity get by uuid

insert into dbversion(version, release, description)
      values(40, now(), 'Work In Progress');

create or replace function swh_entity_get(entity_uuid uuid)
    returns setof entity
    language sql
    stable
as $$
  with recursive entity_hierarchy as (
  select e.*
    from entity e where uuid = entity_uuid
    union
    select p.*
    from entity_hierarchy e
    join entity p on e.parent = p.uuid
  )
  select *
  from entity_hierarchy;
$$;
