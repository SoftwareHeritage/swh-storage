-- SWH DB schema upgrade
-- from_version: 129
-- to_version: 130
-- description: Remove origin_visit update from snapshot_add

insert into dbversion(version, release, description)
      values(130, now(), 'Work In Progress');

create or replace function swh_snapshot_add(snapshot_id snapshot.id%type)
  returns void
  language plpgsql
as $$
declare
  snapshot_object_id snapshot.object_id%type;
begin
  select object_id from snapshot where id = snapshot_id into snapshot_object_id;
  if snapshot_object_id is null then
     insert into snapshot (id) values (snapshot_id) returning object_id into snapshot_object_id;
     insert into snapshot_branch (name, target_type, target)
       select name, target_type, target from tmp_snapshot_branch tmp
       where not exists (
         select 1
         from snapshot_branch sb
         where sb.name = tmp.name
           and sb.target = tmp.target
           and sb.target_type = tmp.target_type
       )
       on conflict do nothing;
     insert into snapshot_branches (snapshot_id, branch_id)
     select snapshot_object_id, sb.object_id as branch_id
       from tmp_snapshot_branch tmp
       join snapshot_branch sb
       using (name, target, target_type)
       where tmp.target is not null and tmp.target_type is not null
     union
     select snapshot_object_id, sb.object_id as branch_id
       from tmp_snapshot_branch tmp
       join snapshot_branch sb
       using (name)
       where tmp.target is null and tmp.target_type is null
         and sb.target is null and sb.target_type is null;
  end if;
end;
$$;

