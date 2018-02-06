-- SWH DB schema upgrade
-- from_version: 116
-- to_version: 117
-- description: update swh_snapshot_add to hit more indexes

insert into dbversion(version, release, description)
      values(117, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_snapshot_add(origin bigint, visit bigint, snapshot_id sha1_git) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
  update origin_visit ov
    set snapshot_id = snapshot_object_id
    where ov.origin=swh_snapshot_add.origin and ov.visit=swh_snapshot_add.visit;
end;
$$;
