-- SWH DB schema upgrade
-- from_version: 169
-- to_version: 170
-- description: Make origin_visit_status.type not null

insert into dbversion(version, release, description)
      values(170, now(), 'Work In Progress');

create or replace function swh_snapshot_count_branches(id sha1_git,
    branch_name_exclude_prefix bytea default NULL)
  returns setof snapshot_size
  language sql
  stable
as $$
  SELECT target_type, count(name)
  from swh_snapshot_get_by_id(swh_snapshot_count_branches.id,
    branch_name_exclude_prefix => swh_snapshot_count_branches.branch_name_exclude_prefix)
  group by target_type;
$$;
