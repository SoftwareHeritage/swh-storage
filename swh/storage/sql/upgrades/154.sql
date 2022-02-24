-- SWH DB schema upgrade
-- from_version: 153
-- to_version: 154
-- description: make sure swh_snapshot_get_by_id doesn't degenerate into a very large index scan

insert into dbversion(version, release, description)
      values(154, now(), 'Work In Progress');

create or replace function swh_snapshot_get_by_id(id sha1_git,
    branches_from bytea default '', branches_count bigint default null,
    target_types snapshot_target[] default NULL)
  returns setof snapshot_result
  language sql
  stable
as $$
  -- with small limits, the "naive" version of this query can degenerate into
  -- using the deduplication index on snapshot_branch (name, target,
  -- target_type); The planner happily scans several hundred million rows.

  -- Do the query in two steps: first pull the relevant branches for the given
  -- snapshot (filtering them by type), then do the limiting. This two-step
  -- process guides the planner into using the proper index.
  with filtered_snapshot_branches as (
    select swh_snapshot_get_by_id.id as snapshot_id, name, target, target_type
      from snapshot_branches
      inner join snapshot_branch on snapshot_branches.branch_id = snapshot_branch.object_id
      where snapshot_id = (select object_id from snapshot where snapshot.id = swh_snapshot_get_by_id.id)
        and (target_types is null or target_type = any(target_types))
      order by name
  )
  select snapshot_id, name, target, target_type
    from filtered_snapshot_branches
    where name >= branches_from
    order by name limit branches_count;
$$;
