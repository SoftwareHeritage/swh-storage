-- SWH DB schema upgrade
-- from_version: 123
-- to_version: 124
-- description: Enable to paginate, filter and count snapshot content

insert into dbversion(version, release, description)
      values(124, now(), 'Work In Progress');

DROP FUNCTION swh_snapshot_get_by_id(id public.sha1_git);

CREATE TYPE snapshot_size AS (
	target_type public.snapshot_target,
	"count" bigint
);

CREATE OR REPLACE FUNCTION swh_snapshot_get_by_id(id public.sha1_git, branches_from bytea = '\x'::bytea, branches_count bigint = NULL::bigint, target_types public.snapshot_target[] = NULL::public.snapshot_target[]) RETURNS SETOF public.snapshot_result
    LANGUAGE sql STABLE
    AS $$
  select
    swh_snapshot_get_by_id.id as snapshot_id, name, target, target_type
  from snapshot_branches
  inner join snapshot_branch on snapshot_branches.branch_id = snapshot_branch.object_id
  where snapshot_id = (select object_id from snapshot where snapshot.id = swh_snapshot_get_by_id.id)
    and (target_types is null or target_type = any(target_types))
    and name >= branches_from
  order by name limit branches_count
$$;

CREATE OR REPLACE FUNCTION swh_snapshot_count_branches(id public.sha1_git) RETURNS SETOF public.snapshot_size
    LANGUAGE sql STABLE
    AS $$
  SELECT target_type, count(name)
  from swh_snapshot_get_by_id(swh_snapshot_count_branches.id)
  group by target_type;
$$;

