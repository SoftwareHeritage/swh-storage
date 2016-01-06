-- SWH DB schema upgrade
-- from_version: 34
-- to_version: 35
-- description: properly limit recursion in swh_revision_list{,_children}

insert into dbversion(version, release, description)
      values(35, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_revision_list(root_revision sha1_git, num_revs bigint = NULL::bigint) RETURNS TABLE(id sha1_git, parents bytea[])
    LANGUAGE sql STABLE
    AS $$
    with recursive full_rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select parent_id
	 from revision_history as h
   join full_rev_list on h.id = full_rev_list.id)
    ),
    rev_list as (select id from full_rev_list limit num_revs)
    select rev_list.id as id, array_agg(rh.parent_id::bytea order by rh.parent_rank) as parent from rev_list
    left join revision_history rh on rev_list.id = rh.id
    group by rev_list.id;
$$;

CREATE OR REPLACE FUNCTION swh_revision_list_children(root_revision sha1_git, num_revs bigint = NULL::bigint) RETURNS TABLE(id sha1_git, parents bytea[])
    LANGUAGE sql STABLE
    AS $$
    with recursive full_rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select h.id
	 from revision_history as h
   join full_rev_list on h.parent_id = full_rev_list.id)
    ),
    rev_list as (select id from full_rev_list limit num_revs)
    select rev_list.id as id, array_agg(rh.parent_id::bytea order by rh.parent_rank) as parent from rev_list
    left join revision_history rh on rev_list.id = rh.id
    group by rev_list.id;
$$;
