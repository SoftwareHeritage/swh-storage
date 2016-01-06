-- SWH DB schema upgrade
-- from_version: 33
-- to_version: 34
-- description: update swh_revision_list{,children} with limits and parents

insert into dbversion(version, release, description)
      values(34, now(), 'Work In Progress');

DROP FUNCTION swh_revision_list(root_revision sha1_git);

DROP FUNCTION swh_revision_list_children(root_revision sha1_git);

DROP FUNCTION swh_revision_log(root_revision sha1_git);

CREATE OR REPLACE FUNCTION swh_revision_list(root_revision sha1_git, num_revs bigint = NULL::bigint) RETURNS TABLE(id sha1_git, parents bytea[])
    LANGUAGE sql STABLE
    AS $$
    with recursive rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select parent_id
	 from revision_history as h
	 join rev_list on h.id = rev_list.id)
    )
    select rev_list.id as id, array_agg(rh.parent_id::bytea order by rh.parent_rank) as parent from rev_list
    left join revision_history rh on rev_list.id = rh.id
    group by rev_list.id
    limit num_revs;
$$;

CREATE OR REPLACE FUNCTION swh_revision_list_children(root_revision sha1_git, num_revs bigint = NULL::bigint) RETURNS TABLE(id sha1_git, parents bytea[])
    LANGUAGE sql STABLE
    AS $$
    with recursive rev_list(id) as (
	(select id from revision where id = root_revision)
	union
	(select h.id
	 from revision_history as h
	 join rev_list on h.parent_id = rev_list.id)
    )
    select rev_list.id as id, array_agg(rh.parent_id::bytea order by rh.parent_rank) as parent from rev_list
    left join revision_history rh on rev_list.id = rh.id
    group by rev_list.id
    limit num_revs;
$$;

CREATE OR REPLACE FUNCTION swh_revision_log(root_revision sha1_git, num_revs bigint = NULL::bigint) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select t.id, r.date, r.date_offset,
           r.committer_date, r.committer_date_offset,
           r.type, r.directory, r.message,
           a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
           t.parents
    from swh_revision_list(root_revision, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

CREATE OR REPLACE FUNCTION swh_revision_find_occurrence(revision_id sha1_git) RETURNS occurrence
    LANGUAGE sql STABLE
    AS $$
  select origin, branch, revision
  from swh_revision_list_children(revision_id) as rev_list
  left join occurrence_history occ_hist
  on rev_list.id = occ_hist.revision
  where occ_hist.origin is not null
  order by upper(occ_hist.validity)  -- TODO filter by authority?
  limit 1;
$$;
