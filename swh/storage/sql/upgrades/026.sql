-- SWH DB schema upgrade
-- from_version: 25
-- to_version: 26
-- description: update swh_revision_find_occurrence for performance

insert into dbversion(version, release, description)
      values(26, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_revision_find_occurrence(revision_id sha1_git) RETURNS occurrence
    LANGUAGE sql STABLE
    AS $$
	select origin, branch, revision
	from swh_revision_list_children(revision_id) as rev_list(sha1_git)
	left join occurrence_history occ_hist
	on rev_list.sha1_git = occ_hist.revision
	where occ_hist.origin is not null
	order by upper(occ_hist.validity)  -- TODO filter by authority?
	limit 1;
$$;
