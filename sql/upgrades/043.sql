-- SWH DB schema upgrade
-- from_version: 42
-- to_version: 43
-- description: Revision listing from multiple root revisions

insert into dbversion(version, release, description)
      values(43, now(), 'Work In Progress');

DROP FUNCTION swh_revision_list(sha1_git,bigint);

create or replace function swh_revision_list(root_revisions bytea[], num_revs bigint default NULL)
    returns table (id sha1_git, parents bytea[])
    language sql
    stable
as $$
    with recursive full_rev_list(id) as (
        (select id from revision where id = ANY(root_revisions))
        union
        (select h.parent_id
         from revision_history as h
         join full_rev_list on h.id = full_rev_list.id)
    ),
    rev_list as (select id from full_rev_list limit num_revs)
    select rev_list.id as id,
           array(select rh.parent_id::bytea
                 from revision_history rh
                 where rh.id = rev_list.id
                 order by rh.parent_rank
                ) as parent
    from rev_list;
$$;

DROP FUNCTION swh_revision_list_children(sha1_git,bigint);

create or replace function swh_revision_list_children(root_revisions bytea[], num_revs bigint default NULL)
    returns table (id sha1_git, parents bytea[])
    language sql
    stable
as $$
    with recursive full_rev_list(id) as (
        (select id from revision where id = ANY(root_revisions))
        union
        (select h.id
         from revision_history as h
         join full_rev_list on h.parent_id = full_rev_list.id)
    ),
    rev_list as (select id from full_rev_list limit num_revs)
    select rev_list.id as id,
           array(select rh.parent_id::bytea
                 from revision_history rh
                 where rh.id = rev_list.id
                 order by rh.parent_rank
                ) as parent
    from rev_list;
$$;

DROP FUNCTION swh_revision_log(sha1_git,bigint);

create or replace function swh_revision_log(root_revisions bytea[], num_revs bigint default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
    select t.id, r.date, r.date_offset,
           r.committer_date, r.committer_date_offset,
           r.type, r.directory, r.message,
           a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
           t.parents
    from swh_revision_list(root_revisions, num_revs) as t
    left join revision r on t.id = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

create or replace function swh_revision_find_occurrence(revision_id sha1_git)
    returns occurrence
    language sql
    stable
as $$
	select origin, branch, revision
  from swh_revision_list_children(ARRAY[revision_id] :: bytea[]) as rev_list
	left join occurrence_history occ_hist
  on rev_list.id = occ_hist.revision
	where occ_hist.origin is not null
	order by upper(occ_hist.validity)  -- TODO filter by authority?
	limit 1;
$$;
