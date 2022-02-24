-- SWH DB schema upgrade
-- from_version: 72
-- to_version: 73
-- description: Add functions to retrieve objects by object_id

insert into dbversion(version, release, description)
      values(73, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_content_list_by_object_id(min_excl bigint, max_incl bigint) RETURNS SETOF content
    LANGUAGE sql STABLE
    AS $$
    select * from content
    where object_id > min_excl and object_id <= max_incl
    order by object_id;
$$;

CREATE OR REPLACE FUNCTION swh_release_list_by_object_id(min_excl bigint, max_incl bigint) RETURNS SETOF release_entry
    LANGUAGE sql STABLE
    AS $$
    with rels as (
        select * from release
        where object_id > min_excl and object_id <= max_incl
    )
    select r.id, r.target, r.target_type, r.date, r.date_offset, r.date_neg_utc_offset, r.name, r.comment,
           r.synthetic, p.id as author_id, p.fullname as author_fullname, p.name as author_name, p.email as author_email, r.object_id
    from rels r
    left join person p on p.id = r.author
    order by r.object_id;
$$;

CREATE OR REPLACE FUNCTION swh_revision_list_by_object_id(min_excl bigint, max_incl bigint) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    with revs as (
        select * from revision
        where object_id > min_excl and object_id <= max_incl
    )
    select r.id, r.date, r.date_offset, r.date_neg_utc_offset,
           r.committer_date, r.committer_date_offset, r.committer_date_neg_utc_offset,
           r.type, r.directory, r.message,
           a.id, a.fullname, a.name, a.email, c.id, c.fullname, c.name, c.email, r.metadata, r.synthetic,
           array(select rh.parent_id::bytea from revision_history rh where rh.id = r.id order by rh.parent_rank)
               as parents, r.object_id
    from revs r
    left join person a on a.id = r.author
    left join person c on c.id = r.committer
    order by r.object_id;
$$;
