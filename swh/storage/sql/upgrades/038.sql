-- SWH DB schema upgrade
-- from_version: 37
-- to_version: 38
-- description: Update swh_*_get_by to take timestamps; do not left-join on revision_history; add index to origin.

insert into dbversion(version, release, description)
      values(38, now(), 'Work In Progress');

DROP FUNCTION swh_occurrence_get_by(origin_id bigint, branch_name text, validity text);

DROP FUNCTION swh_revision_get_by(origin_id bigint, branch_name text, validity text);

CREATE OR REPLACE FUNCTION swh_occurrence_get_by(origin_id bigint, branch_name text = NULL::text, validity timestamptz = NULL::timestamptz) RETURNS SETOF occurrence_history
    LANGUAGE plpgsql
    AS $$
declare
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    q text;
begin
    if origin_id is not null then
        filters := filters || format('origin = %L', origin_id);
    end if;
    if branch_name is not null then
        filters := filters || format('branch = %L', branch_name);
    end if;
    if validity is not null then
        filters := filters || format('validity @> %L::timestamptz', validity);
    end if;

    if cardinality(filters) = 0 then
        raise exception 'At least one filter amongst (origin_id, branch_name, validity) is needed';
    else
        q = format('select * ' ||
                   'from occurrence_history ' ||
                   'where %s ' ||
                   'order by validity desc',
	        array_to_string(filters, ' and '));
        return query execute q;
    end if;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_get() RETURNS SETOF revision_entry
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select t.id, r.date, r.date_offset,
               r.committer_date, r.committer_date_offset,
               r.type, r.directory, r.message,
               a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
         array(select rh.parent_id::bytea from revision_history rh where rh.id = t.id order by rh.parent_rank)
                   as parents
        from tmp_revision t
        left join revision r on t.id = r.id
        left join person a on a.id = r.author
        left join person c on c.id = r.committer;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_revision_get_by(origin_id bigint, branch_name text = NULL::text, validity timestamptz = NULL::timestamptz) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select r.id, r.date, r.date_offset,
        r.committer_date, r.committer_date_offset,
        r.type, r.directory, r.message,
        a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
        array(select rh.parent_id::bytea
            from revision_history rh
            where rh.id = r.id
            order by rh.parent_rank
        ) as parents
    from swh_occurrence_get_by(origin_id, branch_name, validity) as occ
    inner join revision r on occ.revision = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;

CREATE OR REPLACE FUNCTION swh_revision_list(root_revision sha1_git, num_revs bigint = NULL::bigint) RETURNS TABLE(id sha1_git, parents bytea[])
    LANGUAGE sql STABLE
    AS $$
    with recursive full_rev_list(id) as (
        (select id from revision where id = root_revision)
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
    select rev_list.id as id,
           array(select rh.parent_id::bytea
                 from revision_history rh
                 where rh.id = rev_list.id
                 order by rh.parent_rank
                ) as parent
    from rev_list;
$$;


-- to be run manually outside a transaction
-- CREATE INDEX concurrently origin_type_url_idx ON origin USING btree (type, url);
