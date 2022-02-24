-- SWH DB schema upgrade
-- from_version: 35
-- to_version: 36
-- description: Retrieve revision by occurrence criterions filtering

insert into dbversion(version, release, description)
      values(36, now(), 'Work In Progress');

create function swh_occurrence_get_by(
       origin_id bigint,
       branch_name text default NULL,
       validity text default NULL)
    returns setof occurrence_history
    language plpgsql
as $$
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
        filters := filters || format('lower(validity) <= %L and %L <= upper(validity)', validity, validity);
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

CREATE OR REPLACE FUNCTION swh_revision_get_by(origin_id bigint, branch_name text = NULL::text, validity text = NULL::text) RETURNS SETOF revision_entry
    LANGUAGE sql STABLE
    AS $$
    select r.id, r.date, r.date_offset,
        r.committer_date, r.committer_date_offset,
        r.type, r.directory, r.message,
        a.name, a.email, c.name, c.email, r.metadata, r.synthetic,
        array_agg(rh.parent_id::bytea order by rh.parent_rank)
        as parents
    from swh_occurrence_get_by(origin_id, branch_name, validity) as occ
    inner join revision r on occ.revision = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer
    left join revision_history rh on rh.id = r.id
    group by r.id, a.name, a.email, r.date, r.date_offset,
             c.name, c.email, r.committer_date, r.committer_date_offset,
             r.type, r.directory, r.message, r.metadata, r.synthetic;

$$;
