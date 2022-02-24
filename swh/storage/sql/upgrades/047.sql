-- SWH DB schema upgrade
-- from_version: 46
-- to_version: 47
-- description: Change type from text to bytea for release.name, occurrence.branch and occurrence_history.branch

insert into dbversion(version, release, description) values(47, now(), 'Work In Progress');

-- Update types
ALTER TABLE release
ALTER COLUMN name
SET DATA TYPE bytea
USING convert_to(name, 'UTF-8') :: bytea;

ALTER TABLE occurrence
ALTER COLUMN branch
SET DATA TYPE bytea
USING convert_to(branch, 'UTF-8') :: bytea;

ALTER TABLE occurrence_history
ALTER COLUMN branch
SET DATA TYPE bytea
USING convert_to(branch, 'UTF-8') :: bytea;

ALTER TYPE release_entry
ALTER ATTRIBUTE name
SET DATA TYPE bytea
CASCADE;

create or replace function swh_occurrence_get_by(
       origin_id bigint,
       branch_name bytea default NULL,
       validity timestamptz default NULL)
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


create or replace function swh_revision_get_by(
       origin_id bigint,
       branch_name bytea default NULL,
       validity timestamptz default NULL)
    returns setof revision_entry
    language sql
    stable
as $$
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
    inner join revision r on occ.target = r.id
    left join person a on a.id = r.author
    left join person c on c.id = r.committer;
$$;


ALTER TYPE content_occurrence
ALTER ATTRIBUTE branch
SET DATA TYPE bytea
CASCADE;
