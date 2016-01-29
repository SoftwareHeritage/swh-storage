-- SWH DB schema upgrade
-- from_version: 49
-- to_version: 50
-- description: Clean up some old functions + fix error message

insert into dbversion(version, release, description)
      values(50, now(), 'Work In Progres');

DROP FUNCTION swh_occurrence_get_by(bigint,text,timestamp with time zone);
DROP FUNCTION swh_revision_get_by(bigint,text,timestamp with time zone);

CREATE OR REPLACE FUNCTION swh_occurrence_get_by(origin_id bigint, branch_name bytea = NULL::bytea, "date" timestamp with time zone = NULL::timestamp with time zone) RETURNS SETOF occurrence_history
    LANGUAGE plpgsql
    AS $$
declare
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    visit_id bigint;
    q text;
begin
    if origin_id is not null then
        filters := filters || format('origin = %L', origin_id);
    end if;
    if branch_name is not null then
        filters := filters || format('branch = %L', branch_name);
    end if;
    if date is not null then
        if origin_id is null then
            raise exception 'Needs an origin_id to filter by date.';
        end if;
        select visit from swh_visit_find_by_date(origin_id, date) into visit_id;
        if visit_id is null then
            return;
        end if;
        filters := filters || format('%L = any(visits)', visit_id);
    end if;

    if cardinality(filters) = 0 then
        raise exception 'At least one filter amongst (origin_id, branch_name, date) is needed';
    else
        q = format('select * ' ||
                   'from occurrence_history ' ||
                   'where %s',
	        array_to_string(filters, ' and '));
        return query execute q;
    end if;
end
$$;
