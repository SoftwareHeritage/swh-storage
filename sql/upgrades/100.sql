-- SWH DB schema upgrade
-- from_version: 99
-- to_version: 100
-- description: update swh_visit_find_by_date and swh_occurrence_get_by to return sensible results

insert into dbversion(version, release, description)
      values(100, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_occurrence_get_by(origin_id bigint, branch_name bytea = NULL::bytea, "date" timestamp with time zone = NULL::timestamp with time zone) RETURNS SETOF occurrence_history
    LANGUAGE plpgsql
    AS $$
declare
    filters text[] := array[] :: text[];  -- AND-clauses used to filter content
    visit_id bigint;
    q text;
begin
    if origin_id is null then
        raise exception 'Needs an origin_id to get an occurrence.';
    end if;
    filters := filters || format('origin = %L', origin_id);
    if branch_name is not null then
        filters := filters || format('branch = %L', branch_name);
    end if;
    if date is not null then
        select visit from swh_visit_find_by_date(origin_id, date) into visit_id;
    else
        select visit from origin_visit where origin = origin_id order by origin_visit.date desc limit 1 into visit_id;
    end if;
    if visit_id is null then
        return;
    end if;
    filters := filters || format('%L = any(visits)', visit_id);

    q = format('select * from occurrence_history where %s',
               array_to_string(filters, ' and '));
    return query execute q;
end
$$;

CREATE OR REPLACE FUNCTION swh_visit_find_by_date(origin bigint, visit_date timestamp with time zone = now()) RETURNS origin_visit
    LANGUAGE sql STABLE
    AS $$
  with closest_two_visits as ((
    select ov, (date - visit_date) as interval
    from origin_visit ov
    where ov.origin = origin
          and ov.date >= visit_date
    order by ov.date asc
    limit 1
  ) union (
    select ov, (visit_date - date) as interval
    from origin_visit ov
    where ov.origin = origin
          and ov.date < visit_date
    order by ov.date desc
    limit 1
  )) select (ov).* from closest_two_visits order by interval limit 1
$$;
