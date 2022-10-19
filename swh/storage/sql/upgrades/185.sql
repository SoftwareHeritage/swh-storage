-- SWH DB schema upgrade
-- from_version: 184
-- to_version: 185
-- description: origin_visit_find_by_date: Fix invalid alias in query which could lead to wrong visit being returned

insert into dbversion(version, release, description)
    values(185, now(), 'Work In Progress');

create or replace function swh_visit_find_by_date(origin_url text, visit_date timestamptz default NOW())
    returns setof origin_visit
    language plpgsql
    stable
as $$
declare
  origin_id bigint;
begin
  select id into origin_id from origin where url=origin_url;
  return query
  with closest_two_visits as ((
    select ov, (date - visit_date) as interval, visit
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date >= visit_date
    order by ov.date asc, ov.visit desc
    limit 1
  ) union (
    select ov, (visit_date - date) as interval, visit
    from origin_visit ov
    where ov.origin = origin_id
          and ov.date < visit_date
    order by ov.date desc, ov.visit desc
    limit 1
  )) select (ov).* from closest_two_visits order by interval, visit limit 1;
end
$$;