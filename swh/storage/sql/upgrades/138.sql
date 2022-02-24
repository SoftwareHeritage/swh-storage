-- SWH DB schema upgrade
-- from_version: 137
-- to_version: 138
-- description: Make swh_visit_find_by_date break ties using the largest visit id

insert into dbversion(version, release, description)
      values(138, now(), 'Work In Progress');

create or replace function swh_visit_find_by_date(origin bigint, visit_date timestamptz default NOW())
    returns origin_visit
    language sql
    stable
as $$
  with closest_two_visits as ((
    select ov, (date - visit_date), visit as interval
    from origin_visit ov
    where ov.origin = origin
          and ov.date >= visit_date
    order by ov.date asc, ov.visit desc
    limit 1
  ) union (
    select ov, (visit_date - date), visit as interval
    from origin_visit ov
    where ov.origin = origin
          and ov.date < visit_date
    order by ov.date desc, ov.visit desc
    limit 1
  )) select (ov).* from closest_two_visits order by interval, visit limit 1
$$;
