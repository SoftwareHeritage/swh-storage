-- SWH DB schema upgrade
-- from_version: 69
-- to_version: 70
-- description: add origin visit listing

insert into dbversion(version, release, description)
      values(70, now(), 'Work In Progress');


-- Find the visit of origin id closest to date visit_date
create or replace function swh_visit_get(origin bigint)
  returns origin_visit
  language sql
  stable
as $$
  select origin, visit, date
  from origin_visit
  where origin=origin
  order by date desc
$$;
