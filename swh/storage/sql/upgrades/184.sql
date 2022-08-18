-- SWH DB schema upgrade
-- from_version: 183
-- to_version: 184
-- description: origin_visit_add: Fix crash when adding multiple visits to the same origin simultaneously

insert into dbversion(version, release, description)
    values(184, now(), 'Work In Progress');

create or replace function swh_origin_visit_add(origin_url text, date timestamptz, type text)
    returns bigint
    language sql
as $$
  with origin_id as (
    select id
    from origin
    where url = origin_url
  ), last_known_visit as (
    select coalesce(
      (
        select visit
        from origin_visit
        where origin = (select id from origin_id)
        order by visit desc
        limit 1
        for update
      ),
    0) as visit
  )
  insert into origin_visit (origin, date, type, visit)
  values ((select id from origin_id), date, type,
          (select visit from last_known_visit) + 1)
  returning visit;
$$;
