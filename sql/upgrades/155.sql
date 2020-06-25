-- SWH DB schema upgrade
-- from_version: 154
-- to_version: 155
-- description: Drop obsolete origin-visit fields

-- latest schema version
insert into dbversion(version, release, description)
      values(155, now(), 'Work In Progress');

alter table origin_visit drop column snapshot;
alter table origin_visit drop column metadata;
alter table origin_visit drop column status;

create or replace function swh_origin_visit_add(origin_url text, date timestamptz, type text)
    returns bigint
    language sql
as $$
  with origin_id as (
    select id
    from origin
    where url = origin_url
  ), last_known_visit as (
    select coalesce(max(visit), 0) as visit
    from origin_visit
    where origin = (select id from origin_id)
  )
  insert into origin_visit (origin, date, type, visit)
  values ((select id from origin_id), date, type,
          (select visit from last_known_visit) + 1)
  returning visit;
$$;

drop index origin_visit_type_status_date_idx;
create index concurrently origin_visit_type_date on origin_visit(type, date);
