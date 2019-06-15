-- SWH DB schema upgrade
-- from_version: 135
-- to_version: 136
-- description: Add a 'type' column to the origin_visit table.

insert into dbversion(version, release, description)
      values(136, now(), 'Work In Progress');

-- Stop swh-storage first

alter table origin_visit
    add column type text;

comment on column origin_visit.type is 'Type of loader that did the visit (hg, git, ...)';


create or replace function swh_origin_visit_add(origin_id bigint, date timestamptz, type text)
    returns bigint
    language sql
as $$
  with last_known_visit as (
    select coalesce(max(visit), 0) as visit
    from origin_visit
    where origin = origin_id
  )
  insert into origin_visit (origin, date, type, visit, status)
  values (origin_id, date, type, (select visit from last_known_visit) + 1, 'ongoing')
  returning visit;
$$;


-- Start swh-storage here


update origin_visit
    set type = origin.type
    from origin
    where origin_visit.origin = origin.id;

alter table origin_visit
    alter column type set not null;
