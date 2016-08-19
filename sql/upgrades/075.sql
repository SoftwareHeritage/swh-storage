-- SWH DB schema upgrade
-- from_version: 74
-- to_version: 75
-- description: Add completion information to origin_visit

INSERT INTO dbversion(version, release, description)
      VALUES(75, now(), 'Work In Progress');

CREATE TYPE origin_visit_status AS ENUM (
  'ongoing',
  'full',
  'partial'
);

COMMENT ON TYPE origin_visit_status IS 'Possible visit status';

ALTER TABLE origin_visit
            ADD COLUMN status origin_visit_status;

-- Already visited origins are considered full
UPDATE origin_visit SET status = 'full';

-- provide a status for visits is mandatory
ALTER TABLE origin_visit
            ALTER COLUMN status SET NOT NULL;

comment on column origin_visit.origin is 'Visited origin';
comment on column origin_visit.visit is 'The numbered visit occurrence for that origin';
comment on column origin_visit.date is 'Visit date for that origin';
comment on column origin_visit.status is 'Visit status for that origin';

-- add a new origin_visit for origin origin_id at date.
--
-- Returns the new visit id.
create or replace function swh_origin_visit_add(origin_id bigint, date timestamptz)
  returns bigint
  language sql
as $$
  with last_known_visit as (
    select coalesce(max(visit), 0) as visit
    from origin_visit
    where origin = origin_id
  )
  insert into origin_visit (origin, date, visit, status)
  values (origin_id, date, (select visit from last_known_visit) + 1, 'ongoing')
  returning visit;
$$;
