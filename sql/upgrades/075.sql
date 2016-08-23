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

create or replace function swh_mktemp_occurrence_history()
    returns void
    language sql
as $$
    create temporary table tmp_occurrence_history(
        like occurrence_history including defaults,
        visit bigint not null
    ) on commit drop;
    alter table tmp_occurrence_history
      drop column visits,
      drop column object_id;
$$;


create or replace function swh_occurrence_history_add()
    returns void
    language plpgsql
as $$
declare
  origin_id origin.id%type;
begin
  -- Create or update occurrence_history
  with occurrence_history_id_visit as (
    select tmp_occurrence_history.*, object_id, visits from tmp_occurrence_history
    left join occurrence_history using(origin, branch, target, target_type)
  ),
  occurrences_to_update as (
    select object_id, visit from occurrence_history_id_visit where object_id is not null
  ),
  update_occurrences as (
    update occurrence_history
    set visits = array(select unnest(occurrence_history.visits) as e
                        union
                       select occurrences_to_update.visit as e
                       order by e)
    from occurrences_to_update
    where occurrence_history.object_id = occurrences_to_update.object_id
  )
  insert into occurrence_history (origin, branch, target, target_type, visits)
    select origin, branch, target, target_type, ARRAY[visit]
      from occurrence_history_id_visit
      where object_id is null;

  -- update occurrence
  for origin_id in
    select distinct origin from tmp_occurrence_history
  loop
    perform swh_occurrence_update_for_origin(origin_id);
  end loop;
  return;
end
$$;
