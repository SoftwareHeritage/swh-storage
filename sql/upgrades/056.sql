-- SWH DB schema upgrade
-- from_version: 55
-- to_version: 56
-- description: Deduplication of occurrence_history didn't take the branch name into account

insert into dbversion(version, release, description)
      values(56, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_occurrence_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  origin_id origin.id%type;
begin
  -- Create new visits
  with current_visits as (
    select distinct origin, date from tmp_occurrence_history
  ),
  new_visits as (
      select origin, date, (select coalesce(max(visit), 0)
                            from origin_visit ov
                            where ov.origin = cv.origin) as max_visit
        from current_visits cv
        where not exists (select 1 from origin_visit ov
                          where ov.origin = cv.origin and
                                ov.date = cv.date)
  )
  insert into origin_visit (origin, date, visit)
    select origin, date, max_visit + row_number() over
                                     (partition by origin
                                                order by origin, date)
    from new_visits;

  -- Create or update occurrence_history
  with occurrence_history_id_visit as (
    select tmp_occurrence_history.*, object_id, visits, visit from tmp_occurrence_history
    left join occurrence_history using(origin, branch, target, target_type)
    left join origin_visit using(origin, date)
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
