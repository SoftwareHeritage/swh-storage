-- SWH DB schema upgrade
-- from_version: 122
-- to_version: 123
-- description: Remove the occurrence table

insert into dbversion(version, release, description)
      values(123, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_occurrence_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
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
  return;
end
$$;

CREATE OR REPLACE FUNCTION swh_stat_counters() RETURNS SETOF public.counter
    LANGUAGE sql STABLE
    AS $$
    select object_type as label, value as value
    from object_counts
    where object_type in (
        'content',
        'directory',
        'directory_entry_dir',
        'directory_entry_file',
        'directory_entry_rev',
        'occurrence_history',
        'origin',
        'origin_visit',
        'person',
        'entity',
        'entity_history',
        'release',
        'revision',
        'revision_history',
        'skipped_content',
        'snapshot'
    );
$$;

DROP FUNCTION swh_occurrence_update_all();

DROP FUNCTION swh_occurrence_update_for_origin(origin_id bigint);

DROP TABLE occurrence;

DELETE FROM object_counts where object_type = 'occurrence';

