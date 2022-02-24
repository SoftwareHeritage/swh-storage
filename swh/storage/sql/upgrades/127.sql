-- SWH DB schema upgrade
-- from_version: 126
-- to_version: 127
-- description: Drop the now superseded occurrence_history table

insert into dbversion(version, release, description)
      values(127, now(), 'Work In Progress');

DROP FUNCTION swh_mktemp_occurrence_history();

DROP FUNCTION swh_occurrence_get_by(origin_id bigint, branch_name bytea, "date" timestamp with time zone);

DROP FUNCTION swh_occurrence_history_add();

DROP TABLE occurrence_history;

DROP SEQUENCE occurrence_history_object_id_seq;

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
        'origin',
        'origin_visit',
        'person',
        'release',
        'revision',
        'revision_history',
        'skipped_content',
        'snapshot'
    );
$$;
