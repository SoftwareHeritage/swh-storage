-- SWH DB schema upgrade
-- from_version: 108
-- to_version: 109
-- description: Add origin_visit to swh_stat_counters

insert into dbversion(version, release, description)
      values(109, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_stat_counters() RETURNS SETOF counter
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
        'occurrence',
        'occurrence_history',
        'origin',
        'origin_visit',
        'person',
        'entity',
        'entity_history',
        'release',
        'revision',
        'revision_history',
        'skipped_content'
    );
$$;

