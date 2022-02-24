-- SWH DB schema upgrade
-- from_version: 119
-- to_version: 120
-- description: Drop unused functions using temporary tables

insert into dbversion(version, release, description)
      values(120, now(), 'Work In Progress');

-- return statistics about the number of tuples in various SWH tables
--
-- Note: the returned values are based on postgres internal statistics
-- (pg_class table), which are only updated daily (by autovacuum) or so
create or replace function swh_stat_counters()
    returns setof counter
    language sql
    stable
as $$
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
        'skipped_content',
        'snapshot'
    );
$$;
