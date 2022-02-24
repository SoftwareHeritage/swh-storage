-- SWH DB schema upgrade
-- from_version: 104
-- to_version: 105
-- description: more accurate swh_stat_counters()

insert into dbversion(version, release, description)
      values(105, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_stat_counters() RETURNS SETOF counter
    LANGUAGE sql STABLE
    AS $$
    select relname::text as label, n_live_tup::bigint - n_dead_tup::bigint as value
    from pg_stat_user_tables
    where relid in (
        'public.content'::regclass,
        'public.directory'::regclass,
        'public.directory_entry_dir'::regclass,
        'public.directory_entry_file'::regclass,
        'public.directory_entry_rev'::regclass,
        'public.occurrence'::regclass,
        'public.occurrence_history'::regclass,
        'public.origin'::regclass,
        'public.person'::regclass,
        'public.entity'::regclass,
        'public.entity_history'::regclass,
        'public.release'::regclass,
        'public.revision'::regclass,
        'public.revision_history'::regclass,
        'public.skipped_content'::regclass
    );
$$;
