-- SWH DB schema upgrade
-- from_version: 107
-- to_version: 108
-- description: Add a new object_counts table to make counters more relevant

insert into dbversion(version, release, description)
      values(108, now(), 'Work In Progress');

CREATE TABLE object_counts (
	object_type text NOT NULL,
	"value" bigint,
	last_update timestamp with time zone
);

ALTER TABLE object_counts
	ADD CONSTRAINT object_counts_pkey PRIMARY KEY (object_type);

CREATE OR REPLACE FUNCTION swh_update_counter(object_type text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
begin
    execute format('
	insert into object_counts
    (value, last_update, object_type)
  values
    ((select count(*) from %1$I), NOW(), %1$L)
  on conflict (object_type) do update set
    value = excluded.value,
    last_update = excluded.last_update',
  object_type);
    return;
end;
$_$;

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
        'person',
        'entity',
        'entity_history',
        'release',
        'revision',
        'revision_history',
        'skipped_content'
    );
$$;

