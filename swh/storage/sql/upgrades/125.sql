-- SWH DB schema upgrade
-- from_version: 124
-- to_version: 125
-- description: Drop useless entity tables

insert into dbversion(version, release, description)
      values(125, now(), 'Work In Progress');

DROP FUNCTION swh_entity_from_tmp_entity_lister();

DROP FUNCTION swh_entity_get(entity_uuid uuid);

DROP FUNCTION swh_entity_history_add();

DROP FUNCTION swh_mktemp_entity_history();

DROP FUNCTION swh_mktemp_entity_lister();

DROP FUNCTION swh_update_entity_from_entity_history() cascade;

ALTER TABLE origin
	DROP COLUMN lister,
	DROP COLUMN project;

DROP TABLE list_history;

DROP TABLE listable_entity;

DROP TABLE entity_equivalence;

DROP TABLE entity;

DROP TABLE entity_history;

DROP TYPE entity_id;

DROP TYPE entity_type;

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
        'release',
        'revision',
        'revision_history',
        'skipped_content',
        'snapshot'
    );
$$;
