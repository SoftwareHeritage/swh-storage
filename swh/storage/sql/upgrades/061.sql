-- SWH DB schema upgrade
-- from_version: 60
-- to_version: 61
-- description: Drop temporary tables related to entities on commit

insert into dbversion(version, release, description)
      values(61, now(), 'Work In Progress');

CREATE OR REPLACE FUNCTION swh_mktemp_entity_history() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_entity_history (
        like entity_history including defaults) on commit drop;
    alter table tmp_entity_history drop column id;
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_entity_lister() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_entity_lister (
        id bigint,
        lister uuid,
	lister_metadata jsonb
    ) on commit drop;
$$;
