-- SWH DB schema upgrade
-- from_version: XXX TODO
-- to_version: 30
-- description: XXX TODO

insert into dbversion(version, release, description)
      values(30, now(), 'Work In Progress');

create type entity_id as (
    id               bigint,
    uuid             uuid,
    parent           uuid,
    name             text,
    type             entity_type,
    description      text,
    homepage         text,
    active           boolean,
    generated        boolean,
    lister           uuid,
    lister_metadata  jsonb,
    doap             jsonb,
    last_seen        timestamptz,
    last_id          bigint
);

CREATE OR REPLACE FUNCTION swh_entity_from_tmp_entity_lister() RETURNS SETOF entity_id
    LANGUAGE plpgsql
    AS $$
begin
  return query
    select t.id, e.*
    from tmp_entity_lister t
    left join entity e
    on t.lister = e.lister AND e.lister_metadata @> t.lister_metadata;
  return;
end
$$;

CREATE OR REPLACE FUNCTION swh_entity_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into entity_history (
        uuid, parent, name, type, description, homepage, active, generated,
	lister, lister_metadata, doap, validity
    ) select * from tmp_entity_history;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_entity_history() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_entity_history (
        like entity_history including defaults);
    alter table tmp_entity_history drop column id;
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_entity_lister() RETURNS void
    LANGUAGE sql
    AS $$
    create temporary table tmp_entity_lister (
        id bigint,
        lister uuid,
	lister_metadata jsonb
    );
$$;
