-- SWH DB schema upgrade
-- from_version: 62
-- to_version: 63
-- description: Fold the entity.lister column in entity.lister_metadata

insert into dbversion(version, release, description)
      values(63, now(), 'Work In Progress');

ALTER TABLE entity
	DROP CONSTRAINT entity_lister_fkey;

DROP TRIGGER update_entity ON entity_history;

UPDATE entity_history
    SET lister_metadata = lister_metadata || jsonb_build_object('lister', lister);

UPDATE entity
    SET lister_metadata = lister_metadata || jsonb_build_object('lister', lister);

ALTER TABLE entity_history
	DROP COLUMN lister;

ALTER TABLE entity
	DROP COLUMN lister;

CREATE OR REPLACE FUNCTION swh_entity_from_tmp_entity_lister() RETURNS SETOF entity_id
    LANGUAGE plpgsql
    AS $$
begin
  return query
    select t.id, e.*
    from tmp_entity_lister t
    left join entity e
    on e.lister_metadata @> t.lister_metadata;
  return;
end
$$;

CREATE OR REPLACE FUNCTION swh_entity_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into entity_history (
        uuid, parent, name, type, description, homepage, active, generated, lister_metadata, doap, validity
    ) select * from tmp_entity_history;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_mktemp_entity_lister() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_entity_lister (
    id              bigint,
    lister_metadata jsonb
  ) on commit drop;
$$;

CREATE OR REPLACE FUNCTION swh_update_entity_from_entity_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
    insert into entity (uuid, parent, name, type, description, homepage, active, generated,
      lister_metadata, doap, last_seen, last_id)
      select uuid, parent, name, type, description, homepage, active, generated,
             lister_metadata, doap, unnest(validity), id
      from entity_history
      where uuid = NEW.uuid
      order by unnest(validity) desc limit 1
    on conflict (uuid) do update set
      parent = EXCLUDED.parent,
      name = EXCLUDED.name,
      type = EXCLUDED.type,
      description = EXCLUDED.description,
      homepage = EXCLUDED.homepage,
      active = EXCLUDED.active,
      generated = EXCLUDED.generated,
      lister_metadata = EXCLUDED.lister_metadata,
      doap = EXCLUDED.doap,
      last_seen = EXCLUDED.last_seen,
      last_id = EXCLUDED.last_id;

    return null;
end
$$;

CREATE INDEX ON entity USING gin (lister_metadata jsonb_path_ops);

CREATE TRIGGER update_entity
	AFTER INSERT OR UPDATE ON entity_history
	FOR EACH ROW
	EXECUTE PROCEDURE swh_update_entity_from_entity_history();
