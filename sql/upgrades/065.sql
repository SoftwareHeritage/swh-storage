-- SWH DB schema upgrade
-- from_version: 64
-- to_version: 65
-- description: Rename entity{_history}.doap to metadata

insert into dbversion(version, release, description)
      values(65, now(), 'Work In Progress');

ALTER TABLE entity
	RENAME COLUMN doap to metadata;

ALTER TABLE entity_history
	RENAME COLUMN doap to metadata;

ALTER type entity_id
  RENAME ATTRIBUTE doap to metadata;

CREATE OR REPLACE FUNCTION swh_entity_history_add() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into entity_history (
        uuid, parent, name, type, description, homepage, active, generated, lister_metadata, metadata, validity
    ) select * from tmp_entity_history;
    return;
end
$$;

CREATE OR REPLACE FUNCTION swh_update_entity_from_entity_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
    insert into entity (uuid, parent, name, type, description, homepage, active, generated,
      lister_metadata, metadata, last_seen, last_id)
      select uuid, parent, name, type, description, homepage, active, generated,
             lister_metadata, metadata, unnest(validity), id
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
      metadata = EXCLUDED.metadata,
      last_seen = EXCLUDED.last_seen,
      last_id = EXCLUDED.last_id;

    return null;
end
$$;
