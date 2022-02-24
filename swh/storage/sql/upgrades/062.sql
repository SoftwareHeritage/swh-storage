-- SWH DB schema upgrade
-- from_version: 61
-- to_version: 62
-- description: Make the trigger to update entity lighter

insert into dbversion(version, release, description)
      values(62, now(), 'Work In Progress');

DROP TRIGGER update_entity ON entity_history;

CREATE OR REPLACE FUNCTION swh_update_entity_from_entity_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
    insert into entity (uuid, parent, name, type, description, homepage, active, generated, lister,
                        lister_metadata, doap, last_seen, last_id)
      select uuid, parent, name, type, description, homepage, active, generated, lister,
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
      lister = EXCLUDED.lister,
      lister_metadata = EXCLUDED.lister_metadata,
      doap = EXCLUDED.doap,
      last_seen = EXCLUDED.last_seen,
      last_id = EXCLUDED.last_id;

    return null;
end
$$;

CREATE TRIGGER update_entity
	AFTER INSERT OR UPDATE ON entity_history
	FOR EACH ROW
	EXECUTE PROCEDURE swh_update_entity_from_entity_history();
