-- SWH DB schema upgrade
-- from_version: 26
-- to_version: 27
-- description: Change organizations to the entity schema

insert into dbversion(version, release, description)
      values(27, now(), 'Work In Progress');

ALTER TABLE list_history
	DROP CONSTRAINT list_history_organization_fkey;

ALTER TABLE occurrence_history
	DROP CONSTRAINT occurrence_history_origin_branch_revision_authority_validi_excl;

ALTER TABLE occurrence_history
	DROP CONSTRAINT occurrence_history_authority_fkey;

DROP TABLE project_history;

DROP TABLE project;

CREATE SEQUENCE entity_history_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

create type entity_type as enum (
  'organization',
  'group_of_entities',
  'hosting',
  'group_of_persons',
  'person',
  'project'
);

CREATE TABLE entity (
	uuid uuid NOT NULL,
	parent uuid,
	name text NOT NULL,
	type entity_type NOT NULL,
	description text,
	homepage text,
	active boolean NOT NULL,
	generated boolean NOT NULL,
	lister uuid,
	lister_metadata jsonb,
	doap jsonb,
	last_seen timestamp with time zone,
	last_id bigint
);

CREATE TABLE entity_equivalence (
	entity1 uuid NOT NULL,
	entity2 uuid NOT NULL
);

CREATE TABLE entity_history (
	id bigint DEFAULT nextval('entity_history_id_seq'::regclass) NOT NULL,
	uuid uuid,
	parent uuid,
	name text NOT NULL,
	type entity_type NOT NULL,
	description text,
	homepage text,
	active boolean NOT NULL,
	generated boolean NOT NULL,
	lister uuid,
	lister_metadata jsonb,
	doap jsonb,
	validity timestamp with time zone[]
);

CREATE TABLE listable_entity (
	uuid uuid NOT NULL,
	enabled boolean DEFAULT true NOT NULL,
	list_engine text,
	list_url text,
	list_params json,
	latest_list timestamp with time zone
);

ALTER TABLE list_history
	DROP COLUMN organization,
	ADD COLUMN entity uuid;

ALTER TABLE origin
	ADD COLUMN lister uuid,
	ADD COLUMN project uuid;

ALTER SEQUENCE entity_history_id_seq
	OWNED BY entity_history.id;

CREATE OR REPLACE FUNCTION swh_stat_counters() RETURNS SETOF counter
    LANGUAGE sql STABLE
    AS $$
    select relname::text as label, reltuples::bigint as value
    from pg_class
    where oid in (
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

CREATE OR REPLACE FUNCTION swh_update_entity_from_entity_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
    with all_entities as (
      select uuid, parent, name, type, description, homepage, active,
             generated, lister, lister_metadata, doap, last_seen, last_id
      from (
          select row_number() over (partition by uuid order by unnest(validity) desc) as row,
	         id as last_id, uuid, parent, name, type, description, homepage, active,
		 generated, lister, lister_metadata, doap,
	         unnest(validity) as last_seen
          from entity_history
      ) as latest_entities
      where latest_entities.row = 1
    ),
    updated_uuids as (
      update entity set
        parent = all_entities.parent,
        name = all_entities.name,
	type = all_entities.type,
	description = all_entities.description,
	homepage = all_entities.homepage,
	active = all_entities.active,
	generated = all_entities.generated,
	lister = all_entities.lister,
	lister_metadata = all_entities.lister_metadata,
	doap = all_entities.doap,
	last_seen = all_entities.last_seen,
        last_id = all_entities.last_id
      from all_entities
      where entity.uuid = all_entities.uuid
      returning entity.uuid
    )
    insert into entity
    (select * from all_entities
     where uuid not in (select uuid from updated_uuids));
    return null;
end
$$;

ALTER TABLE entity
	ADD CONSTRAINT entity_pkey PRIMARY KEY (uuid);

ALTER TABLE entity_equivalence
	ADD CONSTRAINT entity_equivalence_pkey PRIMARY KEY (entity1, entity2);

ALTER TABLE entity_history
	ADD CONSTRAINT entity_history_pkey PRIMARY KEY (id);

ALTER TABLE listable_entity
	ADD CONSTRAINT listable_entity_pkey PRIMARY KEY (uuid);

ALTER TABLE entity
	ADD CONSTRAINT entity_last_id_fkey FOREIGN KEY (last_id) REFERENCES entity_history(id);

ALTER TABLE entity
	ADD CONSTRAINT entity_lister_fkey FOREIGN KEY (lister) REFERENCES listable_entity(uuid);

ALTER TABLE entity
	ADD CONSTRAINT entity_parent_fkey FOREIGN KEY (parent) REFERENCES entity(uuid) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE entity_equivalence
	ADD CONSTRAINT order_entities CHECK ((entity1 < entity2));

ALTER TABLE entity_equivalence
	ADD CONSTRAINT entity_equivalence_entity1_fkey FOREIGN KEY (entity1) REFERENCES entity(uuid);

ALTER TABLE entity_equivalence
	ADD CONSTRAINT entity_equivalence_entity2_fkey FOREIGN KEY (entity2) REFERENCES entity(uuid);

ALTER TABLE list_history
	ADD CONSTRAINT list_history_entity_fkey FOREIGN KEY (entity) REFERENCES listable_entity(uuid);

ALTER TABLE listable_entity
	ADD CONSTRAINT listable_entity_uuid_fkey FOREIGN KEY (uuid) REFERENCES entity(uuid);

ALTER TABLE origin
	ADD CONSTRAINT origin_lister_fkey FOREIGN KEY (lister) REFERENCES listable_entity(uuid);

ALTER TABLE origin
	ADD CONSTRAINT origin_project_fkey FOREIGN KEY (project) REFERENCES entity(uuid);

CREATE INDEX entity_name_idx ON entity USING btree (name);

CREATE INDEX entity_history_name_idx ON entity_history USING btree (name);

CREATE INDEX entity_history_uuid_idx ON entity_history USING btree (uuid);

CREATE TRIGGER update_entity
	AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON entity_history
	FOR EACH STATEMENT
	EXECUTE PROCEDURE swh_update_entity_from_entity_history();

insert into entity_history
  (uuid, parent, name, type, description, homepage, active, generated, validity)
values
  ('5f4d4c51-498a-4e28-88b3-b3e4e8396cba', NULL, 'softwareheritage',
   'organization', 'Software Heritage',
   'http://www.softwareheritage.org/', true, false, ARRAY[now()]),
  ('6577984d-64c8-4fab-b3ea-3cf63ebb8589', NULL, 'gnu', 'organization',
   'GNU is not UNIX', 'https://gnu.org/', true, false, ARRAY[now()]),
  ('7c33636b-8f11-4bda-89d9-ba8b76a42cec', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU Hosting', 'group_of_entities',
   'GNU Hosting facilities', NULL, true, false, ARRAY[now()]),
  ('4706c92a-8173-45d9-93d7-06523f249398', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU rsync mirror', 'hosting',
   'GNU rsync mirror', 'rsync://mirror.gnu.org/', true, false, ARRAY[now()]),
  ('5cb20137-c052-4097-b7e9-e1020172c48e', '6577984d-64c8-4fab-b3ea-3cf63ebb8589',
   'GNU Projects', 'group_of_entities',
   'GNU Projects', 'https://gnu.org/software/', true, false, ARRAY[now()]),
  ('4bfb38f6-f8cd-4bc2-b256-5db689bb8da4', NULL, 'GitHub', 'organization',
   'GitHub', 'https://github.org/', true, false, ARRAY[now()]),
  ('aee991a0-f8d7-4295-a201-d1ce2efc9fb2', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Hosting', 'group_of_entities',
   'GitHub Hosting facilities', 'https://github.org/', true, false, ARRAY[now()]),
  ('34bd6b1b-463f-43e5-a697-785107f598e4', 'aee991a0-f8d7-4295-a201-d1ce2efc9fb2',
   'GitHub git hosting', 'hosting',
   'GitHub git hosting', 'https://github.org/', true, false, ARRAY[now()]),
  ('e8c3fc2e-a932-4fd7-8f8e-c40645eb35a7', 'aee991a0-f8d7-4295-a201-d1ce2efc9fb2',
   'GitHub asset hosting', 'hosting',
   'GitHub asset hosting', 'https://github.org/', true, false, ARRAY[now()]),
  ('9f7b34d9-aa98-44d4-8907-b332c1036bc3', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Organizations', 'group_of_entities',
   'GitHub Organizations', 'https://github.org/', true, false, ARRAY[now()]),
  ('ad6df473-c1d2-4f40-bc58-2b091d4a750e', '4bfb38f6-f8cd-4bc2-b256-5db689bb8da4',
   'GitHub Users', 'group_of_entities',
   'GitHub Users', 'https://github.org/', true, false, ARRAY[now()]);

ALTER TABLE occurrence_history
	ADD COLUMN authority_new uuid;

update occurrence_history
  set authority_new = auth.authority_new
  from (
    select uuid as authority_new, id as authority
    from organization
    left join entity
    on organization.name = entity.name
  ) auth
  where occurrence_history.authority = auth.authority;

alter table occurrence_history drop column authority;
alter table occurrence_history rename column authority_new to authority;

DROP TABLE organization;

alter table occurrence_history alter column authority set not null;

ALTER TABLE occurrence_history
	ADD CONSTRAINT occurrence_history_origin_branch_revision_authority_validi_excl EXCLUDE USING gist (origin WITH =, branch WITH =, revision WITH =, ((authority)::text) WITH =, validity WITH &&);

ALTER TABLE occurrence_history
	ADD CONSTRAINT occurrence_history_authority_fkey FOREIGN KEY (authority) REFERENCES entity(uuid);
