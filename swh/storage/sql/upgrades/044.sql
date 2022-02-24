-- SWH DB schema upgrade
-- from_version: 43
-- to_version: 44
-- description: add object ids to our object tables

-- You might want to launch all the alter tables in parallel as they are bound
-- to the sequence mutex.

insert into dbversion(version, release, description)
      values(44, now(), 'Work In Progress');

CREATE SEQUENCE content_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE directory_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE occurrence_history_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE release_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE revision_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE skipped_content_object_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

ALTER TABLE content
	ADD COLUMN object_id bigint DEFAULT nextval('content_object_id_seq'::regclass) NOT NULL;

ALTER TABLE directory
	ADD COLUMN object_id bigint DEFAULT nextval('directory_object_id_seq'::regclass) NOT NULL;

ALTER TABLE occurrence_history
	ADD COLUMN object_id bigint DEFAULT nextval('occurrence_history_object_id_seq'::regclass) NOT NULL;

ALTER TABLE "release"
	ADD COLUMN object_id bigint DEFAULT nextval('release_object_id_seq'::regclass) NOT NULL;

ALTER TABLE revision
	ADD COLUMN object_id bigint DEFAULT nextval('revision_object_id_seq'::regclass) NOT NULL;

ALTER TABLE skipped_content
	ADD COLUMN object_id bigint DEFAULT nextval('skipped_content_object_id_seq'::regclass) NOT NULL;

ALTER SEQUENCE content_object_id_seq
	OWNED BY content.object_id;

ALTER SEQUENCE directory_object_id_seq
	OWNED BY directory.object_id;

ALTER SEQUENCE occurrence_history_object_id_seq
	OWNED BY occurrence_history.object_id;

ALTER SEQUENCE release_object_id_seq
	OWNED BY release.object_id;

ALTER SEQUENCE revision_object_id_seq
	OWNED BY revision.object_id;

ALTER SEQUENCE skipped_content_object_id_seq
	OWNED BY skipped_content.object_id;
