-- SWH DB schema upgrade
-- from_version: 109
-- to_version: 110
-- description: add content, revision and origin metadata tables

insert into dbversion(version, release, description)
      values(110, now(), 'Work In Progress');

CREATE SEQUENCE metadata_provider_id_seq
	AS integer
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE origin_metadata_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE SEQUENCE origin_metadata_translation_id_seq
	START WITH 1
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 1;

CREATE TYPE content_metadata_signature AS (
	id sha1,
	translated_metadata jsonb,
	tool_id integer,
	tool_name text,
	tool_version text,
	tool_configuration jsonb
);

CREATE TYPE origin_metadata_signature AS (
	id bigint,
	origin_id bigint,
	discovery_date timestamp with time zone,
	tool_id bigint,
	metadata jsonb,
	provider_id integer,
	provider_name text,
	provider_type text,
	provider_url text
);

CREATE TYPE revision_metadata_signature AS (
	id sha1_git,
	translated_metadata jsonb,
	tool_id integer,
	tool_name text,
	tool_version text,
	tool_configuration jsonb
);

CREATE TABLE content_metadata (
	id sha1 NOT NULL,
	translated_metadata jsonb NOT NULL,
	indexer_configuration_id bigint NOT NULL
);

COMMENT ON TABLE content_metadata IS 'metadata semantically translated from a content file';

COMMENT ON COLUMN content_metadata.id IS 'sha1 of content file';

COMMENT ON COLUMN content_metadata.translated_metadata IS 'result of translation with defined format';

COMMENT ON COLUMN content_metadata.indexer_configuration_id IS 'tool used for translation';

CREATE TABLE metadata_provider (
	id integer DEFAULT nextval('metadata_provider_id_seq'::regclass) NOT NULL,
	provider_name text NOT NULL,
	provider_type text NOT NULL,
	provider_url text,
	metadata jsonb
);

COMMENT ON TABLE metadata_provider IS 'Metadata provider information';

COMMENT ON COLUMN metadata_provider.id IS 'Provider''s identifier';

COMMENT ON COLUMN metadata_provider.provider_name IS 'Provider''s name';

COMMENT ON COLUMN metadata_provider.provider_url IS 'Provider''s url';

COMMENT ON COLUMN metadata_provider.metadata IS 'Other metadata about provider';

CREATE TABLE origin_metadata (
	id bigint DEFAULT nextval('origin_metadata_id_seq'::regclass) NOT NULL,
	origin_id bigint NOT NULL,
	discovery_date timestamp with time zone NOT NULL,
	provider_id bigint NOT NULL,
	tool_id bigint NOT NULL,
	metadata jsonb NOT NULL
);

COMMENT ON TABLE origin_metadata IS 'keeps all metadata found concerning an origin';

COMMENT ON COLUMN origin_metadata.id IS 'the origin_metadata object''s id';

COMMENT ON COLUMN origin_metadata.origin_id IS 'the origin id for which the metadata was found';

COMMENT ON COLUMN origin_metadata.discovery_date IS 'the date of retrieval';

COMMENT ON COLUMN origin_metadata.provider_id IS 'the metadata provider: github, openhub, deposit, etc.';

COMMENT ON COLUMN origin_metadata.tool_id IS 'the tool used for extracting metadata: lister-github, etc.';

COMMENT ON COLUMN origin_metadata.metadata IS 'metadata in json format but with original terms';

CREATE TABLE origin_metadata_translation (
	id bigint DEFAULT nextval('origin_metadata_translation_id_seq'::regclass) NOT NULL,
	"result" jsonb,
	indexer_configuration_id bigint NOT NULL
);

COMMENT ON TABLE origin_metadata_translation IS 'keeps translated for an origin_metadata entry';

COMMENT ON COLUMN origin_metadata_translation.id IS 'the entry id in origin_metadata';

COMMENT ON COLUMN origin_metadata_translation."result" IS 'translated_metadata result after translation with tool';

COMMENT ON COLUMN origin_metadata_translation.indexer_configuration_id IS 'tool used for translation';

CREATE TABLE revision_metadata (
	id sha1_git NOT NULL,
	translated_metadata jsonb NOT NULL,
	indexer_configuration_id bigint NOT NULL
);

COMMENT ON TABLE revision_metadata IS 'metadata semantically detected and translated in a revision';

COMMENT ON COLUMN revision_metadata.id IS 'sha1_git of revision';

COMMENT ON COLUMN revision_metadata.translated_metadata IS 'result of detection and translation with defined format';

COMMENT ON COLUMN revision_metadata.indexer_configuration_id IS 'tool used for detection';

ALTER SEQUENCE metadata_provider_id_seq
	OWNED BY metadata_provider.id;

ALTER SEQUENCE origin_metadata_id_seq
	OWNED BY origin_metadata.id;

ALTER SEQUENCE origin_metadata_translation_id_seq
	OWNED BY origin_metadata_translation.id;

CREATE OR REPLACE FUNCTION swh_content_language_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if conflict_update then
      insert into content_language (id, lang, indexer_configuration_id)
      select id, lang, indexer_configuration_id
    	from tmp_content_language tcl
            on conflict(id, indexer_configuration_id)
                do update set lang = excluded.lang;

    else
        insert into content_language (id, lang, indexer_configuration_id)
        select id, lang, indexer_configuration_id
    	  from tmp_content_language tcl
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_language_add(conflict_update boolean) IS 'Add new content languages';

CREATE OR REPLACE FUNCTION swh_content_metadata_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if conflict_update then
      insert into content_metadata (id, translated_metadata, indexer_configuration_id)
      select id, translated_metadata, indexer_configuration_id
    	from tmp_content_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set translated_metadata = excluded.translated_metadata;

    else
        insert into content_metadata (id, translated_metadata, indexer_configuration_id)
        select id, translated_metadata, indexer_configuration_id
    	from tmp_content_metadata tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_metadata_add(conflict_update boolean) IS 'Add new content metadata';

CREATE OR REPLACE FUNCTION swh_content_metadata_get() RETURNS SETOF content_metadata_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select c.id, translated_metadata, i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join content_metadata c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_metadata_get() IS 'List content''s metadata';

CREATE OR REPLACE FUNCTION swh_content_metadata_missing() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select id::sha1 from tmp_content_metadata_missing as tmp
	where not exists
	    (select 1 from content_metadata as c
             where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id);
    return;
end
$$;

COMMENT ON FUNCTION swh_content_metadata_missing() IS 'Filter missing content metadata';

CREATE OR REPLACE FUNCTION swh_mktemp_content_metadata() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_metadata (
    like content_metadata including defaults
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_metadata() IS 'Helper table to add content metadata';

CREATE OR REPLACE FUNCTION swh_mktemp_content_metadata_missing() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_metadata_missing (
    id sha1,
    indexer_configuration_id integer
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_metadata_missing() IS 'Helper table to filter missing metadata in content_metadata';

CREATE OR REPLACE FUNCTION swh_mktemp_revision_metadata() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_revision_metadata (
    like revision_metadata including defaults
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_revision_metadata() IS 'Helper table to add revision metadata';

CREATE OR REPLACE FUNCTION swh_mktemp_revision_metadata_missing() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_revision_metadata_missing (
    id sha1_git,
    indexer_configuration_id integer
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_revision_metadata_missing() IS 'Helper table to filter missing metadata in revision_metadata';

CREATE OR REPLACE FUNCTION swh_origin_metadata_get_by_origin(origin integer) RETURNS SETOF origin_metadata_signature
    LANGUAGE sql STABLE
    AS $$
    select om.id as id, origin_id, discovery_date, tool_id, om.metadata,
           mp.id as provider_id, provider_name, provider_type, provider_url
    from origin_metadata as om
    inner join metadata_provider mp on om.provider_id = mp.id
    where om.origin_id = origin
    order by discovery_date desc;
$$;

CREATE OR REPLACE FUNCTION swh_origin_metadata_get_by_provider_type(origin integer, type text) RETURNS SETOF origin_metadata_signature
    LANGUAGE sql STABLE
    AS $$
    select om.id as id, origin_id, discovery_date, tool_id, om.metadata,
           mp.id as provider_id, provider_name, provider_type, provider_url
    from origin_metadata as om
    inner join metadata_provider mp on om.provider_id = mp.id
    where om.origin_id = origin
    and mp.provider_type = type
    order by discovery_date desc;
$$;

CREATE OR REPLACE FUNCTION swh_revision_metadata_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if conflict_update then
      insert into revision_metadata (id, translated_metadata, indexer_configuration_id)
      select id, translated_metadata, indexer_configuration_id
    	from tmp_revision_metadata tcm
            on conflict(id, indexer_configuration_id)
                do update set translated_metadata = excluded.translated_metadata;

    else
        insert into revision_metadata (id, translated_metadata, indexer_configuration_id)
        select id, translated_metadata, indexer_configuration_id
    	from tmp_revision_metadata tcm
            on conflict(id, indexer_configuration_id)
            do nothing;
    end if;
    return;
end
$$;

COMMENT ON FUNCTION swh_revision_metadata_add(conflict_update boolean) IS 'Add new revision metadata';

CREATE OR REPLACE FUNCTION swh_revision_metadata_get() RETURNS SETOF revision_metadata_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select c.id, translated_metadata, i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join revision_metadata c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

COMMENT ON FUNCTION swh_revision_metadata_get() IS 'List revision''s metadata';

CREATE OR REPLACE FUNCTION swh_revision_metadata_missing() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select id::sha1 from tmp_revision_metadata_missing as tmp
	where not exists
	    (select 1 from revision_metadata as c
             where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id);
    return;
end
$$;

COMMENT ON FUNCTION swh_revision_metadata_missing() IS 'Filter missing content metadata';

ALTER TABLE content_metadata
	ADD CONSTRAINT content_metadata_pkey PRIMARY KEY (id, indexer_configuration_id);

ALTER TABLE metadata_provider
	ADD CONSTRAINT metadata_provider_pkey PRIMARY KEY (id);

ALTER TABLE origin_metadata
	ADD CONSTRAINT origin_metadata_pkey PRIMARY KEY (id);

ALTER TABLE origin_metadata_translation
	ADD CONSTRAINT origin_metadata_translation_pkey PRIMARY KEY (id, indexer_configuration_id);

ALTER TABLE revision_metadata
	ADD CONSTRAINT revision_metadata_pkey PRIMARY KEY (id, indexer_configuration_id);

ALTER TABLE content_metadata
	ADD CONSTRAINT content_metadata_id_fkey FOREIGN KEY (id) REFERENCES content(sha1);

ALTER TABLE content_metadata
	ADD CONSTRAINT content_metadata_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES indexer_configuration(id);

ALTER TABLE origin_metadata
	ADD CONSTRAINT origin_metadata_origin_fkey FOREIGN KEY (origin_id) REFERENCES origin(id);

ALTER TABLE origin_metadata
	ADD CONSTRAINT origin_metadata_provider_fkey FOREIGN KEY (provider_id) REFERENCES metadata_provider(id);

ALTER TABLE origin_metadata
	ADD CONSTRAINT origin_metadata_tool_fkey FOREIGN KEY (tool_id) REFERENCES indexer_configuration(id);

ALTER TABLE origin_metadata_translation
	ADD CONSTRAINT origin_metadata_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES indexer_configuration(id);

ALTER TABLE revision_metadata
	ADD CONSTRAINT revision_metadata_id_fkey FOREIGN KEY (id) REFERENCES revision(id);

ALTER TABLE revision_metadata
	ADD CONSTRAINT revision_metadata_indexer_configuration_id_fkey FOREIGN KEY (indexer_configuration_id) REFERENCES indexer_configuration(id);

CREATE INDEX metadata_provider_provider_name_provider_url_idx ON metadata_provider USING btree (provider_name, provider_url);

CREATE INDEX origin_metadata_origin_id_provider_id_tool_id_idx ON origin_metadata USING btree (origin_id, provider_id, tool_id);
