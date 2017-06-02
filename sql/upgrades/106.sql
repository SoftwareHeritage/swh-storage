-- SWH DB schema upgrade
-- from_version: 105
-- to_version: 106
-- description: Improve indexer endpoints function to use identifier

insert into dbversion(version, release, description)
      values(106, now(), 'Work In Progress');

drop type content_mimetype_signature cascade;
create type content_mimetype_signature as(
    id sha1,
    mimetype bytea,
    encoding bytea,
    tool_id integer,
    tool_name text,
    tool_version text,
    tool_configuration jsonb
);

drop type content_language_signature cascade;
create type content_language_signature as (
    id sha1,
    lang languages,
    tool_id integer,
    tool_name text,
    tool_version text,
    tool_configuration jsonb
);

drop type content_ctags_signature cascade;
create type content_ctags_signature as (
  id sha1,
  name text,
  kind text,
  line bigint,
  lang ctags_languages,
  tool_id integer,
  tool_name text,
  tool_version text,
  tool_configuration jsonb
);

drop type content_fossology_license_signature cascade;
create type content_fossology_license_signature as (
  id                 sha1,
  tool_id            integer,
  tool_name          text,
  tool_version       text,
  tool_configuration jsonb,
  licenses           text[]
);

CREATE OR REPLACE FUNCTION swh_content_ctags_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if conflict_update then
        delete from content_ctags
        where id in (select tmp.id
                     from tmp_content_ctags tmp
                     inner join indexer_configuration i on i.id=tmp.indexer_configuration_id);
    end if;

    insert into content_ctags (id, name, kind, line, lang, indexer_configuration_id)
    select id, name, kind, line, lang, indexer_configuration_id
    from tmp_content_ctags tct
        on conflict(id, hash_sha1(name), kind, line, lang, indexer_configuration_id)
        do nothing;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_ctags_add(conflict_update boolean) IS 'Add new ctags symbols per content';

CREATE OR REPLACE FUNCTION swh_content_ctags_get() RETURNS SETOF content_ctags_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select c.id, c.name, c.kind, c.line, c.lang,
               i.id as tool_id, i.tool_name, i.tool_version, i.tool_configuration
        from tmp_bytea t
        inner join content_ctags c using(id)
        inner join indexer_configuration i on i.id = c.indexer_configuration_id
        order by line;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_ctags_get() IS 'List content ctags';

CREATE OR REPLACE FUNCTION swh_content_ctags_missing() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
	(select id::sha1 from tmp_content_ctags_missing as tmp
	 where not exists
	     (select 1 from content_ctags as c
              where c.id = tmp.id and c.indexer_configuration_id=tmp.indexer_configuration_id
              limit 1));
    return;
end
$$;

COMMENT ON FUNCTION swh_content_ctags_missing() IS 'Filter missing content ctags';

CREATE OR REPLACE FUNCTION swh_content_ctags_search(expression text, l integer default 10, last_sha1 sha1 = '\x0000000000000000000000000000000000000000'::bytea) RETURNS SETOF content_ctags_signature
    LANGUAGE sql
    AS $$
    select c.id, name, kind, line, lang,
           i.id as tool_id, tool_name, tool_version, tool_configuration
    from content_ctags c
    inner join indexer_configuration i on i.id = c.indexer_configuration_id
    where hash_sha1(name) = hash_sha1(expression)
    and c.id > last_sha1
    order by id
    limit l;
$$;

COMMENT ON FUNCTION swh_content_ctags_search(expression text, l integer, last_sha1 sha1) IS 'Equality search through ctags'' symbols';

CREATE OR REPLACE FUNCTION swh_content_fossology_license_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if conflict_update then
        -- delete from content_fossology_license c
        --   using tmp_content_fossology_license tmp, indexer_configuration i
        --   where c.id = tmp.id and i.id=tmp.indexer_configuration_id
        delete from content_fossology_license
        where id in (select tmp.id
                     from tmp_content_fossology_license tmp
                     inner join indexer_configuration i on i.id=tmp.indexer_configuration_id);
    end if;

    insert into content_fossology_license (id, license_id, indexer_configuration_id)
    select tcl.id,
          (select id from fossology_license where name = tcl.license) as license,
          indexer_configuration_id
    from tmp_content_fossology_license tcl
        on conflict(id, license_id, indexer_configuration_id)
        do nothing;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_fossology_license_add(conflict_update boolean) IS 'Add new content licenses';

CREATE OR REPLACE FUNCTION swh_content_fossology_license_get() RETURNS SETOF content_fossology_license_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
      select cl.id,
             ic.id as tool_id,
             ic.tool_name,
             ic.tool_version,
             ic.tool_configuration,
             array(select name
                   from fossology_license
                   where id = ANY(array_agg(cl.license_id))) as licenses
      from tmp_bytea tcl
      inner join content_fossology_license cl using(id)
      inner join indexer_configuration ic on ic.id=cl.indexer_configuration_id
      group by cl.id, ic.id, ic.tool_name, ic.tool_version, ic.tool_configuration;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_fossology_license_get() IS 'List content licenses';

CREATE OR REPLACE FUNCTION swh_content_fossology_license_missing() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
	(select id::sha1 from tmp_content_fossology_license_missing as tmp
	 where not exists
	     (select 1 from content_fossology_license as c
              where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id));
    return;
end
$$;

COMMENT ON FUNCTION swh_content_fossology_license_missing() IS 'Filter missing content licenses';

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

CREATE OR REPLACE FUNCTION swh_content_language_get() RETURNS SETOF content_language_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select c.id, lang, i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join content_language c on c.id = t.id
        inner join indexer_configuration i on i.id=c.indexer_configuration_id;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_language_get() IS 'List content''s language';

CREATE OR REPLACE FUNCTION swh_content_language_missing() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
	select id::sha1 from tmp_content_language_missing as tmp
	where not exists
	    (select 1 from content_language as c
             where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id);
    return;
end
$$;

COMMENT ON FUNCTION swh_content_language_missing() IS 'Filter missing content languages';

CREATE OR REPLACE FUNCTION swh_content_mimetype_add(conflict_update boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if conflict_update then
        insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
        select id, mimetype, encoding, indexer_configuration_id
        from tmp_content_mimetype tcm
            on conflict(id, indexer_configuration_id)
                do update set mimetype = excluded.mimetype,
                              encoding = excluded.encoding;

    else
        insert into content_mimetype (id, mimetype, encoding, indexer_configuration_id)
        select id, mimetype, encoding, indexer_configuration_id
        from tmp_content_mimetype tcm
            on conflict(id, indexer_configuration_id) do nothing;
    end if;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_mimetype_add(conflict_update boolean) IS 'Add new content mimetypes';

CREATE OR REPLACE FUNCTION swh_content_mimetype_get() RETURNS SETOF content_mimetype_signature
    LANGUAGE plpgsql
    AS $$
begin
    return query
        select c.id, mimetype, encoding,
               i.id as tool_id, tool_name, tool_version, tool_configuration
        from tmp_bytea t
        inner join content_mimetype c on c.id=t.id
        inner join indexer_configuration i on c.indexer_configuration_id=i.id;
    return;
end
$$;

COMMENT ON FUNCTION swh_content_mimetype_get() IS 'List content''s mimetypes';

CREATE OR REPLACE FUNCTION swh_content_mimetype_missing() RETURNS SETOF sha1
    LANGUAGE plpgsql
    AS $$
begin
    return query
	(select id::sha1 from tmp_content_mimetype_missing as tmp
	 where not exists
	     (select 1 from content_mimetype as c
              where c.id = tmp.id and c.indexer_configuration_id = tmp.indexer_configuration_id));
    return;
end
$$;

COMMENT ON FUNCTION swh_content_mimetype_missing() IS 'Filter existing mimetype information';

CREATE OR REPLACE FUNCTION swh_mktemp_content_ctags() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_ctags (
    like content_ctags including defaults
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_ctags() IS 'Helper table to add content ctags';

CREATE OR REPLACE FUNCTION swh_mktemp_content_ctags_missing() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_ctags_missing (
    id           sha1,
    indexer_configuration_id    integer
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_ctags_missing() IS 'Helper table to filter missing content ctags';

CREATE OR REPLACE FUNCTION swh_mktemp_content_fossology_license() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_fossology_license (
    id                       sha1,
    license                  text,
    indexer_configuration_id integer
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_fossology_license() IS 'Helper table to add content license';

CREATE OR REPLACE FUNCTION swh_mktemp_content_fossology_license_missing() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_fossology_license_missing (
    id                       bytea,
    indexer_configuration_id integer
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_fossology_license_missing() IS 'Helper table to add content license';

CREATE OR REPLACE FUNCTION swh_mktemp_content_language() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_language (
    like content_language including defaults
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_language() IS 'Helper table to add content language';

CREATE OR REPLACE FUNCTION swh_mktemp_content_language_missing() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_language_missing (
    id sha1,
    indexer_configuration_id integer
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_language_missing() IS 'Helper table to filter missing language';

CREATE OR REPLACE FUNCTION swh_mktemp_content_mimetype() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_mimetype (
    like content_mimetype including defaults
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_mimetype() IS 'Helper table to add mimetype information';

CREATE OR REPLACE FUNCTION swh_mktemp_content_mimetype_missing() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_content_mimetype_missing (
    id sha1,
    indexer_configuration_id bigint
  ) on commit drop;
$$;

COMMENT ON FUNCTION swh_mktemp_content_mimetype_missing() IS 'Helper table to filter existing mimetype information';

-- Update indexes

DROP INDEX indexer_configuration_tool_name_tool_version_idx;

CREATE UNIQUE INDEX indexer_configuration_tool_name_tool_version_tool_configura_idx ON indexer_configuration USING btree (tool_name, tool_version, tool_configuration);

-- Update data on indexer configuration

insert into indexer_configuration(tool_name, tool_version, tool_configuration)
values ('pygments', '2.0.1+dfsg-1.1+deb8u1', '{"type": "library", "debian-package": "python3-pygments", "max_content_size": 10240}');
